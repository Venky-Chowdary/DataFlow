"""What the scan driver asks of one side (one table on one engine).

Every operation is one bounded statement, so the driver can checkpoint between
them, time each one out and give up on a unit without giving up on the scan.
Values that cross back into Python are only counts, sums, short key/row
encodings and the few rows the judge needs: the population itself stays in the
engine.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from services.parity_scale.plan import ColumnPlan, TablePlan

#: ``(row count, Σa, Σb)`` of one comparison unit, summed exactly.
Digest = tuple[int, int, int]
EMPTY: Digest = (0, 0, 0)
#: A key bound (one value per key column); ``None`` is unbounded.
KeyBound = tuple[Any, ...] | None


class UnitTimeout(Exception):
    """A statement exceeded its time limit; the driver halves or gives up the unit."""


@dataclass(frozen=True)
class LeafRow:
    """One row of a small differing unit.

    ``ident`` is the hex of the key's CE1 bytes (keyed) or of the row hash
    (keyless); ``key`` the native key values used to fetch the row later.
    """

    key: tuple[Any, ...] | None
    ident: str
    a: int
    b: int


def add(left: Digest, right: Digest) -> Digest:
    return (left[0] + right[0], left[1] + right[1], left[2] + right[2])


def range_predicate(
    quoted_keys: Sequence[str], lo: KeyBound, hi: KeyBound, placeholder: str
) -> tuple[str, list[Any]]:
    """``lo <= key < hi`` as a row-value comparison the PK index serves.

    PostgreSQL and SQLite compare row values lexicographically and use the
    primary-key index for it. (SQL Server has no row values: its dialect will
    use the OR/AND expansion of ``services.keyset_pagination``.)
    """
    clauses: list[str] = []
    params: list[Any] = []

    def _row(values: tuple[Any, ...]) -> str:
        params.extend(values)
        marks = ", ".join(placeholder for _ in values)
        return f"({marks})" if len(values) > 1 else marks

    lhs = f"({', '.join(quoted_keys)})" if len(quoted_keys) > 1 else quoted_keys[0]
    if lo is not None:
        clauses.append(f"{lhs} >= {_row(tuple(lo))}")
    if hi is not None:
        clauses.append(f"{lhs} < {_row(tuple(hi))}")
    return (" AND ".join(clauses) or "TRUE"), params


class Side(ABC):
    """One table on one engine, under one plan, as source or destination."""

    def __init__(self, *, role: str, plan: TablePlan, table: str, schema: str = "") -> None:
        if role not in {"source", "destination"}:
            raise ValueError(f"side role must be source or destination, not {role!r}")
        self.role = role
        self.plan = plan
        self.table = table
        self.schema = schema
        self.timeout_seconds: float | None = None
        #: Set by :meth:`pin`: every later statement reads one snapshot of the table.
        self.pinned = False

    def pin(self, max_hold_s: float | None = None) -> dict[str, Any]:
        """Read every later statement from one snapshot; what was established, in words.

        ``{"kind": "pinned", ...}`` only when the engine guarantees it; any
        other answer is ``not_established`` with the reason.
        """
        return {"kind": "not_established", "reason": f"snapshot pinning is not implemented for {type(self).__name__}"}

    def unpin(self) -> None:
        """Leave the pinned snapshot (or a failed attempt) and read statement by statement."""
        self.pinned = False

    # column lists in plan order -------------------------------------------------
    def column_name(self, col: ColumnPlan) -> str:
        return col.source if self.role == "source" else col.target

    def column_type(self, col: ColumnPlan) -> str:
        return col.source_type if self.role == "source" else col.dest_type

    @property
    def is_source(self) -> bool:
        return self.role == "source"

    @property
    def key_names(self) -> list[str]:
        return [s if self.is_source else t for s, t in self.plan.key]

    def set_timeout(self, seconds: float | None) -> None:
        self.timeout_seconds = seconds

    def key_hex(self, key_values: Sequence[Any]) -> str:
        """Hex of the key's CE1 bytes, computed by the Python reference.

        Only valid for keys of typed kinds, where the reference and every
        dialect agree byte for byte (the plan refuses an in-engine key of the
        ``native`` kind on any engine but SQLite).
        """
        from services.parity_scale.ce1 import encode_source_value, encode_value

        parts = []
        for value, col in zip(key_values, self.plan.key_plans(), strict=True):
            spec = col.field_spec(self.plan.dest_engine)
            field = encode_source_value(value, spec) if self.is_source else encode_value(value, spec)
            parts.append(field)
        return b"".join(parts).hex()  # type: ignore[arg-type]

    # operations ---------------------------------------------------------------
    @abstractmethod
    def split_points(self, lo: KeyBound, hi: KeyBound, step: int) -> list[tuple[Any, ...]]:
        """Keys at every ``step``-th row of ``[lo, hi)`` in key order (excluding the first row)."""

    def boundaries(self, segment_rows: int, *, strategy: str = "auto",
                   large_table_rows: int = 2_000_000) -> tuple[list[tuple[Any, ...]], str]:
        """Level-0 segment boundaries and the strategy that chose them.

        The default reads exact quantiles (one ordered index scan); SQL sides
        switch to a linear integer split or a sampled set of keys on large
        tables (:class:`services.parity_scale.dialects.sqlbase.SqlSide`).
        """
        return self.split_points(None, None, segment_rows), "quantile"

    @abstractmethod
    def range_digest(self, lo: KeyBound, hi: KeyBound) -> Digest:
        """Digest of the rows with ``lo <= key < hi``."""

    @abstractmethod
    def range_leaf(self, lo: KeyBound, hi: KeyBound) -> list[LeafRow]:
        """Every row of ``[lo, hi)`` as ``(key, key CE1 hex, a, b)``."""

    @abstractmethod
    def bucket_digests(
        self, level: int, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> dict[tuple[int, ...], Digest]:
        """Digests per bucket path of length ``level + 1`` under ``parents`` (all at level 0)."""

    @abstractmethod
    def bucket_leaf(
        self, parents: Sequence[tuple[int, ...]], lo: KeyBound = None, hi: KeyBound = None
    ) -> list[LeafRow]:
        """Every row in the given bucket paths (keyed: by key; keyless: by row hash)."""

    @abstractmethod
    def fetch_rows(self, keys: Sequence[tuple[Any, ...]]) -> dict[str, list[tuple[Any, ...]]]:
        """Rows for ``keys``: key CE1 hex → rows of every compared column (plan order)."""

    @abstractmethod
    def keyless_examples(self, row_hashes: Sequence[str], limit: int) -> list[tuple[str, tuple[Any, ...]]]:
        """Up to ``limit`` rows whose row hash is in ``row_hashes`` (keyless examples)."""

    @abstractmethod
    def iter_stream(
        self, columns: Sequence[ColumnPlan], batch: int = 5000
    ) -> Iterator[tuple[tuple[Any, ...] | None, tuple[Any, ...]]]:
        """``(key values or None, values of columns)`` for every row, streamed."""

    @abstractmethod
    def held_count(self) -> int:
        """Source rows the write contract held out (0 on a destination)."""

    def close(self) -> None:  # pragma: no cover - overridden
        return None
