"""How a shaping recipe *runs* — pure, streaming, and self-accounting.

The engine is deliberately free of I/O: it takes rows and returns rows plus a
`ShapeEffect` describing exactly what it did — rows in, rows out, rows removed,
rows expanded, rows diverted, cells changed and nulls introduced, per step. That
accounting is the point. Every competitor can apply a transformation; the reason
this one records its own effect is that the ledger downstream must still balance:

    rows_read + rows_expanded = rows_shaped_out + dest_count + held_out + skipped

A row a `filter_rows` step removed is a deliberate exclusion, not a data-quality
finding. A row an `unnest_json` step added is a declared projection, not a
surplus. Both are named so dest COUNT cannot be misread.

Because every step is row-local, one row can be pushed through the whole recipe
independently of its neighbours. That is what makes the same recipe safe in a
preview of 50 rows, in Validate's population scan and in a chunked 1M-row
Execute — and it is why an instance can be reused across batches while the
counters keep accumulating.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from decimal import Decimal, DecimalException, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from services.shape_expr import (
    FUNCTIONS,
    EvalError,
    _as_number,
    _as_text,
    compile_expression,
    is_blank,
)
from services.shape_models import ROUTE_COLUMN, ShapeRecipe, ShapeStep

__all__ = [
    "ShapeRowError",
    "StepEffect",
    "ShapeEffect",
    "DivertedRow",
    "ShapeEngine",
    "shape_records",
]

# Samples are for the operator's eyes, not for accounting; the counts are exact
# regardless of how many samples were kept.
_DEFAULT_SAMPLES = 25


class ShapeRowError(ValueError):
    """A row a step could not shape under the ``refuse`` policy."""

    def __init__(self, message: str, *, step: ShapeStep, step_index: int, row_index: int, column: str):
        super().__init__(message)
        self.step = step
        self.step_index = step_index
        self.row_index = row_index
        self.column = column

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step_index + 1,
            "op": self.step.op,
            "column": self.column,
            "row": self.row_index + 1,
            "message": str(self),
        }


@dataclass
class StepEffect:
    """What one step did, across every batch this engine has seen."""

    index: int
    op: str
    label: str
    active: bool
    rows_in: int = 0
    rows_out: int = 0
    rows_removed: int = 0
    rows_expanded: int = 0
    rows_diverted: int = 0
    rows_buffered: int = 0
    cells_changed: int = 0
    nulls_introduced: int = 0
    errors: int = 0
    error_samples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.index + 1,
            "op": self.op,
            "label": self.label,
            "active": self.active,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_removed": self.rows_removed,
            "rows_expanded": self.rows_expanded,
            "rows_diverted": self.rows_diverted,
            "rows_buffered": self.rows_buffered,
            "cells_changed": self.cells_changed,
            "nulls_introduced": self.nulls_introduced,
            "errors": self.errors,
            "error_samples": list(self.error_samples),
        }


@dataclass
class DivertedRow:
    """A row a step sent to quarantine, with the reason stated."""

    row_index: int
    reason: str
    step: int
    op: str
    record: dict[str, Any]
    # Dedupe losers also name the key they lost on and the winner's row
    # number, so the ledger (and the per-step preview) can pair them.
    dedupe_key: tuple[Any, ...] | None = None
    winner_row_index: int | None = None

    def to_dict(self) -> dict[str, Any]:
        out = {
            "row": self.row_index + 1,
            "reason": self.reason,
            "step": self.step + 1,
            "op": self.op,
            "record": dict(self.record),
        }
        if self.dedupe_key is not None:
            out["dedupe_key"] = list(self.dedupe_key)
        if self.winner_row_index is not None:
            out["winner_row"] = self.winner_row_index + 1
        return out


@dataclass
class ShapeEffect:
    """The recipe's total effect — the numbers the ledger and the UI both use."""

    rows_in: int = 0
    rows_out: int = 0
    rows_shaped_out: int = 0
    rows_expanded: int = 0
    rows_diverted: int = 0
    rows_buffered: int = 0
    cells_changed: int = 0
    nulls_introduced: int = 0
    # Rows a union step's secondary input appended at drain — also counted in
    # rows_in (they enter the pipeline) but tracked separately so the write
    # ledger can fold them into the expansion term: source_row_count covers
    # reads only, so without this the appended rows read as unexplained
    # extras on the destination.
    rows_union_appended: int = 0
    steps: list[StepEffect] = field(default_factory=list)
    diverted_samples: list[DivertedRow] = field(default_factory=list)
    # Rows per named router output — the per-route conservation ledger.
    routes: dict[str, int] = field(default_factory=dict)

    @property
    def balanced(self) -> bool:
        """Every row read, plus every row a step added, is kept, filtered,
        diverted, or held in an end-of-stream buffer (aggregate/dedupe/sort)."""
        return (
            self.rows_in + self.rows_expanded
            == self.rows_out
            + self.rows_shaped_out
            + self.rows_diverted
            + self.rows_buffered
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_shaped_out": self.rows_shaped_out,
            "rows_expanded": self.rows_expanded,
            "rows_diverted": self.rows_diverted,
            "rows_buffered": self.rows_buffered,
            "cells_changed": self.cells_changed,
            "nulls_introduced": self.nulls_introduced,
            "balanced": self.balanced,
            "steps": [s.to_dict() for s in self.steps],
            "diverted_samples": [d.to_dict() for d in self.diverted_samples],
            "routes": dict(self.routes),
        }


class _Drop(Exception):
    """Internal: this row leaves the stream."""

    def __init__(self, *, diverted: bool, reason: str, step_index: int, op: str,
                 dedupe_key: tuple | None = None, winner_row_index: int | None = None):
        self.diverted = diverted
        self.reason = reason
        self.step_index = step_index
        self.op = op
        self.dedupe_key = dedupe_key
        self.winner_row_index = winner_row_index


class _Held:
    """Internal: a buffering step keeps the row until end-of-stream."""


_HELD = _Held()

# A step that must see the whole population before it can emit holds rows in a
# buffer. Unbounded buffers are how transform engines die, so every buffering
# op has a declared cap that a recipe can raise — deliberately, never silently.
_MAX_BUFFERED = 500_000

# Source-declared logical types that change how a dedupe ordering column
# compares. The wire carries every value as text, so a bare string compare
# keeps '9' over '10' and '9.50' over '10.00' (DD-1): when the column is
# declared numeric/date/timestamp/bool, compare by value, not by bytes.
_DEDUPE_INT_TYPES = {
    "int", "integer", "bigint", "smallint", "tinyint", "mediumint",
    "serial", "bigserial", "long", "int2", "int4", "int8",
}
_DEDUPE_NUMERIC_TYPES = {
    "decimal", "numeric", "number", "money", "float", "double", "real",
    "bignumeric", "bigdecimal", "fixed", "float4", "float8", "double precision",
}
_DEDUPE_DATE_TYPES = {"date"}
_DEDUPE_TS_TYPES = {
    "timestamp", "timestamptz", "datetime", "datetime2", "smalldatetime",
    "timestamp_tz", "timestamp_ntz", "timestamp_ltz", "datetimeoffset",
    "timestamp with time zone", "timestamp without time zone",
}
_DEDUPE_BOOL_TYPES = {"bool", "boolean", "bit"}


def _dedupe_instant(text: Any, *, date_only: bool = False) -> float | None:
    """A date/timestamp wire value as an epoch instant, or ``None``.

    Timestamps compare as instants, not strings — '2024-01-02T00:00:00+05:00'
    and '2024-01-01T20:00:00Z' are the same instant. Naive values are taken
    as UTC so an offset-less timestamp never outranks a later instant just
    because its string sorts first.
    """
    s = str(text or "").strip()
    if not s:
        return None
    try:
        from datetime import date, datetime, timezone

        if date_only:
            d = date.fromisoformat(s[:10])
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
        v = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError):
        return None


def _dedupe_sort_val(value: Any, decl: str | None = None) -> tuple[int, Any]:
    """One ordering position, honouring the column's declared source type.

    Buckets: 0 = parsed under the declared type, 1 = untyped text, 2 = a
    declared-typed value that failed to parse (deterministic tail), 3 =
    null. Column values are homogeneous, so same-column comparisons never
    mix bucket types.
    """
    if _dedupe_is_null(value):
        return (3, "")
    kind = str(decl or "").strip().casefold()
    # "numeric(10,2)" → "numeric"; "int unsigned" → "int".
    kind = kind.split("(", 1)[0].strip()
    if kind.endswith(" unsigned"):
        kind = kind[: -len(" unsigned")]
    text = _as_text(value)
    if kind in _DEDUPE_INT_TYPES or kind in _DEDUPE_NUMERIC_TYPES:
        try:
            from decimal import Decimal

            return (0, Decimal(str(text).strip()))
        except Exception:
            return (2, text or "")
    if kind in _DEDUPE_BOOL_TYPES:
        v = str(text or "").strip().casefold()
        return (0, 1 if v in {"1", "true", "t", "yes", "y", "on"} else 0)
    if kind in _DEDUPE_DATE_TYPES:
        instant = _dedupe_instant(text, date_only=True)
        return (0, instant) if instant is not None else (2, text or "")
    if kind in _DEDUPE_TS_TYPES:
        instant = _dedupe_instant(text)
        return (0, instant) if instant is not None else (2, text or "")
    # Undeclared columns keep the legacy mixed-type ordering: real Python
    # numbers as numbers, everything else as text.
    return _sort_val(value)


#: Per-position class ranks — value direction compares only inside the
#: winnable class. NULLs sort last by default in BOTH directions: "keep the
#: latest" must never let an unknown stamp displace a known one (DD-4;
#: dbt/Fivetran dedupe is NULLS LAST). nulls="first" restores SQL NULLS
#: FIRST. Bucket 2 (a declared-typed value that failed to parse) is never
#: winnable — it always sorts below parsed values, under either direction.
_NULL_LAST_CLASS = {0: 0, 1: 0, 2: 1, 3: 2}
_NULL_FIRST_CLASS = {0: 1, 1: 1, 2: 2, 3: 0}


def _dedupe_position_cmp(
    nv: tuple[int, Any], cv: tuple[int, Any], direction: str, nulls: str
) -> int:
    """Signed compare of one ordering position under direction + null rule.

    The NULL/parse-failure class compares first — direction-independent so a
    NULL never wins by accident of sign (DD-4). Same class compares the
    (bucket, value) pair under the column's own direction.
    """
    classes = _NULL_FIRST_CLASS if nulls == "first" else _NULL_LAST_CLASS
    nc = classes.get(nv[0] if isinstance(nv, tuple) else 0, 0)
    cc = classes.get(cv[0] if isinstance(cv, tuple) else 0, 0)
    if nc != cc:
        return 1 if nc < cc else -1
    if nv == cv:
        return 0
    return 1 if (nv > cv if direction == "desc" else nv < cv) else -1


def _dedupe_rank(
    options: Mapping[str, Any],
    row: dict[str, Any],
    arrival: int,
    types: Mapping[str, str] | None = None,
) -> tuple[tuple[Any, ...], tuple[tuple[str, str], ...]]:
    """The ordering positions a member is ranked on, with each spec.

    Order: order_by first, then each then_by column, then — only as the
    declared tie-break — arrival. keep=last with no ordering columns ranks on
    arrival alone (last-seen); keep=first always ranks earliest arrival best.
    ``types`` is the source column -> declared logical type map; ordering
    columns compare under their declared type so PG-carried text values like
    '9' and '10' order numerically. The spec pairs ``(direction, nulls)`` —
    ``nulls`` is 'last' (default, NULL never wins) or 'first'.
    """
    keep = str(options.get("keep") or "first").casefold()
    if keep == "first":
        return (arrival,), (("asc", "last"),)
    decls = types or {}
    opt_nulls = str(options.get("nulls") or "last").casefold()
    parts: list[Any] = []
    spec: list[tuple[str, str]] = []
    order_col = str(options.get("order_by") or "")
    if order_col:
        parts.append(_dedupe_sort_val(row.get(order_col), decls.get(order_col)))
        spec.append(
            (str(options.get("order_dir") or "desc").casefold(), opt_nulls)
        )
    for entry in options.get("then_by") or []:
        if isinstance(entry, Mapping):
            tcol = str(entry.get("column") or "")
            parts.append(_dedupe_sort_val(row.get(tcol), decls.get(tcol)))
            spec.append(
                (
                    str(entry.get("dir") or "desc").casefold(),
                    str(entry.get("nulls") or opt_nulls).casefold(),
                )
            )
        else:
            tcol = str(entry)
            parts.append(_dedupe_sort_val(row.get(tcol), decls.get(tcol)))
            spec.append(
                (str(options.get("order_dir") or "desc").casefold(), opt_nulls)
            )
    if not parts:
        # No ordering columns: arrival is the rank — last-seen wins.
        parts.append(arrival)
        spec.append(("desc", "last"))
    return tuple(parts), tuple(spec)


def _dedupe_rank_cmp(
    new_parts: tuple[Any, ...],
    cur_parts: tuple[Any, ...],
    spec: tuple[tuple[str, str], ...],
) -> int:
    """Signed compare of two rank tuples under their (direction, nulls) spec."""
    for nv, cv, (direction, nulls) in zip(new_parts, cur_parts, spec):
        result = _dedupe_position_cmp(nv, cv, direction, nulls)
        if result:
            return result
    return 0


def _dedupe_member_better(
    new_parts: tuple[Any, ...],
    new_arrival: int,
    cur_parts: tuple[Any, ...],
    cur_arrival: int,
    spec: tuple[tuple[str, str], ...],
    tie_last: bool,
) -> bool:
    """True when the challenger outranks the incumbent — including ties.

    Unlike _dedupe_better this takes the arrival positions so a full ordering
    tie resolves under the declared tie_break instead of assuming the
    challenger arrived later and won.
    """
    cmp_ = _dedupe_rank_cmp(new_parts, cur_parts, spec)
    if cmp_:
        return cmp_ > 0
    return new_arrival > cur_arrival if tie_last else new_arrival < cur_arrival


def _dedupe_better(
    new_parts: tuple[Any, ...],
    cur_parts: tuple[Any, ...],
    spec: tuple[tuple[str, str], ...],
    tie_last: bool,
) -> bool:
    """True when the challenger strictly outranks the incumbent.

    Positions compare in order under their own (direction, nulls) spec; a
    full tie is decided by the declared tie_break — first_seen keeps the
    incumbent, last_seen replaces it.
    """
    for nv, cv, (direction, nulls) in zip(new_parts, cur_parts, spec):
        result = _dedupe_position_cmp(nv, cv, direction, nulls)
        if result:
            return result > 0
    return tie_last


def _dedupe_rule(options: Mapping[str, Any]) -> str:
    keep = str(options.get("keep") or "first").casefold()
    order_col = str(options.get("order_by") or "")
    bits = [f"keep={keep}"]
    if order_col:
        bits.append(f"order_by={order_col} {options.get('order_dir') or 'desc'}")
    if order_col or options.get("then_by"):
        # NULL ordering is part of the rule's identity — a NULL stamp never
        # displaces a real one unless the recipe says NULLS FIRST (DD-4).
        bits.append(f"nulls={options.get('nulls') or 'last'}")
    for entry in options.get("then_by") or []:
        if isinstance(entry, Mapping):
            bits.append(f"then_by={entry.get('column')} {entry.get('dir') or 'desc'}")
        else:
            bits.append(f"then_by={entry}")
    if str(options.get("merge") or "").casefold() == "non_null":
        bits.append("merge=non_null")
    bits.append(f"tie={options.get('tie_break') or 'first_seen'}")
    return ", ".join(bits)


_SQL_NULL_SENTINEL = "__DF_SQL_NULL__"


def _dedupe_is_null(value: Any) -> bool:
    """Null under either wire: Python None or the SQL NULL sentinel string
    the transfer layer uses so NULL and '' stay distinct."""
    return value is None or value == _SQL_NULL_SENTINEL


def _dedupe_feed_fallback(
    fallbacks: dict[str, tuple],
    row: dict[str, Any],
    parts: tuple[Any, ...],
    arrival: int,
    spec: tuple[tuple[str, str], ...] = (),
    tie_last: bool = False,
) -> None:
    """Offer a member's non-null values as merge fill candidates.

    Per column the BEST-ranked member value wins the fill, under the same
    (direction, nulls) spec and tie_break the winner election uses —
    ascending rules (order_dir=asc, keep=first) fill from the
    smallest-ranked member, not the largest (DD-2).
    """
    for col, value in row.items():
        if _dedupe_is_null(value):
            continue
        current = fallbacks.get(col)
        if current is None or _dedupe_member_better(
            parts, arrival, current[0], current[1], spec, tie_last
        ):
            fallbacks[col] = (parts, arrival, value)


def _max_buffer(options: Mapping[str, Any]) -> int:
    try:
        return max(1, int(options.get("max_keys") or _MAX_BUFFERED))
    except (TypeError, ValueError):
        return _MAX_BUFFERED


def _sort_val(value: Any) -> tuple[int, Any]:
    """A stable, mixed-type sort key: numbers compare as numbers, everything
    else as text, nulls last — never a TypeError on a real column."""
    if value is None:
        return (2, "")
    if isinstance(value, bool):
        return (0, float(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            return (0, float(value))
    except ImportError:  # pragma: no cover - decimal is stdlib
        pass
    return (1, _as_text(value) or "")


def _sort_order(options: Mapping[str, Any]) -> list[tuple[str, bool]]:
    """Normalise the sort spec into (column, descending) pairs.

    Accepts ``order``: [{"column": ..., "direction": "asc"|"desc"}] or the
    simpler ``order_by`` + ``direction`` pair.
    """
    out: list[tuple[str, bool]] = []
    for entry in options.get("order", []) or []:
        if isinstance(entry, Mapping) and entry.get("column"):
            out.append(
                (
                    str(entry["column"]),
                    str(entry.get("direction") or "asc").casefold() == "desc",
                )
            )
        elif isinstance(entry, str) and entry:
            out.append((entry, False))
    if not out:
        col = str(options.get("order_by") or "")
        if col:
            out.append((col, str(options.get("direction") or "asc").casefold() == "desc"))
    return out


def _agg_init() -> dict[str, Any]:
    return {"n": 0, "sum": None, "min": None, "max": None, "first": None, "last": None}


def _agg_add(state: dict[str, Any], fn: str, value: Any) -> None:
    if fn == "count":
        state["n"] += 1
        return
    if value is None:
        return
    if fn in ("sum", "avg"):
        try:
            num = _as_number(value)
        except (ValueError, TypeError, ArithmeticError):
            return
        if num is None:
            return
        state["sum"] = (state["sum"] or 0) + num
        state["n"] += 1
    elif fn == "min":
        state["min"] = value if state["min"] is None or _sort_val(value) < _sort_val(state["min"]) else state["min"]
    elif fn == "max":
        state["max"] = value if state["max"] is None or _sort_val(value) > _sort_val(state["max"]) else state["max"]
    elif fn == "first":
        if state["first"] is None:
            state["first"] = value
    elif fn == "last":
        state["last"] = value


def _agg_finish(state: dict[str, Any], fn: str) -> Any:
    if fn == "count":
        return state["n"]
    if fn == "sum":
        return state["sum"]
    if fn == "avg":
        if not state["n"]:
            return None
        try:
            return state["sum"] / state["n"]
        except (ArithmeticError, TypeError):
            return None
    if fn == "min":
        return state["min"]
    if fn == "max":
        return state["max"]
    if fn == "first":
        return state["first"]
    if fn == "last":
        return state["last"]
    return None


class ShapeEngine:
    """Applies a recipe to rows, accumulating its effect across batches."""

    def __init__(
        self,
        recipe: ShapeRecipe,
        *,
        sample_limit: int = _DEFAULT_SAMPLES,
        keep_diverted_records: bool = True,
        workspace_id: str = "",
    ):
        self.recipe = recipe
        self.steps = recipe.enabled_steps
        self.workspace_id = workspace_id
        self.sample_limit = max(0, sample_limit)
        self.keep_diverted_records = keep_diverted_records
        self.effect = ShapeEffect(
            steps=[
                StepEffect(
                    index=i,
                    op=s.op,
                    label=s.describe(),
                    active=s.active,
                )
                for i, s in enumerate(self.steps)
            ]
        )
        self._row_index = -1
        self._output_columns: list[str] = list(recipe.output_columns or recipe.input_columns)
        # Streaming state, per step index: dedupe seen-sets, surrogate counters,
        # lookup tables built once.
        self._state: dict[int, Any] = {}
        # Rows a buffering step (dedupe keep-last, aggregate, sort, rank) holds
        # until the stream ends; drained in step order so a later step still
        # sees them through the rest of the recipe.
        self._buffers: dict[int, Any] = {}
        self._held: dict[int, int] = {}
        self._drained = False
        # Diverted rows awaiting the caller's persist (take_diverted per page).
        self._diverted_pending: list[DivertedRow] = []
        # Dedupe decisions for the per-step preview: every loser, its key,
        # and the winning row number. Capped like diverted_samples — the
        # tallies in effect.steps stay exact regardless.
        self.dedupe_decisions: list[dict[str, Any]] = []
        self._dedupe_diverted: dict[int, int] = {}
        # merge=non_null: per step, per key, the best-ranked non-null member
        # value per column — the fill candidates for the winner's nulls.
        self._dedupe_merge: dict[int, dict[tuple, dict[str, tuple]]] = {}
        # Source column -> declared logical type, when the recipe was parsed
        # with the run's schema: ordering columns compare under it (DD-1).
        self._column_types: dict[str, str] = dict(
            getattr(recipe, "source_types", None) or {}
        )

    @property
    def output_columns(self) -> tuple[str, ...]:
        """Columns the recipe declared, plus any a flatten/unnest step discovered."""
        return tuple(self._output_columns)

    # -- public API ---------------------------------------------------------

    @property
    def is_noop(self) -> bool:
        return not self.steps

    def apply_batch(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Shape a batch, continuing the row numbering from earlier batches."""
        out: list[dict[str, Any]] = []
        for record in records:
            out.extend(self.apply_records(record))
        return out

    def apply_records(self, record: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Shape one source row into zero or more output rows.

        ``unnest_json`` and ``join`` (fanout=allow) may return more than
        one row. Filter and divert return zero. Every other step returns
        exactly one.
        """
        self._row_index += 1
        self.effect.rows_in += 1
        current: list[dict[str, Any]] = [dict(record)]
        if not self.steps:
            self.effect.rows_out += 1
            self._discover(current[0])
            return current

        for index, step in enumerate(self.steps):
            tally = self.effect.steps[index]
            next_rows: list[dict[str, Any]] = []
            for row in current:
                tally.rows_in += 1
                try:
                    result = self._apply_step(step, index, row, tally)
                except _Drop as drop:
                    if drop.diverted:
                        tally.rows_diverted += 1
                        self.effect.rows_diverted += 1
                        self._sample_diverted(drop, row)
                    else:
                        tally.rows_removed += 1
                        self.effect.rows_shaped_out += 1
                    continue
                if result is _HELD:
                    # The step holds the row until end-of-stream; drain() emits.
                    tally.rows_buffered += 1
                    self.effect.rows_buffered += 1
                    self._held[index] = self._held.get(index, 0) + 1
                    continue
                produced = result if isinstance(result, list) else [result]
                extra = max(len(produced) - 1, 0)
                if extra:
                    tally.rows_expanded += extra
                    self.effect.rows_expanded += extra
                tally.rows_out += len(produced)
                next_rows.extend(produced)
            current = next_rows

        for row in current:
            self._discover(row)
        self.effect.rows_out += len(current)
        return current

    def apply_row(self, record: Mapping[str, Any]) -> dict[str, Any] | None:
        """Shape one row when the recipe does not expand.

        ``None`` means the recipe removed or diverted it. A recipe that expands
        this row raises so callers cannot silently keep only the first child.
        Population paths use :meth:`apply_batch` / :meth:`apply_records`.
        """
        produced = self.apply_records(record)
        if len(produced) <= 1:
            return produced[0] if produced else None
        step = next((s for s in self.steps if s.op == "unnest_json"), self.steps[0])
        raise ShapeRowError(
            f"transform recipe expanded source row {self._row_index + 1} into "
            f"{len(produced)} rows; every produced row must be kept — use apply_batch",
            step=step,
            step_index=0,
            row_index=self._row_index,
            column=step.column,
        )

    def rows_accounted_for(self) -> bool:
        return self.effect.balanced

    # -- end-of-stream drain -------------------------------------------------

    def drain(self) -> list[dict[str, Any]]:
        """Emit the rows buffering steps held, run through the rest of the recipe.

        dedupe keep-last, aggregate, sort and rank cannot judge a row until they
        have seen the population, so they hold; the end of the read is when they
        must answer. Each emitted row then continues through the steps after it,
        so a cleanse downstream of an aggregate still applies — and a second
        buffering step still holds until its own drain, reached in step order.
        """
        if self._drained:
            return []
        self._drained = True
        out: list[dict[str, Any]] = []
        for index, step in enumerate(self.steps):
            buffer = self._buffers.pop(index, None)
            if buffer is None and step.op != "union":
                continue
            tally = self.effect.steps[index]
            if step.op == "union":
                # Appended secondary input — real input population, counted
                # in rows_in (not rows_expanded) because the write path's
                # committed-offset accounting already counts them in the
                # batch population; expansion would double-count.
                materialized = self._union_rows(step, index)
                emitted = len(materialized)
                if emitted:
                    tally.rows_in += emitted
                    self.effect.rows_in += emitted
                    self.effect.rows_union_appended += emitted
            else:
                materialized = self._drain_step(step, index, buffer)
                held = self._held.pop(index, 0)
                emitted = len(materialized)
                dropped = max(held - emitted, 0)
                # The held rows either become output or dedupe/rank losers — the
                # conservation ledger must name which.
                tally.rows_buffered -= held
                self.effect.rows_buffered -= held
                if dropped:
                    if step.op == "dedupe_rows":
                        # Dedupe losers were diverted to quarantine at the
                        # moment they lost — charge the remainder (a diverted
                        # row must never count as a removal too).
                        already = self._dedupe_diverted.pop(index, 0)
                        remainder = dropped - already
                        if remainder < 0:
                            remainder = 0
                        if remainder:
                            tally.rows_removed += remainder
                            self.effect.rows_shaped_out += remainder
                    else:
                        tally.rows_removed += dropped
                        self.effect.rows_shaped_out += dropped
            tally.rows_out += emitted
            for row in materialized:
                for final in self._run_tail(row, index + 1):
                    self._discover(final)
                    out.append(final)
        self.effect.rows_out += len(out)
        return out

    def _secondary_rows(self, step: ShapeStep, index: int, tag: str) -> list[Any]:
        """A step's rows-or-reference secondary input, loaded once per run."""
        cache_key = f"{tag}:{index}"
        cached = self._state.get(cache_key)
        if cached is None:
            reference = step.options.get("reference")
            if reference is not None:
                from services.lookup_reference import load_reference_rows

                cached = load_reference_rows(
                    reference, workspace_id=self.workspace_id
                )
            else:
                cached = list(step.options.get("rows") or [])
            self._state[cache_key] = cached
        return cached

    def _union_rows(self, step: ShapeStep, index: int) -> list[dict[str, Any]]:
        """Align the union input to output columns by name (fail-closed).

        ``map`` renames a reference column to an output column explicitly;
        an unmapped column with no output counterpart refuses unless
        ``extra=drop`` — an appended column cannot silently appear or vanish.
        """
        rows = self._secondary_rows(step, index, "union")
        mapping = {str(k): str(v) for k, v in (step.options.get("map") or {}).items()}
        extra = str(step.options.get("extra") or "refuse").casefold()
        out: list[dict[str, Any]] = []
        for pos, raw in enumerate(rows, start=1):
            if not isinstance(raw, Mapping):
                raise ValueError(
                    f"union step {index + 1}: appended row {pos} is not an object"
                )
            aligned: dict[str, Any] = {}
            for ref_col, value in raw.items():
                name = str(ref_col)
                if name in mapping:
                    aligned[mapping[name]] = value
                    continue
                if name not in self._output_columns:
                    if extra == "refuse":
                        raise ValueError(
                            f"union step {index + 1}: column '{name}' is not a "
                            "stream column — map it explicitly or set extra=drop"
                        )
                    continue
                aligned[name] = value
            out.append(aligned)
        return out

    def _join_index(self, step: ShapeStep, index: int) -> dict[tuple, list[dict[str, Any]]]:
        """The join input indexed by its 'on' key tuple, built once per run."""
        cache_key = f"join:{index}"
        built = self._state.get(cache_key)
        if built is None:
            built = {}
            ref_cols = [str(c) for c in step.options["on"].values()]
            for raw in self._secondary_rows(step, index, "join"):
                if not isinstance(raw, Mapping):
                    continue
                key = tuple(_as_text(raw.get(c)) for c in ref_cols)
                built.setdefault(key, []).append(dict(raw))
            self._state[cache_key] = built
        return built

    def _drain_step(
        self, step: ShapeStep, index: int, buffer: Any
    ) -> list[dict[str, Any]]:
        """Materialize one step's held rows into emittable dicts."""
        op = step.op
        options = step.options

        if op == "dedupe_rows":
            merge = str(options.get("merge") or "").casefold() == "non_null"
            out = []
            for key, entry in buffer.items():
                parts, winner_row, winner_index = entry
                if merge:
                    fallbacks = (self._dedupe_merge.get(index) or {}).get(key) or {}
                    merged = dict(winner_row)
                    for col, fb in fallbacks.items():
                        if _dedupe_is_null(merged.get(col)) and fb and not _dedupe_is_null(fb[2]):
                            merged[col] = fb[2]
                    out.append(merged)
                else:
                    out.append(winner_row)
            return out

        if op == "aggregate":
            out = []
            measures = [
                (
                    str(m.get("as") or ""),
                    str(m.get("function") or "").casefold(),
                )
                for m in options.get("measures", [])
            ]
            for acc in buffer.values():
                row = dict(acc["group"])
                for name, fn in measures:
                    if not name:
                        continue
                    row[name] = _agg_finish(acc["measures"].get(name) or _agg_init(), fn)
                out.append(row)
            return out

        if op == "sort_rows":
            order = _sort_order(options)
            rows = list(buffer)
            # Stable-sort one key at a time, last key first, so mixed
            # asc/desc per column works with a single comparator.
            for col, descending in reversed(order):
                rows.sort(key=lambda r, c=col: _sort_val(r.get(c)), reverse=descending)
            return rows

        if op == "rank_rows":
            order_col = str(options.get("order_by") or "")
            descending = str(options.get("direction") or "top").casefold() != "bottom"
            limit = max(0, int(options.get("limit") or 0))
            partitions = [str(c) for c in options.get("partition_by", [])]
            groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
            for row in buffer:
                pkey = (
                    tuple(_as_text(row.get(c)) for c in partitions)
                    if partitions
                    else ("",)
                )
                groups.setdefault(pkey, []).append(row)
            out = []
            for rows in groups.values():
                rows.sort(key=lambda r: _sort_val(r.get(order_col)), reverse=descending)
                out.extend(rows[:limit] if limit else rows)
            return out

        return list(buffer.values()) if isinstance(buffer, dict) else list(buffer)

    def _run_tail(self, row: dict[str, Any], start: int) -> list[dict[str, Any]]:
        """Run one drained row through the steps after the step that held it."""
        current = [row]
        for index in range(start, len(self.steps)):
            step = self.steps[index]
            tally = self.effect.steps[index]
            next_rows: list[dict[str, Any]] = []
            for r in current:
                tally.rows_in += 1
                try:
                    result = self._apply_step(step, index, r, tally)
                except _Drop as drop:
                    if drop.diverted:
                        tally.rows_diverted += 1
                        self.effect.rows_diverted += 1
                        self._sample_diverted(drop, r)
                    else:
                        tally.rows_removed += 1
                        self.effect.rows_shaped_out += 1
                    continue
                if result is _HELD:
                    tally.rows_buffered += 1
                    self.effect.rows_buffered += 1
                    self._held[index] = self._held.get(index, 0) + 1
                    continue
                produced = result if isinstance(result, list) else [result]
                extra = max(len(produced) - 1, 0)
                if extra:
                    tally.rows_expanded += extra
                    self.effect.rows_expanded += extra
                tally.rows_out += len(produced)
                next_rows.extend(produced)
            current = next_rows
        return current

    def _match_route(
        self,
        step: ShapeStep,
        index: int,
        row: dict[str, Any],
        tally: StepEffect,
    ) -> dict[str, Any] | None:
        """First route whose condition matches this row, else the default.

        Conditions were compiled once at parse (a bad one refused the
        recipe); the cache here only avoids re-compiling per row — an
        ExpressionError at this point would still surface through
        ``_guarded`` as a step error.
        """
        routes = self._state.get(f"router:{index}")
        if routes is None:
            routes = []
            for raw_route in step.options.get("routes") or []:
                expr = None
                if not raw_route.get("default"):
                    expr = compile_expression(
                        str(raw_route["condition"]),
                        known_columns=None,
                        label=f"router route '{raw_route['name']}'",
                    )
                routes.append((raw_route, expr))
            self._state[f"router:{index}"] = routes
        default_route = None
        for raw_route, expr in routes:
            if raw_route.get("default"):
                default_route = raw_route
                continue
            assert expr is not None
            matched = self._guarded(
                step, index, tally, row, lambda e=expr: e.matches(row)
            )
            if matched:
                return raw_route
        return default_route

    def _lookup_table(self, step: ShapeStep, index: int) -> dict[str, dict[str, Any]]:
        """The reference rows as a key-indexed dict, built once per run.

        Inline ``rows`` are part of the recipe; a ``reference`` block resolves
        its saved connector's table at first use — the run's cache boundary is
        the run itself, so a rerun after reference data changed sees the new
        values and a run never re-reads mid-flight.
        """
        table = self._state.get(index)
        if table is None:
            table = {}
            key_col = str(step.options.get("lookup_key") or "")
            reference = step.options.get("reference")
            if reference is not None:
                from services.lookup_reference import load_reference_rows

                rows = load_reference_rows(
                    reference, workspace_id=self.workspace_id
                )
            else:
                rows = step.options.get("rows") or []
            match_policy = str(step.options.get("match") or "first").casefold()
            for raw in rows:
                if not isinstance(raw, Mapping):
                    continue
                key = _as_text(raw.get(key_col)) or ""
                if key in table:
                    if match_policy == "refuse":
                        raise ValueError(
                            f"lookup step {index + 1}: reference key '{key}' "
                            "appears more than once and match=refuse — a "
                            "second hit would pick a row arbitrarily"
                        )
                    continue  # match=first: earliest row keeps the key
                table[key] = dict(raw)
            self._state[index] = table
        return table

    # -- step dispatch -----------------------------------------------------

    def _apply_step(
        self,
        step: ShapeStep,
        index: int,
        row: dict[str, Any],
        tally: StepEffect,
    ) -> Any:
        # dict → one row; list → fan-out (unnest_json, join fanout=allow);
        # _HELD → buffered for drain; _Drop raises to skip.
        op = step.op
        options = step.options

        if op == "drop_column":
            row.pop(step.column, None)
            return row

        if op == "keep_columns":
            wanted = [str(c) for c in options.get("columns", [])]
            return {name: row.get(name) for name in wanted if name in row}

        if op == "rename_column":
            target = str(options.get("to") or "")
            if step.column not in row or not target:
                return row
            return {
                (target if key == step.column else key): value
                for key, value in row.items()
            }

        if op == "constant_column":
            target = str(options.get("to") or "")
            value = options.get("value")
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op in ("derive_column", "set_value"):
            target = str(options.get("to") or "") if op == "derive_column" else step.column
            assert step.expression is not None
            value = self._guarded(step, index, tally, row, lambda: step.expression.evaluate(row))
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "concat_columns":
            target = str(options.get("to") or "")
            separator = str(options.get("separator") or "")
            parts = [_as_text(row.get(name)) for name in options.get("columns", [])]
            value = separator.join(p for p in parts if p is not None)
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "hash_identity":
            target = str(options.get("to") or "_df_row_key")
            value = _hash_identity_value(row, [str(c) for c in options.get("columns", [])])
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "split_column":
            separator = str(options.get("separator") or "")
            targets = [str(name) for name in options.get("into", [])]
            text = _as_text(row.get(step.column))
            pieces = text.split(separator) if text is not None else []
            for position, name in enumerate(targets):
                value = pieces[position] if position < len(pieces) else None
                self._record_write(tally, before=row.get(name), after=value, existed=name in row)
                row[name] = value
            return row

        if op == "filter_rows":
            assert step.expression is not None
            keep = bool(options.get("keep", True))
            matched = self._guarded(step, index, tally, row, lambda: step.expression.matches(row))
            if bool(matched) != keep:
                raise _Drop(diverted=False, reason="filtered out", step_index=index, op=op)
            return row

        if op == "router":
            route = self._match_route(step, index, row, tally)
            if route is None:
                if str(options.get("unmatched") or "") == "divert":
                    raise _Drop(
                        diverted=True,
                        reason="no route matched",
                        step_index=index,
                        op=op,
                    )
                raise ShapeRowError(
                    f"transform step {index + 1} (router), source row "
                    f"{self._row_index + 1}: no route matched and unmatched "
                    "is not divert — refusing to lose the row",
                    step=step,
                    step_index=index,
                    row_index=self._row_index,
                    column="",
                )
            # The route map is applied at write time in _split_routed_batch —
            # rewriting the row here mixes row shapes inside one batch: the
            # mapped row loses the shared columns, list-alignment then drops
            # its values, and the data-quality audit reads them as NULLs
            # (RT-3).
            row[ROUTE_COLUMN] = route["name"]
            self.effect.routes[route["name"]] = (
                self.effect.routes.get(route["name"], 0) + 1
            )
            return row

        if op == "divert_rows":
            assert step.expression is not None
            matched = self._guarded(step, index, tally, row, lambda: step.expression.matches(row))
            if matched:
                raise _Drop(
                    diverted=True,
                    reason=str(options.get("reason") or "diverted by a transform rule"),
                    step_index=index,
                    op=op,
                )
            return row

        if op == "surrogate_key":
            target = str(options.get("to") or "")
            start = int(options.get("start") or 1)
            step_by = int(options.get("step") or 1)
            counter = self._state.get(index)
            if counter is None:
                counter = start
            value = counter
            self._state[index] = counter + step_by
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "lookup":
            table = self._lookup_table(step, index)
            key = _as_text(row.get(step.column))
            match = table.get(key) if key is not None else None
            targets = {str(k): str(v) for k, v in (options.get("map") or {}).items()}
            if match is None:
                unmatched = str(options.get("unmatched") or "null").casefold()
                if unmatched == "drop":
                    raise _Drop(diverted=False, reason="no lookup match", step_index=index, op=op)
                if unmatched == "divert":
                    raise _Drop(
                        diverted=True,
                        reason=str(options.get("reason") or f"no lookup match for '{key}'"),
                        step_index=index,
                        op=op,
                    )
                if unmatched == "default":
                    defaults = options.get("defaults") or {}
                    for source_col, target in targets.items():
                        value = defaults.get(target, defaults.get(source_col))
                        self._record_write(tally, before=row.get(target), after=value, existed=target in row)
                        row[target] = value
                    return row
                for target in targets.values():
                    self._record_write(tally, before=row.get(target), after=None, existed=target in row)
                    row[target] = None
                return row
            for source_col, target in targets.items():
                value = match.get(source_col)
                self._record_write(tally, before=row.get(target), after=value, existed=target in row)
                row[target] = value
            return row

        if op == "join":
            join_index = self._join_index(step, index)
            on = {str(s): str(r) for s, r in (options.get("on") or {}).items()}
            key = tuple(_as_text(row.get(c)) for c in on)
            matches = join_index.get(key) or []
            mapping = {str(k): str(v) for k, v in (options.get("map") or {}).items()}
            if not matches:
                if str(options.get("type") or "inner") == "left":
                    # Left join keeps the row; every output column is null.
                    for target in mapping.values():
                        self._record_write(tally, before=row.get(target), after=None, existed=target in row)
                        row[target] = None
                    return row
                if str(options.get("unmatched") or "drop") == "divert":
                    raise _Drop(
                        diverted=True,
                        reason=str(options.get("reason") or f"no join match for key {key}"),
                        step_index=index,
                        op=op,
                    )
                raise _Drop(diverted=False, reason="no join match", step_index=index, op=op)
            fanout = str(options.get("fanout") or "refuse").casefold()
            if len(matches) > 1 and fanout == "refuse":
                raise ValueError(
                    f"join step {index + 1}: key {key} matches {len(matches)} "
                    "reference rows and fanout=refuse — duplicate keys would "
                    "multiply the row silently; pick fanout=first or allow"
                )
            picked = matches if fanout == "allow" else matches[:1]

            def _merged(base: dict[str, Any], hit: dict[str, Any]) -> dict[str, Any]:
                merged = dict(base)
                for ref_col, target in mapping.items():
                    value = hit.get(ref_col)
                    self._record_write(tally, before=merged.get(target), after=value, existed=target in merged)
                    merged[target] = value
                return merged

            merged_rows = [_merged(row, hit) for hit in picked]
            return merged_rows[0] if len(merged_rows) == 1 else merged_rows

        if op == "union":
            # Pass the streamed row through untouched — the appended input
            # emits at drain() once the output column set is complete.
            return row

        if op == "dedupe_rows":
            keys = [str(c) for c in options.get("keys", [])]
            key = tuple(_as_text(row.get(c)) for c in keys)
            keep = str(options.get("keep") or "first").casefold()
            merge = str(options.get("merge") or "").casefold() == "non_null"
            rule = _dedupe_rule(options)
            cap = _max_buffer(options)
            if keep == "first" and not merge:
                # Streaming first-seen: the winner emitted in place, later
                # members quarantined with the winning row named.
                seen = self._state.setdefault(index, {})
                if len(seen) >= cap and key not in seen:
                    raise ValueError(
                        f"dedupe_rows key space passed {cap} keys — "
                        "the buffer cap exists so one transfer cannot hold the "
                        "population in memory; raise 'max_keys' only deliberately"
                    )
                winner_index = seen.get(key)
                if winner_index is not None:
                    raise _Drop(
                        diverted=True,
                        reason=(
                            f"dedupe loser — key {key} already kept row "
                            f"#{winner_index + 1} under {rule}"
                        ),
                        step_index=index, op=op,
                        dedupe_key=key,
                        winner_row_index=winner_index,
                    )
                seen[key] = self._row_index
                return row
            # Buffered winner-election: keep=last (latest-by-column, then_by
            # order, declared tie_break; no ordering = last-seen) or any
            # merge — merge must see the whole group before emitting.
            winners = self._buffers.setdefault(index, {})
            if len(winners) >= cap and key not in winners:
                raise ValueError(
                    f"dedupe_rows key space passed {cap} keys — "
                    "raise 'max_keys' only deliberately"
                )
            parts, spec = _dedupe_rank(
                options, row, self._row_index, self._column_types
            )
            tie_last = str(options.get("tie_break") or "first_seen").casefold() == "last_seen"
            current = winners.get(key)
            fallbacks = self._dedupe_fallbacks(index, key) if merge else None
            if current is None:
                winners[key] = (parts, dict(row), self._row_index)
                if merge:
                    _dedupe_feed_fallback(
                        fallbacks, row, parts, self._row_index, spec, tie_last
                    )
            elif _dedupe_better(parts, current[0], spec, tie_last):
                # Displaced winner — it is a loser now, quarantined with the
                # new winner's row number named.
                self._dedupe_lose(
                    index, tally, key, current[1],
                    loser_index=current[2],
                    winner_index=self._row_index,
                    reason=(
                        f"dedupe loser — key {key} displaced by row "
                        f"#{self._row_index + 1} under {rule}"
                    ),
                )
                if merge:
                    _dedupe_feed_fallback(
                        fallbacks, current[1], current[0], current[2], spec, tie_last
                    )
                winners[key] = (parts, dict(row), self._row_index)
                if merge:
                    _dedupe_feed_fallback(
                        fallbacks, row, parts, self._row_index, spec, tie_last
                    )
            else:
                self._dedupe_lose(
                    index, tally, key, row,
                    loser_index=self._row_index,
                    winner_index=current[2],
                    reason=(
                        f"dedupe loser — key {key} kept row "
                        f"#{current[2] + 1} under {rule}"
                    ),
                )
                if merge:
                    _dedupe_feed_fallback(
                        fallbacks, row, parts, self._row_index, spec, tie_last
                    )
            return _HELD

        if op == "aggregate":
            groups = self._buffers.setdefault(index, {})
            if len(groups) >= _max_buffer(options):
                raise ValueError(
                    f"aggregate passed {_max_buffer(options)} groups — "
                    "raise 'max_keys' only deliberately"
                )
            group_cols = [str(c) for c in options.get("group_by", [])]
            gkey = tuple(_as_text(row.get(c)) for c in group_cols)
            acc = groups.get(gkey)
            if acc is None:
                acc = {"group": {c: row.get(c) for c in group_cols}, "measures": {}}
                groups[gkey] = acc
            for measure in options.get("measures", []):
                name = str(measure.get("as") or "")
                fn = str(measure.get("function") or "").casefold()
                mcol = str(measure.get("column") or "")
                state = acc["measures"].setdefault(name, _agg_init())
                _agg_add(state, fn, row.get(mcol))
            return _HELD

        if op == "sort_rows":
            buf = self._buffers.setdefault(index, [])
            if len(buf) >= _max_buffer(options):
                raise ValueError(
                    f"sort_rows passed {_max_buffer(options)} rows — "
                    "raise 'max_keys' only deliberately"
                )
            buf.append(dict(row))
            return _HELD

        if op == "rank_rows":
            buf = self._buffers.setdefault(index, [])
            if len(buf) >= _max_buffer(options):
                raise ValueError(
                    f"rank_rows passed {_max_buffer(options)} rows — "
                    "raise 'max_keys' only deliberately"
                )
            buf.append(dict(row))
            return _HELD

        if op == "unnest_json":
            produced = self._guarded(
                step, index, tally, row, lambda: self._explode_array(step, row)
            )
            if produced is None:
                clone = dict(row)
                target = str(options.get("to") or f"{step.column}_item")
                self._record_write(
                    tally, before=row.get(target), after=None, existed=target in row
                )
                clone[target] = None
                return [clone]
            return produced

        if op == "flatten_json":
            flattened = self._guarded(
                step, index, tally, row, lambda: self._flatten_object(step, row, tally)
            )
            return [row if flattened is None else flattened]

        if op == "nest_json":
            nested = self._guarded(
                step, index, tally, row, lambda: self._nest_fields(step, row, tally)
            )
            return [row if nested is None else nested]

        if op == "scd2":
            # Declarative history contract — it declares the key, tracked
            # columns and stamp names the destination merge honours; it does
            # not rewrite the row. The merge applies it at write time.
            return row

        # Everything else is a single-cell value transform.
        before = row.get(step.column)
        after = self._guarded(
            step, index, tally, row, lambda: _transform_value(step, before)
        )
        self._record_write(tally, before=before, after=after, existed=True)
        row[step.column] = after
        return row

    # -- nested JSON (json_intelligence SSOT — not a second parser) --------

    def _explode_array(self, step: ShapeStep, row: dict[str, Any]) -> list[dict[str, Any]]:
        """One parent row → N child rows. Cap and empty-array policy are fail-closed."""
        from services.json_intelligence import (
            ARRAY_EXPLODE_MAX,
            json_cell_text,
            parse_json_array,
        )

        raw = row.get(step.column)
        arr = parse_json_array(raw)
        if arr is None:
            raise ValueError(
                f"'{step.column}' is not a JSON array — unnest_json will not guess"
            )
        if len(arr) > ARRAY_EXPLODE_MAX:
            raise ValueError(
                f"'{step.column}' has {len(arr)} elements; unnest_json caps at "
                f"{ARRAY_EXPLODE_MAX} so one row cannot explode the transfer"
            )
        if not arr:
            raise ValueError(
                f"'{step.column}' is an empty array; unnest_json will not invent a "
                "row or drop this one silently"
            )
        target = str(step.options.get("to") or f"{step.column}_item")
        index_to = str(step.options.get("index_to") or "")
        keep_parent = bool(step.options.get("keep_parent", True))
        out: list[dict[str, Any]] = []
        for position, elem in enumerate(arr):
            clone = dict(row)
            clone[target] = json_cell_text(elem)
            if index_to:
                clone[index_to] = position
            if not keep_parent:
                clone.pop(step.column, None)
            out.append(clone)
        return out

    def _flatten_object(
        self,
        step: ShapeStep,
        row: dict[str, Any],
        tally: StepEffect,
    ) -> dict[str, Any]:
        """Promote JSON object keys onto this row. Parent blob stays unless dropped."""
        from services.json_intelligence import (
            DEFAULT_FLATTEN_DEPTH,
            STRUCT_FLATTEN_DEPTH,
            flatten_struct_field,
            json_cell_text,
            parse_json_object,
        )

        raw = row.get(step.column)
        # SQL NULL and a JSON null are absent objects. Flattening them is a
        # no-op so one null cell does not fail the run. A present non-object
        # still refuses — flatten will not guess a shape.
        if raw is None or (
            isinstance(raw, str) and raw.strip().lower() in {"", "null", "none"}
        ):
            return dict(row)
        obj = parse_json_object(raw)
        if obj is None:
            raise ValueError(
                f"'{step.column}' is not a JSON object — flatten_json will not guess"
            )
        wanted = [str(name) for name in step.options.get("keys", []) if name]
        clone = dict(row)
        if wanted:
            for name in wanted:
                value = obj.get(name)
                after = json_cell_text(value) if isinstance(value, (dict, list)) else value
                self._record_write(
                    tally, before=row.get(name), after=after, existed=name in row
                )
                clone[name] = after
            return clone
        depth = (
            STRUCT_FLATTEN_DEPTH
            if str(step.options.get("depth") or "top") == "top"
            else DEFAULT_FLATTEN_DEPTH
        )
        flat = flatten_struct_field(raw, parent_key=step.column, max_depth=depth)
        for key, value in flat.items():
            if key == step.column or str(key).startswith("__flatten_"):
                continue
            after = json_cell_text(value) if isinstance(value, (dict, list)) else value
            if key not in clone or clone.get(key) is None:
                self._record_write(
                    tally, before=row.get(key), after=after, existed=key in row
                )
                clone[key] = after
        return clone

    def _nest_fields(
        self, step: ShapeStep, row: dict[str, Any], tally: StepEffect
    ) -> dict[str, Any]:
        """Group listed columns into one JSON-object column.

        ``fields`` entries are a column name (kept under its own key) or
        ``{from|column: source, to: key}`` to rename inside the object. A field
        naming a column the row does not carry is refused, not nulled — an
        object missing keys silently is a data-integrity event. ``drop`` folds
        the sources away so the destination keeps only the nested column.
        """
        from services.json_intelligence import json_cell_text

        options = step.options
        target = str(options.get("to") or "")
        if not target:
            raise ValueError("nest_json requires 'to'")
        entries: list[tuple[str, str]] = []
        for item in options.get("fields") or []:
            if isinstance(item, Mapping):
                source = str(item.get("from") or item.get("column") or item.get("to") or "")
                key = str(item.get("to") or item.get("from") or item.get("column") or "")
            else:
                source = key = str(item)
            if not source or not key:
                raise ValueError("nest_json field needs both a source column and a key")
            if source not in row:
                raise ValueError(
                    f"'{source}' is not a column on this row — nest_json will not invent a key"
                )
            entries.append((source, key))
        clone = dict(row)
        obj = {key: row[source] for source, key in entries}
        after = json_cell_text(obj)
        self._record_write(
            tally, before=row.get(target), after=after, existed=target in row
        )
        clone[target] = after
        if options.get("drop"):
            for source, _ in entries:
                if source != target and source in clone:
                    clone.pop(source)
        return clone

    # -- bookkeeping -------------------------------------------------------

    def _discover(self, row: Mapping[str, Any]) -> None:
        for key in row:
            name = str(key)
            if name and name not in self._output_columns:
                self._output_columns.append(name)

    def _record_write(self, tally: StepEffect, *, before: Any, after: Any, existed: bool) -> None:
        if existed and _same_value(before, after):
            return
        tally.cells_changed += 1
        self.effect.cells_changed += 1
        if is_blank(after) and not is_blank(before):
            tally.nulls_introduced += 1
            self.effect.nulls_introduced += 1

    def _guarded(
        self,
        step: ShapeStep,
        index: int,
        tally: StepEffect,
        row: dict[str, Any],
        work: Any,
    ) -> Any:
        """Run a step's computation under its declared error policy.

        ``refuse`` (the default) fails the run and names the row — a value the
        recipe cannot compute is a decision for a human, not something to paper
        over. ``divert`` keeps the row but sends it to quarantine with the
        reason. ``null`` is the only policy that discards information, and it is
        counted so the proof pack can state how often it happened.
        """
        try:
            return work()
        except (EvalError, ValueError, ArithmeticError, TypeError) as exc:
            tally.errors += 1
            column = step.column or str(step.options.get("to") or "")
            # A refusal surfaces to the operator as the run's failure message, so
            # it has to say where to look: step 3 of the recipe, row 431, column
            # arr_time — not just "not a number" for a million-row file.
            located = f"transform step {index + 1} ({step.op})"
            if column:
                located += f" on column '{column}'"
            # One-based, like every other row citation the operator sees.
            located += f", source row {self._row_index + 1}: {exc}"
            failure = ShapeRowError(
                located,
                step=step,
                step_index=index,
                row_index=self._row_index,
                column=column,
            )
            if len(tally.error_samples) < self.sample_limit:
                tally.error_samples.append(failure.as_dict())
            if step.on_error == "refuse":
                raise failure from exc
            if step.on_error == "divert":
                raise _Drop(
                    diverted=True,
                    reason=f"{step.op} could not be applied: {exc}",
                    step_index=index,
                    op=step.op,
                ) from exc
            return None

    def _sample_diverted(self, drop: _Drop, row: dict[str, Any]) -> None:
        record = DivertedRow(
            row_index=self._row_index,
            reason=drop.reason,
            step=drop.step_index,
            op=drop.op,
            record=dict(row) if self.keep_diverted_records else {},
            dedupe_key=drop.dedupe_key,
            winner_row_index=drop.winner_row_index,
        )
        self._queue_diverted(record)
        if drop.dedupe_key is not None and drop.winner_row_index is not None:
            self._dedupe_diverted[drop.step_index] = (
                self._dedupe_diverted.get(drop.step_index, 0) + 1
            )
            if len(self.dedupe_decisions) < _MAX_BUFFERED:
                self.dedupe_decisions.append(
                    {
                        "step": drop.step_index + 1,
                        "key": list(drop.dedupe_key),
                        "loser_row": self._row_index + 1,
                        "winner_row": drop.winner_row_index + 1,
                        "reason": drop.reason,
                    }
                )

    def _queue_diverted(self, record: DivertedRow) -> None:
        # Every diverted row is quarantine matter, not a sampled curiosity:
        # the transfer drains this per page and persists it to the DLQ.
        if len(self._diverted_pending) >= _MAX_BUFFERED:
            raise ValueError(
                f"shape divert queue passed {_MAX_BUFFERED} rows without being "
                "drained — the stream persists diverted rows per page; "
                "reaching the cap means the caller never collected them"
            )
        self._diverted_pending.append(record)
        if len(self.effect.diverted_samples) >= self.sample_limit:
            return
        self.effect.diverted_samples.append(record)

    def _dedupe_lose(
        self,
        index: int,
        tally: "StepEffect",
        key: tuple[Any, ...],
        row: dict[str, Any],
        *,
        loser_index: int,
        winner_index: int,
        reason: str,
    ) -> None:
        """Ledger a dedupe loser: quarantined with the winner named.

        Losers are diverted (quarantine matter — never silently dropped), and
        the decision is logged so the per-step preview can pair every loser
        with the row that beat it.
        """
        tally.rows_diverted += 1
        self.effect.rows_diverted += 1
        self._dedupe_diverted[index] = self._dedupe_diverted.get(index, 0) + 1
        self._queue_diverted(
            DivertedRow(
                row_index=loser_index,
                reason=reason,
                step=index,
                op="dedupe_rows",
                record=dict(row) if self.keep_diverted_records else {},
                dedupe_key=key,
                winner_row_index=winner_index,
            )
        )
        if len(self.dedupe_decisions) < _MAX_BUFFERED:
            self.dedupe_decisions.append(
                {
                    "step": index + 1,
                    "key": list(key),
                    "loser_row": loser_index + 1,
                    "winner_row": winner_index + 1,
                    "reason": reason,
                }
            )

    def _dedupe_fallbacks(self, index: int, key: tuple) -> dict[str, tuple]:
        return self._dedupe_merge.setdefault(index, {}).setdefault(key, {})

    def take_diverted(self) -> list[DivertedRow]:
        """Diverted rows since the last call, then cleared.

        The transfer pulls this after every page and persists the rows to the
        job's quarantine store — a shape-diverted row is replayable quarantine,
        not a counter in a ledger.
        """
        rows = self._diverted_pending
        self._diverted_pending = []
        return rows


# ---------------------------------------------------------------------------
# Single-cell transforms — one implementation, shared by preview and execute
# ---------------------------------------------------------------------------


def _call(name: str, *args: Any) -> Any:
    """Reuse the expression function library so both planes agree exactly.

    A `trim` step and a `trim(col)` expression must produce the same value; the
    only way to guarantee that is for there to be one implementation.
    """
    return FUNCTIONS[name].impl(*args)


def _transform_value(step: ShapeStep, value: Any) -> Any:
    op = step.op
    options = step.options

    if op == "trim":
        return _call("trim", value)
    if op == "collapse_whitespace":
        return _call("collapse_whitespace", value)
    if op == "case":
        mode = options.get("mode")
        if mode == "upper":
            return _call("upper", value)
        if mode == "lower":
            return _call("lower", value)
        return _call("title_case", value)
    if op == "strip_characters":
        return _call("strip_characters", value, options.get("characters"))
    if op == "pad":
        fn = "lpad" if options.get("side", "left") == "left" else "rpad"
        return _call(fn, value, options.get("width"), options.get("fill", " "))
    if op == "replace":
        if options.get("regex"):
            return _call("regex_replace", value, options.get("search"), options.get("replacement", ""))
        return _call("replace", value, options.get("search"), options.get("replacement", ""))
    if op == "default_if_null":
        return options.get("value") if is_blank(value) else value
    if op == "null_if":
        if is_blank(value):
            return None
        sentinels = {
            text
            for raw in options.get("values", [])
            if (text := _as_text(raw)) is not None
        }
        text = _as_text(value)
        return None if text is not None and text in sentinels else value
    if op == "round_number":
        return _call("round", value, options.get("places", 0)) if not is_blank(value) else None
    if op == "truncate_number":
        return _call("truncate", value, options.get("places", 0)) if not is_blank(value) else None
    if op == "absolute":
        return _call("abs", value)
    if op == "clamp":
        low = options.get("min")
        high = options.get("max")
        if is_blank(value):
            return None
        number = _call("to_number", value)
        if low not in (None, ""):
            number = _call("greatest", number, _call("to_number", low))
        if high not in (None, ""):
            number = _call("least", number, _call("to_number", high))
        return number
    if op == "parse_number":
        return _call("to_number", value)
    if op == "parse_date":
        parsed = _call("to_date", value, options.get("format"))
        zone = options.get("timezone")
        if parsed is not None and zone:
            parsed = _with_timezone(parsed, zone)
        output = options.get("output_format")
        if parsed is not None and output:
            return _call("format_date", parsed, output)
        return parsed
    if op == "parse_boolean":
        return _call("to_boolean", value)
    if op == "normalize_unicode":
        return _call("normalize_unicode", value, options.get("form", "NFC"))
    if op == "cast_column":
        return _cast(
            value,
            str(options.get("to_type")),
            options.get("format"),
            precision=options.get("precision"),
            scale=options.get("scale"),
            timezone=options.get("timezone"),
        )
    if op in ("hash_pii", "mask_pii", "redact"):
        # All three governance ops are deterministic — the proof re-applies
        # them so a dropped step or a wrong token becomes a provable defect
        # (D15), not a silently skipped fingerprint.
        if op in ("hash_pii", "mask_pii") and is_blank(value):
            return None
        if op == "mask_pii" and options.get("pattern"):
            return _mask_pattern(str(value), str(options["pattern"]))
        from services.transform_engine import apply_transform

        digested, err = apply_transform(value, op)
        if err:
            raise EvalError(err)
        return digested

    raise ValueError(f"'{op}' has no value implementation")


def _mask_pattern(value: str, pattern: str) -> str:
    """Pattern masks that keep a hint of the value's shape.

    ``last4`` keeps the final four characters (cards/SSNs), ``first4`` the
    leading four, ``email`` preserves the domain so the masked value still
    sorts and reads plausibly. Anything else is refused — an unknown pattern
    must not degrade to "reveal" or "all stars" silently.
    """
    if pattern == "last4":
        tail = value[-4:] if len(value) > 4 else ""
        return "*" * (len(value) - len(tail)) + tail
    if pattern == "first4":
        head = value[:4] if len(value) > 4 else value
        return head + "*" * (len(value) - len(head))
    if pattern == "email":
        local, sep, domain = value.partition("@")
        if not sep:
            raise EvalError("email mask needs an address with a domain")
        visible = local[0] if local else ""
        return f"{visible}***@{domain}"
    raise EvalError(
        f"mask pattern '{pattern}' is not one of last4 | first4 | email"
    )


def _cast(
    value: Any,
    to_type: str,
    fmt: Any,
    *,
    precision: Any = None,
    scale: Any = None,
    timezone: Any = None,
) -> Any:
    """Declare a value's logical type, so the profiler is told rather than asked.

    This is the direct answer to a profiler collapsing `1.50000000` to a
    narrower scale: the operator states the type, and Map sees the declaration
    instead of an inference drawn from a sample.
    """
    if is_blank(value):
        return None
    if to_type == "text":
        return _call("to_text", value)
    if to_type == "integer":
        number = _call("to_number", value)
        whole = number.to_integral_value(rounding="ROUND_HALF_UP")
        if whole != number:
            raise EvalError(f"'{value}' is not a whole number")
        return int(whole)
    if to_type == "decimal":
        return _fit_decimal(_call("to_number", value), precision, scale)
    if to_type == "boolean":
        return _call("to_boolean", value)
    if to_type == "date":
        parsed = _call("to_date", value, fmt)
        return None if parsed is None else parsed.date()
    if to_type == "timestamp":
        parsed = _to_timestamp(value, fmt)
        if parsed is None:
            return None
        return _with_timezone(parsed, timezone) if timezone else parsed
    raise EvalError(f"'{to_type}' is not a logical type")


def _fit_decimal(number: Any, precision: Any, scale: Any) -> Any:
    """Quantize to the declared scale, then refuse a value that does not fit.

    Half-up is the same rule ``round()`` uses. Integer digits that overflow
    the precision are not truncated: the row is refused so a NUMBER(5,2)
    cannot silently become 1000.00.
    """
    from decimal import ROUND_HALF_UP

    if not isinstance(number, Decimal):
        number = Decimal(str(number))
    if scale not in (None, ""):
        number = number.quantize(Decimal(1).scaleb(-int(scale)), rounding=ROUND_HALF_UP)
    if precision in (None, ""):
        return number
    width = int(precision)
    _sign, digits, exponent = number.as_tuple()
    if not isinstance(exponent, int):
        raise EvalError(f"'{number}' is not a finite decimal")
    if digits == (0,):
        return number
    scale_digits = -exponent if exponent < 0 else 0
    int_digits = len(digits) - scale_digits
    if int_digits < 0:
        int_digits = 0
    used = int_digits + scale_digits
    if used > width:
        declared = f"precision {width}"
        if scale not in (None, ""):
            declared += f", scale {int(scale)}"
        raise EvalError(f"'{number}' needs {used} digits but the cast allows {declared}")
    return number


def _to_timestamp(value: Any, fmt: Any) -> Any:
    """A timestamp keeps its clock and any offset it states.

    ``to_date`` is the calendar-date parser: routing a timestamp cast through it
    landed every row at midnight, so ``cast -> assume_timezone`` shifted a
    midnight that never existed. Unformatted text is read by the write path's
    own ``datetime`` parser, the one a Map ``datetime`` transform uses.
    """
    from datetime import date as _date
    from datetime import datetime as _datetime

    if is_blank(value):
        return None
    if isinstance(value, _datetime):
        return value
    if isinstance(value, _date):
        return _datetime(value.year, value.month, value.day)  # noqa: DTZ001
    if fmt is not None and _as_text(fmt):
        return _call("to_date", value, fmt)
    text = (_as_text(value) or "").strip()
    from services.transform_engine import apply_transform

    iso, err = apply_transform(text, "datetime")
    if err or iso is None:
        raise EvalError(
            f"'{text}' is not a timestamp; pass an explicit format "
            "(day/month order is never guessed)"
        )
    try:
        return _datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvalError(f"'{text}' is not a timestamp") from exc


def _with_timezone(moment: Any, zone_name: Any) -> Any:
    """Read a naive clock in the named zone and store the UTC instant.

    A value that already carries an offset keeps that instant. A blank zone
    never reaches here, so a naive source clock is not shifted by default.
    """
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    if moment is None:
        return None
    name = str(zone_name or "").strip()
    try:
        zone = ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise EvalError(f"'{name}' is not a known time zone") from exc
    # One rule with Map's ``assume_timezone:`` — a wall clock in a DST gap is
    # refused and an ambiguous fall-back hour takes PostgreSQL's fold.
    from services.transform_engine import attach_timezone

    local, err = attach_timezone(moment, zone, zone_name=name)
    if err:
        raise EvalError(err)
    return local.astimezone(ZoneInfo("UTC"))


def _hash_identity_value(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    """Stable SHA-256 of the named cells — row-local, no clock, no randomness.

    Empty and missing stay distinct so two sparse rows cannot collide into one
    key. Gate-8 uses this as the align key when the source has no natural PK
    (flights-1m ``FL_DATE`` repeats).
    """
    parts: list[str] = []
    for name in columns:
        if name not in row:
            parts.append("\x1eABSENT")
            continue
        text = _as_text(row.get(name))
        parts.append("\x1eEMPTY" if text is None else text)
    payload = "\x1f".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _same_value(before: Any, after: Any) -> bool:
    """Whether a write actually changed the cell, for an honest change count."""
    if before is after:
        return True
    if is_blank(before) and is_blank(after):
        return True
    if is_blank(before) or is_blank(after):
        return False
    if isinstance(before, (int, float, Decimal)) and isinstance(after, (int, float, Decimal)):
        if isinstance(before, bool) != isinstance(after, bool):
            return False
        try:
            return Decimal(str(before)) == Decimal(str(after))
        except (InvalidOperation, DecimalException, ValueError):
            # NaN or infinity: not comparable as equal, so the cell counts as written.
            return False
    return _as_text(before) == _as_text(after)


def shape_records(
    recipe: ShapeRecipe,
    records: Sequence[Mapping[str, Any]],
    *,
    sample_limit: int = _DEFAULT_SAMPLES,
) -> tuple[list[dict[str, Any]], ShapeEffect]:
    """One-shot convenience for preview and tests."""
    engine = ShapeEngine(recipe, sample_limit=sample_limit)
    shaped = engine.apply_batch(records)
    shaped.extend(engine.drain())
    return shaped, engine.effect
