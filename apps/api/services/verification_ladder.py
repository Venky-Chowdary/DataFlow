"""Property 5 — five-layer population verification (not sample screening).

Layers
------
L1  Row-count balance — overwrite ``reader == dest``; append dest-Δ;
    keyed upsert/CDC ``dest_delta == inserts - deletes``
L2  Per-column aggregates (null count / min / max / sum)
L3  Full-population order-independent checksum (Gate-8 core)
L4  Typed per-column digests — localize mismatch to COLUMN
L5  Binary-search on ordered PK — localize mismatch to ROW + values

Sample compare remains *screening* and must never claim population proof.
This module extends the canonical reconcile path; it is not a parallel product.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Operator-facing alias — 500-row probes are screening, never population proof.
DEFAULT_SCREENING_LIMIT = 500

# Rows pulled per round trip while streaming a population read.
_READ_BATCH = 10_000


def _fetch_size(budget: int, read_so_far: int) -> int:
    """Never pull more than one row past the budget we are about to refuse."""
    if not budget:
        return _READ_BATCH
    return max(min(_READ_BATCH, budget - read_so_far + 1), 1)


# Full-table load for L2/L4/L5 is correct but memory-bound. Above this cap we
# refuse to pretend we finished population localization (honest skip).
try:
    from services.brand_env import getenv_brand as _getenv_brand

    MAX_LADDER_ROWS = int(_getenv_brand("VERIFICATION_LADDER_MAX_ROWS", "250000") or "250000")
except Exception:
    MAX_LADDER_ROWS = 250_000


class PopulationTooLarge(RuntimeError):
    """Raised by the readers before an oversized population reaches memory."""

    def __init__(self, rows_read: int, budget: int) -> None:
        super().__init__(
            f"population exceeds the {budget}-row ladder budget "
            f"(stopped after reading {rows_read})"
        )
        self.rows_read = rows_read
        self.budget = budget


@dataclass(frozen=True)
class ColumnAggregate:
    column: str
    null_count: int
    non_null_count: int
    distinct_count: int | None
    min_value: str | None
    max_value: str | None
    sum_value: str | None  # Decimal text; None when non-numeric
    # Order-independent Gate-8 cell fingerprints. Min/max wires can spell the
    # same instant two ways; this is what L2 treats as the value population.
    value_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LayerReport:
    layer: str
    passed: bool
    population_proof: bool
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "passed": self.passed,
            "population_proof": bool(self.population_proof),
            "details": dict(self.details or {}),
        }


def layer_l1_row_balance(
    *,
    source_rows: int,
    target_rows: int,
    rejected_rows: int = 0,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
    allow_extra_rows: bool = False,
    target_rows_before: int | None = None,
    keyed_cardinality: bool = False,
    keyed_expected_delta: int | None = None,
) -> LayerReport:
    """L1 — the same three cardinality identities ``row_conservation`` closes.

    Overwrite: ``source - dropped - skipped == dest COUNT(*)``.
    Append: ``dest - dest_before == expected`` (Full Append dest-Δ —
    ``dest >= expected`` is not proof; the table may already have held more).
    Keyed upsert/CDC: ``dest - dest_before == inserts - deletes``. Updates
    do not change COUNT(*). Applying the append identity to a snapshot
    upsert fails a correct write (3 updates + 1 insert → dest Δ 1 vs
    reader 4) while Gate-8 cells match — the operator sees Failed with
    "Row fidelity verified". Without a dest-engine key census, keyed L1
    is unmeasured: it must not veto matching L3 cells, and it must not
    claim population proof. Keys the writer held out (quarantined) are not
    classified as new or existing, so with hold-outs the keyed identity is the
    bound ``expected - dropped <= dest Δ <= expected``.
    """
    dropped = max(max(int(rejected_rows or 0), 0) - max(int(coerced_null_rows or 0), 0), 0)
    skipped = max(int(rows_skipped or 0), 0)
    batch_expected = max(int(source_rows) - dropped - skipped, 0)
    target = int(target_rows)
    details: dict[str, Any] = {
        "source_rows": int(source_rows),
        "target_rows": target,
        "rejected_rows": int(rejected_rows or 0),
        "coerced_null_rows": int(coerced_null_rows or 0),
        "rows_skipped": skipped,
        "dropped_rows": dropped,
        "expected_rows": batch_expected,
        "target_rows_before": target_rows_before,
        "keyed_expected_delta": keyed_expected_delta,
    }
    if keyed_cardinality:
        details["equation"] = "target - target_rows_before == inserts - deletes"
        if keyed_expected_delta is None or target_rows_before is None:
            details["skipped"] = True
            details["reason"] = (
                "keyed_census_unmeasured"
                if keyed_expected_delta is None
                else "dest_before_unmeasured"
            )
            return LayerReport(
                layer="L1",
                passed=True,
                population_proof=False,
                details=details,
            )
        delta = target - int(target_rows_before)
        details["dest_delta"] = delta
        details["expected_rows"] = int(keyed_expected_delta)
        # The census is taken before the write, so a key the writer then held
        # out is still counted in it: as an insert if the destination lacked
        # the key, as nothing if it held it. Which one is unknown (the ledger
        # says the same), so held-out keys widen the identity to a bound
        # instead of failing a write that landed every other key (D27).
        floor = int(keyed_expected_delta) - dropped
        details["expected_rows_floor"] = floor
        return LayerReport(
            layer="L1",
            passed=floor <= delta <= int(keyed_expected_delta),
            population_proof=False,
            details=details,
        )
    equation = "source - dropped - skipped == target"
    if allow_extra_rows and target_rows_before is not None:
        delta = target - int(target_rows_before)
        ok = delta == batch_expected
        equation = "target - target_rows_before == expected"
        details["dest_delta"] = delta
    else:
        ok = target == batch_expected or (allow_extra_rows and target >= batch_expected)
    details["equation"] = equation
    return LayerReport(
        layer="L1",
        passed=ok,
        population_proof=not allow_extra_rows,
        details=details,
    )


def _cell_wire(value: Any) -> str:
    """One ladder population cell. Same wire as SQL extract."""
    from services.value_serializer import cell_to_string

    return cell_to_string(value, preserve_sql_null=True)


def _row_cells(row: dict[str, Any], columns: list[str] | None = None) -> dict[str, str]:
    keys = columns if columns else list(row.keys())
    return {c: _cell_wire(row.get(c)) for c in keys}


def _cell_text(value: Any) -> str | None:
    """One L2 cell. Same wire as SQL readers; NULL is absence, not a string.

    ``str(value)`` invented ``True`` / ``1E+2`` / a Python ``b'...'`` repr.
    ``SQL_NULL_SENTINEL`` was counted as a non-null string, so L2 under-counted
    NULLs after PostgreSQL / Iceberg / procedure extract. Empty string stays a
    value. Gate-8 leftover ``\\x00NULL\\x00`` stays null.
    """
    from services.value_serializer import (
        NULL_WIRE_SENTINELS,
        is_missing_sentinel,
    )

    if value is None or is_missing_sentinel(value):
        return None
    if isinstance(value, str):
        if value in NULL_WIRE_SENTINELS or value == "\x00NULL\x00":
            return None
        return value
    text = _cell_wire(value)
    if text in NULL_WIRE_SENTINELS or text == "\x00NULL\x00":
        return None
    return text


def _l2_same_value(left: Any, right: Any) -> bool:
    """True when two L2 aggregates are the same cell under Gate-8 fingerprints.

    The stored wire stays the SQL reader spelling (``true``, ``2024-...Z``,
    ``1,234.567``). Destinations persist ``1`` / ``+00:00`` / ``1234.567``.
    Those pairs already share a checksum; treating the spelling as a miss
    rolled a faithful batch back.
    """
    if left == right:
        return True
    if left is None or right is None:
        return False
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    from services.reconciliation import normalize_cell

    return normalize_cell(left) == normalize_cell(right)


def _l2_sum_decimal(text: str) -> Decimal | None:
    """Decimal contribution of one L2 cell.

    Strict bool tokens are ``1``/``0`` on the fingerprint path. Summing the
    raw ``true``/``false`` wire left the source sum unset while SQLite's
    ``1``/``0`` summed, and the ladder vetoed a matching checksum.
    """
    parsed = _try_decimal(text)
    if parsed is not None:
        return parsed
    from services.transform_engine import _STRICT_BOOL_FALSE, _STRICT_BOOL_TRUE

    lowered = text.strip().lower()
    if lowered in _STRICT_BOOL_TRUE:
        return Decimal(1)
    if lowered in _STRICT_BOOL_FALSE:
        return Decimal(0)
    return None


_IDENTITY_TRANSFORMS = frozenset({"", "none", "identity", "passthrough"})


def mappings_rewrite_written_cells(mappings: list[dict] | None) -> bool:
    """True when a raw source-table re-read is not the image that was written.

    Renames, crosswalks, and non-identity transforms change the cell. L2 on
    the pre-image reports every rewritten value as corruption.
    """
    for raw in mappings or []:
        if not isinstance(raw, dict):
            continue
        src = str(raw.get("source") or raw.get("source_column") or "").strip()
        tgt = str(raw.get("target") or raw.get("target_column") or "").strip()
        if src and tgt and src.casefold() != tgt.casefold():
            return True
        transform = str(raw.get("transform") or "none").strip().lower()
        if transform not in _IDENTITY_TRANSFORMS:
            return True
        if raw.get("code_crosswalk") or raw.get("crosswalk"):
            return True
    return False


def _l2_population_skip_reason(
    *,
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    source_row_count: int,
    target_row_count: int | None,
    rejected_rows: int,
    coerced_null_rows: int,
    rows_skipped: int,
) -> str:
    """Why L2 must not grade this load as the written batch.

    Called only after the written-batch checksums already match. A digest
    disagreement still keeps L2 so a real cell miss stays a failure.
    """
    held_out = max(int(rejected_rows), 0) - max(int(coerced_null_rows), 0)
    omitted = held_out + max(int(rows_skipped), 0)
    written = max(int(source_row_count) - omitted, 0)
    if omitted > 0 and len(source_rows) > written:
        return "source_includes_rows_that_did_not_land"
    if target_row_count is not None and len(target_rows) != int(target_row_count):
        return "dest_population_is_not_this_batch"
    if int(coerced_null_rows) > 0:
        return "coerced_nulls_follow_the_checksum_not_the_raw_source"
    return ""


def _try_decimal(text: str | None) -> Decimal | None:
    if text is None:
        return None
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(text)


def _wire_less(left: str, right: str) -> bool:
    """True when ``left`` is smaller than ``right`` for L2 min/max.

    Dest-canonical numbers compare as Decimal (``9`` before ``10``). Mixed
    or unparseable cells stay wire-lexicographic — Auto ``1,234`` still
    refuses numeric invent.
    """
    left_n = _try_decimal(left)
    right_n = _try_decimal(right)
    if left_n is not None and right_n is not None:
        return left_n < right_n
    return left < right


def compare_present_wires(left: str, right: str) -> int:
    """``-1`` / ``0`` / ``1`` using the same order as L2 min/max."""
    if _wire_less(left, right):
        return -1
    if _wire_less(right, left):
        return 1
    return 0


def compute_column_aggregates(
    rows: Iterable[dict[str, Any]],
    columns: list[str],
) -> dict[str, ColumnAggregate]:
    """In-memory population aggregates over a full row set."""
    nulls = {c: 0 for c in columns}
    non_null = {c: 0 for c in columns}
    mins: dict[str, str | None] = {c: None for c in columns}
    maxs: dict[str, str | None] = {c: None for c in columns}
    sums: dict[str, Decimal | None] = {c: Decimal(0) for c in columns}
    sum_ok = {c: True for c in columns}
    distinct: dict[str, set[str]] = {c: set() for c in columns}
    fingerprints: dict[str, list[str]] = {c: [] for c in columns}
    from services.reconciliation import normalize_cell

    for rec in rows:
        if not isinstance(rec, dict):
            continue
        for col in columns:
            raw = rec.get(col)
            text = _cell_text(raw)
            if text is None:
                nulls[col] += 1
                continue
            non_null[col] += 1
            distinct[col].add(text)
            fingerprints[col].append(normalize_cell(text))
            if mins[col] is None or _wire_less(text, mins[col]):
                mins[col] = text
            if maxs[col] is None or _wire_less(maxs[col], text):
                maxs[col] = text
            if sum_ok[col]:
                d = _l2_sum_decimal(text)
                if d is None:
                    sum_ok[col] = False
                    sums[col] = None
                else:
                    sums[col] = (sums[col] or Decimal(0)) + d

    out: dict[str, ColumnAggregate] = {}
    for col in columns:
        fps = fingerprints[col]
        fps.sort()
        digest = hashlib.sha256()
        for fp in fps:
            digest.update(fp.encode("utf-8"))
            digest.update(b"\n")
        out[col] = ColumnAggregate(
            column=col,
            null_count=nulls[col],
            non_null_count=non_null[col],
            distinct_count=len(distinct[col]),
            min_value=mins[col],
            max_value=maxs[col],
            sum_value=None if not sum_ok[col] or sums[col] is None else format(sums[col], "f"),
            value_fingerprint=digest.hexdigest(),
        )
    return out


def compare_column_aggregates(
    source: dict[str, ColumnAggregate],
    target: dict[str, ColumnAggregate],
) -> LayerReport:
    """L2 — compare population aggregates column-by-column."""
    mismatched: list[dict[str, Any]] = []
    cols = sorted(set(source) | set(target))
    for col in cols:
        s = source.get(col)
        t = target.get(col)
        if s is None or t is None:
            mismatched.append({"column": col, "reason": "missing_side"})
            continue
        # Same cells under the checksum contract. Storage spelling
        # (``true``/``1``, ``Z``/``+00:00``, ``1,234.567``/``1234.567``)
        # is not a second failure mode.
        if (
            s.null_count == t.null_count
            and s.non_null_count == t.non_null_count
            and s.value_fingerprint
            and s.value_fingerprint == t.value_fingerprint
        ):
            continue
        diffs: dict[str, Any] = {}
        for field_name in ("null_count", "non_null_count", "min_value", "max_value", "sum_value"):
            left = getattr(s, field_name)
            right = getattr(t, field_name)
            same = left == right if field_name.endswith("_count") else _l2_same_value(left, right)
            if not same:
                diffs[field_name] = {"source": left, "target": right}
        if diffs:
            mismatched.append({"column": col, "diffs": diffs})
    return LayerReport(
        layer="L2",
        passed=not mismatched,
        population_proof=True,
        details={
            "columns_compared": len(cols),
            "mismatched_columns": [m["column"] for m in mismatched],
            "mismatches": mismatched[:50],
        },
    )


def column_typed_checksums(
    rows: Iterable[dict[str, Any]],
    columns: list[str],
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> dict[str, str]:
    """L4 — per-column SHA-256 of sorted typed cell fingerprints."""
    from services.reconciliation import fingerprint_for_reconcile

    types = dest_types or {}
    buckets: dict[str, list[str]] = {c: [] for c in columns}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        for col in columns:
            fp = fingerprint_for_reconcile(
                rec.get(col),
                ddl_type=str(types.get(col) or ""),
                engine=dest_db_type,
            )
            buckets[col].append(fp)
    out: dict[str, str] = {}
    for col, fps in buckets.items():
        fps.sort()
        h = hashlib.sha256()
        for fp in fps:
            h.update(fp.encode("utf-8"))
            h.update(b"\n")
        out[col] = h.hexdigest()
    return out


def localize_checksum_mismatch_by_column(
    source_digests: dict[str, str],
    target_digests: dict[str, str],
) -> LayerReport:
    """L4 — report columns whose typed digests diverge."""
    mismatched = sorted(
        c
        for c in set(source_digests) | set(target_digests)
        if source_digests.get(c) != target_digests.get(c)
    )
    return LayerReport(
        layer="L4",
        passed=not mismatched,
        population_proof=True,
        details={
            "mismatched_columns": mismatched,
            "columns_compared": sorted(set(source_digests) | set(target_digests)),
        },
    )


def _row_fingerprint(
    rec: dict[str, Any],
    columns: list[str],
    *,
    dest_db_type: str,
    dest_types: dict[str, str] | None,
) -> str:
    from services.reconciliation import fingerprint_for_reconcile

    types = dest_types or {}
    parts: list[str] = []
    for col in columns:
        fp = fingerprint_for_reconcile(
            rec.get(col),
            ddl_type=str(types.get(col) or ""),
            engine=dest_db_type,
        )
        parts.append(f"{col}={fp}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _subset_digest(
    by_pk: dict[str, dict[str, Any]],
    pks: list[str],
    columns: list[str],
    *,
    dest_db_type: str,
    dest_types: dict[str, str] | None,
) -> str:
    fps = [
        _row_fingerprint(
            by_pk[pk],
            columns,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
        for pk in pks
        if pk in by_pk
    ]
    fps.sort()
    h = hashlib.sha256()
    for fp in fps:
        h.update(fp.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def binary_search_row_diff(
    *,
    source_by_pk: dict[str, dict[str, Any]],
    target_by_pk: dict[str, dict[str, Any]],
    columns: list[str],
    pk_column: str,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    focus_columns: list[str] | None = None,
    max_leaves: int = 32,
) -> LayerReport:
    """L5 — binary-search ordered PK space to localize mismatched rows."""
    from services.reconciliation import fingerprint_for_reconcile

    compare_cols = list(focus_columns or columns)
    if not compare_cols:
        compare_cols = list(columns)
    all_pks = sorted(set(source_by_pk) | set(target_by_pk), key=lambda x: (len(x), x))
    mismatches: list[dict[str, Any]] = []

    def _emit(pk: str) -> None:
        if len(mismatches) >= max_leaves:
            return
        srec = source_by_pk.get(pk)
        trec = target_by_pk.get(pk)
        if srec is None or trec is None:
            mismatches.append(
                {
                    "pk_column": pk_column,
                    "pk": pk,
                    "reason": "missing_side",
                    "source_present": srec is not None,
                    "target_present": trec is not None,
                }
            )
            return
        for col in compare_cols:
            sfp = fingerprint_for_reconcile(
                srec.get(col),
                ddl_type=str((dest_types or {}).get(col) or ""),
                engine=dest_db_type,
            )
            tfp = fingerprint_for_reconcile(
                trec.get(col),
                ddl_type=str((dest_types or {}).get(col) or ""),
                engine=dest_db_type,
            )
            if sfp != tfp:
                from services.value_serializer import operator_binary_cell_text

                _bin_ddl = str((dest_types or {}).get(col) or "")
                mismatches.append(
                    {
                        "pk_column": pk_column,
                        "pk": pk,
                        "column": col,
                        "source_value": operator_binary_cell_text(
                            _cell_text(srec.get(col)), _bin_ddl
                        ),
                        "target_value": operator_binary_cell_text(
                            _cell_text(trec.get(col)), _bin_ddl
                        ),
                        "source_fingerprint": sfp,
                        "target_fingerprint": tfp,
                    }
                )
                if len(mismatches) >= max_leaves:
                    return

    def _search(lo: int, hi: int) -> None:
        if lo > hi or len(mismatches) >= max_leaves:
            return
        if lo == hi:
            _emit(all_pks[lo])
            return
        mid = (lo + hi) // 2
        left = all_pks[lo : mid + 1]
        right = all_pks[mid + 1 : hi + 1]
        left_s = _subset_digest(
            source_by_pk, left, compare_cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        left_t = _subset_digest(
            target_by_pk, left, compare_cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        if left_s != left_t:
            _search(lo, mid)
        right_s = _subset_digest(
            source_by_pk, right, compare_cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        right_t = _subset_digest(
            target_by_pk, right, compare_cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        if right_s != right_t:
            _search(mid + 1, hi)

    if all_pks:
        _search(0, len(all_pks) - 1)

    return LayerReport(
        layer="L5",
        passed=not mismatches,
        population_proof=len(mismatches) < max_leaves or not mismatches,
        details={
            "pk_column": pk_column,
            "rows_scanned_space": len(all_pks),
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
            "capped": len(mismatches) >= max_leaves,
            "focus_columns": compare_cols,
        },
    )


def _pk_key(value: Any) -> str | None:
    """Address one ladder row. Same wire as extract; NULL is not a key.

    ``str(True)`` / ``str(Decimal('1E+2'))`` invented a second PK spelling, so
    L5 reported missing_side against dest text ``true`` / ``100``. Reader-wired
    ``SQL_NULL_SENTINEL`` looked like a present key.
    """
    from services.value_serializer import is_null_evidence

    if is_null_evidence(value):
        return None
    text = _cell_wire(value) if not isinstance(value, str) else value
    if is_null_evidence(text):
        return None
    return text


def _index_by_pk(
    rows: Iterable[dict[str, Any]],
    pk_column: str | list[str],
) -> dict[str, dict[str, Any]]:
    """Address rows by their key. A composite key names every column (D18).

    Indexing a composite key by its first column alone collapsed rows that
    share it, and the culprit the ladder named dropped the other columns —
    ``id=7`` for what was ``region=eu, id=7``.
    """
    cols = [pk_column] if isinstance(pk_column, str) else [c for c in pk_column if c]
    out: dict[str, dict[str, Any]] = {}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        parts = [_pk_key(rec.get(col)) for col in cols]
        if not parts or any(part is None for part in parts):
            continue
        key = parts[0] if len(cols) == 1 else ", ".join(
            f"{col}={part}" for col, part in zip(cols, parts)
        )
        out[key] = rec
    return out


def target_rows_on_batch_keys(
    target_rows: list[dict[str, Any]],
    batch_rows: list[dict[str, Any]],
    key_cols: list[str],
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> list[dict[str, Any]] | None:
    """The destination rows that carry a key of the compared batch.

    Gate-8 proved an append into an occupied table WHERE key IN (written keys)
    (``checksum_scope = written_batch_keys``, granted only when one key names
    one destination row). The ladder has to grade that same population: rows
    earlier runs wrote are outside this job's proof, and a held-out key was
    never written (D205). Keys compare as the digest compares cells (typed by
    the destination column), so a source ``...Z`` and a destination
    ``...+00:00`` timestamp are one key. ``None`` when the batch has no
    addressable key.
    """
    from services.reconciliation import fingerprint_for_reconcile

    cols = [c for c in key_cols if c]
    types = dest_types or {}

    def _key(row: dict[str, Any]) -> tuple[str, ...] | None:
        parts = []
        for col in cols:
            if _pk_key(row.get(col)) is None:
                return None
            parts.append(fingerprint_for_reconcile(
                row.get(col), ddl_type=str(types.get(col) or ""), engine=dest_db_type
            ))
        return tuple(parts)

    keys = {k for r in batch_rows if isinstance(r, dict) and (k := _key(r)) is not None}
    if not cols or not keys:
        return None
    return [r for r in target_rows if isinstance(r, dict) and _key(r) in keys]


def _row_address(pk_label: str, key_cols: list[str], key: Any) -> str:
    """``id='7'`` for one key column; a composite key already names its columns."""
    if len(key_cols) > 1:
        return f"({key})"
    return f"{pk_label}={key!r}"


def read_sqlite_rows(
    *,
    database: str,
    table: str,
    columns: list[str] | None = None,
    connection_string: str = "",
    host: str = "",
    max_rows: int = MAX_LADDER_ROWS,
) -> list[dict[str, Any]]:
    """Population read of a SQLite table, bounded by the ladder row budget."""
    import sqlite3

    from connectors.sqlite_common import sqlite_file_path
    from connectors.sql_identifiers import quote_table_ref

    path = sqlite_file_path(database, connection_string, host)
    if not path:
        raise ValueError("SQLite path required for verification ladder")
    table_ref = quote_table_ref(table, dialect="sqlite")
    conn = sqlite3.connect(str(path))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(f"SELECT * FROM {table_ref}")  # nosec B608
        rows: list[dict[str, Any]] = []
        while True:
            batch = cur.fetchmany(_fetch_size(max_rows, len(rows)))
            if not batch:
                return rows
            for r in batch:
                rows.append(_row_cells(dict(r), columns))
            if max_rows and len(rows) > max_rows:
                raise PopulationTooLarge(len(rows), max_rows)
    finally:
        conn.close()


def read_postgres_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    table: str,
    columns: list[str] | None = None,
    connection_string: str = "",
    ssl: bool = False,
    max_rows: int = MAX_LADDER_ROWS,
) -> list[dict[str, Any]]:
    """Population read of a PostgreSQL table, bounded by the ladder row budget.

    Uses a server-side cursor: the previous ``fetchall()`` on a client-side
    cursor buffered the whole result set in libpq *and* again as dicts, so a
    10M-row destination cost ~15 GB in the verification step — after the data
    was already written, which is the worst possible place to run out of memory.
    """
    from connectors.postgresql_conn import get_connection
    from psycopg2 import sql

    schema_n = schema or "public"
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    cursor_name = f"df_ladder_{uuid.uuid4().hex}"
    try:
        # Project json/jsonb as engine text — psycopg2 decodes jsonb into
        # Python, after which JSON ``null`` is indistinguishable from SQL
        # NULL (and ``"1"``/``1`` collapse). The transfer reader projects
        # ``col::text`` for the same reason; the independent re-read must
        # fingerprint the same values (D75).
        # Introspection runs on a normal cursor: psycopg2 named cursors
        # accept exactly one execute(), which must be the population SELECT.
        from connectors.postgresql_reader import (
            _json_column_names,
            _ordered_column_names,
        )

        with conn.cursor() as probe:
            json_cols = _json_column_names(probe, schema_n, table)
            names = columns
            if names is None:
                names = _ordered_column_names(probe, schema_n, table)
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = _READ_BATCH
            if json_cols:
                col_sql = sql.SQL(", ").join(
                    sql.SQL(
                        "CASE WHEN {c} IS NULL THEN NULL ELSE {c}::text END AS {c}"
                    ).format(c=sql.Identifier(n))
                    if n in json_cols
                    else sql.Identifier(n)
                    for n in names
                )
            else:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, names))
            q = sql.SQL("SELECT {} FROM {}.{}").format(
                col_sql,
                sql.Identifier(schema_n),
                sql.Identifier(table),
            )
            cur.execute(q)
            rows: list[dict[str, Any]] = []
            names: list[str] = []
            while True:
                batch = cur.fetchmany(_fetch_size(max_rows, len(rows)))
                if not batch:
                    return rows
                if not names:
                    names = [d[0] for d in cur.description] if cur.description else []
                rows.extend(_row_cells(dict(zip(names, rec))) for rec in batch)
                if max_rows and len(rows) > max_rows:
                    raise PopulationTooLarge(len(rows), max_rows)
    finally:
        conn.close()


def read_mysql_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    table: str,
    columns: list[str] | None = None,
    connection_string: str = "",
    ssl: bool = False,
    max_rows: int = MAX_LADDER_ROWS,
) -> list[dict[str, Any]]:
    """Population read of a MySQL table, bounded by the ladder row budget.

    Same contract as :func:`read_postgres_rows` — without it every MySQL
    destination and source silently declined the cell-value ladder and the
    verdict stayed row_count-only green on a tampered table (X-22).
    """
    from connectors.mysql_conn import get_connection
    from connectors.sql_identifiers import (
        quote_column_list,
        quote_table_ref,
        require_safe_identifier,
    )

    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    col_sql = (
        "*"
        if not columns
        else quote_column_list(
            [require_safe_identifier(c, preserve_case=True) for c in columns],
            quote_char="`",
        )
    )
    table_ref = quote_table_ref(table, dialect="mysql")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {col_sql} FROM {table_ref}")  # nosec B608
            rows: list[dict[str, Any]] = []
            names: list[str] = []
            while True:
                batch = cur.fetchmany(_fetch_size(max_rows, len(rows)))
                if not batch:
                    return rows
                if not names:
                    names = [d[0] for d in cur.description] if cur.description else []
                rows.extend(_row_cells(dict(zip(names, rec))) for rec in batch)
                if max_rows and len(rows) > max_rows:
                    raise PopulationTooLarge(len(rows), max_rows)
    finally:
        conn.close()


def read_sqlserver_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    table: str,
    columns: list[str] | None = None,
    max_rows: int = MAX_LADDER_ROWS,
) -> list[dict[str, Any]]:
    """Population read of a SQL Server table, bounded by the ladder row budget.

    Mirrors the MySQL reader: pymssql streams through the same row-budget
    accounting so a tampered norecipe run can no longer stay count-only (X-22).
    """
    import pymssql

    from connectors.sql_identifiers import quote_table_ref, require_safe_identifier

    col_sql = (
        "*"
        if not columns
        else ", ".join(
            f"[{require_safe_identifier(c, preserve_case=True).replace(']', ']]')}]"
            for c in columns
        )
    )
    sch = (schema or "dbo").strip() or "dbo"
    table_ref = quote_table_ref(table, schema=sch, dialect="sqlserver")
    conn = pymssql.connect(
        server=host or "127.0.0.1",
        port=int(port or 1433),
        user=username or "sa",
        password=password or "",
        database=database or "master",
        login_timeout=10,
        timeout=30,
    )
    try:
        cur = conn.cursor()
        from connectors.sqlserver_tick import full_precision_select_list

        # DATETIME2(7): pymssql keeps six digits; read the seventh as text.
        col_sql = full_precision_select_list(cur, table_ref, list(columns) if columns else None)
        cur.execute(f"SELECT {col_sql} FROM {table_ref}")  # nosec B608
        rows: list[dict[str, Any]] = []
        names: list[str] = []
        while True:
            batch = cur.fetchmany(_fetch_size(max_rows, len(rows)))
            if not batch:
                return rows
            if not names:
                names = [d[0] for d in cur.description] if cur.description else []
            rows.extend(_row_cells(dict(zip(names, rec))) for rec in batch)
            if max_rows and len(rows) > max_rows:
                raise PopulationTooLarge(len(rows), max_rows)
    finally:
        conn.close()


def read_snowflake_rows(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    schema: str,
    table: str,
    columns: list[str] | None = None,
    connection_string: str = "",
    warehouse: str = "",
    max_rows: int = MAX_LADDER_ROWS,
) -> list[dict[str, Any]]:
    """Population read of a Snowflake table, bounded by the ladder row budget.

    Without it every Snowflake destination declined the cell-value ladder
    (``destination_engine_not_in_ladder``) and a keyed incremental run could
    never earn the independent re-read verdict (D3). The connector returns
    VARIANT/OBJECT/ARRAY as their JSON text, so no ``TO_VARCHAR`` projection
    is needed — the write-pass fingerprint sees the same shape.
    """
    from connectors.snowflake_conn import (
        get_connection,
        normalize_account,
        resolve_or_fold_snowflake_table,
        snowflake_physical_name_map,
        snowflake_qualified_table,
        snowflake_select_list,
    )
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        require_safe_identifier,
        snowflake_fold_identifier,
    )

    conn = get_connection(
        account=normalize_account(host),
        username=username,
        password=password,
        database=database,
        schema=snowflake_fold_identifier(schema or "PUBLIC"),
        warehouse=warehouse,
        connection_string=connection_string,
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception:
                    pass
            resolved = resolve_or_fold_snowflake_table(
                cur, snowflake_fold_identifier(schema or "PUBLIC"), table
            )
            qualified = snowflake_qualified_table(
                snowflake_fold_identifier(schema or "PUBLIC"), resolved
            )
            col_sql = (
                "*"
                if not columns
                else snowflake_select_list(
                    [
                        require_safe_identifier(c, preserve_case=True)
                        for c in columns
                    ],
                    snowflake_physical_name_map(
                        cur, snowflake_fold_identifier(schema or "PUBLIC"), resolved
                    ),
                )
            )
            cur.execute(f"SELECT {col_sql} FROM {qualified}")  # nosec B608
            rows: list[dict[str, Any]] = []
            names: list[str] = []
            while True:
                batch = cur.fetchmany(_fetch_size(max_rows, len(rows)))
                if not batch:
                    return rows
                if not names:
                    names = (
                        [d[0] for d in cur.description] if cur.description else []
                    )
                rows.extend(_row_cells(dict(zip(names, rec))) for rec in batch)
                if max_rows and len(rows) > max_rows:
                    raise PopulationTooLarge(len(rows), max_rows)
    finally:
        conn.close()


def run_five_layer_verification(
    *,
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    columns: list[str],
    pk_column: str,
    pk_columns: list[str] | None = None,
    source_row_count: int | None = None,
    target_row_count: int | None = None,
    rejected_rows: int = 0,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
    source_checksum: str = "",
    target_checksum: str = "",
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    allow_extra_rows: bool = False,
    always_localize: bool = False,
    checksum_scope: str = "",
    target_rows_before: int | None = None,
    keyed_cardinality: bool = False,
    keyed_expected_delta: int | None = None,
    l2_skip_reason: str = "",
) -> dict[str, Any]:
    """Execute L1–L5 on two in-memory populations (SQL-loaded or buffered).

    L4/L5 run when L3 fails, or when ``always_localize`` is True (maximum mode).
    """
    cols = [c for c in columns if c]
    pk = (pk_column or "").strip()
    key_cols = [str(c).strip() for c in (pk_columns or []) if str(c or "").strip()]
    if not key_cols and pk:
        key_cols = [pk]
    pk_label = pk if len(key_cols) <= 1 else ", ".join(key_cols)
    src_n = int(source_row_count if source_row_count is not None else len(source_rows))
    tgt_n = int(target_row_count if target_row_count is not None else len(target_rows))

    if max(len(source_rows), len(target_rows), src_n, tgt_n) > MAX_LADDER_ROWS:
        return {
            "layers": {},
            "passed": False,
            "assurance_level": "none",
            "population_proof": False,
            "population_checksum_proof": False,
            "skipped": True,
            "reason": (
                f"Population exceeds VERIFICATION_LADDER_MAX_ROWS={MAX_LADDER_ROWS}; "
                "refuse in-memory L2/L4/L5. Gate-8 L1/L3 checksum still applies."
            ),
            "localization": {},
            "localization_summary": "",
            "screening_note": (
                f"Sample probes (limit {DEFAULT_SCREENING_LIMIT}) are screening only — "
                "never population proof."
            ),
        }

    l1 = layer_l1_row_balance(
        source_rows=src_n,
        target_rows=tgt_n,
        rejected_rows=rejected_rows,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        allow_extra_rows=allow_extra_rows,
        target_rows_before=target_rows_before,
        keyed_cardinality=keyed_cardinality,
        keyed_expected_delta=keyed_expected_delta,
    )

    src_agg = compute_column_aggregates(source_rows, cols)
    tgt_agg = compute_column_aggregates(target_rows, cols)
    l2 = compare_column_aggregates(src_agg, tgt_agg)

    from services.reconciliation import aggregate_checksum

    # Prefer caller digests when present (independent Gate-8 path); else compute.
    src_chk = (source_checksum or "").strip() or aggregate_checksum(
        source_rows, cols, dest_db_type=dest_db_type, dest_types=dest_types
    )
    tgt_chk = (target_checksum or "").strip() or aggregate_checksum(
        target_rows, cols, dest_db_type=dest_db_type, dest_types=dest_types
    )
    from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE

    incomparable = str(checksum_scope or "") == WHOLE_TABLE_NOT_COMPARABLE
    empty_population = src_n == 0 and tgt_n == 0 and not incomparable
    # sha256('') — a caller checksum of an empty write. Matching digests are
    # not an L3 pass when the job compared no rows (R4-D05 / R2-062).
    _empty_input_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    empty_digest = src_chk == _empty_input_sha256 and tgt_chk == _empty_input_sha256
    if incomparable:
        # Whole-table hashes cover rows this job never wrote. Gate-8 already
        # judged the dest-before delta; treating the digest gap as L3 failure
        # marks a healthy append as a failed transfer.
        l3 = LayerReport(
            layer="L3",
            passed=True,
            population_proof=False,
            details={
                "skipped": True,
                "reason": WHOLE_TABLE_NOT_COMPARABLE,
                "source_checksum": src_chk,
                "target_checksum": tgt_chk,
                "algorithm": "order_independent_sha256_row_fingerprints",
            },
        )
    elif empty_population:
        # sha256('') matches on both sides of a 0-row run. That is not proof
        # the population was compared (QA R2-062). L1 still owns the count.
        l3 = LayerReport(
            layer="L3",
            passed=False,
            population_proof=False,
            details={
                "skipped": True,
                "reason": "empty_population",
                "source_checksum": src_chk,
                "target_checksum": tgt_chk,
                "algorithm": "order_independent_sha256_row_fingerprints",
                "note": "sha256 of an empty population is not a checksum pass",
            },
        )
    elif empty_digest:
        l3 = LayerReport(
            layer="L3",
            passed=False,
            population_proof=False,
            details={
                "skipped": True,
                "reason": "empty_digest",
                "source_checksum": src_chk,
                "target_checksum": tgt_chk,
                "algorithm": "order_independent_sha256_row_fingerprints",
                "note": "sha256 of an empty input is not a checksum pass",
            },
        )
    else:
        l3 = LayerReport(
            layer="L3",
            passed=bool(src_chk and tgt_chk and src_chk == tgt_chk),
            population_proof=True,
            details={
                "source_checksum": src_chk,
                "target_checksum": tgt_chk,
                "algorithm": "order_independent_sha256_row_fingerprints",
            },
        )

    # L2 graded a different population than the digest (quarantine, history,
    # coercion, or a pre-transform re-read). When the written-batch checksums
    # match, that profile is not a cell failure and must not roll the load back.
    checksums_agree = bool(src_chk and tgt_chk and src_chk == tgt_chk) and not empty_digest
    if checksums_agree:
        reason = (l2_skip_reason or "").strip() or _l2_population_skip_reason(
            source_rows=source_rows,
            target_rows=target_rows,
            source_row_count=src_n,
            target_row_count=target_row_count,
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
        )
        if reason:
            l2 = LayerReport(
                layer="L2",
                passed=True,
                population_proof=False,
                details={
                    "skipped": True,
                    "reason": reason,
                    "note": (
                        "Written-batch checksums match. This L2 load is not "
                        "that batch, so it is not a fidelity failure."
                    ),
                },
            )

    layers = {
        "L1": l1.to_dict(),
        "L2": l2.to_dict(),
        "L3": l3.to_dict(),
    }
    localization: dict[str, Any] = {}

    run_localize = ((not l3.passed) or always_localize) and not incomparable
    if run_localize and key_cols and cols:
        src_col = column_typed_checksums(
            source_rows, cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        tgt_col = column_typed_checksums(
            target_rows, cols, dest_db_type=dest_db_type, dest_types=dest_types
        )
        l4 = localize_checksum_mismatch_by_column(src_col, tgt_col)
        layers["L4"] = l4.to_dict()
        focus = list(l4.details.get("mismatched_columns") or cols)
        l5 = binary_search_row_diff(
            source_by_pk=_index_by_pk(source_rows, key_cols),
            target_by_pk=_index_by_pk(target_rows, key_cols),
            columns=cols,
            pk_column=pk_label,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
            focus_columns=focus or cols,
        )
        layers["L5"] = l5.to_dict()
        localization = {
            # Only columns L4 actually found divergent. Falling back to every
            # column here made the summary claim all 35 columns mismatched on
            # a run where L4 found none (QA round 12).
            "columns": list(l4.details.get("mismatched_columns") or []),
            "rows": (l5.details.get("mismatches") or [])[:16],
        }
    elif run_localize:
        layers["L4"] = LayerReport(
            layer="L4",
            passed=False,
            population_proof=False,
            details={"skipped": True, "reason": "pk_column_required"},
        ).to_dict()
        layers["L5"] = LayerReport(
            layer="L5",
            passed=False,
            population_proof=False,
            details={"skipped": True, "reason": "pk_column_required"},
        ).to_dict()

    # Required for green: L1 + L3. Keyed L1 without a dest-engine census is
    # unmeasured (passed, skipped) — it must not veto L3, and it must not
    # mint population_checksum_proof.
    l1_skipped = bool((l1.details or {}).get("skipped"))
    # L2 is a real miss when aggregates disagree. An empty population skips L3
    # rather than failing the checksum, so a legitimate 0-row incremental run
    # still completes when L1 balances (QA R2-066 / RT-02).
    l2_failed = (not l2.passed) and not bool((l2.details or {}).get("skipped"))
    if empty_population:
        passed = bool(l1.passed and not l2_failed)
    elif incomparable:
        # Whole-table aggregates cover rows this job did not write.
        passed = bool(l1.passed and l3.passed)
    else:
        passed = bool(l1.passed and l3.passed and not l2_failed)
    expected = int((l1.details or {}).get("expected_rows") or 0)
    extra_dest = bool(allow_extra_rows and tgt_n > expected)
    l3_skipped = bool((l3.details or {}).get("skipped"))
    if l1_skipped:
        assurance = "full_checksum" if l3.passed else "failed"
    elif (incomparable or empty_population) and passed:
        assurance = "row_count"
    else:
        assurance = "five_layer" if passed and "L4" in layers and layers["L4"].get("passed") else (
            "full_checksum" if passed else "failed"
        )
    summary = ""
    if localization.get("rows"):
        first = localization["rows"][0]
        summary = (
            f"L4/L5 localized: row {_row_address(pk_label, key_cols, first.get('pk'))} "
            f"column {first.get('column')!r} "
            f"source={first.get('source_value')!r} "
            f"target={first.get('target_value')!r}"
        )
    elif localization.get("columns"):
        summary = f"L4 localized mismatched columns: {', '.join(localization['columns'])}"

    return {
        "layers": layers,
        "passed": passed,
        "assurance_level": assurance,
        # Population RI (FK/constraints) is still not claimed — only typed fidelity.
        "population_proof": False,
        "population_checksum_proof": bool(
            l3.passed
            and l1.passed
            and not incomparable
            and not extra_dest
            and not l3_skipped
            and not l1_skipped
        ),
        "localization": localization,
        "localization_summary": summary,
        "screening_note": (
            f"Sample probes (limit {DEFAULT_SCREENING_LIMIT}) are screening only — "
            "never population proof."
        ),
    }


def attach_ladder_to_reconcile_report(
    report: dict[str, Any],
    ladder: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge ladder into a Gate-8 report without inventing population RI proof."""
    out = dict(report or {})
    if not ladder:
        return out
    out["verification_ladder"] = ladder
    # Evidence changed — recompute the fidelity claim from the single procedure.
    from services.signed_proof_pack import apply_fidelity_veto

    out = apply_fidelity_veto(out)
    if out.get("phase") == "post_write_failed":
        return out
    if ladder.get("localization_summary") and not out.get("passed"):
        base = str(out.get("message") or "").rstrip()
        loc = str(ladder["localization_summary"])
        if loc and loc not in base:
            # Failure-first (X-44): the localized culprit is the cause and
            # must lead; a "Row count verified …" fact is context after it.
            out["message"] = f"{loc} — {base}" if base else loc
    # Never let sample screening language override ladder localization.
    if out.get("assurance_level") == "sample" and ladder.get("population_checksum_proof"):
        out["assurance_level"] = ladder.get("assurance_level") or out["assurance_level"]
    if ladder.get("assurance_level") == "five_layer" and out.get("passed"):
        out["assurance_level"] = "five_layer"
    return out
