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
from decimal import Decimal, InvalidOperation
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
    claim population proof.
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
        return LayerReport(
            layer="L1",
            passed=delta == int(keyed_expected_delta),
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


def _try_decimal(text: str | None) -> Decimal | None:
    if text is None:
        return None
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(text)


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
            if mins[col] is None or text < mins[col]:
                mins[col] = text
            if maxs[col] is None or text > maxs[col]:
                maxs[col] = text
            if sum_ok[col]:
                d = _try_decimal(text)
                if d is None:
                    sum_ok[col] = False
                    sums[col] = None
                else:
                    sums[col] = (sums[col] or Decimal(0)) + d

    out: dict[str, ColumnAggregate] = {}
    for col in columns:
        out[col] = ColumnAggregate(
            column=col,
            null_count=nulls[col],
            non_null_count=non_null[col],
            distinct_count=len(distinct[col]),
            min_value=mins[col],
            max_value=maxs[col],
            sum_value=None if not sum_ok[col] or sums[col] is None else format(sums[col], "f"),
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
        diffs: dict[str, Any] = {}
        for field_name in ("null_count", "non_null_count", "min_value", "max_value", "sum_value"):
            if getattr(s, field_name) != getattr(t, field_name):
                diffs[field_name] = {
                    "source": getattr(s, field_name),
                    "target": getattr(t, field_name),
                }
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
                mismatches.append(
                    {
                        "pk_column": pk_column,
                        "pk": pk,
                        "column": col,
                        "source_value": _cell_text(srec.get(col)),
                        "target_value": _cell_text(trec.get(col)),
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
    pk_column: str,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        key = _pk_key(rec.get(pk_column))
        if key is None:
            continue
        out[key] = rec
    return out


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
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = _READ_BATCH
            if columns:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
                q = sql.SQL("SELECT {} FROM {}.{}").format(
                    col_sql,
                    sql.Identifier(schema_n),
                    sql.Identifier(table),
                )
            else:
                q = sql.SQL("SELECT * FROM {}.{}").format(
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


def run_five_layer_verification(
    *,
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    columns: list[str],
    pk_column: str,
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
) -> dict[str, Any]:
    """Execute L1–L5 on two in-memory populations (SQL-loaded or buffered).

    L4/L5 run when L3 fails, or when ``always_localize`` is True (maximum mode).
    """
    cols = [c for c in columns if c]
    pk = (pk_column or "").strip()
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

    layers = {
        "L1": l1.to_dict(),
        "L2": l2.to_dict(),
        "L3": l3.to_dict(),
    }
    localization: dict[str, Any] = {}

    run_localize = ((not l3.passed) or always_localize) and not incomparable
    if run_localize and pk and cols:
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
            source_by_pk=_index_by_pk(source_rows, pk),
            target_by_pk=_index_by_pk(target_rows, pk),
            columns=cols,
            pk_column=pk,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
            focus_columns=focus or cols,
        )
        layers["L5"] = l5.to_dict()
        localization = {
            "columns": focus,
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
    passed = bool(l1.passed and l3.passed)
    expected = int((l1.details or {}).get("expected_rows") or 0)
    extra_dest = bool(allow_extra_rows and tgt_n > expected)
    l3_skipped = bool((l3.details or {}).get("skipped"))
    if l1_skipped:
        assurance = "full_checksum" if l3.passed else "failed"
    elif incomparable and passed:
        assurance = "row_count"
    else:
        assurance = "five_layer" if passed and "L4" in layers and layers["L4"].get("passed") else (
            "full_checksum" if passed else "failed"
        )
    summary = ""
    if localization.get("rows"):
        first = localization["rows"][0]
        summary = (
            f"L4/L5 localized: row {pk}={first.get('pk')!r} "
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
            out["message"] = f"{base} — {loc}"
    # Never let sample screening language override ladder localization.
    if out.get("assurance_level") == "sample" and ladder.get("population_checksum_proof"):
        out["assurance_level"] = ladder.get("assurance_level") or out["assurance_level"]
    if ladder.get("assurance_level") == "five_layer" and out.get("passed"):
        out["assurance_level"] = "five_layer"
    return out
