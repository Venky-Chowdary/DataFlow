"""Shared row mapping utilities for database writers."""

from __future__ import annotations

import json
import os
from services.brand_env import getenv_brand
import re
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from services.reconciliation import _iter_fingerprints, checksum_rows
from services.transform_engine import apply_transform
from services.transform_resolver import resolve_transform
from services.value_serializer import SQL_NULL_SENTINEL

from connectors.sql_identifiers import (  # noqa: F401 — re-export canonical helpers
    quote_column_list,
    quote_sql_identifier,
    quote_table_ref,
    require_safe_identifier,
    sanitize_identifier,
)

# Configurable batch size — default 20 000 rows per commit (enterprise scale)
CHUNK_SIZE = int(getenv_brand("CHUNK_SIZE", "20000"))
TRANSFORM_ERROR_POLICY = getenv_brand("TRANSFORM_ERROR_POLICY", "quarantine").lower()
VALID_ERROR_POLICIES = {"fail", "quarantine", "coerce_null"}

# Active Map mappings while write-quarantine matrix runs — enables dual-stamp
# of source_values without threading mappings through every holdout helper.
_active_quarantine_mappings: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "df_quarantine_mappings", default=None
)
# Staging / explicit contract-escape hatch for job-level coerce_null.
# Primary-table writes must never invent coerce_null from env alone.
_allow_job_coerce_null: ContextVar[bool] = ContextVar(
    "df_allow_job_coerce_null", default=False
)


def allow_job_coerce_null_writes(enabled: bool = True):
    """Context manager: permit job-level coerce_null (staging diagnose only)."""
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        token = _allow_job_coerce_null.set(bool(enabled))
        try:
            yield
        finally:
            _allow_job_coerce_null.reset(token)

    return _cm()


def mapped_row_quarantine_values(row: Any, target_cols: list[str]) -> dict[str, str]:
    """Target-column dict for quarantine replay (full row, not single bad cell)."""
    from services.value_serializer import cell_to_string, is_missing_sentinel

    out: dict[str, str] = {}
    seq = list(row) if row is not None else []
    for i, col in enumerate(target_cols or []):
        if i >= len(seq):
            out[str(col)] = ""
            continue
        v = seq[i]
        if v is None or is_missing_sentinel(v):
            out[str(col)] = ""
        else:
            out[str(col)] = cell_to_string(v)
    return out


def project_quarantine_source_values(
    target_values: dict[str, Any],
    mappings: list[dict[str, Any]] | None,
) -> dict[str, str]:
    """Project target-shaped quarantine values onto source column names.

    Airbyte/Fivetran-class DLQ keeps the original record payload for replay.
    Write-matrix holdouts only see destination cells — reverse Map so operators
    edit source-shaped fields and rewrite does not invent NULLs.
    """
    if not target_values or not mappings:
        return {}
    out: dict[str, str] = {}
    for m in mappings:
        src = str(m.get("source") or m.get("source_column") or "").strip()
        tgt = str(m.get("target") or m.get("target_column") or "").strip() or src
        if not src:
            continue
        if tgt in target_values:
            v = target_values[tgt]
            out[src] = "" if v is None else str(v)
        elif src in target_values:
            v = target_values[src]
            out[src] = "" if v is None else str(v)
    return out


def append_write_quarantine_detail(
    rejected_details: list[dict[str, Any]],
    detail: dict[str, Any],
    *,
    mapped_row: Any,
    target_cols: list[str],
    mappings: list[dict[str, Any]] | None = None,
) -> None:
    """Append a quarantine detail, dual-stamping target + source ``values``.

    ``values`` stays destination-shaped (write bind image). ``source_values`` is
    the Map-projected source payload when mappings are known — preferred by
    quarantine replay (Wave 32). Never invent source_values from target keys
    without a Map (that would poison canonicalize).

    Module 9: stamp first-class quarantine contract fields before append.
    """
    d = dict(detail)
    if not (isinstance(d.get("values"), dict) and d["values"]):
        d["values"] = mapped_row_quarantine_values(mapped_row, target_cols)
    if not (isinstance(d.get("source_values"), dict) and d["source_values"]):
        maps = mappings if mappings is not None else _active_quarantine_mappings.get()
        if maps:
            src = project_quarantine_source_values(d["values"], maps)
            if src:
                d["source_values"] = src
    try:
        from services.quarantine_row_contract import normalize_quarantine_row

        d = normalize_quarantine_row(
            d,
            job_id=str(d.get("job_id") or ""),
            connector=str(d.get("connector") or ""),
        )
    except Exception:
        pass
    rejected_details.append(d)

def resolve_writer_backfill(
    *,
    backfill_new_fields: bool = False,
    mappings: list | None = None,
    schema_policy: str | None = None,
) -> bool:
    """Defense-in-depth: every SQL writer re-resolves ADD COLUMN intent.

    Callers (engine / adapter) should already pass the effective flag, but writers
    must not trust a stale ``False`` when mappings include ``create_compatible_new``
    — that is the Snowflake ``invalid identifier '"id_text"'`` failure class across
    every typed destination (Postgres, MySQL, BigQuery, SQL Server, SQLite, …).
    """
    from services.batch_progress import effective_backfill_new_fields

    return effective_backfill_new_fields(
        backfill_new_fields=backfill_new_fields,
        schema_policy=schema_policy,
        mappings=mappings,
    )


def desired_types_honoring_map_stamps(
    *,
    target_cols: list[str],
    current_target_types: list[str],
    mappings: list[dict[str, Any]] | None,
    candidate_by_col: dict[str, str] | None = None,
    preserve_case: bool = False,
    explicit_columns: set[str] | frozenset[str] | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Map≡ALTER: never widen past an explicit Map ``target_type`` stamp.

    ``current_target_types`` are the Map-resolved CREATE types (already
    honor_explicit). ``candidate_by_col`` may propose a wider source/batch DDL.
    Explicit stamps are a hard ceiling — overflow cells quarantine on write;
    silent ALTER past the approved mapping is refuse-closed.

    When ``explicit_columns`` is provided, that set is the authoritative
    Map-stamp membership (needed when writers temporarily stamp
    ``target_type`` onto non-explicit widen proposals). When omitted, a column
    is explicit iff its mapping carries ``target_type``.

    Returns ``(desired_types, refusals)`` where each refusal is audit evidence:
    ``{column, mapped_type, refused_wider, reason}``.
    """
    from connectors.schema_drift import is_wider_type
    from services.mapping_constraints import write_mappings

    candidates = candidate_by_col or {}
    by_tgt: dict[str, dict[str, Any]] = {}
    for mapping in write_mappings(list(mappings or [])):
        tgt = sanitize_identifier(
            str(mapping.get("target") or ""), preserve_case=preserve_case
        )
        if tgt and tgt not in by_tgt:
            by_tgt[tgt] = mapping

    desired: list[str] = []
    refusals: list[dict[str, Any]] = []
    for col, cur_type in zip(target_cols, current_target_types):
        mapping = by_tgt.get(col) or {}
        candidate = str(candidates.get(col) or cur_type or "").strip() or str(cur_type)
        ceiling = str(cur_type or "").strip() or candidate
        if explicit_columns is not None:
            is_explicit = col in explicit_columns
        else:
            is_explicit = bool(str(mapping.get("target_type") or "").strip())
        if is_explicit:
            if candidate and is_wider_type(ceiling, candidate):
                refusals.append(
                    {
                        "column": col,
                        "mapped_type": ceiling,
                        "refused_wider": candidate,
                        "reason": "explicit_map_stamp_ceiling",
                    }
                )
            desired.append(ceiling)
            continue
        if candidate and is_wider_type(ceiling, candidate):
            desired.append(candidate)
        else:
            desired.append(ceiling)
    return desired, refusals


def to_json_value(value: Any, col: str, dest_types: dict[str, str]) -> Any:
    """Convert a mapped cell to a JSON-serializable scalar.

    Preserves strings/dates as text; parses structural and numeric JSON values
    into native Python types so object-store exports contain numbers/objects
    instead of quoted Decimal strings. Temporal logical types are normalized via
    the shared SQL temporal helpers so ISO-Z does not leak inconsistently across
    S3/GCS/ADLS/SFTP/Kafka JSON exports.

    ``DF_MISSING`` (STOP_COLUMN / sparse CDC omit) never serializes as the
    sentinel string — treat as JSON null so dense document writers cannot
    corrupt payloads with ``__DF_MISSING__``.
    """
    try:
        from services.value_serializer import is_missing_sentinel

        if is_missing_sentinel(value):
            return None
    except Exception:
        pass
    if value is None:
        return None
    try:
        from services.type_system import normalize_logical_type
    except Exception:
        normalize_logical_type = lambda x: str(x or "").lower()  # type: ignore[assignment]
    ctype = normalize_logical_type(dest_types.get(col, "")) if dest_types else ""
    if ctype in {"date", "datetime", "time"}:
        from connectors.sql_temporal import (
            coerce_sql_temporal,
            format_wire_value,
            logical_to_temporal_ddl,
        )

        ddl = logical_to_temporal_ddl(ctype) or "DATETIME"
        coerced = coerce_sql_temporal(value, ddl)
        wire = format_wire_value(value, ddl)
        if wire is not None:
            return wire
        if coerced is not value:
            return coerced
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return value
        if ctype in {"json", "array", "object", "struct"}:
            try:
                def _reject(name: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {name}")

                return json.loads(text, parse_constant=_reject)
            except ValueError:
                return value  # leave raw; transform path should have rejected
            except json.JSONDecodeError:
                return value
        if ctype in {"text", "string", "varchar", "uuid", "binary"}:
            return value
        try:
            def _reject2(name: str) -> None:
                raise ValueError(f"non-finite JSON constant: {name}")

            return json.loads(text, parse_constant=_reject2)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def normalize_temporal_cells(
    mapped_rows: list[tuple],
    target_types: list[str] | dict[str, str],
    target_cols: list[str] | None = None,
    *,
    engine: str = "",
) -> list[tuple]:
    """Normalize temporal cells in mapped tuples for any destination engine.

    Dispatches Snowflake/BigQuery to warehouse formatters; all other engines use
    ``coerce_sql_temporal`` so MySQL/PG/Oracle/SQLite/Mongo share one parse path.
    Non-temporal columns are left untouched. Empty input is a no-op.
    """
    if not mapped_rows:
        return mapped_rows

    eng = (engine or "").strip().lower()
    if isinstance(target_types, dict):
        cols = target_cols or list(target_types.keys())
        types_list = [target_types.get(c, "string") for c in cols]
    else:
        types_list = list(target_types)
        cols = target_cols or []

    if eng == "snowflake":
        from connectors.warehouse_temporal import coerce_mapped_rows_snowflake

        return coerce_mapped_rows_snowflake(mapped_rows, types_list)
    if eng == "bigquery":
        from connectors.warehouse_temporal import (
            bigquery_temporal_ddl,
            format_bigquery_bind,
        )

        out: list[tuple] = []
        for row in mapped_rows:
            cells = list(row)
            for i, typ in enumerate(types_list):
                if i >= len(cells) or cells[i] is None:
                    continue
                if bigquery_temporal_ddl(typ):
                    cells[i] = format_bigquery_bind(cells[i], typ)
            out.append(tuple(cells))
        return out

    from connectors.sql_temporal import (
        coerce_sql_temporal,
        is_temporal_ddl,
        logical_to_temporal_ddl,
    )

    if not any(is_temporal_ddl(t) or logical_to_temporal_ddl(t) for t in types_list):
        return mapped_rows

    out = []
    for row in mapped_rows:
        cells = list(row)
        for i, typ in enumerate(types_list):
            if i >= len(cells) or cells[i] is None:
                continue
            ddl = logical_to_temporal_ddl(typ) or (typ if is_temporal_ddl(typ) else None)
            if not ddl:
                continue
            cells[i] = coerce_sql_temporal(cells[i], ddl)
        out.append(tuple(cells))
    return out


@dataclass
class WriteResult:
    """Canonical result object returned by all destination writers."""

    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = ""
    rejected_rows: int = 0
    rejected_details: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    load_method: str | None = None
    # Distinct source rows kept but with >=1 cell forced to NULL because a
    # coercion failed (quarantine/coerce_null). This is a data-ALTERATION count,
    # separate from dropped rows, so reconciliation cannot claim 100% fidelity
    # when values were silently changed. Genuine empty->NULL sentinels are NOT
    # counted here (they produce no transform error).
    coerced_null_rows: int = 0
    # Rows intentionally not written because they are stale/duplicate under CDC
    # LSN guards. They are not data loss and must be excluded from rows_written.
    rows_skipped: int = 0
    # Writer-stashed Gate-8 aids (reconcile_sample, written_ids) — never secrets.
    meta: dict[str, Any] = field(default_factory=dict)


def row_checksum(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    return checksum_rows(
        rows, columns, dest_db_type=dest_db_type, dest_types=dest_types
    )


def filter_stale_lsn_rows(
    cursor: Any,
    table_name: str,
    schema: str | None,
    conflict_cols: list[str],
    rows: list[tuple],
    target_cols: list[str],
    *,
    quote: str = '"',
    placeholder: str = "%s",
) -> tuple[list[tuple], int]:
    """Return (rows_to_write, rows_skipped) for an LSN-guarded batch.

    Queries the destination for the existing ``_df_lsn`` value for each conflict
    key in the batch, then drops rows whose incoming LSN is not strictly newer.
    Fail closed: if the lookup cannot run, raise rather than assume no prior LSN.
    """
    if not rows or not conflict_cols or DF_LSN_COL not in target_cols:
        return rows, 0

    conflict_idxs = [target_cols.index(c) for c in conflict_cols]
    lsn_idx = target_cols.index(DF_LSN_COL)

    # Build OR clauses for non-null conflict keys.
    params: list[Any] = []
    clauses: list[str] = []
    q = quote_sql_identifier
    if schema:
        qualified = f"{q(schema, quote)}.{q(table_name, quote)}"
    else:
        qualified = f"{q(table_name, quote)}"
    for row in rows:
        if any(row[idx] in (None, "") for idx in conflict_idxs):
            continue
        parts = []
        for idx in conflict_idxs:
            val = row[idx]
            col = conflict_cols[conflict_idxs.index(idx)]
            if val is None:
                parts.append(f"{q(col, quote)} IS NULL")
            else:
                parts.append(f"{q(col, quote)} = {placeholder}")
                params.append(val)
        if parts:
            clauses.append("(" + " AND ".join(parts) + ")")
    if not clauses:
        return rows, 0

    existing: dict[tuple[Any, ...], Any] = {}
    select_cols = ", ".join(q(c, quote) for c in conflict_cols) + f", {q(DF_LSN_COL, quote)}"
    stmt = f"SELECT {select_cols} FROM {qualified} WHERE " + " OR ".join(clauses)  # nosec B608
    cursor.execute(stmt, params)
    for found in cursor.fetchall():
        key = tuple(found[i] for i in range(len(conflict_cols)))
        existing[key] = found[-1]

    to_write: list[tuple] = []
    skipped = 0
    for row in rows:
        key = tuple(row[idx] for idx in conflict_idxs)
        incoming = row[lsn_idx]
        prior = existing.get(key)
        if incoming is not None and compare_lsn(incoming, prior) <= 0:
            skipped += 1
            continue
        to_write.append(row)
    return to_write, skipped


def row_fingerprints(rows: list[Any], columns: list[str] | None = None, *, sort_key: str | None = None) -> list[tuple[str, str]]:
    """Return the unsorted (row_key, fingerprint) tuples for a list of rows.

    Streaming producers can accumulate these tuples across batches and then call
    ``services.reconciliation.fingerprint_checksum`` once at the end, avoiding a
    full materialization of every row as a dict/list.
    """
    return list(_iter_fingerprints(rows, columns, sort_key=sort_key))


def dedupe_rows(
    rows: list[tuple],
    conflict_columns: list[str],
    target_cols: list[str],
) -> list[tuple]:
    """Keep the last occurrence of each conflict key, preserving tuple order."""
    if not conflict_columns or not rows:
        return rows
    indices = [target_cols.index(c) for c in conflict_columns if c in target_cols]
    if not indices:
        return rows
    seen: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[i] for i in indices)
        seen[key] = row
    return list(seen.values())


# Destination metadata column for CDC monotonic apply (PK + LSN guard).
DF_LSN_COL = "_df_lsn"


def null_safe_merge_on(
    columns: list[str],
    *,
    left_alias: str,
    right_alias: str,
    quote_column: Any = None,
) -> str:
    """Airbyte-class NULL-safe MERGE ON predicate.

    ``NULL = NULL`` is UNKNOWN in SQL three-valued logic, so equality-only ON
    clauses re-INSERT rows whose composite PK components are NULL (unbounded
    duplicates). Prefer the OR form over ``IS NOT DISTINCT FROM`` on BigQuery
    (hash-join friendly). Same pattern as destination-bigquery / Snowflake and
    our Redshift/MSSQL MERGE paths.
    """
    q = quote_column if callable(quote_column) else (lambda c: str(c))
    parts: list[str] = []
    for c in columns:
        left = f"{left_alias}.{q(c)}"
        right = f"{right_alias}.{q(c)}"
        parts.append(
            f"(({left} = {right}) OR ({left} IS NULL AND {right} IS NULL))"
        )
    return " AND ".join(parts)


def gate8_writer_meta(
    mapped_rows: list[Any],
    target_cols: list[str],
    written_ids: list[str] | None = None,
    *,
    sample_limit: int = 50,
    id_limit: int = 500,
) -> dict[str, Any]:
    """Stamp Gate-8 reconcile_sample / written_ids onto WriteResult.meta.

    Reverse-ETL writers (HubSpot, Salesforce, Stripe, …) and warehouse writers
    that cannot prove full-table cardinality still need independent sample
    compare — this is the single shape ``reconcile_step`` consumes.
    """
    sample: list[dict[str, Any]] = []
    for row in mapped_rows[: max(0, int(sample_limit))]:
        if isinstance(row, dict):
            sample.append(dict(row))
        else:
            sample.append(
                {
                    c: (row[i] if i < len(row) else None)
                    for i, c in enumerate(target_cols)
                }
            )
    meta: dict[str, Any] = {
        "reconcile_sample": sample,
        "source_row_count": len(mapped_rows),
    }
    if written_ids is not None:
        meta["written_ids"] = [str(x) for x in written_ids[: max(0, int(id_limit))]]
    return meta


def vector_gate8_meta(
    records: list[dict[str, Any]],
    *,
    id_key: str = "id",
) -> dict[str, Any]:
    """Gate-8 meta for vector destinations (metadata/payload; embeddings opaque).

    Airbyte-class vector destinations prove identity via record id + metadata,
    not opaque float arrays — match that honesty bar.
    """
    ids: list[str] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        vid = rec.get(id_key)
        if vid is not None and str(vid).strip() != "":
            ids.append(str(vid))
    cols = sorted({k for r in records if isinstance(r, dict) for k in r}) or [id_key]
    return gate8_writer_meta(records, cols, ids)


def lsn_family(lsn: Any) -> str:
    """Return CDC stamp family for LSN-guard compares (Debezium-class).

    Families: ``empty``, ``pg_wal``, ``mysql_binlog``, ``mysql_gtid``,
    ``oracle_scn``, ``mongo_resume``, ``mssql_lsn``, ``numeric_version``,
    ``opaque``. Cross-family compares are incomparable — never invent
    ``newer`` across dialects (silent regression). Oracle SCN must not share
    ``numeric_version`` with SQL Server CT (bare integers collide).
    """
    if lsn is None:
        return "empty"
    text = str(lsn).strip()
    if not text:
        return "empty"
    lower = text.lower()
    if lower.startswith("gtid:"):
        return "mysql_gtid"
    if lower.startswith("scn:"):
        return "oracle_scn"
    if lower.startswith("mongo:"):
        return "mongo_resume"
    # Postgres WAL LSN: hex/hex
    if "/" in text:
        hi, _, lo = text.partition("/")
        if hi and lo and all(c in "0123456789abcdefABCDEF" for c in hi + lo):
            return "pg_wal"
    # MySQL binlog file:pos
    if ":" in text:
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return "mysql_binlog"
    # SQL Server binary LSN hex (0x… or long hex)
    if lower.startswith("0x") and all(c in "0123456789abcdef" for c in lower[2:]):
        return "mssql_lsn"
    if len(text) >= 10 and all(c in "0123456789abcdefABCDEF" for c in text) and not text.isdigit():
        return "mssql_lsn"
    if text.isdigit():
        return "numeric_version"
    return "opaque"


def lsn_sort_key(lsn: Any) -> tuple:
    """Return a sortable key for PG ``hi/lo``, MySQL ``file:pos``, versions, or opaque tokens.

    Kind order is only valid **within** the same ``lsn_family``. Use
    ``compare_lsn`` for guards — it refuses cross-family invent.
    """
    if lsn is None:
        return (0, -1, -1, "")
    text = str(lsn).strip()
    if not text:
        return (0, -1, -1, "")
    lower = text.lower()
    if lower.startswith("scn:"):
        body = text.split(":", 1)[1].strip()
        try:
            return (1, int(body), 0, "")
        except (TypeError, ValueError):
            return (0, 0, 0, body)
    if lower.startswith("mongo:"):
        return (0, 0, 0, text.split(":", 1)[1])
    # Postgres WAL LSN: hex/hex (reject paths that look like URLs).
    if "/" in text and not lower.startswith("gtid:"):
        hi, _, lo = text.partition("/")
        if hi and lo and all(c in "0123456789abcdefABCDEF" for c in hi + lo):
            try:
                return (3, int(hi, 16), int(lo, 16), "")
            except ValueError:
                pass
    # MySQL binlog file:pos (pos may already be zero-padded from extract_cdc_lsn).
    if ":" in text and not lower.startswith("gtid:"):
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return (2, file_name, int(pos), "")
    # Zero-padded / numeric versions (SQL Server CT, etc.).
    if text.isdigit():
        return (1, int(text), 0, "")
    return (0, 0, 0, text)


def compare_lsn(left: Any, right: Any) -> int:
    """Compare two LSN-like values. Returns -1, 0, or 1.

    Same-family stamps compare numerically/lexically. Cross-family pairs are
    **incomparable** and return ``0`` so LSN guards refuse invent overwrite
    (Debezium at-least-once + PK high-water-mark class). Empty is older than
    any concrete stamp.
    """
    left_empty = left is None or str(left).strip() == ""
    right_empty = right is None or str(right).strip() == ""
    if left_empty and right_empty:
        return 0
    if left_empty:
        return -1
    if right_empty:
        return 1
    fa, fb = lsn_family(left), lsn_family(right)
    if fa != fb:
        return 0
    a, b = lsn_sort_key(left), lsn_sort_key(right)
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def lsn_is_newer(incoming: Any, existing: Any) -> bool:
    """True when ``incoming`` should replace ``existing`` under at-least-once CDC.

    Requires a strictly greater same-family stamp. Equal and cross-family are
    not newer (idempotent redelivery / refuse invent).
    """
    if existing is None or str(existing).strip() == "":
        return True
    if incoming is None or str(incoming).strip() == "":
        return False
    return compare_lsn(incoming, existing) > 0


def parse_mysql_gtid_set(gtid_set: Any) -> dict[str, list[tuple[int, int]]]:
    """Parse MySQL ``gtid_executed`` into ``{uuid: [(start, end), ...]}``.

    Research: Debezium read-only incremental snapshots use executed GTID sets
    as low/high watermarks (DBZ-3577). Intervals are inclusive.
    """
    text = str(gtid_set or "").strip()
    if text.lower().startswith("gtid:"):
        text = text[5:].strip()
    out: dict[str, list[tuple[int, int]]] = {}
    if not text:
        return out
    for chunk in text.replace("\n", ",").split(","):
        part = chunk.strip()
        if not part or ":" not in part:
            continue
        uuid, _, ranges = part.partition(":")
        uuid = uuid.strip()
        if not uuid:
            continue
        intervals: list[tuple[int, int]] = []
        for rng in ranges.split(":"):
            rng = rng.strip()
            if not rng:
                continue
            if "-" in rng:
                a, _, b = rng.partition("-")
                try:
                    start, end = int(a), int(b)
                except ValueError:
                    continue
                if end < start:
                    start, end = end, start
                intervals.append((start, end))
            else:
                try:
                    n = int(rng)
                except ValueError:
                    continue
                intervals.append((n, n))
        if intervals:
            out.setdefault(uuid, []).extend(intervals)
    return out


def gtid_set_contains(haystack: Any, needle: Any) -> bool:
    """True when every GTID interval in ``needle`` is covered by ``haystack``.

    Used for Debezium-class watermark checks: high watermark contains a
    streamed event's GTID ⇒ window can close. Cross-empty: empty needle is
    contained; empty haystack contains nothing non-empty.
    """
    needle_map = parse_mysql_gtid_set(needle)
    if not needle_map:
        return True
    hay = parse_mysql_gtid_set(haystack)
    if not hay:
        return False
    for uuid, intervals in needle_map.items():
        covers = hay.get(uuid) or []
        if not covers:
            return False
        for start, end in intervals:
            for n in range(start, end + 1):
                if not any(a <= n <= b for a, b in covers):
                    return False
    return True


def gtid_watermark_window_closed(
    *,
    low: Any,
    high: Any,
    event_gtid: Any = None,
) -> bool:
    """True when read-only incremental snapshot GTID window can close.

    Research: Debezium DBZ-3577 — executed GTID set as low/high watermarks.
    Window closes when ``high`` contains ``low`` (and optional event GTID).
    Never invent closed from lexicographic string order.
    """
    if not gtid_set_contains(high, low):
        return False
    if event_gtid is None or str(event_gtid).strip() == "":
        return True
    return gtid_set_contains(high, event_gtid)


def dedupe_rows_by_pk_and_lsn(
    rows: list[tuple],
    conflict_columns: list[str],
    target_cols: list[str],
    *,
    lsn_column: str = DF_LSN_COL,
) -> list[tuple]:
    """Keep the highest-LSN row per PK; fall back to last-wins when LSN absent."""
    if not conflict_columns or not rows:
        return rows
    if lsn_column not in target_cols:
        return dedupe_rows(rows, conflict_columns, target_cols)
    indices = [target_cols.index(c) for c in conflict_columns if c in target_cols]
    if not indices:
        return rows
    lsn_idx = target_cols.index(lsn_column)
    best: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[i] for i in indices)
        prev = best.get(key)
        if prev is None or compare_lsn(row[lsn_idx], prev[lsn_idx]) >= 0:
            best[key] = row
    return list(best.values())


def _format_file_pos_lsn(file_name: str, pos: Any) -> str:
    """Format file:pos LSN for downstream SQL guards.

    MySQL binlog files use zero-padded numeric suffixes (e.g. ``mysql-bin.000003``);
    for those we zero-pad the position so lexicographic text ordering stays
    monotonic.  For unpadded file names we emit the plain integer position so
    unit-test fixtures like ``bin.1:9`` stay readable.
    """
    try:
        int_pos = int(pos)
    except (TypeError, ValueError):
        return f"{file_name}:{pos}"
    # Detect zero-padded numeric token in the file name (MySQL binlog style).
    if re.search(r"(?<!\d)0\d+(?!\d)", file_name):
        return f"{file_name}:{int_pos:020d}"
    return f"{file_name}:{int_pos}"


def extract_cdc_lsn(resume_token: Any) -> str | None:
    """Pull a sortable LSN/position string from a CDC resume token.

    Supports PG ``lsn=``, MySQL ``file:pos`` / ``gtid``, Mongo ``_data``,
    SQL Server LSN hex, and Oracle SCN. Used to stamp ``_df_lsn`` for
    at-least-once upsert guards (not exactly-once).
    """
    if resume_token is None:
        return None
    if isinstance(resume_token, dict):
        # Nested PG hold / incremental wrappers
        nested = resume_token.get("token")
        if isinstance(nested, (dict, str)) and nested:
            nested_lsn = extract_cdc_lsn(nested)
            if nested_lsn:
                return nested_lsn
        file_name = resume_token.get("file") or resume_token.get("filename")
        pos = resume_token.get("pos")
        if file_name is not None and pos is not None:
            return _format_file_pos_lsn(file_name, pos)
        gtid = resume_token.get("gtid") or resume_token.get("gtid_set")
        if gtid is not None and str(gtid).strip():
            return f"gtid:{str(gtid).strip()}"
        for key in ("lsn", "scn", "version", "position", "resume_lsn", "pos", "_data"):
            value = resume_token.get(key)
            if value is None or not str(value).strip():
                continue
            if key == "scn":
                body = str(value).strip()
                return body if body.lower().startswith("scn:") else f"scn:{body}"
            if key == "_data":
                body = str(value).strip()
                return body if body.lower().startswith("mongo:") else f"mongo:{body}"
            if key == "version":
                try:
                    return f"{int(value):020d}"
                except (TypeError, ValueError):
                    return str(value).strip()
            return str(value).strip()
        return None
    text = str(resume_token).strip()
    if not text or text in {"None", "null"}:
        return None
    # Bare MySQL file:pos strings — pad pos for lexicographic guards.
    if ":" in text and not text.lower().startswith("gtid:") and "/" not in text and not text.startswith("{"):
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return _format_file_pos_lsn(file_name, pos)
    # JSON CDC tokens (SQL Server native / CT, Oracle LogMiner, etc.)
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except Exception:
            data = None
        if isinstance(data, dict):
            kind = str(data.get("kind") or "")
            if kind == "mssql-cdc":
                lsn = data.get("lsn")
                if lsn is not None and str(lsn).strip():
                    return str(lsn).strip()
            if kind in {"mssql-ct", "sqlserver-ct"}:
                ver = data.get("version")
                if ver is not None and str(ver).strip():
                    # Zero-pad so lexicographic compare stays monotonic for versions.
                    try:
                        return f"{int(ver):020d}"
                    except (TypeError, ValueError):
                        return str(ver).strip()
            nested = extract_cdc_lsn(data)
            if nested:
                return nested
    if "lsn=" in text:
        for part in text.split("|"):
            if part.startswith("lsn=") and part[4:].strip():
                return part[4:].strip()
    return text


def postgres_lsn_update_guard_sql(table_name: str, lsn_column: str = DF_LSN_COL) -> str:
    """WHERE fragment for ON CONFLICT when ``_df_lsn`` is present.

    Real PG ``hi/lo`` LSNs use ``::pg_lsn``. Mixed CDC stamps use family-aware
    compare for ``file:pos`` / numeric versions / opaque tokens — never invent
    cross-family ``newer`` via bare text ``>`` (mirrors :func:`compare_lsn`).
    """
    pg_pat = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_pat = r"^[0-9]+$"
    excl = f'EXCLUDED."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'
    dest_c = f"COALESCE({dest}, '')"
    both_filepos = (
        f"({excl} LIKE '%%:%%' AND {excl} NOT LIKE 'gtid:%%' "
        f"AND {dest_c} LIKE '%%:%%' AND {dest_c} NOT LIKE 'gtid:%%')"
    )
    # split_part is Postgres-native (same dialect as ON CONFLICT).
    filepos_newer = (
        f"(split_part({excl}, ':', 1) > split_part({dest}, ':', 1) "
        f"OR (split_part({excl}, ':', 1) = split_part({dest}, ':', 1) "
        f"AND NULLIF(split_part({excl}, ':', 2), '')::bigint "
        f"> NULLIF(split_part({dest}, ':', 2), '')::bigint))"
    )
    both_numeric = f"({excl} ~ '{num_pat}' AND {dest_c} ~ '{num_pat}')"
    # Opaque: neither side looks like pg / file:pos / all-digits.
    both_opaque = (
        f"({excl} !~ '{pg_pat}' AND {dest_c} !~ '{pg_pat}' "
        f"AND NOT ({excl} LIKE '%%:%%' AND {excl} NOT LIKE 'gtid:%%') "
        f"AND NOT ({dest_c} LIKE '%%:%%' AND {dest_c} NOT LIKE 'gtid:%%') "
        f"AND {excl} !~ '{num_pat}' AND {dest_c} !~ '{num_pat}')"
    )
    return (
        f"( "
        f"({excl} ~ '{pg_pat}' AND {dest_c} ~ '{pg_pat}' "
        f"AND {excl}::pg_lsn > COALESCE(NULLIF({dest}, '')::pg_lsn, '0/0'::pg_lsn)) "
        f"OR "
        f"({excl} !~ '{pg_pat}' AND ("
        f"{dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR ({both_numeric} AND {excl}::bigint > {dest}::bigint) "
        f"OR ({both_opaque} AND {excl} > {dest})"
        f")) "
        f")"
    )


def mysql_lsn_values_newer_sql(lsn_column: str = DF_LSN_COL, *, quote: str = "`") -> str:
    """Boolean SQL: ``VALUES(lsn)`` is strictly newer than the destination cell.

    Handles empty dest, ``file:pos`` (file then integer pos), numeric versions,
    and opaque tokens — refuses cross-family invent. Used inside
    ``ON DUPLICATE KEY UPDATE col=IF(<pred>, VALUES(col), col)``.
    """
    col = f"{quote}{lsn_column}{quote}"
    inc = f"VALUES({col})"
    dest = col
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"
    both_filepos = (
        f"({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%' "
        f"AND {dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%')"
    )
    filepos_newer = (
        f"(SUBSTRING_INDEX({inc}, ':', 1) > SUBSTRING_INDEX({dest}, ':', 1) "
        f"OR (SUBSTRING_INDEX({inc}, ':', 1) = SUBSTRING_INDEX({dest}, ':', 1) "
        f"AND CAST(SUBSTRING_INDEX({inc}, ':', -1) AS UNSIGNED) "
        f"> CAST(SUBSTRING_INDEX({dest}, ':', -1) AS UNSIGNED)))"
    )
    both_numeric = f"({inc} REGEXP '{num_re}' AND {dest} REGEXP '{num_re}')"
    both_opaque = (
        f"(NOT ({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%') "
        f"AND NOT ({dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%') "
        f"AND {inc} NOT REGEXP '{pg_re}' AND {dest} NOT REGEXP '{pg_re}' "
        f"AND {inc} NOT REGEXP '{num_re}' AND {dest} NOT REGEXP '{num_re}')"
    )
    return (
        f"({dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR ({both_numeric} AND CAST({inc} AS UNSIGNED) > CAST({dest} AS UNSIGNED)) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )


def sqlite_lsn_update_guard_sql(table_name: str, lsn_column: str = DF_LSN_COL) -> str:
    """WHERE fragment for SQLite ``ON CONFLICT DO UPDATE``.

    Family-aware for ``file:pos`` / numeric / opaque. PG ``hi/lo`` hex is
    best-effort opaque text here (SQLite lacks portable hex→int); writers also
    run :func:`filter_stale_lsn_rows` / :func:`compare_lsn` in Python before bind.
    Cross-family pairs never invent ``newer`` via bare text ``>``.
    """
    excl = f'excluded."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'
    both_filepos = (
        f"({excl} LIKE '%:%' AND {excl} NOT LIKE 'gtid:%' "
        f"AND {dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%')"
    )
    # instr/substr — portable without REGEXP extension.
    excl_file = f"substr({excl}, 1, instr({excl}, ':') - 1)"
    dest_file = f"substr({dest}, 1, instr({dest}, ':') - 1)"
    excl_pos = f"CAST(substr({excl}, instr({excl}, ':') + 1) AS INTEGER)"
    dest_pos = f"CAST(substr({dest}, instr({dest}, ':') + 1) AS INTEGER)"
    filepos_newer = (
        f"({excl_file} > {dest_file} "
        f"OR ({excl_file} = {dest_file} AND {excl_pos} > {dest_pos}))"
    )
    # GLOB [0-9]* matches empty too — require at least one digit via length.
    excl_numeric = (
        f"({excl} GLOB '[0-9]*' AND {excl} NOT GLOB '*[^0-9]*' AND length({excl}) > 0)"
    )
    dest_numeric = (
        f"({dest} GLOB '[0-9]*' AND {dest} NOT GLOB '*[^0-9]*' AND length({dest}) > 0)"
    )
    both_numeric = f"({excl_numeric} AND {dest_numeric})"
    excl_opaque = (
        f"(NOT ({excl} LIKE '%:%' AND {excl} NOT LIKE 'gtid:%') "
        f"AND {excl} NOT LIKE '%/%' AND NOT {excl_numeric})"
    )
    dest_opaque = (
        f"(NOT ({dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%') "
        f"AND {dest} NOT LIKE '%/%' AND NOT {dest_numeric})"
    )
    both_opaque = f"({excl_opaque} AND {dest_opaque})"
    return (
        f"({dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR ({both_numeric} AND CAST({excl} AS INTEGER) > CAST({dest} AS INTEGER)) "
        f"OR ({both_opaque} AND {excl} > {dest}))"
    )


def snowflake_lsn_match_predicate(
    target_alias: str = "t",
    source_alias: str = "s",
    lsn_column: str = DF_LSN_COL,
) -> str:
    """MATCHED guard for Snowflake MERGE — mirrors :func:`compare_lsn` families.

    Bare ``s.lsn > t.lsn`` mis-orders PG ``0/100`` vs ``0/20`` and invents
    cross-family ``newer``. Parse pg / file:pos / numeric; opaque text only
    when both sides are the same opaque family.
    """
    inc = f'{source_alias}."{lsn_column}"'
    dest = f'COALESCE({target_alias}."{lsn_column}", \'\')'
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"
    both_filepos = (
        f"({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%' "
        f"AND {dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%')"
    )
    # SPLIT_PART(..., -1) = last segment (pos) in Snowflake.
    filepos_newer = (
        f"(SPLIT_PART({inc}, ':', 1) > SPLIT_PART({dest}, ':', 1) "
        f"OR (SPLIT_PART({inc}, ':', 1) = SPLIT_PART({dest}, ':', 1) "
        f"AND TRY_TO_NUMBER(SPLIT_PART({inc}, ':', -1)) "
        f"> TRY_TO_NUMBER(SPLIT_PART({dest}, ':', -1))))"
    )
    both_pg = (
        f"(REGEXP_LIKE({inc}, '{pg_re}') AND REGEXP_LIKE({dest}, '{pg_re}'))"
    )
    # Hex hi/lo via TO_NUMBER with hex format mask.
    inc_hi = f"TRY_TO_NUMBER(SPLIT_PART({inc}, '/', 1), 'XXXXXXXXXXXXXXXX')"
    dest_hi = f"TRY_TO_NUMBER(SPLIT_PART({dest}, '/', 1), 'XXXXXXXXXXXXXXXX')"
    inc_lo = f"TRY_TO_NUMBER(SPLIT_PART({inc}, '/', 2), 'XXXXXXXXXXXXXXXX')"
    dest_lo = f"TRY_TO_NUMBER(SPLIT_PART({dest}, '/', 2), 'XXXXXXXXXXXXXXXX')"
    pg_newer = (
        f"({inc_hi} > {dest_hi} OR ({inc_hi} = {dest_hi} AND {inc_lo} > {dest_lo}))"
    )
    both_numeric = (
        f"(REGEXP_LIKE({inc}, '{num_re}') AND REGEXP_LIKE({dest}, '{num_re}'))"
    )
    inc_opaque = (
        f"(NOT REGEXP_LIKE({inc}, '{pg_re}') AND NOT REGEXP_LIKE({inc}, '{num_re}') "
        f"AND NOT ({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%'))"
    )
    dest_opaque = (
        f"(NOT REGEXP_LIKE({dest}, '{pg_re}') AND NOT REGEXP_LIKE({dest}, '{num_re}') "
        f"AND NOT ({dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%'))"
    )
    both_opaque = f"({inc_opaque} AND {dest_opaque})"
    return (
        f"({dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND TRY_TO_NUMBER({inc}) > TRY_TO_NUMBER({dest})) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )


def bigquery_lsn_match_predicate(
    target_alias: str = "T",
    source_alias: str = "S",
    lsn_column: str = DF_LSN_COL,
) -> str:
    """MATCHED guard for BigQuery MERGE — mirrors :func:`compare_lsn` families.

    Plain ``S.lsn > T.lsn`` is unsafe for PG ``hi/lo`` hex and invents
    cross-family ``newer``. Parse pg / file:pos / numeric; opaque text only
    within the same opaque family.
    """
    inc = f"{source_alias}.`{lsn_column}`"
    dest = f"COALESCE({target_alias}.`{lsn_column}`, '')"
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"
    both_filepos = (
        f"({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%' "
        f"AND {dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%')"
    )
    filepos_newer = (
        f"(SPLIT({inc}, ':')[OFFSET(0)] > SPLIT({dest}, ':')[OFFSET(0)] "
        f"OR (SPLIT({inc}, ':')[OFFSET(0)] = SPLIT({dest}, ':')[OFFSET(0)] "
        f"AND SAFE_CAST(ARRAY_REVERSE(SPLIT({inc}, ':'))[OFFSET(0)] AS INT64) "
        f"> SAFE_CAST(ARRAY_REVERSE(SPLIT({dest}, ':'))[OFFSET(0)] AS INT64)))"
    )
    both_pg = (
        f"(REGEXP_CONTAINS({inc}, r'{pg_re}') AND REGEXP_CONTAINS({dest}, r'{pg_re}'))"
    )
    inc_hi = f"SAFE_CAST(CONCAT('0x', SPLIT({inc}, '/')[OFFSET(0)]) AS INT64)"
    dest_hi = f"SAFE_CAST(CONCAT('0x', SPLIT({dest}, '/')[OFFSET(0)]) AS INT64)"
    inc_lo = f"SAFE_CAST(CONCAT('0x', SPLIT({inc}, '/')[OFFSET(1)]) AS INT64)"
    dest_lo = f"SAFE_CAST(CONCAT('0x', SPLIT({dest}, '/')[OFFSET(1)]) AS INT64)"
    pg_newer = (
        f"({inc_hi} > {dest_hi} OR ({inc_hi} = {dest_hi} AND {inc_lo} > {dest_lo}))"
    )
    both_numeric = (
        f"(REGEXP_CONTAINS({inc}, r'{num_re}') AND REGEXP_CONTAINS({dest}, r'{num_re}'))"
    )
    inc_opaque = (
        f"(NOT REGEXP_CONTAINS({inc}, r'{pg_re}') "
        f"AND NOT REGEXP_CONTAINS({inc}, r'{num_re}') "
        f"AND NOT ({inc} LIKE '%:%' AND {inc} NOT LIKE 'gtid:%'))"
    )
    dest_opaque = (
        f"(NOT REGEXP_CONTAINS({dest}, r'{pg_re}') "
        f"AND NOT REGEXP_CONTAINS({dest}, r'{num_re}') "
        f"AND NOT ({dest} LIKE '%:%' AND {dest} NOT LIKE 'gtid:%'))"
    )
    both_opaque = f"({inc_opaque} AND {dest_opaque})"
    return (
        f"({dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND SAFE_CAST({inc} AS INT64) > SAFE_CAST({dest} AS INT64)) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )


def transform_error_policy(
    policy: str | None = None,
    *,
    allow_coerce_null: bool | None = None,
) -> str:
    """Resolve job error policy — ``coerce_null`` is gated.

    Job/env ``coerce_null`` silently NULLs failed cells in the primary table.
    That is forbidden unless:
    - a Migration Risk Contract quarantine_policy asks for NULL/COERCE (resolved
      per-mapping in ``resolve_write_action_for_mapping``), or
    - the caller explicitly allows it (pre-ingestion staging diagnose path).
    """
    selected = (policy or TRANSFORM_ERROR_POLICY or "quarantine").strip().lower()
    if selected not in VALID_ERROR_POLICIES:
        return "quarantine"
    if selected != "coerce_null":
        return selected
    allowed = (
        bool(allow_coerce_null)
        if allow_coerce_null is not None
        else bool(_allow_job_coerce_null.get())
    )
    if allowed:
        return "coerce_null"
    import logging

    logging.getLogger(__name__).warning(
        "Job/env TRANSFORM_ERROR_POLICY=coerce_null demoted to quarantine — "
        "primary writes require a Risk Contract NULL/COERCE policy or staging allow"
    )
    return "quarantine"


def reject_on_strict_policy(
    policy: str | None,
    rejected_details: list[dict[str, Any]] | None,
    label: str,
) -> str | None:
    """Return an error message when the write must refuse a partial primary write.

    Module 1b: Migration Risk Contract FAIL_JOB aborts even when the job-level
    error_policy is ``quarantine``. Continue-policy contract failures
    (CAST_AND_CONTINUE / QUARANTINE_ROW) may hold out rows without aborting.
    """
    details = list(rejected_details or [])
    if not details:
        return None

    # In-tree SSOT — never soft-import and demote FAIL_JOB / continue contracts.
    from services.migration_risk_contract import (
        JOB_ABORT_POLICIES,
        rejected_details_are_continue_contract_only,
        rejected_details_require_job_abort,
    )

    if rejected_details_require_job_abort(details):
        n = sum(
            1
            for d in details
            if str(d.get("execution_policy") or "").upper() in JOB_ABORT_POLICIES
            or str(d.get("policy") or "").lower()
            in {"fail", "stop_table", "abort_transaction", "retry_then_fail"}
        )
        scopes = sorted(
            {
                str(d.get("stop_scope") or d.get("execution_policy") or "FAIL_JOB")
                for d in details
                if str(d.get("execution_policy") or "").upper() in JOB_ABORT_POLICIES
                or str(d.get("policy") or "").lower()
                in {"fail", "stop_table", "abort_transaction", "retry_then_fail"}
            }
        )
        scope_note = f" ({', '.join(scopes[:4])})" if scopes else ""
        return (
            f"{label} rejected {n or len(details)} row(s); "
            f"Migration Risk Contract abort policy blocks partial write{scope_note}"
        )

    if rejected_details_are_continue_contract_only(details):
        # Operator contracted continue + quarantine — do not abort the batch.
        return None

    if transform_error_policy(policy) == "fail":
        return (
            f"{label} rejected {len(details)} row(s); "
            "strict error policy blocks partial write"
        )
    return None


_VALIDATION_MODE_POLICIES = {
    "maximum": "fail",
    "strict": "fail",
    "balanced": "quarantine",
}


def transform_error_policy_for_validation_mode(validation_mode: str | None) -> str:
    """Strict/maximum modes fail the transfer on bad cells — no silent row drops."""
    mode = (validation_mode or "strict").strip().lower()
    if mode in _VALIDATION_MODE_POLICIES:
        return _VALIDATION_MODE_POLICIES[mode]
    return transform_error_policy()


def _rejected_row_count(
    data_rows: list[list[str]],
    mapped_rows: list[tuple],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    sparse_rows: list[tuple] | None = None,
) -> int:
    """Return the number of rows that were rejected or quarantined.

    For ``fail`` / ``quarantine`` the held-out rows are
    ``len(data_rows) - len(mapped_rows) - len(sparse_rows)`` (quarantine never
    writes NULL into the primary table for a bad cell; sparse CDC rows are
    still written via omit-from-SET and must not inflate rejected counts).
    For ``coerce_null`` the rows are preserved with a NULL bad cell, so the
    count is distinct source row numbers with at least one rejected cell.
    """
    if policy == "coerce_null":
        return len({d["row"] for d in rejected_details})
    kept = len(mapped_rows) + len(sparse_rows or [])
    return max(0, len(data_rows) - kept)


def _coerced_null_row_count(rejected_details: list[dict[str, Any]], policy: str) -> int:
    """Distinct source rows that were KEPT but had a cell coerced/omitted to NULL.

    Counts job ``coerce_null`` and continue-policy ``STOP_COLUMN`` / contract
    coerce so Gate-8 reconcile cannot treat NULL invent as zero alteration.
    Quarantine / skip / abort holdouts are excluded — those never land on primary.
    """
    holdout_actions = {
        "quarantine",
        "skip_row",
        "fail",
        "stop_table",
        "abort_transaction",
        "retry_then_fail",
        "write_quarantine",
        "write_fail",
    }
    holdout_exec = {
        "QUARANTINE_ROW",
        "SKIP_ROW",
        "FAIL_JOB",
        "STOP_TABLE",
        "ABORT_TRANSACTION",
        "RETRY",
    }
    rows: set[Any] = set()
    for d in rejected_details or []:
        if not isinstance(d, dict) or "row" not in d:
            continue
        action = str(d.get("policy") or "").lower()
        exec_pol = str(d.get("execution_policy") or "").upper()
        if action in holdout_actions or exec_pol in holdout_exec:
            continue
        if (
            policy == "coerce_null"
            or action in {"stop_column", "coerce_null"}
            or exec_pol == "STOP_COLUMN"
        ):
            rows.add(d["row"])
    return len(rows)


def map_rows_for_fingerprint(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = True,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    contract_primary_key: str | None = None,
) -> tuple[list[tuple], list[dict[str, Any]]]:
    """Map rows for Gate-8 fingerprints with write-path quarantine parity.

    Returns ``(mapped_good_rows, rejected_details)``. Callers must pass the same
    ``dest_kind`` / ``destination_pk_columns`` / ``error_policy`` as the writer
    so checksum remap cannot invent a different hold-out set than the load.

    After ``build_mapped_rows_with_details``, runs ``apply_write_quarantine_matrix``
    whenever destination types are known — typed writers quarantine DECIMAL /
    VARCHAR / BINARY / temporal overflow the same way.
    """
    mapped, _errors, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=error_policy,
        dest_types=dest_types,
        preserve_case=preserve_case,
        dest_kind=dest_kind,
        destination_pk_columns=destination_pk_columns,
        stream_contracts=stream_contracts,
        contract_primary_key=contract_primary_key,
    )
    rejected_details = list(rejected or [])
    type_map = dict(dest_types or {}) or dict(column_types or {})
    if mapped and target_cols and type_map:
        target_types = [str(type_map.get(c) or type_map.get(str(c).lower()) or "") for c in target_cols]
        if any(t.strip() for t in target_types):
            policy = transform_error_policy(error_policy)
            mapped = apply_write_quarantine_matrix(
                mapped,
                target_cols,
                target_types,
                rejected_details,
                policy,
                dialect_label=(dest_kind or "destination").strip() or "destination",
                mappings=list(mappings or []) or None,
            )
    return mapped, rejected_details


def build_mapped_rows(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = False,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    contract_primary_key: str | None = None,
) -> tuple[list[tuple], list[str]]:
    """Returns mapped rows and any transform errors (first 10).

    Prefer ``map_rows_for_fingerprint`` / ``build_mapped_rows_with_details`` when
    quarantine / Risk Contracts must be surfaced — this wrapper drops
    rejected_details by design for legacy callers.
    """
    mapped, errors, _ = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=error_policy,
        dest_types=dest_types,
        preserve_case=preserve_case,
        dest_kind=dest_kind,
        destination_pk_columns=destination_pk_columns,
        stream_contracts=stream_contracts,
        contract_primary_key=contract_primary_key,
    )
    return mapped, errors


def build_mapped_rows_with_details(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = False,
    allow_job_coerce_null: bool | None = None,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
) -> tuple[list[tuple], list[str], list[dict[str, Any]]]:
    """Returns mapped rows, error messages, and structured rejected-row details."""
    from services.json_intelligence import materialize_struct_policies

    column_types = column_types or {}
    policy = transform_error_policy(
        error_policy, allow_coerce_null=allow_job_coerce_null
    )
    # Honor Map STRUCT policy (JSON blob vs flatten top-level keys) before bind.
    headers, data_rows = materialize_struct_policies(headers, data_rows, mappings)
    source_indices = {h: i for i, h in enumerate(headers)}
    sanitized_target_cols = [sanitize_identifier(c, preserve_case=preserve_case) for c in target_cols]
    target_index = {c: i for i, c in enumerate(sanitized_target_cols)}
    errors: list[str] = []
    rejected_details: list[dict[str, Any]] = []

    mapping_infos = []
    for m in mappings:
        try:
            from services.mapping_constraints import is_intentional_omit

            if is_intentional_omit(m):
                continue
        except Exception:
            pass
        src = m["source"]
        tgt = sanitize_identifier(m["target"], preserve_case=preserve_case)
        transform = resolve_transform(
            m,
            column_types=column_types,
            dest_types=dest_types or column_types,
        )
        mapping_infos.append((
            source_indices.get(src),
            target_index.get(tgt, -1),
            transform,
            src,
            tgt,
            m,
        ))

    # Resolve once per call, not once per cell. This import sat inside the
    # innermost loop, so a 20-column table paid an import-system lookup per cell
    # — billions of them on a large transfer.
    # Risk Contract module is in-tree: hard-require it. Soft-import previously
    # demoted FAIL_JOB / SKIP_ROW / CAST contracts to bare job policy.
    from services.value_serializer import DF_MISSING_SENTINEL, is_missing_sentinel
    from services.migration_risk_contract import (
        disposition_for_execution_policy,
        resolve_write_action_for_mapping,
    )

    mapped: list[tuple] = []
    for row_number, raw in enumerate(data_rows, start=1):
        out = [None] * len(sanitized_target_cols)
        row_has_error = False
        # ok | fail | stop_table | abort_transaction | retry_then_fail |
        # quarantine | skip_row | stop_column | coerce_null
        row_action = "ok"
        for source_idx, target_idx, transform, src_name, tgt_name, mapping in mapping_infos:
            val = raw[source_idx] if source_idx is not None and source_idx < len(raw) else None
            # Preserve sparse-CDC missing before transforms (omit-from-SET, never NULL wipe).
            if is_missing_sentinel(val):
                if target_idx >= 0:
                    out[target_idx] = DF_MISSING_SENTINEL
                continue
            converted, err = apply_transform(val, transform)
            cell_policy = policy
            exec_pol: str | None = None
            risk_id: str | None = None
            retry_attempted = False
            if err:
                cell_policy, exec_pol, risk_id = resolve_write_action_for_mapping(
                    mapping, policy
                )
                # RETRY: exactly one re-attempt of the same transform, then fail.
                if cell_policy == "retry_then_fail":
                    retry_attempted = True
                    converted, err = apply_transform(val, transform)
                    if not err:
                        cell_policy = "ok"
                    else:
                        cell_policy = "fail"
            if err:
                row_has_error = True
                if exec_pol is None:
                    cell_policy, exec_pol, risk_id = resolve_write_action_for_mapping(
                        mapping, policy
                    )
                values = {
                    h: (str(raw[i]) if i < len(raw) and raw[i] is not None else "")
                    for i, h in enumerate(headers)
                }
                detail: dict[str, Any] = {
                    "row": row_number,
                    "column": src_name,
                    "target": tgt_name,
                    "value": str(val) if val is not None else "",
                    "reason": err,
                    "policy": cell_policy,
                    # Full source row so quarantine replay can rewrite without re-reading.
                    "values": values,
                    # Dual-stamp: transform quarantine is already source-shaped.
                    "source_values": dict(values),
                }
                if exec_pol:
                    detail["execution_policy"] = exec_pol
                    detail["disposition"] = disposition_for_execution_policy(exec_pol)
                    if exec_pol == "STOP_TABLE":
                        detail["stop_scope"] = "table"
                    elif exec_pol == "STOP_COLUMN":
                        detail["stop_scope"] = "column"
                    elif exec_pol == "FAIL_JOB":
                        detail["stop_scope"] = "job"
                    elif exec_pol == "ABORT_TRANSACTION":
                        detail["stop_scope"] = "transaction"
                        detail["transaction_abort_requested"] = True
                        # Writers without a real txn still fail closed — honesty stamp.
                        detail["transaction_available"] = False
                    elif exec_pol == "SKIP_ROW":
                        detail["quarantine_required"] = False
                    elif exec_pol == "QUARANTINE_ROW":
                        detail["quarantine_required"] = True
                if retry_attempted:
                    detail["retry_attempted"] = True
                    detail["retry_count"] = 1
                if risk_id:
                    detail["risk_id"] = risk_id
                # Stamp durable identity for upsert replay — full composite when known.
                pk_cols: list[str] = []
                try:
                    from services.primary_key import resolve_primary_key_source_columns

                    pk_cols = resolve_primary_key_source_columns(
                        mappings=mappings,
                        source_columns=headers,
                        dest_kind=dest_kind or "",
                        purpose="uniqueness",
                        destination_pk_columns=destination_pk_columns,
                        contract_primary_key=contract_primary_key,
                        stream_contracts=stream_contracts,
                    )
                except Exception:
                    pk_cols = []
                if not pk_cols:
                    flagged = [
                        str(mm.get("source") or mm.get("target") or "")
                        for mm in mappings
                        if mm.get("primary_key")
                        or mm.get("is_primary_key")
                        or mm.get("identity")
                    ]
                    pk_cols = [c for c in flagged if c]
                # Never invent id/_id as PK for quarantine replay — only contract/
                # mapping/destination-proven columns. Invented PK poisons DLQ identity.
                if pk_cols:
                    detail["primary_key"] = pk_cols
                    detail["pk_value"] = {
                        c: values.get(c) or values.get(str(c).lower(), "")
                        for c in pk_cols
                    }
                rejected_details.append(detail)
                if len(errors) < 10:
                    errors.append(f"row {row_number} {src_name}→{tgt_name}: {err}")
                # Escalate row_action by severity — abort > skip/quarantine > stop_column.
                abort_actions = {
                    "fail",
                    "stop_table",
                    "abort_transaction",
                    "retry_then_fail",
                }
                if cell_policy in abort_actions:
                    row_action = cell_policy if cell_policy != "retry_then_fail" else "fail"
                    converted = None
                elif cell_policy == "skip_row":
                    if row_action not in abort_actions:
                        row_action = "skip_row"
                    converted = None
                elif cell_policy == "quarantine":
                    if row_action not in abort_actions and row_action != "skip_row":
                        row_action = "quarantine"
                    converted = None
                elif cell_policy == "stop_column":
                    # Omit this cell from SET/INSERT projection (DF_MISSING), never
                    # invent SQL NULL — upsert NULL would wipe a prior good value.
                    if row_action == "ok":
                        row_action = "stop_column"
                    converted = DF_MISSING_SENTINEL
                elif cell_policy == "coerce_null":
                    if row_action == "ok":
                        row_action = "coerce_null"
                    converted = None
                else:
                    continue
                # Write-time NOT NULL parity with G3: refuse NULL invent / omit-as-NULL
                # into required columns when the Map stamp declares non-nullable.
                if cell_policy in {"stop_column", "coerce_null"}:
                    tgt_nullable = mapping.get("target_nullable")
                    if tgt_nullable is None:
                        tgt_nullable = mapping.get("nullable")
                    if tgt_nullable is False or (
                        isinstance(tgt_nullable, str)
                        and tgt_nullable.strip().lower() in {"false", "0", "no"}
                    ):
                        detail["reason"] = (
                            f"{detail.get('reason') or err} — NOT NULL destination "
                            "refuses STOP_COLUMN/coerce NULL invent"
                        )
                        detail["policy"] = (
                            "fail" if policy == "fail" else "quarantine"
                        )
                        cell_policy = detail["policy"]
                        row_action = cell_policy
                        converted = None
            if target_idx >= 0:
                out[target_idx] = converted
        # Rows not written to primary: abort / quarantine / skip.
        if row_has_error and row_action in {
            "fail",
            "stop_table",
            "abort_transaction",
            "quarantine",
            "skip_row",
        }:
            continue
        if row_has_error and row_action == "ok" and policy in {"fail", "quarantine"}:
            # Legacy path when resolve unavailable — keep prior job semantics.
            continue
        # stop_column: DF_MISSING omit-from-SET; coerce_null: NULL cell kept.
        mapped.append(tuple(out))

    return mapped, errors, rejected_details


def prepare_records_for_vector_write(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    contract_primary_key: str | None = None,
    label: str = "vector",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Apply Map transforms + Risk Contracts before embedding/upsert.

    Records stay **source-header keyed** (legacy ``zip(headers, row)``) so
    ``content_column`` / ``metadata_columns`` from Studio ``extra`` still resolve,
    while mapped transforms overlay source+target keys. Unmapped headers pass through.
    Returns ``(records, rejected_details, abort_message)``.
    """
    target_cols: list[str] = []
    source_for_target: list[str] = []
    dest_types: dict[str, str] = {}
    for m in mappings or []:
        try:
            from services.mapping_constraints import is_intentional_omit

            if is_intentional_omit(m):
                continue
        except Exception:
            pass
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or src).strip()
        if not tgt:
            continue
        if tgt not in target_cols:
            target_cols.append(tgt)
            source_for_target.append(src or tgt)
        dest_types[tgt] = str(
            m.get("target_type") or m.get("dest_type") or (column_types or {}).get(src) or "string"
        )
    if not target_cols:
        target_cols = list(headers)
        source_for_target = list(headers)
        dest_types = {h: (column_types or {}).get(h, "string") for h in target_cols}

    mapped, _errors, rejected = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=list(mappings or []),
        target_cols=target_cols,
        column_types=column_types or {},
        dest_types=dest_types,
        error_policy=error_policy,
        dest_kind=dest_kind,
        destination_pk_columns=destination_pk_columns,
        stream_contracts=stream_contracts,
        contract_primary_key=contract_primary_key,
    )
    # Typed specialty holdout (VECTOR(n) length) before embed/upsert — same bar
    # as SQL writers. No-op when dest_types are unbounded strings.
    if mapped and target_cols and any(
        "vector" in str(dest_types.get(c) or "").lower() for c in target_cols
    ):
        policy = transform_error_policy(error_policy)
        target_types = [str(dest_types.get(c) or "") for c in target_cols]
        mapped = apply_write_quarantine_matrix(
            mapped,
            target_cols,
            target_types,
            rejected,
            policy,
            dialect_label=(dest_kind or label or "vector").strip() or "vector",
            mappings=list(mappings or []) or None,
        )
    abort = reject_on_strict_policy(error_policy, rejected, label)
    if abort:
        return [], list(rejected), abort

    # Rows held out of primary write (same set as build_mapped_rows continue).
    holdout_policies = {
        "fail",
        "stop_table",
        "abort_transaction",
        "quarantine",
        "skip_row",
    }
    held_out_rows = {
        int(d["row"])
        for d in rejected
        if d.get("row") is not None
        and str(d.get("policy") or "").lower() in holdout_policies
    }

    def _cell(val: Any) -> Any:
        if val is None:
            return ""
        if isinstance(val, (str, int, float, bool)):
            return val
        return str(val)

    records: list[dict[str, Any]] = []
    mapped_i = 0
    for row_number, raw in enumerate(data_rows, start=1):
        if row_number in held_out_rows:
            continue
        if mapped_i >= len(mapped):
            break
        tup = mapped[mapped_i]
        mapped_i += 1
        # Preserve unmapped source headers for metadata_columns / content_column.
        row: dict[str, Any] = {
            h: _cell(raw[i] if i < len(raw) else None) for i, h in enumerate(headers)
        }
        for i, tgt in enumerate(target_cols):
            val = _cell(tup[i] if i < len(tup) else None)
            row[tgt] = val
            src = source_for_target[i] if i < len(source_for_target) else ""
            if src:
                row[src] = val
        records.append(row)
    return records, list(rejected), None


def sample_values_by_source_from_batch(
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    *,
    limit: int = 200,
) -> dict[str, list[str]]:
    """Collect per-source sample strings from a write batch for DDL safety."""
    index = {h: i for i, h in enumerate(headers)}
    out: dict[str, list[str]] = {}
    for m in mappings:
        try:
            from services.mapping_constraints import is_intentional_omit

            if is_intentional_omit(m):
                continue
        except Exception:
            pass
        src = str(m.get("source") or "")
        if not src or src not in index:
            continue
        col_i = index[src]
        vals: list[str] = []
        for row in data_rows[:limit]:
            if col_i < len(row) and row[col_i] not in (None, "", SQL_NULL_SENTINEL):
                vals.append(str(row[col_i]))
        if vals:
            out[src] = vals
    return out


def resolve_mapping_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str] | None = None,
    *,
    logical_types: list[str] | None = None,
    live_types: dict[str, str] | None = None,
    default: str = "VARCHAR",
) -> dict[str, str]:
    """Resolve per-column carriers for quarantine / coerce (SaaS + Kafka).

    Preference order matches Salesforce/HubSpot honesty:
    1. live destination schema (Describe / Meta / Notion properties)
    2. mapping ``target_type`` / ``dest_type``
    3. ``resolve_target_columns`` logical types
    4. source ``column_types``
    5. ``default`` (never invent unbounded ``string`` when typed Map exists)
    """
    cols = list(target_cols or [])
    maps = list(mappings or [])
    ctypes = column_types or {}
    logical = list(logical_types or [])
    live = {str(k).lower(): str(v) for k, v in (live_types or {}).items() if k and v}
    out: dict[str, str] = {}
    for i, col in enumerate(cols):
        live_hit = live.get(str(col).lower())
        if live_hit:
            out[col] = live_hit
            continue
        mapped = ""
        src = ""
        if i < len(maps):
            m = maps[i]
            mapped = str(m.get("target_type") or m.get("dest_type") or "").strip()
            src = str(m.get("source") or "")
        out[col] = (
            mapped
            or (logical[i] if i < len(logical) else "")
            or ctypes.get(src)
            or ctypes.get(col)
            or default
        )
    return out


def resolve_target_columns(
    mappings: list[dict],
    column_types: dict[str, str],
    preserve_case: bool = False,
    dest_types: dict[str, str] | None = None,
    *,
    sample_values_by_source: dict[str, list[str]] | None = None,
    table_exists: bool | None = None,
) -> tuple[list[str], list[str]]:
    """Return target column names and their intended logical target types.

    Prefers an explicit ``target_type`` on each mapping, then ``dest_types``,
    then the source logical type, and finally ``VARCHAR``.

    Explicit Map ``target_type`` is always preserved (Map≡CREATE) — unfit values
    quarantine on write instead of rewriting approved DDL.

    Enterprise GA: create-new without an explicit Map ``target_type`` must **not**
    invent BOOLEAN/INTEGER/DECIMAL from head samples. Keep the source/carrier
    proposal; values that cannot coerce quarantine on write.
    """
    from services.schema_inference import safe_ddl_logical_type

    target_cols: list[str] = []
    target_types: list[str] = []
    samples = sample_values_by_source or {}
    for m in mappings:
        try:
            from services.mapping_constraints import is_intentional_omit

            if is_intentional_omit(m) or not m.get("target"):
                continue
        except Exception:
            if not m.get("target"):
                continue
        tgt = sanitize_identifier(m["target"], preserve_case=preserve_case)
        if tgt not in target_cols:
            target_cols.append(tgt)
            explicit_target = bool(m.get("target_type"))
            proposed = (
                m.get("target_type")
                or (dest_types or {}).get(tgt)
                or column_types.get(m["source"], "VARCHAR")
            )
            src = str(m.get("source") or "")
            src_type = column_types.get(src) or m.get("source_type")
            if table_exists is False:
                # Explicit Map stamp OR non-explicit source/carrier proposal:
                # honor_explicit=True prevents sample-driven invent of tighter types.
                proposed = safe_ddl_logical_type(
                    str(proposed),
                    samples.get(src) if explicit_target else None,
                    field_name=src,
                    source_type=str(src_type) if src_type else None,
                    honor_explicit=True,
                )
            target_types.append(str(proposed))
    return target_cols, target_types


# ---------------------------------------------------------------------------
# Shared DECIMAL(p,s) / NUMBER(p,s) / BIGNUMERIC(p,s) fit — fail-closed.
# Used by Snowflake, MySQL, Postgres, generic_sql, BigQuery writers.
# ---------------------------------------------------------------------------

_DECIMAL_TYPE_RE = re.compile(
    r"^(?:NUMBER|DECIMAL|NUMERIC|BIGNUMERIC)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$",
    re.IGNORECASE,
)

# Platform defaults when the type is bare (BigQuery SchemaField without params).
_BARE_DECIMAL_DEFAULTS: dict[str, tuple[int, int]] = {
    "NUMERIC": (38, 9),
    "BIGNUMERIC": (76, 38),
}


def parse_decimal_precision_scale(type_str: str) -> tuple[int, int] | None:
    """Parse DECIMAL/NUMERIC/NUMBER/BIGNUMERIC(p[,s]) → (precision, scale).

    Bare ``NUMERIC`` / ``BIGNUMERIC`` use BigQuery platform defaults (38,9) /
    (76,38). Bare ``DECIMAL`` / ``NUMBER`` return None (ambiguous capacity).
    MONEY / SMALLMONEY map to SQL Server currency scales.
    """
    text = (type_str or "").strip()
    upper = re.sub(r"\s+COLLATE\s+\S+", "", text, flags=re.I).strip().upper()
    if upper in {"MONEY", "CURRENCY"}:
        return 19, 4
    if upper == "SMALLMONEY":
        return 10, 4
    m = _DECIMAL_TYPE_RE.match(text)
    if not m:
        return None
    base_m = re.match(r"^(NUMBER|DECIMAL|NUMERIC|BIGNUMERIC)", text, re.IGNORECASE)
    if not base_m:
        return None
    base = base_m.group(1).upper()
    if m.group(1) is None:
        return _BARE_DECIMAL_DEFAULTS.get(base)
    precision = int(m.group(1))
    scale = int(m.group(2)) if m.group(2) is not None else 0
    if precision < 1 or scale < 0 or scale > precision:
        return None
    return precision, scale


def decimal_int_digits_and_scale(value: Any) -> tuple[int, int]:
    """Return (integer_digits, fractional_scale) for a cell value."""
    from decimal import Decimal, InvalidOperation, Overflow

    try:
        text = str(value).strip() if value is not None else ""
        if not text:
            return 0, 0
        d = Decimal(text)
        if not d.is_finite():
            return 0, 0
        _sign, digits, exponent = d.as_tuple()
        scale = -exponent if exponent < 0 else 0
        int_digits = max(0, len(digits) + exponent)
        return int_digits, scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return 0, 0


def fits_decimal(value: Any, precision: int, scale: int) -> bool:
    """True if value can be stored in DECIMAL/NUMBER(precision, scale).

    Scale overflow is fail-closed — never silently quantize/round into (p,s).
    """
    from decimal import Decimal, InvalidOperation, Overflow

    if value is None:
        return True
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return True
    try:
        d = Decimal(str(value).strip())
        if not d.is_finite():
            return False
        int_digits, value_scale = decimal_int_digits_and_scale(d)
        if value_scale > scale:
            return False
        max_int = max(0, precision - scale)
        return int_digits <= max_int and value_scale <= scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return False


def quarantine_unfit_decimals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "DECIMAL",
) -> list[tuple]:
    """Hold out / NULL cells that cannot fit DECIMAL/NUMBER(p,s).

    ``quarantine`` omits the whole row from the primary write (no NULL invent).
    ``coerce_null`` keeps the row with a NULL cell. ``fail`` stamps unfit cells and holds out rows like ``quarantine`` so
    ``reject_on_strict_policy`` can abort before bind (never rely on soft drivers).
    """
    number_cols: list[tuple[int, int, int]] = []
    for i, typ in enumerate(target_types):
        parsed = parse_decimal_precision_scale(typ)
        if parsed:
            number_cols.append((i, parsed[0], parsed[1]))
    if not number_cols:
        return mapped_rows

    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, precision, scale in number_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            if fits_decimal(cells[col_idx], precision, scale):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"decimal does not fit {dialect_label}({precision},{scale}) "
                    "— quarantined (would truncate/overflow on write)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def string_storage_units(
    value: Any,
    type_str: str,
    *,
    dialect_label: str = "",
) -> int:
    """Length units for bounded string DDL fit checks.

    - Oracle ``VARCHAR2(n BYTE)`` → UTF-8 byte length (AL32UTF8-class)
    - Redshift ``VARCHAR(n)`` → UTF-8 byte length (AWS docs; not code points)
    - SQL Server / Oracle national types → UTF-16 code units
    - Default ``VARCHAR(n)`` / ``VARCHAR2(n CHAR)`` → Unicode code points
    """
    from services.value_serializer import cell_to_string

    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            text = cell_to_string(value)
    else:
        text = value if isinstance(value, str) else cell_to_string(value)
    upper = (type_str or "").upper()
    dialect = (dialect_label or "").upper()
    # Oracle BYTE semantics — multi-byte chars consume >1 unit (Informatica FAQ).
    if re.search(r"\(\s*\d+\s*BYTE\s*\)", upper):
        return len(text.encode("utf-8"))
    # Redshift VARCHAR(n) is byte-length (AWS) — CJK/emoji would false-green on
    # code-point counts then truncate/error on write.
    if "REDSHIFT" in dialect:
        return len(text.encode("utf-8"))
    # SQL Server / Oracle national types store UTF-16 code units.
    if any(token in upper for token in ("NVARCHAR", "NCHAR", "NVARCHAR2")):
        return len(text.encode("utf-16-le")) // 2
    return len(text)


def fits_varchar(
    value: Any,
    width: int,
    type_str: str = "",
    *,
    dialect_label: str = "",
) -> bool:
    """True if value fits a bounded VARCHAR/CHAR/NVARCHAR(width) column."""
    if value is None:
        return True
    return string_storage_units(value, type_str, dialect_label=dialect_label) <= width


def binary_storage_bytes(value: Any) -> bytes | None:
    """Decode binary wire to bytes, or None when wire is invalid / empty skip."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        if not value:
            return b""
        try:
            import base64

            return base64.b64decode(value, validate=True)
        except Exception:
            return None
    return None


def fits_binary(value: Any, width: int) -> bool:
    """True if binary wire fits a bounded BINARY/VARBINARY(width) column."""
    if value is None:
        return True
    raw = binary_storage_bytes(value)
    if raw is None:
        return False
    return len(raw) <= width


def quarantine_unfit_bitstrings(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells that are not valid 0/1 bitstrings or exceed BIT(n)/VARBIT(n).

    BIT destinations must not receive base64/UTF-8 invent (BYTEA path).
    """

    from connectors.sql_bind import coerce_bitstring_wire
    from services.type_system import (
        is_bitstring_carrier,
        is_varying_bitstring_carrier,
        parse_bitstring_width,
    )
    from services.value_serializer import cell_to_string

    bit_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if is_bitstring_carrier(typ):
            bit_cols.append((i, typ))
    if not bit_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in bit_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                bits = coerce_bitstring_wire(
                    cells[col_idx],
                    width=parse_bitstring_width(typ),
                    varying=is_varying_bitstring_carrier(typ),
                )
            except ValueError as exc:
                sample = cell_to_string(cells[col_idx])[:120]
                append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": f"{exc} — quarantined",
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    cells[col_idx] = None
                else:
                    hold_out = True
                    break
                continue
            if bits is not None:
                cells[col_idx] = bits
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_binaries(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "VARBINARY",
) -> list[tuple]:
    """Hold out / NULL cells that overflow BINARY(n) or fail base64 wire decode.

    Preflight samples miss production outliers; silent truncate / UTF-8 invent
    is forbidden. Invalid base64 is quarantined (not re-encoded).
    BIT/VARBIT columns are handled by ``quarantine_unfit_bitstrings``.
    """

    from services.type_system import (
        is_bitstring_carrier,
        normalize_logical_type,
        parse_binary_carrier_width,
    )

    bin_cols: list[tuple[int, int | None, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "binary":
            continue
        if is_bitstring_carrier(typ):
            continue
        width = parse_binary_carrier_width(typ)
        bin_cols.append((i, width, typ))
    if not bin_cols:
        return mapped_rows

    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, width, typ in bin_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            raw = binary_storage_bytes(cells[col_idx])
            if raw is None:
                sample = cell_to_string(cells[col_idx])[:120]
                append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"binary wire is not valid base64 for {dialect_label} "
                    "— quarantined (refuse silent UTF-8 encode)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    cells[col_idx] = None
                else:
                    hold_out = True
                    break
                continue
            if width is not None and len(raw) > width:
                sample = cell_to_string(cells[col_idx])[:120]
                append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"binary length {len(raw)} exceeds {dialect_label}({width}) "
                    "— quarantined (would truncate on write)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    cells[col_idx] = None
                else:
                    hold_out = True
                    break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_enum_set(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    set_joiner: str = ",",
) -> list[tuple]:
    """Hold out / NULL cells outside a destination ENUM/SET member domain.

    MySQL non-strict ENUM stores invalid values as '' — silent wipe. Fail closed.
    HubSpot checkbox / Salesforce multipicklist use ``set_joiner=';'``.
    """

    from services.type_system import parse_enum_or_set_ordered_members
    from services.value_serializer import cell_to_string

    domain_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        parsed = parse_enum_or_set_ordered_members(typ)
        if not parsed:
            continue
        kind, members = parsed
        if not members:
            continue
        domain_cols.append((i, kind, typ))
    if not domain_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_enum_wire, coerce_set_wire

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind, typ in domain_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                if kind == "ENUM":
                    cells[col_idx] = coerce_enum_wire(cells[col_idx], ddl_type=typ)
                else:
                    cells[col_idx] = coerce_set_wire(
                        cells[col_idx], ddl_type=typ, joiner=set_joiner
                    )
                continue
            except ValueError:
                raw = cell_to_string(cells[col_idx])
            sample = raw[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"value not in {kind} domain — quarantined "
                    "(MySQL would store '' / drop SET members silently)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_years(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out / NULL cells outside MySQL YEAR range (0 or 1901–2155).

    Non-strict MySQL stores invalid YEAR as 0000 — silent wipe. Fail closed.
    """

    from services.type_system import is_year_carrier, year_value_fits
    from services.value_serializer import cell_to_string

    year_cols = [i for i, typ in enumerate(target_types) if is_year_carrier(typ)]
    if not year_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_year_wire

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in year_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                cells[col_idx] = coerce_year_wire(cells[col_idx])
                continue
            except ValueError:
                pass
            if year_value_fits(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                        "year outside MySQL YEAR range (0 or 1901–2155) "
                        "— quarantined (non-strict MySQL would store 0000)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_booleans(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out / NULL cells that are not canonical boolean wire forms.

    Accepts bool / 0|1 / true|false|t|f only. Refuses silent ``yes``/``Y``/``2``
    invent into BOOLEAN / BIT / TINYINT(1) destinations.
    """

    from services.type_system import boolean_value_fits, normalize_logical_type
    from services.value_serializer import cell_to_string

    bool_cols = [
        i
        for i, typ in enumerate(target_types)
        if normalize_logical_type(typ) == "boolean"
    ]
    if not bool_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in bool_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            if boolean_value_fits(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    "non-canonical boolean wire token "
                    "(accept 0|1|true|false|t|f only) — quarantined"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_temporals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out temporal cells that would silently lose FSP or timezone polarity.

    - ``TIME(6)`` → ``TIME(0)`` truncates fractional seconds
    - Offset-aware / ``Z`` wire into NTZ/DATETIME strips the offset (Airbyte invent)
    """

    from services.type_system import (
        datetime_timezone_polarity,
        normalize_logical_type,
        parse_temporal_fractional_precision,
        temporal_value_exceeds_precision,
        temporal_value_has_timezone,
    )
    from services.value_serializer import cell_to_string

    temporal_cols: list[tuple[int, str, bool, bool]] = []
    for i, typ in enumerate(target_types):
        logical = normalize_logical_type(typ)
        if logical not in {"time", "datetime"}:
            continue
        check_fsp = parse_temporal_fractional_precision(typ) is not None
        check_tz = logical == "datetime" and datetime_timezone_polarity(typ) == "ntz"
        if not check_fsp and not check_tz:
            continue
        temporal_cols.append((i, typ, check_fsp, check_tz))
    if not temporal_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ, check_fsp, check_tz in temporal_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            reason = ""
            if check_fsp and temporal_value_exceeds_precision(cells[col_idx], typ):
                reason = (
                    f"fractional seconds exceed destination {typ} "
                    "— quarantined (refuse silent truncate)"
                )
            elif check_tz and temporal_value_has_timezone(cells[col_idx]):
                reason = (
                    f"timezone-aware value into NTZ destination {typ} "
                    "— quarantined (refuse silent offset strip; use explicit UTC transform)"
                )
            if not reason:
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": reason,
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_currency_markers_into_numeric(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells that still carry currency symbols into numeric/MONEY columns.

    Identity-mapped ``$1,234.56`` must not be silently stripped to 1234.56 or
    fail mid-batch. Operator must apply an explicit currency transform first.
    """

    from services.type_system import (
        has_currency_marker,
        is_money_carrier,
        normalize_logical_type,
    )
    from services.value_serializer import cell_to_string

    numeric_cols: list[int] = []
    for i, typ in enumerate(target_types):
        logical = normalize_logical_type(typ)
        if logical in {"decimal", "integer", "float"} or is_money_carrier(typ):
            numeric_cols.append(i)
    if not numeric_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in numeric_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            if not has_currency_marker(cells[col_idx]):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    "currency marker present in numeric/MONEY cell "
                    "— quarantined (refuse silent strip; use currency transform)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def fits_integer(value: Any, type_str: str) -> bool:
    """True if value fits the signed/unsigned integer destination carrier."""
    from decimal import Decimal, InvalidOperation
    from services.type_system import integer_storage_bounds

    bounds = integer_storage_bounds(type_str)
    if bounds is None:
        return True
    if value is None:
        return True
    if isinstance(value, bool):
        # bool is a subclass of int — treat as 0/1.
        n = int(value)
    elif isinstance(value, int):
        n = value
    else:
        try:
            text = str(value).strip()
            if not text:
                return True
            n = int(Decimal(text))
        except (InvalidOperation, ValueError, TypeError, OverflowError):
            # Non-numeric — leave for type coercion / other quarantine paths.
            return True
    lo, hi = bounds
    return lo <= n <= hi


def quarantine_unfit_integers(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "INTEGER",
) -> list[tuple]:
    """Hold out / NULL cells that overflow signed/unsigned integer destinations.

    Mirrors DECIMAL/VARCHAR write quarantine: preflight samples can miss production
    outliers (UINT32 into INT, BIGINT into SMALLINT). Fail-closed — never wrap.
    """

    from services.type_system import integer_storage_bounds, normalize_logical_type

    int_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "integer":
            continue
        if integer_storage_bounds(typ) is None:
            continue
        int_cols.append((i, typ))
    if not int_cols:
        return mapped_rows

    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in int_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            if fits_integer(cells[col_idx], typ):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"integer does not fit {dialect_label}({typ}) "
                    "— quarantined (would overflow/wrap on write)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_strings(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "VARCHAR",
) -> list[tuple]:
    """Hold out / NULL cells that exceed bounded VARCHAR/NVARCHAR/CHAR width.

    Preflight only sees samples — production rows longer than samples used to
    reach SQL Server and silently truncate. Quarantine is fail-closed.
    Unlimited carriers (TEXT, NVARCHAR(MAX), STRING) are skipped.
    """

    from services.ddl_compatibility import parse_varchar_width

    width_cols: list[tuple[int, int, str]] = []
    for i, typ in enumerate(target_types):
        width = parse_varchar_width(typ)
        if width is not None:
            width_cols.append((i, width, typ))
    if not width_cols:
        return mapped_rows

    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, width, typ in width_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            if fits_varchar(
                cells[col_idx], width, typ, dialect_label=dialect_label
            ):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            units = string_storage_units(
                cells[col_idx], typ, dialect_label=dialect_label
            )
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"value length {units} exceeds {dialect_label}({width}) "
                    "— quarantined (would truncate on write)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


# Engines whose ARRAY element slot is non-nullable by contract.
# BigQuery raises when a result ARRAY contains NULL elements
# (https://docs.cloud.google.com/bigquery/docs/arrays); ClickHouse ``Array(T)``
# only accepts NULL when declared ``Array(Nullable(T))``.
_ARRAY_NULL_STRICT_DIALECTS = ("bigquery", "big query", "bq", "clickhouse")

# BigQuery does not support arrays of arrays — an ARRAY of STRUCT is required.
_ARRAY_NESTED_FORBIDDEN_DIALECTS = ("bigquery", "big query", "bq")


def parse_array_wire_elements(value: Any) -> tuple[list[Any] | None, str | None]:
    """Parse array wire into elements for element-level fidelity checks.

    Returns ``(elements, error)``. ``(None, None)`` means *ambiguous* — a bare
    scalar that may legitimately be a SET joiner payload or engine-native
    literal. Ambiguity is never quarantined; only unambiguous breakage is,
    so this gate cannot produce false holdouts.
    """
    if value is None:
        return None, None
    if isinstance(value, (list, tuple)):
        return list(value), None
    if isinstance(value, dict):
        return None, "object/dict payload cannot populate an ARRAY column"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None, "binary payload cannot populate an ARRAY column"
    if not isinstance(value, str):
        return None, None

    text = value.strip()
    if not text:
        return None, None
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except Exception:
            return None, "malformed JSON array payload"
        if not isinstance(parsed, list):
            return None, "JSON payload is not an array"
        return parsed, None
    if text.startswith("{") and text.endswith("}"):
        # Postgres array literal ``{a,b,NULL}`` (unquoted NULL is a real NULL;
        # quoted "NULL" is the literal string) — PG docs 8.15.
        try:
            parsed_obj = json.loads(text)
        except Exception:
            return _parse_pg_array_literal(text), None
        if isinstance(parsed_obj, dict):
            return None, "JSON object payload cannot populate an ARRAY column"
        return _parse_pg_array_literal(text), None
    return None, None


def _parse_pg_array_literal(text: str) -> list[Any]:
    """Split a Postgres ``{a,b,"c,d",NULL}`` literal into elements."""
    body = text[1:-1]
    if not body.strip():
        return []
    elements: list[Any] = []
    buf: list[str] = []
    in_quotes = False
    escaped = False
    for ch in body:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == "," and not in_quotes:
            elements.append(_pg_array_element(("".join(buf)).strip()))
            buf = []
            continue
        buf.append(ch)
    elements.append(_pg_array_element(("".join(buf)).strip()))
    return elements


def _pg_array_element(raw: str) -> Any:
    if raw.upper() == "NULL":
        return None
    if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw


def _is_numeric_wire(value: Any) -> bool:
    """True when a cell parses as a finite number (never invent 0 from text)."""
    from decimal import Decimal, InvalidOperation

    if isinstance(value, bool) or isinstance(value, (int, float)):
        return True
    try:
        return Decimal(str(value).strip()).is_finite()
    except (InvalidOperation, ValueError, TypeError, ArithmeticError):
        return False


def _is_temporal_wire(value: Any) -> bool:
    """True when a cell parses as an ISO-8601 date / time / timestamp."""
    from datetime import date, datetime, time

    if isinstance(value, (datetime, date, time)):
        return True
    text = str(value).strip()
    if not text:
        return True
    # ``fromisoformat`` gained ``Z`` support in 3.11; normalize for older runtimes.
    normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    normalized = normalized.replace(" ", "T", 1) if " " in normalized else normalized
    for parser in (datetime.fromisoformat, date.fromisoformat, time.fromisoformat):
        try:
            parser(normalized)
            return True
        except (ValueError, TypeError):
            continue
    return False


def array_element_unfit_reason(element: Any, carrier: str) -> str | None:
    """Reason an element cannot fit the ARRAY element carrier, else None.

    Reuses the scalar fit SSOT (``fits_integer`` / ``fits_decimal`` /
    ``fits_varchar`` / ``boolean_value_fits``) so array fidelity can never
    drift from column fidelity.
    """
    if element is None:
        return None
    carrier = (carrier or "").strip()
    if not carrier:
        return None

    from services.type_system import (
        boolean_value_fits,
        integer_storage_bounds,
        normalize_logical_type,
    )

    if isinstance(element, (list, tuple, dict)):
        # Structured elements are validated by the nested/struct path, not here.
        return None

    logical = normalize_logical_type(carrier)
    decimal_parsed = parse_decimal_precision_scale(carrier)
    if decimal_parsed and not fits_decimal(element, decimal_parsed[0], decimal_parsed[1]):
        return f"element does not fit {carrier}"
    if logical == "integer" and integer_storage_bounds(carrier) is not None:
        # ``fits_integer`` intentionally passes non-numeric text through so the
        # column-level coercion gate can report it. Inside an ARRAY there is no
        # such follow-up gate, so parseability is enforced here.
        if not _is_numeric_wire(element):
            return f"element is not numeric for {carrier}"
        if not fits_integer(element, carrier):
            return f"element does not fit {carrier}"
    if logical == "boolean" and not boolean_value_fits(element):
        return f"element is not a canonical boolean for {carrier}"
    if logical in {"date", "time", "datetime"} and not _is_temporal_wire(element):
        return f"element is not a parseable temporal for {carrier}"

    from services.ddl_compatibility import parse_varchar_width

    width = parse_varchar_width(carrier)
    if width is not None and not fits_varchar(element, width, carrier):
        return f"element exceeds {carrier}"
    return None


def quarantine_unfit_arrays(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "destination",
) -> list[tuple]:
    """Hold out ARRAY cells whose payload or elements cannot reach the column.

    Scalar carriers each had a fail-closed gate while ARRAY columns had none —
    malformed payloads and unfit elements reached PG ``int[]``, BigQuery
    ``ARRAY<INT64>``, ClickHouse ``Array(T)``, and lakehouse list columns
    unchecked. Engine rules enforced here:

    - BigQuery result ARRAYs may not contain NULL elements, and arrays of
      arrays are unsupported (ARRAY of STRUCT is required).
    - ClickHouse ``Array(T)`` rejects NULL unless declared ``Array(Nullable(T))``.
    - Postgres permits both NULL arrays and NULL elements.

    Ambiguous scalars (SET joiner text, engine-native literals) are never held
    out — only unambiguous breakage is.
    """

    from services.type_system import (
        LOGICAL_ARRAY,
        normalize_logical_type,
        parse_array_element,
    )

    array_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        raw = (typ or "").strip()
        if not raw:
            continue
        is_array = raw.endswith("[]") or normalize_logical_type(raw) == LOGICAL_ARRAY
        if not is_array:
            continue
        element = (
            raw[:-2].strip() if raw.endswith("[]") else (parse_array_element(raw) or "")
        )
        array_cols.append((i, element, raw))
    if not array_cols:
        return mapped_rows

    label = (dialect_label or "destination").strip() or "destination"
    low = label.lower()
    null_strict = any(d in low for d in _ARRAY_NULL_STRICT_DIALECTS)
    nested_forbidden = any(d in low for d in _ARRAY_NESTED_FORBIDDEN_DIALECTS)

    from services.value_serializer import cell_to_string, is_missing_sentinel

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, element_carrier, typ in array_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            if is_missing_sentinel(cells[col_idx]):
                continue
            elements, parse_error = parse_array_wire_elements(cells[col_idx])
            reason = ""
            if parse_error:
                reason = f"{parse_error} — quarantined (would not load into {typ})"
            elif elements is not None:
                for element in elements:
                    if element is None:
                        if null_strict:
                            reason = (
                                f"{label} ARRAY element may not be NULL "
                                f"(declare Array(Nullable(T)) or drop the element) "
                                f"— quarantined"
                            )
                            break
                        continue
                    if nested_forbidden and isinstance(element, (list, tuple)):
                        reason = (
                            f"{label} does not support arrays of arrays "
                            "(use ARRAY of STRUCT) — quarantined"
                        )
                        break
                    unfit = array_element_unfit_reason(element, element_carrier)
                    if unfit:
                        reason = f"{unfit} — quarantined (would not load into {typ})"
                        break
            if not reason:
                continue
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": cell_to_string(cells[col_idx])[:120],
                    "reason": f"{label} ARRAY: {reason}",
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_json(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "destination",
) -> list[tuple]:
    """Hold out payloads that intend to be JSON documents but are malformed.

    ``coerce_json_wire`` losslessly wraps bare scalars, so plain text into a
    JSON column is not a fidelity loss and is left alone. A payload that is
    clearly an intended object/array but fails to parse would instead be
    wrapped as a JSON *string* — silently degrading a document into text.
    That is the fail-closed case.
    """

    from services.type_system import LOGICAL_JSON, normalize_logical_type

    json_cols = [
        i
        for i, typ in enumerate(target_types)
        if normalize_logical_type(typ) == LOGICAL_JSON
    ]
    if not json_cols:
        return mapped_rows

    label = (dialect_label or "destination").strip() or "destination"

    from services.value_serializer import cell_to_string, is_missing_sentinel

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in json_cols:
            value = cells[col_idx] if col_idx < len(cells) else None
            if value is None or not isinstance(value, str):
                continue
            if is_missing_sentinel(value):
                continue
            text = value.strip()
            looks_structured = (text.startswith("{") and text.endswith("}")) or (
                text.startswith("[") and text.endswith("]")
            )
            if not looks_structured:
                continue
            try:
                json.loads(text)
                continue
            except Exception:
                pass
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": cell_to_string(value)[:120],
                    "reason": (
                        f"{label} JSON: malformed document payload — quarantined "
                        "(would silently store as a JSON string, not an object)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def apply_write_quarantine_matrix(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "destination",
    mappings: list[dict[str, Any]] | None = None,
) -> list[tuple]:
    """Shared fail-closed quarantine matrix for every typed write path.

    Object stores (S3/GCS/ADLS), lakehouse, SQL, and SaaS reverse-ETL must not
    invent UTF-8 binaries, silently overflow DECIMAL/VARCHAR, or leak unfit
    temporals — same honesty bar as Postgres/BQ (Airbyte/Fivetran class).
    Unbounded carriers (JSON STRING, Iceberg string) no-op for width checks.

    When ``mappings`` is provided, holdouts dual-stamp ``source_values`` for
    quarantine replay (Wave 34).
    """
    token = _active_quarantine_mappings.set(mappings)
    try:
        label = (dialect_label or "destination").strip() or "destination"
        mapped_rows = quarantine_currency_markers_into_numeric(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_decimals(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=f"{label} DECIMAL",
        )
        mapped_rows = quarantine_unfit_years(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_booleans(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_temporals(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_specialty_types(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_integers(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=f"{label} INTEGER",
        )
        mapped_rows = quarantine_unfit_bitstrings(
            mapped_rows, target_cols, target_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_binaries(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=f"{label} BINARY",
        )
        mapped_rows = quarantine_unfit_enum_set(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            set_joiner=(
                ";"
                if str(dialect_label or "").lower()
                in {"hubspot", "salesforce", "zendesk"}
                else ","
            ),
        )
        mapped_rows = quarantine_unfit_strings(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=f"{label} VARCHAR",
        )
        mapped_rows = quarantine_unfit_arrays(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=label,
        )
        mapped_rows = quarantine_unfit_json(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=label,
        )
        return mapped_rows
    finally:
        _active_quarantine_mappings.reset(token)


def _specialty_column_kind(type_str: str) -> str | None:
    """Return specialty kind when destination DDL needs wire-shape quarantine.

    String/VARCHAR carriers (Databricks/Iceberg) are skipped for geo/interval —
    quarantine applies only when the destination expects specialty bind.
    """
    from services.type_system import normalize_logical_type, parse_vector_dimension

    logical = normalize_logical_type(type_str)
    if logical == "geography":
        return "geography"
    if logical == "interval":
        return "interval"
    if logical == "vector" and parse_vector_dimension(type_str) is not None:
        return "vector"
    upper = (type_str or "").upper()
    if re.search(
        r"\b(GEOGRAPHY|GEOMETRY|SDO_GEOMETRY|POINT|LINESTRING|POLYGON|"
        r"MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\b",
        upper,
    ):
        return "geography"
    if re.search(r"\bINTERVAL\b", upper):
        return "interval"
    return None


def quarantine_unfit_specialty_types(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells unfit for GEOGRAPHY / INTERVAL / VECTOR(n) destinations.

    Specialty types travel as identity payloads (WKT/GeoJSON/ISO-8601/float lists).
    Fail-closed — never invent empty geometry, wrong interval family, or pad/truncate
    embedding dimensions (pgvector/Snowflake VECTOR reject wrong width).
    """
    specialty_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        kind = _specialty_column_kind(typ)
        if kind:
            specialty_cols.append((i, kind, typ))
    if not specialty_cols:
        return mapped_rows

    from services.schema_inference import (
        geography_wire_srid,
        interval_wire_family,
        is_geography_wire,
        is_interval_wire,
    )
    from services.type_system import (
        interval_family,
        parse_geography_srid,
        parse_vector_dimension,
        parse_vector_length,
    )
    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind, typ in specialty_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]):
                continue
            reason = ""
            ok = True
            if kind == "geography":
                ok = is_geography_wire(cells[col_idx])
                if ok:
                    dest_srid = parse_geography_srid(typ)
                    wire_srid = geography_wire_srid(cells[col_idx])
                    if (
                        dest_srid is not None
                        and wire_srid is not None
                        and dest_srid != wire_srid
                    ):
                        ok = False
                        reason = (
                            f"geography SRID mismatch wire={wire_srid} dest={dest_srid} "
                            "— quarantined (refuse silent reproject)"
                        )
            elif kind == "interval":
                ok = is_interval_wire(cells[col_idx])
                if ok:
                    dest_fam = interval_family(typ)
                    wire_fam = interval_wire_family(cells[col_idx])
                    if dest_fam and wire_fam and dest_fam != wire_fam:
                        ok = False
                        reason = (
                            f"interval family mismatch wire={wire_fam} dest={dest_fam} "
                            "— quarantined (YEAR-MONTH ↔ DAY-SECOND collapse)"
                        )
            elif kind == "vector":
                dest_dim = parse_vector_dimension(typ)
                wire_len = parse_vector_length(cells[col_idx])
                if dest_dim is None:
                    ok = True
                elif wire_len is None:
                    ok = False
                    reason = (
                        f"value is not a parseable VECTOR({dest_dim}) payload "
                        "— quarantined (refuse invent embedding)"
                    )
                elif wire_len != dest_dim:
                    ok = False
                    reason = (
                        f"vector length {wire_len} ≠ destination VECTOR({dest_dim}) "
                        "— quarantined (refuse pad/truncate embedding)"
                    )
            if ok:
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": reason
                    or (
                    f"value is not a valid {kind} wire payload "
                    "— quarantined (would fail destination bind or invent a cast)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                cells[col_idx] = None
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def omit_generated_always_columns(
    target_cols: list[str],
    target_types: list[str],
    mapped_rows: list[tuple],
) -> tuple[list[str], list[str], list[tuple], list[str]]:
    """Drop GENERATED ALWAYS columns from INSERT projections.

    Returns (cols, types, rows, omitted_names). SERIAL / BY DEFAULT / AUTO_INCREMENT
    stay — clients may supply explicit keys for migrations. ALWAYS must be omitted
    or the engine rejects / overwrites sequences mid-batch.
    """
    from services.type_system import is_generated_always_column

    keep: list[int] = []
    omitted: list[str] = []
    for i, typ in enumerate(target_types):
        if is_generated_always_column(typ):
            omitted.append(target_cols[i])
        else:
            keep.append(i)
    if not omitted:
        return target_cols, target_types, mapped_rows, []
    new_cols = [target_cols[i] for i in keep]
    new_types = [target_types[i] for i in keep]
    new_rows = [tuple(row[i] for i in keep if i < len(row)) for row in mapped_rows]
    return new_cols, new_types, new_rows, omitted


def row_has_missing_sentinel(row: tuple | list) -> bool:
    from services.value_serializer import is_missing_sentinel

    return any(is_missing_sentinel(v) for v in row)


def materialize_missing_as_null_for_dense_write(
    mapped_rows: list[tuple],
) -> list[tuple]:
    """Dense INSERT/COPY/MERGE stage: absent fields → SQL NULL (never bind sentinel).

    Sparse CDC upsert must keep ``DF_MISSING`` and omit columns from SET.
    Full-refresh / create-new / dense bulk loads union a schemaless schema —
    missing keys are SQL NULL, not the literal ``__DF_MISSING__`` string
    (Snowflake BOOL / Postgres BOOLEAN reject that string).
    """
    from services.value_serializer import is_missing_sentinel

    if not mapped_rows or not any(row_has_missing_sentinel(r) for r in mapped_rows):
        return mapped_rows
    out: list[tuple] = []
    for row in mapped_rows:
        if not row_has_missing_sentinel(row):
            out.append(row)
            continue
        out.append(tuple(None if is_missing_sentinel(v) else v for v in row))
    return out


def split_dense_sparse_rows(
    mapped_rows: list[tuple],
) -> tuple[list[tuple], list[tuple]]:
    """Partition mapped rows for bulk vs per-row sparse CDC upsert."""
    dense: list[tuple] = []
    sparse: list[tuple] = []
    for row in mapped_rows:
        (sparse if row_has_missing_sentinel(row) else dense).append(row)
    return dense, sparse


def combined_mapped_rows_for_checksum(
    dense_rows: list[tuple],
    sparse_rows: list[tuple] | None = None,
) -> list[tuple]:
    """Dense + sparse rows for writer ack checksum (sparse must not vanish from proof)."""
    if not sparse_rows:
        return list(dense_rows)
    return list(dense_rows) + list(sparse_rows)


def sparse_present_bindings(
    row: tuple | list,
    target_cols: list[str],
) -> dict[str, Any]:
    """Column→value for cells that are present (not DF_MISSING)."""
    from services.value_serializer import is_missing_sentinel

    out: dict[str, Any] = {}
    for col, val in zip(target_cols, row):
        if is_missing_sentinel(val):
            continue
        out[col] = val
    return out


def materialize_sparse_row_for_checksum(
    present: dict[str, Any],
    existing: dict[str, Any] | None,
    target_cols: list[str],
) -> tuple:
    """Post-apply row image for checksum — absent CDC fields keep destination values.

    Omit-from-SET never writes DF_MISSING; fingerprinting the sentinel would
    falsely fail Gate-8 read-back against preserved destination cells.
    """
    out: list[Any] = []
    for col in target_cols:
        if col in present:
            out.append(present[col])
        elif existing is not None and col in existing:
            out.append(existing[col])
        else:
            out.append(None)
    return tuple(out)


def run_sparse_cdc_upsert(
    *,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
    fetch_existing_row: Callable[[list[Any]], tuple | list | None],
    update_non_pk: Callable[[dict[str, Any], list[Any]], int],
    insert_present: Callable[[dict[str, Any]], None],
) -> tuple[int, int, list[tuple]]:
    """Shared sparse CDC upsert: omit DF_MISSING, LSN skip, hydrate checksum rows.

    ``fetch_existing_row(pk_vals)`` must return a full row in ``target_cols`` order
    (or ``None``). ``update_non_pk(non_pk, pk_vals)`` returns affected rowcount.
    """
    from services.cdc_effectively_once import should_apply_pk_row

    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("sparse CDC upsert requires conflict_columns")
    written = 0
    skipped = 0
    checksum_rows: list[tuple] = []
    for row in sparse_rows:
        present = sparse_present_bindings(row, target_cols)
        assert_sparse_upsert_has_pk(present, conflict)
        non_pk = {k: v for k, v in present.items() if k not in conflict}
        pk_vals = [present[c] for c in conflict]
        existing_tuple = fetch_existing_row(pk_vals)
        existing = (
            dict(zip(target_cols, existing_tuple))
            if existing_tuple is not None
            else None
        )
        if (
            existing is not None
            and DF_LSN_COL in present
            and DF_LSN_COL in target_cols
        ):
            if not should_apply_pk_row(
                existing_lsn=existing.get(DF_LSN_COL),
                incoming_lsn=present[DF_LSN_COL],
            ).applied:
                skipped += 1
                continue
        if non_pk:
            affected = update_non_pk(non_pk, pk_vals)
            if affected and affected > 0:
                written += 1
                checksum_rows.append(
                    materialize_sparse_row_for_checksum(present, existing, target_cols)
                )
                continue
        try:
            insert_present(present)
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
        except Exception:
            if not non_pk:
                raise
            update_non_pk(non_pk, pk_vals)
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
    return written, skipped, checksum_rows


def assert_sparse_upsert_has_pk(
    present: dict[str, Any],
    conflict_columns: list[str],
) -> None:
    """Refuse sparse upsert that omits the conflict key (would invent a row)."""
    missing = [c for c in conflict_columns if c not in present]
    if missing:
        raise ValueError(
            "sparse CDC upsert is missing primary-key column(s) "
            f"{missing}; refuse silent invent (require binlog_row_image=FULL / "
            "REPLICA IDENTITY FULL)"
        )
