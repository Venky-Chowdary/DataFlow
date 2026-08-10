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


def quarantine_cell_wire(value: Any) -> str:
    """Serialize one quarantine cell without inventing empty string for SQL NULL.

    Replay / DLQ must round-trip polarity:
    * ``None`` / ``SQL_NULL_SENTINEL`` → ``SQL_NULL_SENTINEL`` (apply_transform → NULL)
    * ``DF_MISSING`` → ``DF_MISSING_SENTINEL`` (sparse omit on rewrite)
    * NaN / NA → ``SQL_NULL_SENTINEL`` (not empty invent)
    * other values → ``cell_to_string`` (preserve_sql_null)
    """
    from services.value_serializer import (
        DF_MISSING_SENTINEL,
        SQL_NULL_SENTINEL,
        cell_to_string,
        is_missing_sentinel,
    )

    if value is None:
        return SQL_NULL_SENTINEL
    if is_missing_sentinel(value):
        return DF_MISSING_SENTINEL
    if isinstance(value, str) and value.strip() == SQL_NULL_SENTINEL:
        return SQL_NULL_SENTINEL
    # cell_to_string(preserve_sql_null=True) maps NA/NaN → SQL_NULL_SENTINEL.
    return cell_to_string(value, preserve_sql_null=True)


def mapped_row_quarantine_values(row: Any, target_cols: list[str]) -> dict[str, str]:
    """Target-column dict for quarantine replay (full row, not single bad cell).

    Accepts tuple/list bind images and SQLAlchemy/sparse ``dict`` rows — never
    ``list(dict)`` key-order invent (that poisoned generic_sql salvage replay).
    """
    from services.value_serializer import DF_MISSING_SENTINEL

    out: dict[str, str] = {}
    if isinstance(row, dict):
        for col in target_cols or []:
            key = str(col)
            if key in row:
                out[key] = quarantine_cell_wire(row[key])
            elif col in row:
                out[key] = quarantine_cell_wire(row[col])
            else:
                out[key] = DF_MISSING_SENTINEL
        return out
    seq = list(row) if row is not None else []
    for i, col in enumerate(target_cols or []):
        if i >= len(seq):
            # Column absent from the mapped tuple — sparse omit, not empty invent.
            out[str(col)] = DF_MISSING_SENTINEL
            continue
        out[str(col)] = quarantine_cell_wire(seq[i])
    return out


def omit_missing_fields(
    pairs: Any,
    *,
    drop_empty: bool = True,
) -> dict[str, Any]:
    """Build a write payload omitting ``DF_MISSING`` (STOP_COLUMN / coerce_null).

    SaaS/document writers must never serialize ``__DF_MISSING__`` as a live
    property value. Empty strings are dropped when ``drop_empty`` (CRM upsert
    class); pass ``drop_empty=False`` when empty is a meaningful clear.
    """
    from services.value_serializer import is_missing_sentinel

    out: dict[str, Any] = {}
    for item in pairs:
        if not isinstance(item, (tuple, list)) or len(item) < 2:
            continue
        k, v = item[0], item[1]
        if v is None or is_missing_sentinel(v):
            continue
        if drop_empty and str(v) == "":
            continue
        out[str(k)] = v
    return out


def mapped_rows_to_json_records(
    mapped_rows: list[tuple],
    target_cols: list[str],
    dest_types: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Object-store / document JSON records — omit ``DF_MISSING`` keys (Kafka-class).

    Dense CSV still gets empty cells for omitted keys via DictWriter fieldnames;
    JSON/JSONL must never emit ``\"col\": null`` for STOP_COLUMN / sparse CDC.
    """
    from services.value_serializer import is_missing_sentinel

    dest_types = dest_types or {}
    records: list[dict[str, Any]] = []
    for row in mapped_rows:
        rec: dict[str, Any] = {}
        for col, val in zip(target_cols, row):
            if is_missing_sentinel(val):
                continue
            rec[col] = to_json_value(val, col, dest_types)
        records.append(rec)
    return records


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
            out[src] = quarantine_cell_wire(target_values[tgt])
        elif src in target_values:
            out[src] = quarantine_cell_wire(target_values[src])
    return out


def saas_quarantine_values(payload: dict[str, Any] | None) -> dict[str, str]:
    """Wire a CRM/SaaS payload dict for quarantine replay (NULL polarity honest)."""
    return {
        str(k): quarantine_cell_wire(v) for k, v in (payload or {}).items()
    }


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
    # Normalize the fault-cell sample so replay overwrite cannot re-invent "".
    d["value"] = quarantine_cell_wire(d.get("value"))
    # Full mapped-row image first (SQL NULL polarity), then overlay any CRM
    # payload keys — HubSpot/SF omit None from props so bag-only would drop NULLs.
    base_values = mapped_row_quarantine_values(mapped_row, target_cols)
    if isinstance(d.get("values"), dict) and d["values"]:
        wired = {str(k): quarantine_cell_wire(v) for k, v in d["values"].items()}
        d["values"] = {**base_values, **wired}
    else:
        d["values"] = base_values
    if not (isinstance(d.get("source_values"), dict) and d["source_values"]):
        maps = mappings if mappings is not None else _active_quarantine_mappings.get()
        if maps:
            src = project_quarantine_source_values(d["values"], maps)
            if src:
                d["source_values"] = src
    elif isinstance(d.get("source_values"), dict):
        d["source_values"] = {
            str(k): quarantine_cell_wire(v) for k, v in d["source_values"].items()
        }
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
    sentinel string. Prefer ``mapped_rows_to_json_records`` which omits the key
    entirely (Kafka-class). This helper still maps the sentinel to ``None`` as a
    last-resort safety net so dense serializers cannot leak ``__DF_MISSING__``.
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
        try:
            coerced = coerce_sql_temporal(value, ddl)
            wire = format_wire_value(value, ddl)
        except ValueError:
            # Dense JSON serializers: keep empty for quarantine upstream —
            # never invent SQL/JSON null from "".
            return value
        if wire is not None:
            return wire
        if coerced is not value:
            return coerced
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            # Typed sinks: leave raw empty so quarantine_unfit_* / bind hold out —
            # never invent JSON null / 0 from "".
            if ctype in {
                "float",
                "integer",
                "decimal",
                "number",
                "boolean",
                "uuid",
                "json",
                "array",
                "struct",
                "map",
            }:
                raise ValueError(
                    f"empty string cannot coerce to {ctype} — "
                    "refuse silent NULL invent (quarantine or remap upstream)"
                )
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
        if ctype in {"text", "string", "varchar", "uuid", "binary", ""}:
            # Empty/unknown logical type: keep string — never invent int/bool/float
            # via json.loads (object-store export type invent).
            return value
        if ctype in {"boolean", "bool", "bit"}:
            from connectors.sql_bind import coerce_boolean_wire

            coerced = coerce_boolean_wire(value)
            if not isinstance(coerced, bool):
                raise ValueError(
                    f"JSON export BOOLEAN refused unrecognized value {value!r}"
                )
            return coerced
        if ctype in {
            "integer",
            "int",
            "bigint",
            "smallint",
            "tinyint",
            "decimal",
            "numeric",
            "float",
            "double",
            "real",
            "number",
        }:
            try:
                def _reject_num(name: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {name}")

                parsed = json.loads(text, parse_constant=_reject_num)
            except (json.JSONDecodeError, ValueError):
                return value
            if isinstance(parsed, (int, float)) and not isinstance(parsed, bool):
                return parsed
            return value
        # Unknown typed column: leave text — refuse schema invent.
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
                    try:
                        cells[i] = format_bigquery_bind(cells[i], typ)
                    except ValueError:
                        pass
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
            try:
                cells[i] = coerce_sql_temporal(cells[i], ddl)
            except ValueError:
                # Leave raw for bind_sql_mapped_rows_with_quarantine — do not
                # abort the whole batch on one empty DATE/TIMESTAMP cell.
                pass
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
        # Align with lsn_is_newer: empty/None incoming must not overwrite a
        # stamped destination row (at-least-once redelivery regress).
        if not lsn_is_newer(incoming, prior):
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
    """Keep the last occurrence of each conflict key, preserving tuple order.

    Conflict names are resolved case-insensitively (strict). Partial composite
    PKs raise — never silently dedupe on a weaker key and drop sibling rows.
    """
    if not conflict_columns or not rows:
        return rows
    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        return rows
    indices = [target_cols.index(c) for c in conflict]
    seen: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[i] for i in indices)
        seen[key] = row
    return list(seen.values())


# Destination metadata column for CDC monotonic apply — owned by lsn_guards.
from connectors.lsn_guards import DF_LSN_COL  # noqa: E402


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


def written_ids_from_mapped_rows(
    mapped_rows: list[Any],
    target_cols: list[str],
    conflict_columns: list[str] | None,
    *,
    id_limit: int = 500,
) -> list[str] | None:
    """Extract single-column PK values for keyed Gate-8 read-back.

    Composite PKs return ``None`` (full-table checksum remains the authority).
    Upsert/append into a non-empty sink must fingerprint only the batch keys —
    whole-table digests are not comparable to a partial batch (Airbyte/Fivetran
    honesty: prove what we wrote, report full cardinality separately).
    """
    if not mapped_rows or not target_cols or not conflict_columns:
        return None
    if len(conflict_columns) != 1:
        return None
    pk = str(conflict_columns[0] or "").strip()
    if not pk or pk not in target_cols:
        return None
    idx = target_cols.index(pk)
    out: list[str] = []
    seen: set[str] = set()
    for row in mapped_rows:
        if isinstance(row, dict):
            raw = row.get(pk)
        else:
            raw = row[idx] if idx < len(row) else None
        if raw is None or raw == "":
            continue
        marker = str(raw)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(marker)
        if len(out) >= max(0, int(id_limit)):
            break
    return out or None


def gate8_writer_meta(
    mapped_rows: list[Any],
    target_cols: list[str],
    written_ids: list[str] | None = None,
    *,
    sample_limit: int = 50,
    id_limit: int = 500,
    conflict_columns: list[str] | None = None,
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
    ids = written_ids
    if ids is None and conflict_columns:
        ids = written_ids_from_mapped_rows(
            mapped_rows, target_cols, conflict_columns, id_limit=id_limit
        )
    meta: dict[str, Any] = {
        "reconcile_sample": sample,
        "source_row_count": len(mapped_rows),
    }
    if ids is not None:
        meta["written_ids"] = [str(x) for x in ids[: max(0, int(id_limit))]]
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


# CDC LSN guards (compare / dedupe / dialect predicates) live in
# ``connectors.lsn_guards``; re-exported for the historical import surface.
from connectors.lsn_guards import (  # noqa: E402,F401 — re-export
    bigquery_lsn_match_predicate,
    compare_lsn,
    dedupe_rows_by_pk_and_lsn,
    extract_cdc_lsn,
    gtid_set_contains,
    gtid_watermark_window_closed,
    lsn_family,
    lsn_is_newer,
    lsn_sort_key,
    mysql_lsn_values_newer_sql,
    parse_mysql_gtid_set,
    postgres_lsn_update_guard_sql,
    snowflake_lsn_match_predicate,
    sqlite_lsn_update_guard_sql,
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
    transform_errors: list[str] | None = None,
) -> str | None:
    """Return an error message when the write must refuse a partial primary write.

    Module 1b: Migration Risk Contract FAIL_JOB aborts even when the job-level
    error_policy is ``quarantine``. Continue-policy contract failures
    (CAST_AND_CONTINUE / QUARANTINE_ROW) may hold out rows without aborting.

    ``transform_errors`` is the writer audit list. Strict mode previously aborted
    on ``transform_errors and policy==fail`` *after* continue-contract holdouts
    were applied — inventing a full-job failure while quarantine already held
    empty url/email cells (MySQL→Postgres image→TEXT). Continue-only details
    must clear both paths.
    """
    details = list(rejected_details or [])
    errs = [str(e) for e in (transform_errors or []) if e]

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
        # Details are per-cell findings; counting them as "rows" invented 28k
        # rejects on a 2k-row Excel load (operator panic / wrong Resume hint).
        cell_n = int(n or len(details))
        row_n = len({int(d.get("row") or 0) for d in details if int(d.get("row") or 0) > 0})
        return (
            f"{label} rejected {cell_n} cell finding(s) across {row_n or cell_n} row(s); "
            f"Migration Risk Contract abort policy blocks partial write{scope_note}"
        )

    if details and rejected_details_are_continue_contract_only(details):
        # Operator contracted continue + quarantine — do not abort the batch,
        # even when transform_errors still list the held-out cells for audit.
        return None

    if transform_error_policy(policy) == "fail":
        if details:
            cell_n = len(details)
            row_n = len(
                {int(d.get("row") or 0) for d in details if int(d.get("row") or 0) > 0}
            )
            return (
                f"{label} rejected {cell_n} cell finding(s) across {row_n or cell_n} row(s); "
                "strict error policy blocks partial write"
            )
        if errs:
            return f"Transform errors: {'; '.join(errs[:3])}"
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
    empty_cells_as_null: bool = False,
    destination_column_nullability: dict[str, bool] | None = None,
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
        empty_cells_as_null=empty_cells_as_null,
        destination_column_nullability=destination_column_nullability,
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


def _is_blank_cell(val: Any) -> bool:
    if val is None:
        return True
    return str(val).strip() == ""


def _is_empty_typed_coerce_error(err: str | None) -> bool:
    msg = str(err or "").strip().lower()
    return msg.startswith("empty value cannot coerce")


def _target_explicitly_not_null(
    mapping: dict[str, Any] | None,
    tgt_name: str,
    dest_nullability: dict[str, bool],
) -> bool:
    """True only when Map/live catalog proves NOT NULL (unknown ⇒ nullable)."""
    m = mapping if isinstance(mapping, dict) else {}
    tgt_nullable = m.get("target_nullable")
    if tgt_nullable is None:
        tgt_nullable = m.get("nullable")
    if tgt_nullable is None and dest_nullability:
        key = str(tgt_name or "").strip()
        if key in dest_nullability:
            tgt_nullable = dest_nullability[key]
        else:
            tgt_nullable = dest_nullability.get(key.lower())
    if tgt_nullable is False:
        return True
    if isinstance(tgt_nullable, str) and tgt_nullable.strip().lower() in {
        "false",
        "0",
        "no",
    }:
        return True
    return False


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
    destination_column_nullability: dict[str, bool] | None = None,
    empty_cells_as_null: bool = False,
) -> tuple[list[tuple], list[str], list[dict[str, Any]]]:
    """Returns mapped rows, error messages, and structured rejected-row details.

    ``empty_cells_as_null`` (file/Excel/CSV sources): blank cells into nullable
    typed columns become SQL NULL — spreadsheet absence, not silent loss of a
    present value. NOT NULL destinations still fail/quarantine. DB→DB empty
    strings keep requiring a Risk Contract unless this flag is set.
    """
    from services.json_intelligence import materialize_struct_policies

    column_types = column_types or {}
    policy = transform_error_policy(
        error_policy, allow_coerce_null=allow_job_coerce_null
    )
    # Honor Map STRUCT policy (JSON blob vs flatten top-level keys) before bind.
    headers, data_rows = materialize_struct_policies(headers, data_rows, mappings)
    source_indices = {h: i for i, h in enumerate(headers)}
    # Case-insensitive fallback — Map/header drift must not invent NULL wipes.
    source_indices_ci = {str(h).lower(): i for i, h in enumerate(headers)}
    sanitized_target_cols = [sanitize_identifier(c, preserve_case=preserve_case) for c in target_cols]
    target_index = {c: i for i, c in enumerate(sanitized_target_cols)}
    errors: list[str] = []
    rejected_details: list[dict[str, Any]] = []

    # Live dest nullability (case-insensitive) — write-time NOT NULL escalate SSOT.
    dest_nullability: dict[str, bool] = {}
    for k, v in (destination_column_nullability or {}).items():
        key = str(k or "").strip()
        if not key:
            continue
        dest_nullability[key] = bool(v)
        dest_nullability[key.lower()] = bool(v)
        dest_nullability[sanitize_identifier(key, preserve_case=preserve_case)] = bool(v)

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
        # Never pass source ``column_types`` as dest_types — that invents a
        # "live" VARCHAR dest and suppresses Map ``target_type`` coercion
        # (INTEGER/BOOLEAN/DECIMAL stamps become trim_id/none). Empty dest
        # means create-new / unknown physical: honor Map target_type.
        transform = resolve_transform(
            m,
            column_types=column_types,
            dest_types=dest_types if dest_types is not None else {},
        )
        src_idx = source_indices.get(src)
        if src_idx is None:
            src_idx = source_indices_ci.get(str(src).lower())
        mapping_infos.append((
            src_idx,
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
            # Mapped source missing from headers after casefold — refuse NULL invent
            # (would wipe a good destination column on upsert).
            if source_idx is None and src_name:
                converted, err = None, (
                    f"mapped source column {src_name!r} not found in batch headers "
                    f"{list(headers)!r} — refuse silent NULL invent"
                )
                val = None
            else:
                val = raw[source_idx] if source_idx is not None and source_idx < len(raw) else None
                # Preserve sparse-CDC missing before transforms (omit-from-SET, never NULL wipe).
                if is_missing_sentinel(val):
                    if target_idx >= 0:
                        out[target_idx] = DF_MISSING_SENTINEL
                    continue
                converted, err = apply_transform(val, transform)
                # File/spreadsheet path: blank cell → SQL NULL on nullable typed cols
                # (Airbyte-class empty→null for non-string). Never invent NULL into
                # proven NOT NULL destinations.
                if (
                    err
                    and empty_cells_as_null
                    and _is_empty_typed_coerce_error(err)
                    and _is_blank_cell(val)
                    and not _target_explicitly_not_null(
                        mapping if isinstance(mapping, dict) else None,
                        tgt_name,
                        dest_nullability,
                    )
                ):
                    converted, err = None, None
            cell_policy = policy
            exec_pol: str | None = None
            risk_id: str | None = None
            retry_attempted = False
            if err and source_idx is not None:
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
            elif err and source_idx is None:
                # Unresolvable Map source — always quarantine/fail, not Risk invent.
                cell_policy = "fail" if policy == "fail" else "quarantine"
                exec_pol = None
                risk_id = None
                retry_attempted = False
            if err:
                row_has_error = True
                if exec_pol is None:
                    cell_policy, exec_pol, risk_id = resolve_write_action_for_mapping(
                        mapping, policy
                    )
                from services.value_serializer import DF_MISSING_SENTINEL

                values = {
                    h: (
                        quarantine_cell_wire(raw[i])
                        if i < len(raw)
                        else DF_MISSING_SENTINEL
                    )
                    for i, h in enumerate(headers)
                }
                detail: dict[str, Any] = {
                    "row": row_number,
                    "column": src_name,
                    "target": tgt_name,
                    "value": quarantine_cell_wire(val),
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
                    # Omit this cell from SET/INSERT projection (Missing), never
                    # invent SQL NULL — upsert NULL would wipe a prior good value.
                    if row_action == "ok":
                        row_action = "stop_column"
                    from services.value_serializer import Missing

                    converted = Missing
                elif cell_policy == "coerce_null":
                    # Job coerce_null = dense SQL NULL (INSERT / full-refresh).
                    # Upsert omit-from-SET must use STOP_COLUMN / sparse CDC Missing —
                    # never leak the ``__DF_MISSING__`` wire string into mapped rows.
                    if row_action == "ok":
                        row_action = "coerce_null"
                    converted = None
                else:
                    continue
                # Write-time NOT NULL parity with G3: refuse NULL invent / omit-as-NULL
                # into required columns when the Map stamp or dest nullability says so.
                if cell_policy in {"stop_column", "coerce_null"}:
                    tgt_nullable = mapping.get("target_nullable")
                    if tgt_nullable is None:
                        tgt_nullable = mapping.get("nullable")
                    if tgt_nullable is None and dest_nullability:
                        key = str(tgt_name or "").strip()
                        if key in dest_nullability:
                            tgt_nullable = dest_nullability[key]
                        else:
                            tgt_nullable = dest_nullability.get(key.lower())
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
        # stop_column: Missing omit-from-SET; coerce_null: dense NULL cell.
        # Never emit the ``__DF_MISSING__`` wire string into public mapped tuples.
        from services.value_serializer import Missing, public_mapped_cell

        mapped.append(
            tuple(
                public_mapped_cell(c, dense_null=False)
                if c is not None
                else None
                for c in out
            )
        )

    return mapped, errors, rejected_details


def flush_normalized_child_batches(
    *,
    headers: list[str],
    data_rows: list[list[Any]],
    mappings: list[dict[str, Any]] | None,
    dest_db: str,
    create_table: bool = True,
    cursor: Any = None,
    sa_conn: Any = None,
    quote: str = "`",
    placeholder: str = "%s",
    schema: str | None = None,
) -> dict[str, Any]:
    """CREATE + INSERT child tables for normalize/hybrid array Map policies.

    Parent rows are written separately (JSON retained). Fail-closed when the
    operator chose normalize/hybrid without a valid ``child_table_spec``.
    """
    from services.structural_array import (
        array_strategy_gate_issues,
        build_normalized_child_batches,
    )

    known = set(headers or [])
    gate = array_strategy_gate_issues(mappings, known_source_columns=known)
    if gate:
        return {
            "ok": False,
            "rows_written": 0,
            "tables": [],
            "errors": gate,
        }
    batches, build_errors = build_normalized_child_batches(
        headers, data_rows, mappings, dest_db=dest_db
    )
    if build_errors:
        return {
            "ok": False,
            "rows_written": 0,
            "tables": [],
            "errors": build_errors,
        }
    if not batches:
        return {"ok": True, "rows_written": 0, "tables": [], "errors": []}

    written = 0
    tables: list[str] = []
    errors: list[str] = []

    def _qualified(name: str) -> str:
        safe = require_safe_identifier(name)
        qname = quote_sql_identifier(safe, quote)
        if schema:
            return f"{quote_sql_identifier(require_safe_identifier(schema), quote)}.{qname}"
        return qname

    for batch in batches:
        child = str(batch.get("child_table") or "")
        cols = list(batch.get("columns") or [])
        types = list(batch.get("ddl_types") or [])
        rows = list(batch.get("rows") or [])
        if not child or not cols or not rows:
            continue
        try:
            table_q = _qualified(child)
            col_q = ", ".join(quote_sql_identifier(c, quote) for c in cols)
            if create_table:
                col_defs = ", ".join(
                    f"{quote_sql_identifier(c, quote)} {t}"
                    for c, t in zip(cols, types)
                )
                # columns = parent_keys + ordinal + value cols — unique for at-least-once.
                pk_n = int(batch.get("parent_key_count") or 1)
                uniq_cols = cols[: max(1, pk_n) + 1]
                uniq_sql = ", ".join(quote_sql_identifier(c, quote) for c in uniq_cols)
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {table_q} ({col_defs}, "
                    f"UNIQUE ({uniq_sql}))"
                )
                if cursor is not None:
                    cursor.execute(ddl)
                elif sa_conn is not None:
                    import sqlalchemy as sa

                    sa_conn.execute(sa.text(ddl))
                else:
                    errors.append("no cursor/connection for child DDL")
                    return {"ok": False, "rows_written": written, "tables": tables, "errors": errors}

            ph = ", ".join([placeholder] * len(cols))
            insert_sql = f"INSERT INTO {table_q} ({col_q}) VALUES ({ph})"  # nosec B608
            if cursor is not None:
                try:
                    cursor.executemany(insert_sql, rows)
                except Exception:
                    # Fallback: ignore duplicate on at-least-once replay.
                    for row in rows:
                        try:
                            cursor.execute(insert_sql, row)
                        except Exception as row_exc:
                            msg = str(row_exc).lower()
                            if "duplicate" in msg or "unique" in msg:
                                continue
                            raise
            else:
                import sqlalchemy as sa

                for row in rows:
                    params = {f"p{i}": v for i, v in enumerate(row)}
                    named_ph = ", ".join(f":p{i}" for i in range(len(cols)))
                    sql = f"INSERT INTO {table_q} ({col_q}) VALUES ({named_ph})"  # nosec B608
                    try:
                        sa_conn.execute(sa.text(sql), params)
                    except Exception as row_exc:
                        msg = str(row_exc).lower()
                        if "duplicate" in msg or "unique" in msg:
                            continue
                        raise
            written += len(rows)
            tables.append(child)
        except Exception as exc:
            errors.append(f"child table {child!r}: {exc}")
            return {
                "ok": False,
                "rows_written": written,
                "tables": tables,
                "errors": errors,
            }

    return {"ok": True, "rows_written": written, "tables": tables, "errors": errors}


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
    destination_column_nullability: dict[str, bool] | None = None,
    destination_column_types: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Apply Map transforms + Risk Contracts before embedding/upsert.

    Records stay **source-header keyed** (legacy ``zip(headers, row)``) so
    ``content_column`` / ``metadata_columns`` from Studio ``extra`` still resolve,
    while mapped transforms overlay source+target keys. Unmapped headers pass through.
    Returns ``(records, rejected_details, abort_message)``.
    """
    target_cols: list[str] = []
    source_for_target: list[str] = []
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
    if not target_cols:
        target_cols = list(headers)
        source_for_target = list(headers)

    live = destination_column_types or {}
    # Studio/live present → fail-closed coverage (never Map VARCHAR gap-fill).
    # Create-new with no Studio → Map stamps only.
    dest_types, cov_err = resolve_studio_or_map_dest_types(
        list(target_cols or []),
        list(mappings or []),
        column_types or {},
        studio_types=live if isinstance(live, dict) and live else None,
        product=(label or dest_kind or "vector").strip() or "vector",
    )
    if cov_err:
        return [], [], cov_err
    # Map path may use VARCHAR default — vector quarantine treats unbounded
    # string carriers as no-op unless VECTOR(n) specialty is stamped.

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
        destination_column_nullability=destination_column_nullability,
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
        from services.value_serializer import is_missing_sentinel

        # STOP_COLUMN / coerce_null — omit from vector metadata (never leak sentinel).
        if val is None or is_missing_sentinel(val):
            return None
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
        row: dict[str, Any] = {}
        for i, h in enumerate(headers):
            cell = _cell(raw[i] if i < len(raw) else None)
            if cell is not None:
                row[h] = cell
        for i, tgt in enumerate(target_cols):
            val = _cell(tup[i] if i < len(tup) else None)
            if val is None:
                # Omit DF_MISSING / null overlay — do not invent "" into metadata.
                continue
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


def resolve_studio_or_map_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str] | None = None,
    *,
    logical_types: list[str] | None = None,
    studio_types: dict[str, Any] | None = None,
    product: str = "destination",
) -> tuple[dict[str, str], str | None]:
    """Studio present → fail-closed coverage; else Map stamps for create-new.

    Partial Studio must never soft-fill Map ``VARCHAR`` for gaps (empty→NULL /
    polarity invent). Callers with an existing typed sink should rematerialize
    from live DDL after this helper; create-new / empty sinks must refuse
    ``error`` when Studio was incomplete.
    """
    from connectors.saas_common import merge_saas_live_types

    studio = (
        studio_types
        if isinstance(studio_types, dict) and studio_types
        else None
    )
    if studio is not None:
        live = {
            str(k): str(v).strip()
            for k, v in studio.items()
            if k and str(v or "").strip()
        }
        return merge_saas_live_types(
            live,
            list(target_cols or []),
            studio_types=None,
            product=product,
        )
    return (
        resolve_mapping_dest_types(
            target_cols,
            mappings,
            column_types or {},
            logical_types=logical_types,
            live_types=None,
            default="VARCHAR",
        ),
        None,
    )


def gate_additive_types_under_partial_studio(
    *,
    target_cols: list[str],
    target_types: list[str],
    existing: set[str] | frozenset[str] | set,
    mappings: list[dict] | None,
    studio_err: str | None,
    product: str,
    materialize_stamp: Any,
    col_in_existing: Any = None,
) -> tuple[list[str], str | None]:
    """Under partial Studio, additive ADD must use explicit Map ``target_type``.

    Existing columns keep ``target_types`` (rematerialize/physical owns them).
    New columns without an operator Map stamp refuse — never invent from
    source DDL / bare VARCHAR (BigQuery / generic_sql parity).
    """
    if not studio_err:
        return list(target_types), None
    # Pad to target_cols length — deferred Map under partial Studio often leaves
    # target_types=[] before rematerialize. Append-only stamping mis-zips ADD
    # types onto the wrong columns (existing col gets the additive stamp).
    out = [str(t or "") for t in list(target_types or [])]
    if len(out) < len(target_cols or []):
        out.extend([""] * (len(target_cols) - len(out)))
    elif len(out) > len(target_cols or []):
        out = out[: len(target_cols)]
    from services.mapping_constraints import write_mappings

    by_tgt: dict[str, dict] = {}
    for mapping in write_mappings(list(mappings or [])):
        tgt = str(mapping.get("target") or "").strip()
        if tgt and tgt not in by_tgt:
            by_tgt[tgt] = mapping
            by_tgt.setdefault(tgt.lower(), mapping)
    contains = col_in_existing or (lambda col, ex: col in ex)
    for i, col in enumerate(target_cols):
        if not col or contains(col, existing):
            continue
        mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
        explicit = str(
            mapping.get("target_type") or mapping.get("dest_type") or ""
        ).strip()
        if not explicit:
            return out, (
                f"{product} additive column {col!r} lacks Studio/live type and "
                "Map target_type under partial Studio — refuse Map VARCHAR ADD "
                "invent. Stamp the column on Map or disable backfill_new_fields."
            )
        stamped = materialize_stamp(explicit)
        if not str(stamped or "").strip():
            return out, (
                f"{product} additive column {col!r} Map target_type {explicit!r} "
                "did not materialize to DDL — refuse Map VARCHAR ADD invent."
            )
        out[i] = str(stamped)
    return out, None


def rematerialize_live_dest_types(
    physical: dict[str, str] | None,
    target_cols: list[str],
    *,
    product: str,
) -> dict[str, str] | None:
    """Live sink carriers only for rematerialize — never Map VARCHAR gap-fill.

    Returns ``None`` when ``physical`` is empty or does not cover every mapped
    column (caller must have fail-closed via ``require_physical`` / Studio).
    """
    if not physical:
        return None
    from connectors.saas_common import merge_saas_live_types

    live_map = {
        str(k): str(v)
        for k, v in physical.items()
        if k and str(v or "").strip()
    }
    merged, err = merge_saas_live_types(
        live_map,
        list(target_cols or []),
        studio_types=None,
        product=product,
    )
    if err:
        return None
    return merged


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

    For rematerialize over live DDL/Registry/AttrDefs, prefer
    ``rematerialize_live_dest_types`` so gaps never soft-fill Map VARCHAR.
    """
    cols = list(target_cols or [])
    maps = list(mappings or [])
    ctypes = column_types or {}
    logical = list(logical_types or [])
    live = {str(k).lower(): str(v) for k, v in (live_types or {}).items() if k and v}
    # By-target Map stamp — never index-zip mappings[i] onto target_cols[i]
    # (omits / reorder mis-stamp invents wrong carriers).
    from services.mapping_constraints import write_mappings

    by_tgt: dict[str, dict] = {}
    for mapping in write_mappings(list(maps)):
        tgt = str(mapping.get("target") or "").strip()
        if tgt and tgt not in by_tgt:
            by_tgt[tgt] = mapping
            by_tgt.setdefault(tgt.lower(), mapping)
    out: dict[str, str] = {}
    for i, col in enumerate(cols):
        live_hit = live.get(str(col).lower())
        if live_hit:
            out[col] = live_hit
            continue
        m = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
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

    Prefers live ``dest_types`` on existing tables (table_exists is not False),
    then explicit Map ``target_type``, then source logical type, then ``VARCHAR``.

    Create-new (``table_exists is False``): Map ``target_type`` is preserved
    (Map≡CREATE) — unfit values quarantine on write instead of rewriting DDL.

    Enterprise GA: create-new without an explicit Map ``target_type`` must **not**
    invent BOOLEAN/INTEGER/DECIMAL from head samples. Keep the source/carrier
    proposal; values that cannot coerce quarantine on write.
    """
    from services.schema_inference import safe_ddl_logical_type

    target_cols: list[str] = []
    target_types: list[str] = []
    samples = sample_values_by_source or {}
    live = dest_types or {}
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
            live_hit = (
                live.get(tgt)
                or live.get(str(tgt).lower())
                or live.get(str(tgt).upper())
            )
            # Existing table: live DDL beats Map stamp (same honesty as
            # resolve_mapping_dest_types / Validate live-first). Create-new and
            # missing live keep Map≡CREATE / source proposal.
            if table_exists is not False and live_hit:
                proposed = live_hit
            else:
                proposed = (
                    m.get("target_type")
                    or live_hit
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


def parse_decimal_precision_scale(
    type_str: str,
    *,
    dest_db: str = "",
) -> tuple[int, int] | None:
    """Parse DECIMAL/NUMERIC/NUMBER/BIGNUMERIC(p[,s]) → (precision, scale).

    Bare ``NUMERIC`` / ``BIGNUMERIC`` use BigQuery / Spanner platform defaults
    only when ``dest_db`` is bigquery/spanner (or unset for backward compat on
    BIGNUMERIC). On PostgreSQL / Redshift, bare ``NUMERIC``/``DECIMAL`` are
    unbounded → ``None`` (skip fit quarantine). Bare ``DECIMAL`` / ``NUMBER``
    stay ``None`` when ambiguous. MONEY / SMALLMONEY → SQL Server currency.
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
        dialect = (dest_db or "").strip().lower()
        # Postgres bare NUMERIC/DECIMAL is unbounded — never invent BQ (38,9).
        if dialect in {"postgresql", "postgres", "redshift", "greenplum", "cockroach", "cockroachdb"}:
            return None
        if dialect and dialect not in {"bigquery", "spanner", "bq"}:
            # Other engines: bare DECIMAL/NUMBER stay ambiguous; bare BIGNUMERIC
            # only exists on BQ-class — still use platform default when named.
            if base == "BIGNUMERIC":
                return _BARE_DECIMAL_DEFAULTS.get(base)
            return None
        return _BARE_DECIMAL_DEFAULTS.get(base)
    precision = int(m.group(1))
    scale = int(m.group(2)) if m.group(2) is not None else 0
    if precision < 1 or scale < 0 or scale > precision:
        return None
    return precision, scale


def decimal_int_digits_and_scale(value: Any) -> tuple[int, int]:
    """Return (integer_digits, fractional_scale) for a cell value.

    Fractional scale ignores trailing zeros in the wire representation
    (``52.310500000000000`` → scale 4) so DECIMAL fit matches PostgreSQL /
    warehouse bind capacity rather than string padding from MySQL DOUBLE dumps.

    SSOT: ``services.decimal_observe.cell_int_digits_and_scale``.
    """
    from services.decimal_observe import cell_int_digits_and_scale

    return cell_int_digits_and_scale(value)


PG_DECIMAL_ROUND_DIALECTS = frozenset({
    "postgresql", "postgres", "redshift", "greenplum", "cockroach", "cockroachdb",
})
# Back-compat alias for private import sites.
_PG_DECIMAL_ROUND_DIALECTS = PG_DECIMAL_ROUND_DIALECTS


def fits_decimal(
    value: Any,
    precision: int,
    scale: int,
    *,
    dest_db: str = "",
) -> bool:
    """True if value can be stored in DECIMAL/NUMBER(precision, scale).

    Trailing wire zeros are stripped (``52.310500000000000`` → scale 4).

    Dialect honesty (PostgreSQL docs): excess *fractional* digits are rounded
    at bind — do not invent a quarantine block PG would never raise. Integer /
    precision overflow still fail-closed (PG ``numeric field overflow``).

    MySQL / Snowflake / SQL Server stay fail-closed on significant scale
    overflow (STRICT / warehouse reject class) unless ``dest_db`` is PG-family.
    """
    from decimal import (
        ROUND_HALF_UP,
        Context,
        Decimal,
        InvalidOperation,
        Overflow,
        localcontext,
    )

    if value is None:
        return True
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return True
    try:
        text = str(value).strip()
        if not text:
            return True
        d = Decimal(text)
        if not d.is_finite():
            return False
        prec = int(precision)
        scl = int(scale)
        max_int = max(0, prec - scl)
        dialect = (dest_db or "").strip().lower()
        pg_rounds_scale = dialect in _PG_DECIMAL_ROUND_DIALECTS
        int_digits, value_scale = decimal_int_digits_and_scale(d)
        if value_scale > scl:
            if not pg_rounds_scale:
                return False
            # Match PG: round fractional excess, then prove integer capacity.
            with localcontext(Context(prec=max(prec + 16, 80), rounding=ROUND_HALF_UP)):
                try:
                    rounded = d.quantize(Decimal(1).scaleb(-scl))
                except (InvalidOperation, Overflow):
                    return False
            int_digits, _ = decimal_int_digits_and_scale(rounded)
            return int_digits <= max_int
        return int_digits <= max_int
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
    dest_db: str = "",
) -> list[tuple]:
    """Hold out / NULL cells that cannot fit DECIMAL/NUMBER(p,s).

    ``quarantine`` omits the whole row from the primary write (no NULL invent).
    ``coerce_null`` keeps the row with a NULL cell. ``fail`` stamps unfit cells and holds out rows like ``quarantine`` so
    ``reject_on_strict_policy`` can abort before bind (never rely on soft drivers).
    """
    number_cols: list[tuple[int, int, int]] = []
    for i, typ in enumerate(target_types):
        parsed = parse_decimal_precision_scale(typ, dest_db=dest_db)
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
            if fits_decimal(cells[col_idx], precision, scale, dest_db=dest_db):
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
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
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
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
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def _mysql_timestamp_range_violation(
    target_type: str, value: Any, *, dest_db: str = ""
) -> bool:
    """True for a MySQL ``TIMESTAMP`` cell outside the 1970..2038 epoch window."""
    from services.type_system import _normalize_dest_db

    if not dest_db or _normalize_dest_db(dest_db) != "mysql":
        return False
    from services.timezone_policy import (
        is_mysql_timestamp_carrier,
        mysql_timestamp_out_of_range,
    )

    if not is_mysql_timestamp_carrier(target_type):
        return False
    return mysql_timestamp_out_of_range(value)


def quarantine_unfit_temporals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dest_db: str = "",
) -> list[tuple]:
    """Hold out temporal cells that would invent NULL, lose FSP, or strip TZ.

    - Empty ``\"\"`` into DATE/TIME/DATETIME → refuse silent NULL invent (Iceberg
      Arrow / SQL bind parity)
    - ``TIME(6)`` → ``TIME(0)`` truncates fractional seconds
    - Offset-aware / ``Z`` wire into NTZ/DATETIME strips the offset (Airbyte invent)
    - MySQL ``TIMESTAMP`` outside 1970..2038 — the carrier is an instant, but an
      epoch-bounded one, so out-of-range values are held out rather than zeroed

    ``dest_db`` decides bare ``TIMESTAMP`` polarity: on MySQL it is a UTC instant
    carrier, so an offset-bearing wire is *not* a strip and must not quarantine.
    """

    from services.type_system import (
        datetime_timezone_polarity,
        normalize_logical_type,
        parse_temporal_fractional_precision,
        temporal_value_exceeds_precision,
        temporal_value_has_timezone,
    )
    from services.value_serializer import cell_to_string, is_missing_sentinel

    temporal_cols: list[tuple[int, str, bool, bool]] = []
    for i, typ in enumerate(target_types):
        logical = normalize_logical_type(typ)
        if logical not in {"date", "time", "datetime"}:
            continue
        check_fsp = parse_temporal_fractional_precision(typ) is not None
        check_tz = (
            logical == "datetime"
            and datetime_timezone_polarity(typ, dest_db=dest_db) == "ntz"
        )
        # Always include temporal columns so empty refuse runs even without FSP/TZ.
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
            if is_missing_sentinel(cells[col_idx]):
                continue
            reason = ""
            raw = cells[col_idx]
            if isinstance(raw, str) and not raw.strip():
                reason = (
                    f"empty string into temporal destination {typ} "
                    "— quarantined (refuse silent NULL invent)"
                )
            elif check_fsp and temporal_value_exceeds_precision(raw, typ):
                reason = (
                    f"fractional seconds exceed destination {typ} "
                    "— quarantined (refuse silent truncate)"
                )
            elif _mysql_timestamp_range_violation(typ, raw, dest_db=dest_db):
                reason = (
                    f"value outside the MySQL TIMESTAMP epoch range for {typ} "
                    "— quarantined; map to DATETIME(6) with a UTC-normalize "
                    "contract to carry instants beyond 2038"
                )
            elif check_tz and temporal_value_has_timezone(raw):
                reason = (
                    f"timezone-aware value into NTZ destination {typ} "
                    "— quarantined (refuse silent offset strip; use explicit UTC transform)"
                )
            if not reason:
                continue
            sample = cell_to_string(raw)[:120]
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def fits_integer(value: Any, type_str: str, *, dest_db: str = "") -> bool:
    """True if value fits the signed/unsigned integer destination carrier."""
    from decimal import Decimal, InvalidOperation
    from services.type_system import integer_storage_bounds

    bounds = integer_storage_bounds(type_str, dest_db=dest_db)
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
                # Empty ≠ natural NULL on INTEGER — bind must not invent NULL;
                # quarantine_unfit_integers / bind quarantine hold the row out.
                return False
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
    dest_db: str = "",
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
        if integer_storage_bounds(typ, dest_db=dest_db) is None:
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
            if fits_integer(cells[col_idx], typ, dest_db=dest_db):
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_floats(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "FLOAT",
) -> list[tuple]:
    """Hold out empty / non-finite / non-numeric cells into FLOAT/DOUBLE sinks.

    Matrix SSOT for Kafka/S3/GCS/Iceberg — SQL bind already refuses via
    ``coerce_float_wire``. Empty ``\"\"`` must never invent JSON null / 0.0.
    """
    from services.type_system import normalize_logical_type
    from services.value_serializer import cell_to_string, is_missing_sentinel

    float_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "float":
            continue
        float_cols.append((i, typ))
    if not float_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_float_wire

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in float_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            if is_missing_sentinel(cells[col_idx]):
                continue
            raw = cells[col_idx]
            reason = ""
            try:
                coerced = coerce_float_wire(raw, ddl_type=typ)
            except ValueError as exc:
                reason = str(exc)
                coerced = None
            if not reason and isinstance(coerced, float) and (
                coerced != coerced or coerced in {float("inf"), float("-inf")}
            ):
                reason = (
                    f"non-finite float into {dialect_label}({typ}) "
                    "— quarantined (refuse invent)"
                )
            if not reason:
                continue
            sample = cell_to_string(raw)[:120]
            append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                        f"{reason} — quarantined ({dialect_label} refuse silent "
                        "NULL/0.0 invent)"
                        if "quarantined" not in reason
                        else reason
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL

                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def bind_sql_mapped_rows_with_quarantine(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    engine: str,
    dialect_label: str = "SQL",
    mappings: list[dict[str, Any]] | None = None,
) -> list[tuple]:
    """Bind cells via ``normalize_sql_bind_value``; quarantine refusals (no crash invent).

    Physical DDL can be INT/FLOAT/DECIMAL after Map stamped VARCHAR — empty ``\"\"``
    must not become SQL NULL on upsert (destination wipe). Raise from sql_bind is
    held out / STOP_COLUMN under quarantine|fail; ``coerce_null`` (gated) keeps NULL.
    """
    from connectors.sql_bind import normalize_sql_bind_value
    from services.value_serializer import (
        DF_MISSING_SENTINEL,
        cell_to_string,
        is_missing_sentinel,
    )

    if not mapped_rows:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for idx in range(len(cells)):
            val = cells[idx]
            if val is None or is_missing_sentinel(val):
                continue
            ddl = target_types[idx] if idx < len(target_types) else ""
            if not ddl:
                continue
            try:
                cells[idx] = normalize_sql_bind_value(val, ddl, engine=engine)
            except ValueError as exc:
                sample = cell_to_string(val)[:120]
                col = target_cols[idx] if idx < len(target_cols) else f"col_{idx}"
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": row_idx + 1,
                        "column": col,
                        "target": col,
                        "value": sample,
                        "reason": (
                            f"{dialect_label} bind refused {sample!r} for {ddl}: {exc} "
                            "— quarantined (refuse silent NULL invent)"
                        ),
                        "policy": (
                            "coerce_null" if policy == "coerce_null" else "write_quarantine"
                        ),
                        "chars": [],
                    },
                    mapped_row=cells,
                    target_cols=target_cols,
                    mappings=mappings,
                )
                if policy == "coerce_null":
                    cells[idx] = DF_MISSING_SENTINEL
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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


def array_element_unfit_reason(
    element: Any,
    carrier: str,
    *,
    dest_db: str = "",
) -> str | None:
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
    decimal_parsed = parse_decimal_precision_scale(carrier, dest_db=dest_db)
    if decimal_parsed and not fits_decimal(
        element, decimal_parsed[0], decimal_parsed[1], dest_db=dest_db
    ):
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
    dest_db: str = "",
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
                    unfit = array_element_unfit_reason(
                        element, element_carrier, dest_db=dest_db
                    )
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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
            # coerce_json_wire wraps failed parses as JSON string literals
            # (\"{not:valid}\"). Detect that wrap so we still quarantine intended
            # documents instead of inventing a JSON string scalar.
            candidate = text
            if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
                try:
                    inner = json.loads(text)
                except Exception:
                    inner = None
                if isinstance(inner, str):
                    candidate = inner.strip()
            looks_structured = (
                (candidate.startswith("{") and candidate.endswith("}"))
                or (candidate.startswith("[") and candidate.endswith("]"))
            )
            if not looks_structured:
                continue
            try:
                def _reject(name: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {name}")

                json.loads(candidate, parse_constant=_reject)
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
                        f"{label} JSON: malformed or non-finite document payload — "
                        "quarantined (NaN/Infinity must not become null; invalid "
                        "JSON must not store as a bare string)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def _infer_dest_db_from_dialect_label(dialect_label: str) -> str:
    """Best-effort dest_db for bare DECIMAL/NUMERIC/INTEGER parse honesty.

    Engines whose bare ``INTEGER``/``DECIMAL`` keyword is *not* the SQL-standard
    32-bit / (38,x) carrier must be recognised here, otherwise the shared write
    quarantine invents a bound the destination never enforces and holds out
    perfectly writable rows (SQLite ``INTEGER`` is 8 bytes; document and object
    sinks have no integer column at all).
    """
    low = (dialect_label or "").strip().lower()
    if not low:
        return ""
    for token, db in (
        ("sqlite", "sqlite"),
        ("mongo", "mongodb"),
        ("dynamodb", "dynamodb"),
        ("elasticsearch", "elasticsearch"),
        ("opensearch", "elasticsearch"),
        ("redis", "redis"),
        ("kafka", "kafka"),
        ("salesforce", "salesforce"),
        ("hubspot", "hubspot"),
        ("airtable", "airtable"),
        ("notion", "notion"),
    ):
        if token in low:
            return db
    if low in {"s3", "gcs", "adls"}:
        return low
    if "redshift" in low:
        return "redshift"
    if "postgres" in low or "cockroach" in low or "greenplum" in low:
        return "postgresql"
    if "bigquery" in low or low.startswith("bq"):
        return "bigquery"
    if "spanner" in low:
        return "spanner"
    if "snowflake" in low:
        return "snowflake"
    if "mysql" in low or "mariadb" in low:
        return "mysql"
    if "sql server" in low or "mssql" in low or "azure sql" in low:
        return "sqlserver"
    if "oracle" in low:
        return "oracle"
    return ""


def apply_write_quarantine_matrix(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "destination",
    mappings: list[dict[str, Any]] | None = None,
    dest_db: str = "",
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
        decimal_dest = (dest_db or "").strip() or _infer_dest_db_from_dialect_label(label)
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
            dest_db=decimal_dest,
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
            dest_db=decimal_dest,
        )
        mapped_rows = quarantine_unfit_floats(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=f"{label} FLOAT",
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
            dest_db=decimal_dest,
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
    from services.type_system import (
        normalize_logical_type,
        parse_vector_dimension,
        specialty_carrier_base,
    )

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
    # Network / text / geometric specialty — bind refuse must be mirrored in the
    # matrix so object-store / SaaS paths never green empty HSTORE then crash.
    spec = specialty_carrier_base(type_str)
    if spec in {"INET", "IPV4", "IPV6", "IP"}:
        return "inet"
    if spec == "CIDR":
        return "cidr"
    if spec in {"MACADDR", "MACADDR8"}:
        return "macaddr"
    if spec in {"XML", "XMLTYPE"}:
        return "xml"
    if spec == "LTREE":
        return "ltree"
    if spec:
        # HSTORE / TSVECTOR / POINT / BOX / RANGE / OID / PG_LSN / …
        return "bind"
    return None


def quarantine_unfit_specialty_types(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells unfit for GEOGRAPHY / INTERVAL / VECTOR / INET / … sinks.

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

    from connectors.sql_bind import (
        coerce_cidr_wire,
        coerce_inet_wire,
        coerce_ltree_wire,
        coerce_macaddr_wire,
        coerce_xml_wire,
    )
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
        specialty_carrier_base,
    )
    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind, typ in specialty_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            from services.value_serializer import SQL_NULL_SENTINEL, is_missing_sentinel

            if is_missing_sentinel(cells[col_idx]) or cells[col_idx] == SQL_NULL_SENTINEL:
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
            elif kind in {"inet", "cidr", "macaddr", "xml", "ltree"}:
                try:
                    if kind == "inet":
                        coerce_inet_wire(cells[col_idx])
                    elif kind == "cidr":
                        coerce_cidr_wire(cells[col_idx])
                    elif kind == "macaddr":
                        eui64 = specialty_carrier_base(typ) == "MACADDR8"
                        coerce_macaddr_wire(cells[col_idx], eui64=eui64)
                    elif kind == "xml":
                        coerce_xml_wire(cells[col_idx])
                    else:
                        coerce_ltree_wire(cells[col_idx])
                except ValueError as exc:
                    ok = False
                    reason = str(exc)[:300]
            elif kind == "bind":
                from connectors.sql_bind import normalize_sql_bind_value

                try:
                    normalize_sql_bind_value(cells[col_idx], typ, engine="")
                except ValueError as exc:
                    ok = False
                    reason = str(exc)[:300]
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
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
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


def resolve_conflict_targets(
    conflict_columns: list[str] | None,
    target_cols: list[str],
    *,
    strict: bool = True,
) -> list[str]:
    """Map conflict/PK names onto ``target_cols`` with case-insensitive match.

    Warehouse writers (BQ/Snowflake) often see operator PKs in a different case
    than Map targets — a case-sensitive miss must not silently disable MERGE and
    drop sparse CDC rows.

    When ``strict`` (default), every non-empty conflict name must resolve. A
    partial composite PK (one good name + one typo) must not degrade MERGE to a
    shorter key and touch the wrong rows.
    """
    if not conflict_columns or not target_cols:
        return []
    lower_map = {str(c).lower(): c for c in target_cols}
    out: list[str] = []
    seen: set[str] = set()
    unresolved: list[str] = []
    for raw in conflict_columns:
        key = str(raw or "").strip()
        if not key:
            continue
        resolved = lower_map.get(key.lower())
        if resolved is None:
            unresolved.append(key)
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(resolved)
    if strict and unresolved:
        raise ValueError(
            "conflict_columns do not fully match mapped targets — unresolved "
            f"{unresolved!r} vs targets {list(target_cols)!r}; refuse partial "
            "composite PK degrade"
        )
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
    hydrate_versioned_insert: bool = False,
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> tuple[int, int, list[tuple]]:
    """Shared sparse CDC upsert: omit DF_MISSING, LSN skip, hydrate checksum rows.

    ``fetch_existing_row(pk_vals)`` must return a full row in ``target_cols`` order
    (or ``None``). ``update_non_pk(non_pk, pk_vals)`` returns affected rowcount.

    When ``hydrate_versioned_insert`` is True (ClickHouse ReplacingMergeTree-class),
    INSERT of a sparse image must be a full hydrated row — partial INSERT would
    become the new version and NULL-wipe omitted attributes after merge/FINAL.

    Empty/missing conflict keys quarantine (via ``rejected_details``) instead of
    aborting the whole CDC chunk — parity with Snowflake/BigQuery sparse paths.
    """
    from services.cdc_effectively_once import should_apply_pk_row
    from services.value_serializer import cell_to_string, is_missing_sentinel

    # Strict casefold resolve — never shrink a composite PK to whatever happens
    # to match case-sensitively (wrong-row MERGE / INSERT fallback).
    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("sparse CDC upsert requires conflict_columns")
    written = 0
    skipped = 0
    checksum_rows: list[tuple] = []
    for row_idx, row in enumerate(sparse_rows):
        present = sparse_present_bindings(row, target_cols)
        try:
            assert_sparse_upsert_has_pk(present, conflict)
        except ValueError as exc:
            if rejected_details is None:
                raise
            sample = ""
            try:
                sample = cell_to_string(
                    next(iter(present.values()), "")
                )[:120]
            except Exception:
                sample = ""
            rejected_details.append(
                {
                    "row": row_idx,
                    "column": "*",
                    "value": sample,
                    "reason": str(exc)[:300],
                    "policy": policy,
                }
            )
            continue
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
        if non_pk and not hydrate_versioned_insert:
            affected = update_non_pk(non_pk, pk_vals)
            if affected and affected > 0:
                written += 1
                checksum_rows.append(
                    materialize_sparse_row_for_checksum(present, existing, target_cols)
                )
                continue
        try:
            if hydrate_versioned_insert:
                if existing is None:
                    # Unknown PK + partial CDC image — refuse inventing a
                    # versioned row that would NULL-default omitted attrs.
                    missing = [c for c in target_cols if c not in present]
                    if missing:
                        msg = (
                            "ClickHouse/versioned sparse CDC insert of unknown "
                            f"primary key refused — omitted columns {missing[:8]}; "
                            "require a full row image or an existing destination row"
                        )
                        if rejected_details is None:
                            raise ValueError(msg)
                        sample = ""
                        try:
                            sample = cell_to_string(
                                next(iter(present.values()), "")
                            )[:120]
                        except Exception:
                            sample = ""
                        rejected_details.append(
                            {
                                "row": row_idx,
                                "column": "*",
                                "value": sample,
                                "reason": msg[:300],
                                "policy": policy,
                            }
                        )
                        continue
                    insert_present(dict(present))
                else:
                    hydrated = materialize_sparse_row_for_checksum(
                        present, existing, target_cols
                    )
                    insert_present(
                        {
                            c: v
                            for c, v in zip(target_cols, hydrated)
                            if not is_missing_sentinel(v)
                        }
                    )
            else:
                insert_present(present)
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
        except Exception:
            if hydrate_versioned_insert or not non_pk:
                raise
            update_non_pk(non_pk, pk_vals)
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
    return written, skipped, checksum_rows


def overlay_physical_bind_types(
    target_cols: list[str],
    target_types: list[str],
    physical: dict[str, str],
) -> list[str]:
    """Prefer live DDL when Map stamped a softer carrier over typed / specialty sinks.

    Empty ``\"\"`` must refuse at bind against physical DATE/INT/BOOL/NUMBER —
    never survive as VARCHAR and invent NULL on upsert wipe (MySQL/Snowflake
    parity). Temporal physical types always win. Specialty physical (HSTORE,
    OID, POINT, INET, …) always wins — Map JSON/INTEGER/GEOGRAPHY must not
    invent wrong bind polarity over live specialty. Typed physical (INT/BOOL/
    DECIMAL/JSON/…) always wins over Map stamps on existing tables.
    Bounded physical ``VARCHAR(n)`` / ``CHAR(n)`` / ``NVARCHAR(n)`` always wins
    over Map bare/unbounded/wider string stamps so overflow quarantine sees
    live capacity (Airbyte catalog-width class).
    """
    from connectors.sql_temporal import is_temporal_ddl, sql_base_type
    from services.ddl_compatibility import parse_varchar_width
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        is_unlimited_string_carrier,
        normalize_logical_type,
        parse_enum_or_set_ordered_members,
        parse_string_carrier_width,
        specialty_carrier_base,
    )

    def _bounded_string_width(carrier: str) -> int | None:
        return parse_string_carrier_width(carrier) or parse_varchar_width(carrier)

    def _map_stamp_is_string_class(stamp: str) -> bool:
        text = (stamp or "").strip()
        if not text:
            return True
        if _bounded_string_width(text) is not None:
            return True
        if is_unlimited_string_carrier(text):
            return True
        base = sql_base_type(text)
        if base in {
            "VARCHAR",
            "CHAR",
            "NVARCHAR",
            "NCHAR",
            "CHARACTER",
            "STRING",
            "TEXT",
            "CLOB",
            "NCLOB",
        }:
            return True
        return normalize_logical_type(text) in {LOGICAL_STRING, LOGICAL_TEXT}

    if not physical:
        return list(target_types)
    out = list(target_types)
    typed_bases = {
        "NUMBER",
        "DECIMAL",
        "NUMERIC",
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "MEDIUMINT",
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "FLOAT16",
        "FLOAT32",
        "FLOAT64",
        "HALF",
        "HALFFLOAT",
        "DOUBLE",
        "REAL",
        "BOOLEAN",
        "BOOL",
        "BIT",
        "JSON",
        "JSONB",
        "UUID",
        "UNIQUEIDENTIFIER",
        "MONEY",
        "SMALLMONEY",
        "CURRENCY",
        "BIGNUMERIC",
    }
    temporal_extra = {
        "DATE",
        "TIME",
        "DATETIME",
        "DATETIME64",
        "DATETIME2",
        "SMALLDATETIME",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ",
        "TIMESTAMPTZ",
        "TIMETZ",
        "DATETIMEOFFSET",
        "YEAR",
    }
    # Semi-structured warehouse carriers — Map VARCHAR must not stick.
    typed_bases |= {
        "SUPER",
        "VARIANT",
        "OBJECT",
        "ARRAY",
        "MAP",
        "STRUCT",
        "IPV4",
        "IPV6",
        "ENUM8",
        "ENUM16",
    }
    for i, col in enumerate(target_cols):
        phys = physical.get(col) or physical.get(col.lower()) or physical.get(col.upper())
        if not phys:
            continue
        phys_base = sql_base_type(phys)
        if is_temporal_ddl(phys_base) or phys_base in temporal_extra:
            out[i] = phys
        elif specialty_carrier_base(phys):
            # Live HSTORE/OID/POINT/INET/… beat Map JSON/INTEGER/GEOGRAPHY stamps.
            out[i] = phys
        elif parse_enum_or_set_ordered_members(phys) is not None:
            # MySQL/PG closed ENUM('a','b') / SET('x','y') — Map VARCHAR must not
            # soft-bind open text over live domain (empty→null / ordinal invent).
            out[i] = phys
        elif _bounded_string_width(phys) is not None and _map_stamp_is_string_class(
            out[i]
        ):
            # Live VARCHAR(n)/CHAR(n)/NVARCHAR(n)/STRING(n) beat Map bare VARCHAR
            # / TEXT / wider stamps — overflow quarantine must see physical capacity.
            out[i] = phys
        elif (
            phys_base in typed_bases
            or phys_base.endswith("[]")
            or phys_base.startswith("ARRAY")
        ):
            # Live typed physical beats Map DECIMAL≠INT / BOOL≠INT invent,
            # and Map VARCHAR → DATE/INT/BOOL / ARRAY (empty→NULL refuse path).
            # ``INTEGER[]`` survives sql_base_type as ``INTEGER[]`` (not ARRAY).
            out[i] = phys
    return out


def _is_nullish_conflict_key(val: Any) -> bool:
    """True when a conflict-key cell cannot identify a dense upsert row."""
    if val is None:
        return True
    from services.value_serializer import SQL_NULL_SENTINEL, is_missing_sentinel

    if is_missing_sentinel(val):
        return True
    if isinstance(val, str):
        text = val.strip()
        return not text or text == SQL_NULL_SENTINEL
    return False


def assert_sparse_upsert_has_pk(
    present: dict[str, Any],
    conflict_columns: list[str],
) -> None:
    """Refuse sparse upsert that omits or nulls the conflict key (would invent rows).

    SQL ``NULL = NULL`` is UNKNOWN, so equality-based sparse UPDATE/INSERT paths
    would miss the destination row and INSERT unbounded duplicates. Kafka already
    refuses null keys; all sparse CDC dialects share this assert.
    """
    missing = [c for c in conflict_columns if c not in present]
    if missing:
        raise ValueError(
            "sparse CDC upsert is missing primary-key column(s) "
            f"{missing}; refuse silent invent (require binlog_row_image=FULL / "
            "REPLICA IDENTITY FULL)"
        )
    nullish = [c for c in conflict_columns if _is_nullish_conflict_key(present.get(c))]
    if nullish:
        raise ValueError(
            "sparse CDC upsert has null/empty primary-key column(s) "
            f"{nullish}; refuse NULL=NULL invent duplicates"
        )


def assert_dense_upsert_keys_present(
    rows: list[tuple] | list[list] | list[dict[str, Any]],
    conflict_columns: list[str],
    target_cols: list[str] | None = None,
) -> None:
    """Refuse null/empty conflict keys before dense MERGE / NULL-safe ON.

    ``null_safe_merge_on`` treats NULL≡NULL as a match so one staged null-PK
    row can mass-update every destination row with a null key. Redshift already
    refuses; BQ/Snowflake/generic MERGE must share this gate.
    """
    if not rows or not conflict_columns:
        return
    cols = list(conflict_columns)
    idxs: list[int] | None = None
    if target_cols is not None:
        idxs = [target_cols.index(c) for c in cols]
    for row in rows:
        if isinstance(row, dict):
            for c in cols:
                if _is_nullish_conflict_key(row.get(c)):
                    raise ValueError(
                        f"dense upsert refused null/empty conflict key {c!r} — "
                        "NULL-safe MERGE would mass-touch destination rows"
                    )
            continue
        if idxs is None:
            raise ValueError(
                "dense upsert key check requires target_cols for tuple rows"
            )
        for c, idx in zip(cols, idxs):
            val = row[idx] if idx < len(row) else None
            if _is_nullish_conflict_key(val):
                raise ValueError(
                    f"dense upsert refused null/empty conflict key {c!r} — "
                    "NULL-safe MERGE would mass-touch destination rows"
                )

def partition_dense_upsert_rows(
    rows: list[Any],
    conflict_columns: list[str],
    *,
    target_cols: list[str] | None = None,
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
    row_offset: int = 0,
) -> list[Any]:
    """Hold out null/empty conflict-key rows; return rows safe for MERGE.

    Preserves arrival order. When ``rejected_details`` is None, re-raises on the
    first bad row (fail-closed callers without a quarantine sink).
    """
    if not rows or not conflict_columns:
        return list(rows)
    kept: list[Any] = []
    for i, row in enumerate(rows):
        try:
            assert_dense_upsert_keys_present(
                [row], conflict_columns, target_cols=target_cols
            )
            kept.append(row)
        except ValueError as exc:
            if rejected_details is None:
                raise
            rejected_details.append(
                {
                    "row": row_offset + i + 1,
                    "column": "*",
                    "value": "",
                    "reason": str(exc)[:300],
                    "policy": policy,
                }
            )
    return kept


def require_physical_types_for_existing_table(
    *,
    table_existed: bool,
    physical: dict[str, str] | None,
    dialect_label: str = "destination",
    target_cols: list[str] | None = None,
) -> str | None:
    """Error when an existing table yields empty or incomplete physical DDL.

    Map VARCHAR bind without live types invents NULL on typed sinks — refuse.
    Partial introspect (some cols missing) leaves Map stamps on those cols —
    refuse so overlay cannot soft-green empties into live DATE/INT.
    New / create-new tables may pass empty physical (Map stamps are physical).
    """
    if not table_existed:
        return None
    label = (dialect_label or "destination").strip() or "destination"
    if not physical:
        label_l = label.lower()
        if label_l in {
            "elasticsearch",
            "opensearch",
            "amazon_elasticsearch",
            "elastic_cloud",
        }:
            hint = (
                "Re-check index mapping privileges (get_mapping) and that mapped "
                "fields exist on the index, then retry."
            )
        elif label_l in {"mongodb", "dynamodb", "couchbase", "redis"}:
            hint = (
                "Re-check collection/table describe privileges and that mapped "
                "fields are present, then retry."
            )
        else:
            hint = "Re-check grants / information_schema visibility and retry."
        return (
            f"{label} physical DDL introspection returned empty for an existing "
            "table — refuse silent Map VARCHAR bind (empty→NULL invent risk). "
            f"{hint}"
        )
    if target_cols:
        missing = [
            c
            for c in target_cols
            if not (
                physical.get(c)
                or physical.get(str(c).lower())
                or physical.get(str(c).upper())
            )
        ]
        if missing:
            sample = ", ".join(missing[:12])
            more = f" (+{len(missing) - 12} more)" if len(missing) > 12 else ""
            return (
                f"{label} physical DDL missing for mapped column(s) "
                f"{sample}{more} — refuse silent Map stamp bind on an existing "
                "table (empty→NULL invent risk). Re-introspect or remap."
            )
    return None
