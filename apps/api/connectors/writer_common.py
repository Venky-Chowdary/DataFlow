"""Shared row mapping utilities for database writers."""

from __future__ import annotations

import json
import os
import re
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
CHUNK_SIZE = int(os.getenv("DATAFLOW_CHUNK_SIZE", "20000"))
TRANSFORM_ERROR_POLICY = os.getenv("DATAFLOW_TRANSFORM_ERROR_POLICY", "quarantine").lower()
VALID_ERROR_POLICIES = {"fail", "quarantine", "coerce_null"}


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


def to_json_value(value: Any, col: str, dest_types: dict[str, str]) -> Any:
    """Convert a mapped cell to a JSON-serializable scalar.

    Preserves strings/dates as text; parses structural and numeric JSON values
    into native Python types so object-store exports contain numbers/objects
    instead of quoted Decimal strings. Temporal logical types are normalized via
    the shared SQL temporal helpers so ISO-Z does not leak inconsistently across
    S3/GCS/ADLS/SFTP/Kafka JSON exports.
    """
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
                return json.loads(text, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        if ctype in {"text", "string", "varchar", "uuid", "binary"}:
            return value
        try:
            return json.loads(text, parse_constant=lambda v: None)
        except json.JSONDecodeError:
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


def row_checksum(rows: list[Any], columns: list[str] | None = None) -> str:
    return checksum_rows(rows, columns)


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


def lsn_sort_key(lsn: Any) -> tuple:
    """Return a sortable key for PG ``hi/lo``, MySQL ``file:pos``, versions, or opaque tokens.

    Kind order is only used within the same stamp family; cross-family compares
    fall back to the opaque string tail so mixed CDC→one-sink stays deterministic.
    """
    if lsn is None:
        return (0, -1, -1, "")
    text = str(lsn).strip()
    if not text:
        return (0, -1, -1, "")
    # Postgres WAL LSN: hex/hex (reject paths that look like URLs).
    if "/" in text and not text.lower().startswith("gtid:"):
        hi, _, lo = text.partition("/")
        if hi and lo and all(c in "0123456789abcdefABCDEF" for c in hi + lo):
            try:
                return (3, int(hi, 16), int(lo, 16), "")
            except ValueError:
                pass
    # MySQL binlog file:pos (pos may already be zero-padded from extract_cdc_lsn).
    if ":" in text and not text.lower().startswith("gtid:"):
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return (2, file_name, int(pos), "")
    # Zero-padded / numeric versions (SQL Server CT, etc.).
    if text.isdigit():
        return (1, int(text), 0, "")
    return (0, 0, 0, text)


def compare_lsn(left: Any, right: Any) -> int:
    """Compare two LSN-like values. Returns -1, 0, or 1."""
    a, b = lsn_sort_key(left), lsn_sort_key(right)
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def lsn_is_newer(incoming: Any, existing: Any) -> bool:
    """True when ``incoming`` should replace ``existing`` under at-least-once CDC."""
    if existing is None or str(existing).strip() == "":
        return True
    if incoming is None or str(incoming).strip() == "":
        return False
    return compare_lsn(incoming, existing) > 0


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
            if value is not None and str(value).strip():
                if key == "version":
                    try:
                        return f"{int(value):020d}"
                    except (TypeError, ValueError):
                        return str(value).strip()
                return str(value).strip()
        # Mongo resume token often is the whole dict with ``_data``.
        data = resume_token.get("_data")
        if data is not None and str(data).strip():
            return str(data).strip()
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

    Real PG ``hi/lo`` LSNs use ``::pg_lsn``. Mixed CDC stamps (``file:pos``,
    zero-padded versions, opaque tokens) use strict text ``>`` so an *older*
    redelivery does not win — never ``IS DISTINCT FROM`` (any different stamp).
    GTID sets remain best-effort lexicographic (still at-least-once, not causal).
    """
    pg_pat = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    excl = f'EXCLUDED."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'
    return (
        f"( "
        f"({excl} ~ '{pg_pat}' AND COALESCE({dest}, '') ~ '{pg_pat}' "
        f"AND {excl}::pg_lsn > COALESCE(NULLIF({dest}, '')::pg_lsn, '0/0'::pg_lsn)) "
        f"OR "
        f"({excl} !~ '{pg_pat}' AND ("
        f"{dest} IS NULL OR {dest} = '' OR {excl} > {dest}"
        f")) "
        f")"
    )


def mysql_lsn_values_newer_sql(lsn_column: str = DF_LSN_COL, *, quote: str = "`") -> str:
    """Boolean SQL: ``VALUES(lsn)`` is strictly newer than the destination cell.

    Handles empty dest, ``file:pos`` (file then integer pos), and lexicographic
    fallback for padded versions / opaque tokens. Used inside
    ``ON DUPLICATE KEY UPDATE col=IF(<pred>, VALUES(col), col)``.
    """
    col = f"{quote}{lsn_column}{quote}"
    inc = f"VALUES({col})"
    dest = col
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
    return (
        f"({dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR (NOT ({both_filepos}) AND {inc} > {dest}))"
    )


def sqlite_lsn_update_guard_sql(table_name: str, lsn_column: str = DF_LSN_COL) -> str:
    """WHERE fragment for SQLite ``ON CONFLICT DO UPDATE`` (lexicographic LSN).

    Callers should stamp ``file:pos`` via :func:`extract_cdc_lsn` so positions
    are zero-padded and text order matches :func:`compare_lsn`.
    """
    excl = f'excluded."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'
    return f"({dest} IS NULL OR {dest} = '' OR {excl} > {dest})"


def snowflake_lsn_match_predicate(
    target_alias: str = "t",
    source_alias: str = "s",
    lsn_column: str = DF_LSN_COL,
) -> str:
    """MATCHED guard for Snowflake MERGE using lexicographic LSN text order."""
    return (
        f'{source_alias}."{lsn_column}" > COALESCE({target_alias}."{lsn_column}", \'\')'
    )


def transform_error_policy(policy: str | None = None) -> str:
    selected = (policy or TRANSFORM_ERROR_POLICY or "quarantine").strip().lower()
    return selected if selected in VALID_ERROR_POLICIES else "quarantine"


def reject_on_strict_policy(
    policy: str | None,
    rejected_details: list[dict[str, Any]] | None,
    label: str,
) -> str | None:
    """Return an error message when strict mode must refuse a partial write."""
    if transform_error_policy(policy) == "fail" and rejected_details:
        return (
            f"{label} rejected {len(rejected_details)} row(s); "
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
) -> int:
    """Return the number of rows that were rejected or quarantined.

    For ``fail`` the dropped rows are ``len(data_rows) - len(mapped_rows)``.
    For ``quarantine``/``coerce_null`` the rows are preserved with a NULL bad cell,
    so the count is the number of distinct source row numbers with at least one
    rejected cell.
    """
    if policy in {"quarantine", "coerce_null"}:
        return len({d["row"] for d in rejected_details})
    return len(data_rows) - len(mapped_rows)


def _coerced_null_row_count(rejected_details: list[dict[str, Any]], policy: str) -> int:
    """Distinct source rows that were KEPT but had a cell coerced to NULL.

    Only meaningful under ``quarantine``/``coerce_null`` — under ``fail`` the
    offending rows are dropped, not coerced, so this is 0. ``rejected_details``
    only contains cells whose ``apply_transform`` returned an error, so genuine
    empty->NULL sentinels (no error) are correctly excluded.
    """
    if policy in {"quarantine", "coerce_null"}:
        return len({d["row"] for d in rejected_details})
    return 0


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
) -> tuple[list[tuple], list[str]]:
    """Returns mapped rows and any transform errors (first 10)."""
    mapped, errors, _ = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=error_policy,
        dest_types=dest_types,
        preserve_case=preserve_case,
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
) -> tuple[list[tuple], list[str], list[dict[str, Any]]]:
    """Returns mapped rows, error messages, and structured rejected-row details."""
    from services.json_intelligence import materialize_struct_policies

    column_types = column_types or {}
    policy = transform_error_policy(error_policy)
    # Honor Map STRUCT policy (JSON blob vs flatten top-level keys) before bind.
    headers, data_rows = materialize_struct_policies(headers, data_rows, mappings)
    source_indices = {h: i for i, h in enumerate(headers)}
    sanitized_target_cols = [sanitize_identifier(c, preserve_case=preserve_case) for c in target_cols]
    target_index = {c: i for i, c in enumerate(sanitized_target_cols)}
    errors: list[str] = []
    rejected_details: list[dict[str, Any]] = []

    mapping_infos = []
    for m in mappings:
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
        ))

    mapped: list[tuple] = []
    for row_number, raw in enumerate(data_rows, start=1):
        out = [None] * len(sanitized_target_cols)
        row_has_error = False
        for source_idx, target_idx, transform, src_name, tgt_name in mapping_infos:
            val = raw[source_idx] if source_idx is not None and source_idx < len(raw) else None
            converted, err = apply_transform(val, transform)
            if err:
                row_has_error = True
                detail = {
                    "row": row_number,
                    "column": src_name,
                    "target": tgt_name,
                    "value": str(val) if val is not None else "",
                    "reason": err,
                    "policy": policy,
                    # Full source row so quarantine replay can rewrite without re-reading.
                    "values": {
                        h: (str(raw[i]) if i < len(raw) and raw[i] is not None else "")
                        for i, h in enumerate(headers)
                    },
                }
                rejected_details.append(detail)
                if len(errors) < 10:
                    errors.append(f"row {row_number} {src_name}→{tgt_name}: {err}")
                if policy in {"coerce_null", "quarantine"}:
                    # Quarantine preserves the row; the bad cell becomes NULL and the
                    # error is surfaced as a warning so the transfer does not silently
                    # lose data.
                    converted = None
                else:
                    continue
            if target_idx >= 0:
                out[target_idx] = converted
        if row_has_error and policy == "fail":
            continue
        mapped.append(tuple(out))

    return mapped, errors, rejected_details


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

    For **new tables** (``table_exists is False``), proposed typed DDL is
    widened via ``safe_ddl_logical_type`` when samples cannot all coerce —
    e.g. status enums never CREATE as BOOLEAN.
    """
    from services.schema_inference import safe_ddl_logical_type

    target_cols: list[str] = []
    target_types: list[str] = []
    samples = sample_values_by_source or {}
    for m in mappings:
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
                proposed = safe_ddl_logical_type(
                    str(proposed),
                    samples.get(src),
                    field_name=src,
                    source_type=str(src_type) if src_type else None,
                    honor_explicit=explicit_target,
                )
            target_types.append(str(proposed))
    return target_cols, target_types
