"""Slowly Changing Dimension Type 2 (SCD2) support for SQL destinations.

An SCD2 sync keeps a full history of every version of a row.  Each change to a
non-key attribute closes the previous version (valid_to + is_current=False) and
inserts a new current version (valid_from + is_current=True).  Re-running the
same source snapshot produces no new rows.

Source image (Fivetran / Airbyte incremental-deduped history / dbt snapshot
check strategy): hash non-SCD attributes, expire the current version when
the hash changes, insert a new current row. ``DF_MISSING`` hydrates from
the live current version so STOP_COLUMN cannot invent NULL history.

The source population is the shared ``SourceRowSpool`` — not
``records_to_matrix``. Map + write-quarantine + PK identity run per
bundle. ``apply_scd2`` fail-scans every reject, then merges one bundle
at a time (same two-pass contract as stream SCD2). Peak mapped RAM is
one bundle, not the full dict list. Not exactly-once. CDC default
remains at-least-once upsert. Catalog tiles ≠ transfer-live.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

from services.engine_pool import release_engine

VALID_FROM_COLUMN = "valid_from"
VALID_TO_COLUMN = "valid_to"
IS_CURRENT_COLUMN = "is_current"
ROW_HASH_COLUMN = "row_hash"

SCD2_COLUMNS = [VALID_FROM_COLUMN, VALID_TO_COLUMN, IS_CURRENT_COLUMN, ROW_HASH_COLUMN]
# Unit separator — safe delimiter for composite natural keys in-memory.
_KEY_SEP = "\x1f"


def stores_is_current_as_numeric(dialect: str) -> bool:
    """True when ``is_current`` is 0/1 storage, not ANSI BOOLEAN."""
    from services.dialect_profiles import stores_boolean_as_numeric

    return stores_boolean_as_numeric(dialect)


def scd2_is_current_predicate(dialect: str, quoted_column: str) -> str:
    """Dest-engine predicate for the current SCD2 version.

    SQLite INTEGER, SQL Server BIT, Oracle NUMBER(1) → ``= 1``.
    PostgreSQL / MySQL BOOLEAN → ``IS TRUE``. One rule for merge, expire,
    checksum, and conservation COUNT — not a per-engine patch.
    """
    from services.dialect_profiles import sql_bool_is_true

    return sql_bool_is_true(dialect, quoted_column)


def scd2_is_current_false_sql(dialect: str) -> str:
    """SQL literal that closes a current version (``is_current = …``)."""
    from services.dialect_profiles import sql_bool_false_literal

    return sql_bool_false_literal(dialect)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _compose_key(row: dict[str, Any], columns: list[str]) -> str:
    from services.value_serializer import cell_to_string

    return _KEY_SEP.join(cell_to_string(row.get(c)) for c in columns)


def _pk_or_clause(columns: list[str], keys: set[str], *, prefix: str) -> tuple[str, dict[str, Any]]:
    """Build ``(c1=:p0_0 AND c2=:p0_1) OR …`` for composite PK membership."""
    from connectors.writer_common import quote_sql_identifier

    if not keys or not columns:
        return "1=0", {}
    quoted = [quote_sql_identifier(c) for c in columns]
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, key in enumerate(keys):
        parts = key.split(_KEY_SEP)
        if len(parts) != len(columns):
            continue
        ands = []
        for j, col_q in enumerate(quoted):
            pname = f"{prefix}{i}_{j}"
            ands.append(f"{col_q} = :{pname}")
            params[pname] = parts[j]
        if ands:
            clauses.append("(" + " AND ".join(ands) + ")")
    if not clauses:
        return "1=0", {}
    return "(" + " OR ".join(clauses) + ")", params


def _qualified_name(table: str, schema: str | None) -> str:
    from connectors.writer_common import quote_sql_identifier

    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    return f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted


def _target_columns(records_columns: list[str], mappings: list[dict[str, Any]] | None) -> list[str]:
    from connectors.writer_common import resolve_target_columns

    if mappings:
        target_cols, _ = resolve_target_columns(mappings, {}, preserve_case=True)
        return target_cols
    return records_columns


def _row_hash(row: dict[str, Any], target_cols: list[str]) -> str:
    """Stable hash of the non-SCD target attribute values.

    ``DF_MISSING`` must never participate — callers hydrate omit-from-SET cells
    from the current SCD2 version before hashing so STOP_COLUMN cannot invent
    false history by hashing as empty/NULL.
    """
    from services.value_serializer import cell_to_string, is_missing_sentinel

    parts = []
    for c in target_cols:
        if c in SCD2_COLUMNS:
            continue
        val = row.get(c)
        if is_missing_sentinel(val):
            continue
        parts.append(f"{c}={cell_to_string(val)}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _hydrate_scd2_omit(
    row: dict[str, Any],
    current_attrs: dict[str, Any] | None,
    target_cols: list[str],
) -> dict[str, Any]:
    """Overlay DF_MISSING from the current SCD2 version (never invent NULL history)."""
    from services.value_serializer import is_missing_sentinel

    out = dict(row)
    for c in target_cols:
        if c in SCD2_COLUMNS:
            continue
        if not is_missing_sentinel(out.get(c)):
            continue
        if current_attrs is not None and c in current_attrs:
            out[c] = current_attrs[c]
        else:
            # New PK with STOP_COLUMN omit — NULL is honest for a brand-new version.
            out[c] = None
    return out


def _ensure_scd_columns(engine: Any, table_obj: Any, dialect_name: str) -> None:
    """Add SCD2 columns to an existing table if they are missing."""
    import sqlalchemy as sa

    inspector = sa.inspect(engine)
    schema = table_obj.schema
    table_name = table_obj.name
    existing = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    from connectors.generic_sql import _sa_type_for_logical
    from connectors.writer_common import quote_sql_identifier

    additions = []
    if VALID_FROM_COLUMN not in existing:
        additions.append((VALID_FROM_COLUMN, _sa_type_for_logical("datetime", dialect_name)))
    if VALID_TO_COLUMN not in existing:
        additions.append((VALID_TO_COLUMN, _sa_type_for_logical("datetime", dialect_name)))
    if IS_CURRENT_COLUMN not in existing:
        additions.append((IS_CURRENT_COLUMN, _sa_type_for_logical("boolean", dialect_name)))
    if ROW_HASH_COLUMN not in existing:
        additions.append((ROW_HASH_COLUMN, _sa_type_for_logical("string", dialect_name)))

    qualified = _qualified_name(table_name, schema)
    for col_name, sa_type in additions:
        col_quoted = quote_sql_identifier(col_name)
        type_ddl = sa_type.compile(dialect=engine.dialect)
        ddl = f"ALTER TABLE {qualified} ADD COLUMN {col_quoted} {type_ddl}"
        with engine.begin() as conn:
            try:
                conn.execute(sa.text(ddl))
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)


def _build_scd_table(
    engine: Any,
    table_name: str,
    schema_name: str | None,
    target_cols: list[str],
    column_types: dict[str, str],
    db_type: str,
) -> Any:
    """Build or reflect the destination table with SCD2 audit columns."""
    import sqlalchemy as sa
    from connectors.generic_sql import _build_table_for_write

    all_cols = list(dict.fromkeys(list(target_cols) + SCD2_COLUMNS))
    all_types = {**column_types}
    for scd_col, logical in {
        VALID_FROM_COLUMN: "datetime",
        VALID_TO_COLUMN: "datetime",
        IS_CURRENT_COLUMN: "boolean",
        ROW_HASH_COLUMN: "string",
    }.items():
        if scd_col not in all_types:
            all_types[scd_col] = logical

    table_obj = _build_table_for_write(
        engine,
        table_name,
        schema_name,
        all_cols,
        all_types,
        db_type=db_type,
        conflict_columns=None,
    )

    dialect_name = engine.dialect.name if engine.dialect else ""
    inspector = sa.inspect(engine)
    table_exists = inspector.has_table(table_name, schema=schema_name)
    if table_exists:
        _ensure_scd_columns(engine, table_obj, dialect_name)
    else:
        table_obj.create(engine, checkfirst=True)
    return table_obj


def _fetch_current_snapshots(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    target_cols: list[str],
    keys: set[str],
    dialect_name: str,
) -> dict[str, dict[str, Any]]:
    """Return {composite_key: {hash, attrs}} for current rows in ``keys``."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    if not keys or not pk_columns:
        return {}
    attr_cols = [c for c in target_cols if c not in SCD2_COLUMNS]
    select_cols = list(dict.fromkeys(list(pk_columns) + [ROW_HASH_COLUMN] + attr_cols))
    cols_quoted = ", ".join(quote_sql_identifier(c) for c in select_cols)
    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    where_keys, params = _pk_or_clause(pk_columns, keys, prefix="k")
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    sql = (
        f"SELECT {cols_quoted} FROM {qualified} "  # nosec B608
        f"WHERE {where_keys} AND {current_pred}"
    )
    result = conn.execute(sa.text(sql), params)
    out: dict[str, dict[str, Any]] = {}
    for row in result:
        mapping = dict(row._mapping)
        key = _compose_key(mapping, pk_columns)
        attrs = {c: mapping.get(c) for c in attr_cols}
        out[key] = {
            "hash": str(mapping.get(ROW_HASH_COLUMN) or ""),
            "attrs": attrs,
        }
    return out


def _fetch_current_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str],
    dialect_name: str,
) -> dict[str, str]:
    """Return {composite_key: row_hash} for current rows whose key is in ``keys``."""
    snaps = _fetch_current_snapshots(
        conn, qualified, pk_columns, pk_columns, keys, dialect_name
    )
    return {k: str(v.get("hash") or "") for k, v in snaps.items()}


def _insert_rows(conn: Any, table_obj: Any, rows: list[dict[str, Any]]) -> int:
    """Insert new SCD2 versions and return the number of rows inserted."""
    import sqlalchemy as sa

    if not rows:
        return 0
    result = conn.execute(sa.insert(table_obj), rows)
    return result.rowcount or len(rows)


def _expire_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str],
    timestamp: str,
    dialect_name: str,
) -> int:
    """Mark the current versions of ``keys`` as historical."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    if not keys or not pk_columns:
        return 0
    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    valid_to_quoted = quote_sql_identifier(VALID_TO_COLUMN)
    where_keys, params = _pk_or_clause(pk_columns, keys, prefix="e")
    params["ts"] = timestamp
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    false_lit = scd2_is_current_false_sql(dialect_name)
    sql = (
        f"UPDATE {qualified} "  # nosec B608
        f"SET {valid_to_quoted} = :ts, {current_quoted} = {false_lit} "
        f"WHERE {where_keys} AND {current_pred}"
    )
    result = conn.execute(sa.text(sql), params)
    return result.rowcount or 0


def _active_checksum(
    conn: Any,
    qualified: str,
    target_cols: list[str],
    batch_size: int,
    dialect_name: str,
) -> tuple[int, str]:
    """Current-version digest: one streamed SELECT. Never OFFSET.

    SQL Server FETCH requires ORDER BY; Oracle/DB2 reject LIMIT; OFFSET is
    O(n²) and can skip/duplicate. Same streaming kernel as Gate-8 read-back.
    """
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier
    from services.reconciliation_api import stream_select_checksum

    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    current_pred = scd2_is_current_predicate(dialect_name, current_quoted)
    attr_cols = [c for c in target_cols if c not in SCD2_COLUMNS]
    cols_quoted = ",".join(quote_sql_identifier(c) for c in attr_cols)
    sql = f"SELECT {cols_quoted} FROM {qualified} WHERE {current_pred}"  # nosec B608
    return stream_select_checksum(
        conn,
        sa.text(sql),
        attr_cols,
        itersize=max(1, int(batch_size)),
        dest_db_type=dialect_name,
    )


def _scd2_map_context(
    endpoint: Any,
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str,
) -> dict[str, Any]:
    """Shared Map / dest-type / policy facts for prepare and apply."""
    from connectors.writer_common import (
        sanitize_identifier,
        transform_error_policy_for_validation_mode,
    )
    from src.transfer.connector_capabilities import resolve_driver_type

    if not conflict_columns:
        raise ValueError("SCD2 sync requires a primary key / conflict column")
    pk_columns = [c for c in conflict_columns if c]
    if not pk_columns:
        raise ValueError("SCD2 sync requires a primary key / conflict column")

    target_cols = _target_columns(columns, mappings)
    effective_mappings = mappings or [{"source": c, "target": c} for c in columns]
    dest_types = {c: (schema or {}).get(c, "string") for c in target_cols}
    # Prefer Map target_type stamps when present (DECIMAL(p,s) / VARCHAR(n)).
    # Stamp under sanitized target names — same key space as target_cols / quarantine.
    for m in effective_mappings:
        tgt_raw = str(m.get("target") or "").strip()
        stamped = str(m.get("target_type") or m.get("dest_type") or "").strip()
        if not tgt_raw or not stamped:
            continue
        tgt = sanitize_identifier(tgt_raw, preserve_case=True)
        dest_types[tgt] = stamped
        if tgt_raw != tgt:
            dest_types.pop(tgt_raw, None)
    return {
        "pk_columns": pk_columns,
        "target_cols": target_cols,
        "effective_mappings": effective_mappings,
        "dest_types": dest_types,
        "target_types": [str(dest_types.get(c) or "") for c in target_cols],
        "error_policy": transform_error_policy_for_validation_mode(validation_mode),
        "dest_kind": resolve_driver_type(getattr(endpoint, "format", "") or ""),
        "dest_nullability": dict(
            (getattr(endpoint, "extra", None) or {}).get("schema_nullability") or {}
        ),
        "schema": schema or {},
        "extra": (
            getattr(endpoint, "extra", None)
            if isinstance(getattr(endpoint, "extra", None), dict)
            else {}
        ),
    }


def _pk_validate_mapped_rows(
    mapped_rows: list[dict[str, Any]],
    pk_columns: list[str],
    rejected_details: list[dict[str, Any]],
    *,
    row_numbers: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Empty / DF_MISSING PK identity quarantines — never a silent skip."""
    from services.value_serializer import is_missing_sentinel

    pk_ok_rows: list[dict[str, Any]] = []
    for row_idx, row in enumerate(mapped_rows):
        row_no = (
            int(row_numbers[row_idx])
            if row_numbers is not None and row_idx < len(row_numbers)
            else row_idx + 1
        )
        if any(is_missing_sentinel(row.get(c)) for c in pk_columns):
            rejected_details.append(
                {
                    "row": row_no,
                    "column": ",".join(pk_columns),
                    "target": ",".join(pk_columns),
                    "value": "",
                    "reason": "SCD2 primary key cannot be DF_MISSING / STOP_COLUMN omit",
                    "policy": "quarantine",
                    "chars": [],
                }
            )
            continue
        key = _compose_key(row, pk_columns)
        # Never use truthiness — numeric 0 / "0" are valid PK values.
        parts: list[str] = []
        for c in pk_columns:
            raw = row.get(c)
            if raw is None:
                parts.append("")
            else:
                parts.append(str(raw).strip())
        if not key or any(p == "" for p in parts):
            rejected_details.append(
                {
                    "row": row_no,
                    "column": ",".join(pk_columns),
                    "target": ",".join(pk_columns),
                    "value": "",
                    "reason": "SCD2 row missing primary key identity",
                    "policy": "quarantine",
                    "chars": [],
                }
            )
            continue
        pk_ok_rows.append(row)
    return pk_ok_rows


def _finish_scd2_map_bundle(
    bundle: Any,
    ctx: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Quarantine + PK-validate one mapped bundle. No last-write-wins drop."""
    from connectors.writer_common import (
        apply_write_quarantine_matrix_keeping_numbers,
        transform_error_policy,
    )

    target_cols = list(ctx["target_cols"])
    details = list(bundle.rejected_details)
    errors = list(bundle.transform_errors)
    mapped = list(bundle.mapped_rows)
    nums = list(bundle.accepted_source_rows or [])
    target_types = list(ctx["target_types"])
    if mapped and any(t.strip() for t in target_types):
        policy = transform_error_policy(ctx["error_policy"])
        mapped, nums = apply_write_quarantine_matrix_keeping_numbers(
            mapped,
            target_cols,
            target_types,
            details,
            policy,
            dialect_label=(ctx["dest_kind"] or "SCD2").strip() or "SCD2",
            mappings=list(ctx["effective_mappings"]) or None,
            dest_db=str(ctx["dest_kind"] or ""),
            source_row_numbers=nums or None,
        )
    dicts = [dict(zip(target_cols, row)) for row in mapped]
    pk_ok = _pk_validate_mapped_rows(
        dicts, list(ctx["pk_columns"]), details, row_numbers=nums or None
    )
    return pk_ok, details, errors


def iter_scd2_prepared_bundles(
    *,
    columns: list[str],
    ctx: dict[str, Any],
    records: list[dict[str, Any]] | None = None,
    source_spool: Any = None,
    batch_size: int = 1_000,
):
    """Yield ``(pk_ok_rows, rejected_details, transform_errors)`` per spool bundle.

    Reuses ``iter_mapped_bundles_from_source``. Does not last-write-wins
    inside the bundle — SCD2 merge applies every PK-ok arrival in order
    (intra-batch hash update after insert).
    """
    from connectors.sql_write_materialize import iter_mapped_bundles_from_source

    for bundle in iter_mapped_bundles_from_source(
        headers=columns,
        mappings=ctx["effective_mappings"],
        target_cols=list(ctx["target_cols"]),
        column_types=ctx["schema"],
        dest_types=ctx["dest_types"],
        error_policy=ctx["error_policy"],
        preserve_case=True,
        dest_kind=ctx["dest_kind"],
        destination_pk_columns=list(ctx["pk_columns"]),
        destination_column_nullability=ctx["dest_nullability"],
        records=None if source_spool is not None else records,
        source_spool=source_spool,
        extra=ctx.get("extra") or {},
        batch_size=batch_size,
    ):
        pk_ok, details, errors = _finish_scd2_map_bundle(bundle, ctx)
        yield pk_ok, details, errors
        del bundle, pk_ok


def prepare_scd2_mapped_rows(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str = "strict",
    source_spool: Any = None,
    clear_records: bool = False,
    retain_mapped: bool = True,
    batch_size: int = 1_000,
) -> dict[str, Any]:
    """Map + PK-validate SCD2 rows without writing history.

    Used for stream preflight (abort before any batch commits). Returns
    ``ok=False`` when FAIL_JOB / strict policy blocks a partial history
    write. Source cells come from the shared spool — never
    ``records_to_matrix``. ``retain_mapped=False`` keeps only reject
    details (apply's fail-scan). Hash is finalized in ``apply_scd2``
    after DF_MISSING hydrate from the current version.
    """
    from connectors.sql_write_materialize import ensure_sql_source_spool
    from connectors.writer_common import reject_on_strict_policy

    ctx = _scd2_map_context(
        endpoint,
        columns,
        schema,
        mappings,
        conflict_columns,
        validation_mode=validation_mode,
    )
    pk_columns = list(ctx["pk_columns"])
    target_cols = list(ctx["target_cols"])
    error_policy = ctx["error_policy"]
    spool, close_spool = ensure_sql_source_spool(
        headers=columns,
        records=None if source_spool is not None else records,
        mappings=ctx["effective_mappings"],
        extra=ctx.get("extra") or {},
        source_spool=source_spool,
    )
    if clear_records and records is not None:
        records.clear()
    rejected_details: list[dict[str, Any]] = []
    transform_errors: list[str] = []
    pk_ok_rows: list[dict[str, Any]] = []
    try:
        for pk_ok, details, errors in iter_scd2_prepared_bundles(
            columns=columns,
            ctx=ctx,
            source_spool=spool,
            batch_size=batch_size,
        ):
            rejected_details.extend(details)
            transform_errors.extend(errors)
            if retain_mapped:
                pk_ok_rows.extend(pk_ok)
            del pk_ok
    finally:
        if close_spool:
            spool.close()

    abort = reject_on_strict_policy(error_policy, rejected_details, "SCD2")
    if abort:
        return {
            "ok": False,
            "error": abort,
            "mapped_rows": [],
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": list(rejected_details),
            "rejected_rows": len(rejected_details),
            "transform_errors": list(transform_errors)[:20],
            "error_policy": error_policy,
        }

    return {
        "ok": True,
        "mapped_rows": pk_ok_rows,
        "primary_key_columns": pk_columns,
        "target_columns": target_cols,
        "rejected_details": list(rejected_details),
        "rejected_rows": len(rejected_details),
        "transform_errors": list(transform_errors)[:20],
        "error_policy": error_policy,
    }


def _merge_scd2_bundle(
    conn: Any,
    table_obj: Any,
    qualified: str,
    batch: list[dict[str, Any]],
    pk_columns: list[str],
    target_cols: list[str],
    dialect_name: str,
    timestamp: datetime,
) -> tuple[int, int]:
    """Expire + insert one PK-ok bundle. Intra-bundle last hash wins after insert."""
    keys: set[str] = set()
    for row in batch:
        key = _compose_key(row, pk_columns)
        if key and not all(p == "" for p in key.split(_KEY_SEP)):
            keys.add(key)
    current_snaps = _fetch_current_snapshots(
        conn, qualified, pk_columns, target_cols, keys, dialect_name
    )
    current_hashes = {k: str(v.get("hash") or "") for k, v in current_snaps.items()}

    to_insert: list[dict[str, Any]] = []
    to_expire: set[str] = set()

    for row in batch:
        key = _compose_key(row, pk_columns)
        if not key or all(p == "" for p in key.split(_KEY_SEP)):
            continue
        snap = current_snaps.get(key)
        hydrated = _hydrate_scd2_omit(
            row,
            (snap or {}).get("attrs") if snap else None,
            target_cols,
        )
        new_hash = _row_hash(hydrated, target_cols)
        hydrated[ROW_HASH_COLUMN] = new_hash
        if key in current_hashes and current_hashes[key] == new_hash:
            continue
        if key in current_hashes:
            to_expire.add(key)
        hydrated[VALID_FROM_COLUMN] = timestamp
        hydrated[VALID_TO_COLUMN] = None
        hydrated[IS_CURRENT_COLUMN] = True
        to_insert.append(hydrated)

    expired = 0
    if to_expire:
        expired = _expire_rows(
            conn, qualified, pk_columns, to_expire, timestamp, dialect_name
        )
    inserted = _insert_rows(conn, table_obj, to_insert)
    # Same-connection later bundles must see this bundle's current hash
    # (the original in-memory update after insert).
    for row in to_insert:
        key = _compose_key(row, pk_columns)
        current_hashes[key] = row[ROW_HASH_COLUMN]
        current_snaps[key] = {
            "hash": row[ROW_HASH_COLUMN],
            "attrs": {c: row.get(c) for c in target_cols if c not in SCD2_COLUMNS},
        }
    return inserted, expired


def apply_scd2(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    batch_size: int = 1_000,
    validation_mode: str = "strict",
    source_spool: Any = None,
    clear_records: bool = False,
) -> dict[str, Any]:
    """Apply an SCD2 merge against the SQL destination.

    ``conflict_columns`` is the destination primary key (one or more columns).
    Returns a summary dict with ``rows_written`` (new current versions),
    ``updated_rows`` (closed old versions), ``active_rows``, and ``active_checksum``.

    Fail / FAIL_JOB: scan every spool bundle, collect every reject, then
    refuse the history write — never expire/insert a prefix. Merge pass
    remaps from the same spool one bundle at a time. Peak mapped RAM is
    one bundle. Not exactly-once.
    """

    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from connectors.sql_write_materialize import ensure_sql_source_spool
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    ctx = _scd2_map_context(
        endpoint,
        columns,
        schema,
        mappings,
        conflict_columns,
        validation_mode=validation_mode,
    )
    pk_columns = list(ctx["pk_columns"])
    target_cols = list(ctx["target_cols"])
    spool, close_spool = ensure_sql_source_spool(
        headers=columns,
        records=None if source_spool is not None else records,
        mappings=ctx["effective_mappings"],
        extra=ctx.get("extra") or {},
        source_spool=source_spool,
    )
    if clear_records and records is not None:
        records.clear()

    def _blocked(error: str, rejected: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
        return {
            "ok": False,
            "error": error,
            "rows_written": 0,
            "updated_rows": 0,
            "active_rows": 0,
            "active_checksum": "",
            "mode": "scd2",
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": list(rejected),
            "rejected_rows": len(rejected),
            "transform_errors": list(errors)[:20],
        }

    try:
        scan = prepare_scd2_mapped_rows(
            endpoint,
            [],
            columns,
            schema,
            mappings,
            conflict_columns,
            validation_mode=validation_mode,
            source_spool=spool,
            retain_mapped=False,
            batch_size=batch_size,
        )
        rejected_details = list(scan.get("rejected_details") or [])
        transform_errors = list(scan.get("transform_errors") or [])
        if scan.get("ok") is False:
            return _blocked(
                str(scan.get("error") or "SCD2 map/Risk Contract blocked history merge"),
                rejected_details,
                transform_errors,
            )

        db_type = resolve_driver_type(endpoint.format)
        cfg = resolve_connector_config(endpoint)
        table = endpoint.table or endpoint.collection or "dt_import"
        schema_name = get_sql_schema(cfg)

        engine = get_sqlalchemy_engine(cfg)
        dialect_name = engine.dialect.name if engine.dialect else ""
        column_types: dict[str, str] = {
            c: (schema or {}).get(c, "string") for c in target_cols
        }

        try:
            table_obj = _build_scd_table(
                engine, table, schema_name, target_cols, column_types, db_type
            )
            timestamp = _now_utc()
            inserted_total = 0
            expired_total = 0
            qualified = _qualified_name(table, schema_name)

            with engine.begin() as conn:
                for pk_ok, _details, _errors in iter_scd2_prepared_bundles(
                    columns=columns,
                    ctx=ctx,
                    source_spool=spool,
                    batch_size=batch_size,
                ):
                    if pk_ok:
                        inserted, expired = _merge_scd2_bundle(
                            conn,
                            table_obj,
                            qualified,
                            pk_ok,
                            pk_columns,
                            target_cols,
                            dialect_name,
                            timestamp,
                        )
                        inserted_total += inserted
                        expired_total += expired
                    del pk_ok

                active_rows, active_checksum = _active_checksum(
                    conn, qualified, target_cols, batch_size, dialect_name
                )
        finally:
            release_engine(engine)
    finally:
        if close_spool:
            spool.close()

    return {
        "ok": True,
        "rows_written": inserted_total,
        "updated_rows": expired_total,
        "active_rows": active_rows,
        "active_checksum": active_checksum,
        "mode": "scd2",
        "primary_key_columns": pk_columns,
        "target_columns": target_cols,
        "rejected_details": list(rejected_details),
        "rejected_rows": len(rejected_details),
        "transform_errors": list(transform_errors)[:20],
    }
