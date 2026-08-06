"""Slowly Changing Dimension Type 2 (SCD2) support for SQL destinations.

An SCD2 sync keeps a full history of every version of a row.  Each change to a
non-key attribute closes the previous version (valid_to + is_current=False) and
inserts a new current version (valid_from + is_current=True).  Re-running the
same source snapshot produces no new rows.
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
    """Stable hash of the non-SCD target attribute values."""
    from services.value_serializer import cell_to_string

    parts = []
    for c in target_cols:
        if c in SCD2_COLUMNS:
            continue
        parts.append(f"{c}={cell_to_string(row.get(c))}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


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


def _fetch_current_rows(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    keys: set[str],
    dialect_name: str,
) -> dict[str, str]:
    """Return {composite_key: row_hash} for current rows whose key is in ``keys``."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    if not keys or not pk_columns:
        return {}
    pk_select = ", ".join(quote_sql_identifier(c) for c in pk_columns)
    hash_quoted = quote_sql_identifier(ROW_HASH_COLUMN)
    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    where_keys, params = _pk_or_clause(pk_columns, keys, prefix="k")
    current_pred = f"{current_quoted} = 1" if dialect_name == "sqlite" else f"{current_quoted} IS TRUE"
    sql = (
        f"SELECT {pk_select}, {hash_quoted} FROM {qualified} "  # nosec B608
        f"WHERE {where_keys} AND {current_pred}"
    )
    result = conn.execute(sa.text(sql), params)
    out: dict[str, str] = {}
    for row in result:
        mapping = row._mapping
        key = _compose_key(dict(mapping), pk_columns)
        out[key] = str(mapping[ROW_HASH_COLUMN])
    return out


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
    current_pred = f"{current_quoted} = 1" if dialect_name == "sqlite" else f"{current_quoted} IS TRUE"
    false_lit = "0" if dialect_name == "sqlite" else "FALSE"
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
    """Read all current rows and compute an order-independent checksum."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    from services.reconciliation import canonical_checksum

    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    cols_quoted = ",".join(quote_sql_identifier(c) for c in target_cols if c not in SCD2_COLUMNS)
    rows: list[dict[str, Any]] = []
    count = 0
    offset = 0
    while True:
        sql = (
            f"SELECT {cols_quoted} FROM {qualified} "  # nosec B608
            f"WHERE {current_quoted} IS TRUE "
            f"LIMIT {batch_size} OFFSET {offset}"
        )
        if dialect_name == "sqlite":
            sql = (
                f"SELECT {cols_quoted} FROM {qualified} "  # nosec B608
                f"WHERE {current_quoted} = 1 "
                f"LIMIT {batch_size} OFFSET {offset}"
            )
        result = conn.execute(sa.text(sql))
        batch = result.fetchall()
        if not batch:
            break
        for row in batch:
            count += 1
            rows.append({c: row._mapping.get(c) for c in target_cols if c not in SCD2_COLUMNS})
        if len(batch) < batch_size:
            break
        offset += batch_size
    checksum = canonical_checksum(rows, [c for c in target_cols if c not in SCD2_COLUMNS]) if rows else ""
    return count, checksum


def prepare_scd2_mapped_rows(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Map + PK-validate SCD2 rows without writing history.

    Used for stream preflight (abort before any batch commits) and by
    ``apply_scd2`` before merge. Returns ``ok=False`` when FAIL_JOB / strict
    policy blocks a partial history write.
    """
    from connectors.writer_common import (
        apply_write_quarantine_matrix,
        build_mapped_rows_with_details,
        materialize_missing_as_null_for_dense_write,
        reject_on_strict_policy,
        sanitize_identifier,
        transform_error_policy,
        transform_error_policy_for_validation_mode,
    )
    from src.transfer.adapters import records_to_matrix
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
    error_policy = transform_error_policy_for_validation_mode(validation_mode)
    dest_kind = resolve_driver_type(getattr(endpoint, "format", "") or "")

    _, data_rows = records_to_matrix(records, columns)
    mapped_tuples, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=columns,
        data_rows=data_rows,
        mappings=effective_mappings,
        target_cols=target_cols,
        column_types=schema or {},
        dest_types=dest_types,
        error_policy=error_policy,
        preserve_case=True,
        dest_kind=dest_kind,
        destination_pk_columns=list(pk_columns),
    )
    # Same write-quarantine matrix as typed SQL writers — SCD2 history must not
    # absorb VARCHAR/DECIMAL/temporal overflow that preflight samples missed.
    target_types = [str(dest_types.get(c) or "") for c in target_cols]
    if mapped_tuples and any(t.strip() for t in target_types):
        policy = transform_error_policy(error_policy)
        mapped_tuples = apply_write_quarantine_matrix(
            mapped_tuples,
            target_cols,
            target_types,
            rejected_details,
            policy,
            dialect_label=(dest_kind or "SCD2").strip() or "SCD2",
            mappings=list(effective_mappings) or None,
        )
    # Dense history INSERT — STOP_COLUMN DF_MISSING → NULL (never leak sentinel).
    mapped_tuples = materialize_missing_as_null_for_dense_write(mapped_tuples)
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

    mapped_rows: list[dict[str, Any]] = [dict(zip(target_cols, row)) for row in mapped_tuples]

    # Empty PK identity must quarantine before history merge — never silent skip.
    pk_ok_rows: list[dict[str, Any]] = []
    for row_idx, row in enumerate(mapped_rows):
        key = _compose_key(row, pk_columns)
        # Never use truthiness — numeric 0 / "0" are valid PK values.
        parts: list[str] = []
        for c in pk_columns:
            raw = row.get(c)
            if raw is None:
                parts.append("")
            else:
                parts.append(str(raw).strip())
        # Any blank PK component is incomplete identity (including composites).
        if not key or any(p == "" for p in parts):
            rejected_details.append(
                {
                    "row": row_idx + 1,
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
    abort_pk = reject_on_strict_policy(error_policy, rejected_details, "SCD2")
    if abort_pk:
        return {
            "ok": False,
            "error": abort_pk,
            "mapped_rows": [],
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": list(rejected_details),
            "rejected_rows": len(rejected_details),
            "transform_errors": list(transform_errors)[:20],
            "error_policy": error_policy,
        }

    for row in pk_ok_rows:
        row[ROW_HASH_COLUMN] = _row_hash(row, target_cols)

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
) -> dict[str, Any]:
    """Apply an SCD2 merge to ``records`` against the SQL destination.

    ``conflict_columns`` is the destination primary key (one or more columns).
    Returns a summary dict with ``rows_written`` (new current versions),
    ``updated_rows`` (closed old versions), ``active_rows``, and ``active_checksum``.
    """

    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    prepared = prepare_scd2_mapped_rows(
        endpoint,
        records,
        columns,
        schema,
        mappings,
        conflict_columns,
        validation_mode=validation_mode,
    )
    pk_columns = list(prepared.get("primary_key_columns") or [])
    target_cols = list(prepared.get("target_columns") or [])
    rejected_details = list(prepared.get("rejected_details") or [])
    transform_errors = list(prepared.get("transform_errors") or [])
    if prepared.get("ok") is False:
        return {
            "ok": False,
            "error": prepared.get("error"),
            "rows_written": 0,
            "updated_rows": 0,
            "active_rows": 0,
            "active_checksum": "",
            "mode": "scd2",
            "primary_key_columns": pk_columns,
            "target_columns": target_cols,
            "rejected_details": rejected_details,
            "rejected_rows": len(rejected_details),
            "transform_errors": transform_errors[:20],
        }

    mapped_rows: list[dict[str, Any]] = list(prepared.get("mapped_rows") or [])

    db_type = resolve_driver_type(endpoint.format)
    cfg = resolve_connector_config(endpoint)
    table = endpoint.table or endpoint.collection or "dt_import"
    schema_name = get_sql_schema(cfg)

    engine = get_sqlalchemy_engine(cfg)
    dialect_name = engine.dialect.name if engine.dialect else ""

    column_types: dict[str, str] = {c: (schema or {}).get(c, "string") for c in target_cols}

    try:
        table_obj = _build_scd_table(
            engine, table, schema_name, target_cols, column_types, db_type
        )

        timestamp = _now_utc()
        inserted_total = 0
        expired_total = 0
        qualified = _qualified_name(table, schema_name)

        with engine.begin() as conn:
            for i in range(0, len(mapped_rows), batch_size):
                batch = mapped_rows[i : i + batch_size]
                keys: set[str] = set()
                for r in batch:
                    key = _compose_key(r, pk_columns)
                    if key and not all(p == "" for p in key.split(_KEY_SEP)):
                        keys.add(key)
                current_hashes = _fetch_current_rows(conn, qualified, pk_columns, keys, dialect_name)

                to_insert: list[dict[str, Any]] = []
                to_expire: set[str] = set()

                for row in batch:
                    key = _compose_key(row, pk_columns)
                    if not key or all(p == "" for p in key.split(_KEY_SEP)):
                        continue
                    new_hash = row[ROW_HASH_COLUMN]
                    if key in current_hashes and current_hashes[key] == new_hash:
                        continue
                    if key in current_hashes:
                        to_expire.add(key)
                    row[VALID_FROM_COLUMN] = timestamp
                    row[VALID_TO_COLUMN] = None
                    row[IS_CURRENT_COLUMN] = True
                    row[ROW_HASH_COLUMN] = new_hash
                    to_insert.append(row)

                if to_expire:
                    expired_total += _expire_rows(
                        conn, qualified, pk_columns, to_expire, timestamp, dialect_name
                    )
                inserted_total += _insert_rows(conn, table_obj, to_insert)

                # Update in-memory current_hashes so duplicate keys within the same batch
                # do not create multiple current versions.
                for row in to_insert:
                    current_hashes[_compose_key(row, pk_columns)] = row[ROW_HASH_COLUMN]

            active_rows, active_checksum = _active_checksum(
                conn, qualified, target_cols, batch_size, dialect_name
            )
    finally:
        release_engine(engine)

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
