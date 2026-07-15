"""Slowly Changing Dimension Type 2 (SCD2) support for SQL destinations.

An SCD2 sync keeps a full history of every version of a row.  Each change to a
non-key attribute closes the previous version (valid_to + is_current=False) and
inserts a new current version (valid_from + is_current=True).  Re-running the
same source snapshot produces no new rows.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

VALID_FROM_COLUMN = "valid_from"
VALID_TO_COLUMN = "valid_to"
IS_CURRENT_COLUMN = "is_current"
ROW_HASH_COLUMN = "row_hash"

SCD2_COLUMNS = [VALID_FROM_COLUMN, VALID_TO_COLUMN, IS_CURRENT_COLUMN, ROW_HASH_COLUMN]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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
            except Exception:
                pass


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

    from connectors.generic_sql import _build_table_for_write, _sa_type_for_logical

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
    pk_target: str,
    keys: set[str],
    dialect_name: str,
) -> dict[str, str]:
    """Return {key: row_hash} for current rows whose key is in ``keys``."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    if not keys:
        return {}
    pk_quoted = quote_sql_identifier(pk_target)
    hash_quoted = quote_sql_identifier(ROW_HASH_COLUMN)
    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)

    # SQLite and PostgreSQL both bind lists with ``IN (:k0, :k1, ...)``.
    placeholders = ",".join([f":k{i}" for i in range(len(keys))])
    params = {f"k{i}": k for i, k in enumerate(keys)}
    sql = (
        f"SELECT {pk_quoted}, {hash_quoted} FROM {qualified} "
        f"WHERE {pk_quoted} IN ({placeholders}) AND {current_quoted} IS TRUE"
    )
    # SQLite does not have a TRUE literal in some contexts; fallback to 1.
    if dialect_name == "sqlite":
        sql = (
            f"SELECT {pk_quoted}, {hash_quoted} FROM {qualified} "
            f"WHERE {pk_quoted} IN ({placeholders}) AND {current_quoted} = 1"
        )
    result = conn.execute(sa.text(sql), params)
    return {str(row._mapping[pk_target]): str(row._mapping[ROW_HASH_COLUMN]) for row in result}


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
    pk_target: str,
    keys: set[str],
    timestamp: str,
    dialect_name: str,
) -> int:
    """Mark the current versions of ``keys`` as historical."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    if not keys:
        return 0
    pk_quoted = quote_sql_identifier(pk_target)
    current_quoted = quote_sql_identifier(IS_CURRENT_COLUMN)
    valid_to_quoted = quote_sql_identifier(VALID_TO_COLUMN)
    placeholders = ",".join([f":k{i}" for i in range(len(keys))])
    params = {f"k{i}": k for i, k in enumerate(keys)}
    params["ts"] = timestamp
    sql = (
        f"UPDATE {qualified} "
        f"SET {valid_to_quoted} = :ts, {current_quoted} = FALSE "
        f"WHERE {pk_quoted} IN ({placeholders}) AND {current_quoted} IS TRUE"
    )
    if dialect_name == "sqlite":
        sql = (
            f"UPDATE {qualified} "
            f"SET {valid_to_quoted} = :ts, {current_quoted} = 0 "
            f"WHERE {pk_quoted} IN ({placeholders}) AND {current_quoted} = 1"
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
            f"SELECT {cols_quoted} FROM {qualified} "
            f"WHERE {current_quoted} IS TRUE "
            f"LIMIT {batch_size} OFFSET {offset}"
        )
        if dialect_name == "sqlite":
            sql = (
                f"SELECT {cols_quoted} FROM {qualified} "
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


def apply_scd2(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    batch_size: int = 1_000,
) -> dict[str, Any]:
    """Apply an SCD2 merge to ``records`` against the SQL destination.

    ``conflict_columns`` is the destination primary key (currently one column).
    Returns a summary dict with ``rows_written`` (new current versions),
    ``updated_rows`` (closed old versions), ``active_rows``, and ``active_checksum``.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine, get_sql_schema
    from connectors.writer_common import build_mapped_rows, resolve_target_columns
    from src.transfer.adapters import records_to_matrix, resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    if not conflict_columns:
        raise ValueError("SCD2 sync requires a primary key / conflict column")
    if len(conflict_columns) > 1:
        raise NotImplementedError("composite primary key SCD2 is not yet supported")

    pk_target = conflict_columns[0]

    target_cols = _target_columns(columns, mappings)
    target_set = set(target_cols)

    _, data_rows = records_to_matrix(records, columns)
    mapped_tuples, _ = build_mapped_rows(
        headers=columns,
        data_rows=data_rows,
        mappings=mappings or [{"source": c, "target": c} for c in columns],
        target_cols=target_cols,
        column_types=schema or {},
        error_policy="quarantine",
        preserve_case=True,
    )

    mapped_rows: list[dict[str, Any]] = [dict(zip(target_cols, row)) for row in mapped_tuples]

    for row in mapped_rows:
        row[ROW_HASH_COLUMN] = _row_hash(row, target_cols)

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

        with engine.begin() as conn:
            for i in range(0, len(mapped_rows), batch_size):
                batch = mapped_rows[i : i + batch_size]
                keys = {str(r.get(pk_target, "")) for r in batch if r.get(pk_target) is not None}
                current_hashes = _fetch_current_rows(conn, _qualified_name(table, schema_name), pk_target, keys, dialect_name)

                to_insert: list[dict[str, Any]] = []
                to_expire: set[str] = set()

                for row in batch:
                    key = str(row.get(pk_target, ""))
                    if not key:
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
                    expired_total += _expire_rows(conn, _qualified_name(table, schema_name), pk_target, to_expire, timestamp, dialect_name)
                inserted_total += _insert_rows(conn, table_obj, to_insert)

                # Update in-memory current_hashes so duplicate keys within the same batch
                # do not create multiple current versions.
                for row in to_insert:
                    current_hashes[str(row.get(pk_target, ""))] = row[ROW_HASH_COLUMN]

            active_rows, active_checksum = _active_checksum(
                conn, _qualified_name(table, schema_name), target_cols, batch_size, dialect_name
            )
    finally:
        engine.dispose()

    return {
        "rows_written": inserted_total,
        "updated_rows": expired_total,
        "active_rows": active_rows,
        "active_checksum": active_checksum,
        "mode": "scd2",
        "target_columns": target_cols,
    }
