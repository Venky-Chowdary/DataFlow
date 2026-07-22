"""Snowflake table reader — batched extraction for warehouse migrations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.snowflake_conn import (
    get_connection,
    normalize_account,
    resolve_or_fold_snowflake_table,
    snowflake_qualified_table,
)
from connectors.sql_identifiers import (
    quote_column_list,
    quote_sql_identifier,
    require_safe_identifier,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _use_warehouse(cur, warehouse: str) -> None:
    if not warehouse:
        return
    wh = require_safe_identifier(warehouse, preserve_case=True)
    from connectors.sql_identifiers import snowflake_fold_identifier

    # Warehouse names are usually uppercase; fold all-lower defaults safely.
    wh = snowflake_fold_identifier(wh) if wh == wh.lower() or wh == wh.upper() else wh
    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")


def _snowflake_schema(schema: str | None) -> str:
    from connectors.sql_identifiers import snowflake_fold_identifier

    return snowflake_fold_identifier((schema or "PUBLIC").strip() or "PUBLIC")


def _table_ref(cur: Any, schema: str, table: str) -> str:
    """Resolve + quote a Snowflake table (handles legacy lowercase quoted names)."""
    resolved = resolve_or_fold_snowflake_table(cur, schema, table)
    return snowflake_qualified_table(schema, resolved)


def count_table_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    role: str = "",
) -> int:
    del port
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def read_table_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 100_000,
    known_total_rows: int | None = None,
    role: str = "",
) -> ReadBatch:
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            if known_total_rows is not None:
                total = known_total_rows
            else:
                total = count_table_rows(
                    host=host, port=port, database=database, username=username, password=password,
                    schema=schema, connection_string=connection_string, warehouse=warehouse, table=table,
                    role=role,
                )
            col_sql = (
                quote_column_list([require_safe_identifier(c, preserve_case=True) for c in columns])
                if columns
                else "*"
            )
            cur.execute(
                f"SELECT {col_sql} FROM {table_ref} LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v, preserve_sql_null=True) for v in row] for row in cur.fetchall()]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        conn.close()


def read_table_cursor_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    cursor_column: str,
    cursor_after: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
    role: str = "",
) -> ReadBatch:
    """Read rows where cursor_column > watermark — incremental sync."""
    del port
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            col_sql = (
                quote_column_list([require_safe_identifier(c, preserve_case=True) for c in columns])
                if columns
                else "*"
            )
            cursor_q = quote_sql_identifier(require_safe_identifier(cursor_column, preserve_case=True))
            if cursor_after:
                cur.execute(
                    f"SELECT {col_sql} FROM {table_ref} "
                    f"WHERE {cursor_q} > %s ORDER BY {cursor_q} LIMIT %s",
                    (cursor_after, limit),
                )
            else:
                cur.execute(
                    f"SELECT {col_sql} FROM {table_ref} "
                    f"ORDER BY {cursor_q} LIMIT %s",
                    (limit,),
                )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v, preserve_sql_null=True) for v in row] for row in cur.fetchall()]
        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=len(rows))
    finally:
        conn.close()
