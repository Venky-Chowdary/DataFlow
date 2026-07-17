"""Snowflake table reader — batched extraction for warehouse migrations."""

from __future__ import annotations

import sys
from pathlib import Path

from connectors.base import ReadBatch
from connectors.snowflake_conn import get_connection, normalize_account

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


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
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema or "PUBLIC",
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            sch = schema or "PUBLIC"
            cur.execute(f'SELECT COUNT(*) FROM "{sch}"."{table}"')
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
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema or "PUBLIC",
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            sch = schema or "PUBLIC"
            if known_total_rows is not None:
                total = known_total_rows
            else:
                total = count_table_rows(
                    host=host, port=port, database=database, username=username, password=password,
                    schema=schema, connection_string=connection_string, warehouse=warehouse, table=table,
                    role=role,
                )
            col_sql = ", ".join(f'"{c}"' for c in columns) if columns else "*"
            cur.execute(
                f'SELECT {col_sql} FROM "{sch}"."{table}" LIMIT {int(limit)} OFFSET {int(offset)}'
            )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v) for v in row] for row in cur.fetchall()]
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
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema or "PUBLIC",
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            sch = schema or "PUBLIC"
            col_sql = ", ".join(f'"{c}"' for c in columns) if columns else "*"
            if cursor_after:
                cur.execute(
                    f'SELECT {col_sql} FROM "{sch}"."{table}" '
                    f'WHERE "{cursor_column}" > %s ORDER BY "{cursor_column}" LIMIT %s',
                    (cursor_after, limit),
                )
            else:
                cur.execute(
                    f'SELECT {col_sql} FROM "{sch}"."{table}" '
                    f'ORDER BY "{cursor_column}" LIMIT %s',
                    (limit,),
                )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v) for v in row] for row in cur.fetchall()]
        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=len(rows))
    finally:
        conn.close()
