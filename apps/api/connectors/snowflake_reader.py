"""Snowflake table reader — batched extraction for warehouse migrations."""

from __future__ import annotations

from dataclasses import dataclass

from connectors.snowflake_conn import get_connection, normalize_account


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]


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
    limit: int = 100_000,
) -> ReadBatch:
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
    )
    try:
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            sch = schema or "PUBLIC"
            cur.execute(f'SELECT * FROM "{sch}"."{table}" LIMIT {int(limit)}')
            headers = [desc[0] for desc in cur.description]
            rows = [[str(v) if v is not None else "" for v in row] for row in cur.fetchall()]
        return ReadBatch(headers=headers, rows=rows)
    finally:
        conn.close()
