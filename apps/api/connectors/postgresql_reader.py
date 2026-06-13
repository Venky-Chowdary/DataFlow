"""PostgreSQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from connectors.postgresql_conn import get_connection


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int
    total_rows: int


def count_table_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
) -> int:
    import psycopg2
    from psycopg2 import sql

    schema = schema or "public"
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            )
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
    ssl: bool,
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
) -> ReadBatch:
    import psycopg2
    from psycopg2 import sql

    schema = schema or "public"
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    try:
        with conn.cursor() as cur:
            total = count_table_rows(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                schema=schema,
                connection_string=connection_string,
                ssl=ssl,
                table=table,
            )
            if columns:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
                query = sql.SQL("SELECT {} FROM {}.{} ORDER BY 1 LIMIT %s OFFSET %s").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            else:
                query = sql.SQL("SELECT * FROM {}.{} ORDER BY 1 LIMIT %s OFFSET %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [["" if v is None else str(v) for v in row] for row in fetched]
            return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        conn.close()


def read_table_sample(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
    limit: int = 100,
) -> tuple[list[str], list[list[str]]]:
    batch = read_table_batch(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        schema=schema,
        connection_string=connection_string,
        ssl=ssl,
        table=table,
        offset=0,
        limit=limit,
    )
    return batch.headers, batch.rows
