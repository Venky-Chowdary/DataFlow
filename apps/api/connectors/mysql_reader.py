"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from connectors.mysql_conn import get_connection


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int
    total_rows: int


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
    known_total_rows: int | None = None,
) -> ReadBatch:
    del schema
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
            if known_total_rows is not None:
                total = known_total_rows
            else:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                total = int(cur.fetchone()[0])
            if columns:
                col_list = ", ".join(f"`{c}`" for c in columns)
                query = f"SELECT {col_list} FROM `{table}` LIMIT %s OFFSET %s"
            else:
                query = f"SELECT * FROM `{table}` LIMIT %s OFFSET %s"
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
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
    ssl: bool,
    table: str,
    cursor_column: str,
    cursor_after: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
) -> ReadBatch:
    """Read rows where cursor_column > watermark — for incremental sync."""
    del schema
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
            if columns:
                col_list = ", ".join(f"`{c}`" for c in columns)
                base = f"SELECT {col_list} FROM `{table}`"
            else:
                base = f"SELECT * FROM `{table}`"
            if cursor_after:
                query = (
                    f"{base} WHERE `{cursor_column}` > %s "
                    f"ORDER BY `{cursor_column}` LIMIT %s"
                )
                cur.execute(query, (cursor_after, limit))
            else:
                query = f"{base} ORDER BY `{cursor_column}` LIMIT %s"
                cur.execute(query, (limit,))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
            return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=len(rows))
    finally:
        conn.close()
