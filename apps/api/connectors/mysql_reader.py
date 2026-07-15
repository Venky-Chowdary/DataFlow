"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.mysql_conn import get_connection

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _cell(value: Any) -> str:
    return cell_to_string(value)


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int
    total_rows: int


def _primary_key_columns(cur, table: str) -> list[str] | None:
    """Return the ordered list of PRIMARY KEY columns for ``table`` if one exists."""
    try:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY' "
            "ORDER BY ORDINAL_POSITION",
            (table,),
        )
        rows = cur.fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception:
        pass
    return None


def _order_by_clause(cur, table: str, columns: list[str] | None) -> str:
    """Build a deterministic ORDER BY clause for stable pagination.

    Uses the primary key when available; otherwise falls back to the first column
    so LIMIT/OFFSET batches are reproducible and do not drop or duplicate rows.
    """
    pk = _primary_key_columns(cur, table)
    if pk:
        return ", ".join(f"`{c}`" for c in pk)
    if columns:
        return f"`{columns[0]}`"
    return "1"


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
            order_by = _order_by_clause(cur, table, columns)
            if columns:
                col_list = ", ".join(f"`{c}`" for c in columns)
                query = f"SELECT {col_list} FROM `{table}` ORDER BY {order_by} LIMIT %s OFFSET %s"
            else:
                query = f"SELECT * FROM `{table}` ORDER BY {order_by} LIMIT %s OFFSET %s"
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
