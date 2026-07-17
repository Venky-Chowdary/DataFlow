"""PostgreSQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.postgresql_conn import get_connection
from connectors.driver_guard import require_driver

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _ensure_psycopg2() -> None:
    try:
        import psycopg2  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(require_driver("psycopg2", "psycopg2-binary")) from exc


def _cell(value: Any) -> str:
    return cell_to_string(value)


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


def _primary_key_columns(cur, schema: str, table: str) -> list[str] | None:
    """Return ordered PRIMARY KEY columns for ``table`` if one exists."""
    try:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        rows = cur.fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception:
        pass
    return None


def _order_by_clause(cur, schema: str, table: str, columns: list[str] | None) -> str:
    """Return a deterministic ORDER BY for stable LIMIT/OFFSET pagination."""
    from psycopg2 import sql
    pk = _primary_key_columns(cur, schema, table)
    if pk:
        return ", ".join(sql.Identifier(c).as_string(cur) for c in pk)
    if columns:
        return sql.Identifier(columns[0]).as_string(cur)
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
            if known_total_rows is not None:
                total = known_total_rows
            else:
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
            order_by = _order_by_clause(cur, schema, table, columns)
            if columns:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
                query = sql.SQL("SELECT {} FROM {}.{} ORDER BY " + order_by + " LIMIT %s OFFSET %s").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            else:
                query = sql.SQL("SELECT * FROM {}.{} ORDER BY " + order_by + " LIMIT %s OFFSET %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
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
    """Read rows with cursor_column > watermark — for incremental sync."""
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
            if columns:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
                base = sql.SQL("SELECT {} FROM {}.{}").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            else:
                base = sql.SQL("SELECT * FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            if cursor_after:
                query = sql.SQL("{} WHERE {} > %s ORDER BY {} LIMIT %s").format(
                    base,
                    sql.Identifier(cursor_column),
                    sql.Identifier(cursor_column),
                )
                cur.execute(query, (cursor_after, limit))
            else:
                query = sql.SQL("{} ORDER BY {} LIMIT %s").format(
                    base,
                    sql.Identifier(cursor_column),
                )
                cur.execute(query, (limit,))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
            return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=len(rows))
    finally:
        conn.close()
