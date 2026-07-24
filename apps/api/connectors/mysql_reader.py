"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.mysql_conn import get_connection
from connectors.sql_identifiers import (
    quote_column_list,
    quote_sql_identifier,
    quote_table_ref,
    require_safe_identifier,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _cell(value: Any) -> str:
    return cell_to_string(value, preserve_sql_null=True)


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
        return ", ".join(
            quote_sql_identifier(require_safe_identifier(c, preserve_case=True), "`") for c in pk
        )
    if columns:
        return quote_sql_identifier(require_safe_identifier(columns[0], preserve_case=True), "`")
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
    table_ref = quote_table_ref(table, dialect="mysql")
    safe_table = require_safe_identifier(table, preserve_case=True)
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
                cur.execute(f"SELECT COUNT(*) FROM {table_ref}")
                total = int(cur.fetchone()[0])
            order_by = _order_by_clause(cur, safe_table, columns)
            if columns:
                col_list = quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in columns],
                    quote_char="`",
                )
                query = f"SELECT {col_list} FROM {table_ref} ORDER BY {order_by} LIMIT %s OFFSET %s"
            else:
                query = f"SELECT * FROM {table_ref} ORDER BY {order_by} LIMIT %s OFFSET %s"
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
    cursor_primary_key: str | None = None,
) -> ReadBatch:
    """Read rows where cursor_column > watermark — for incremental sync.

    Optional ``cursor_primary_key`` enables lexicographic ``(cursor, pk)`` so
    timestamp ties are not skipped forever.
    """
    del schema
    table_ref = quote_table_ref(table, dialect="mysql")
    cursor_q = quote_sql_identifier(require_safe_identifier(cursor_column, preserve_case=True), "`")
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
                col_list = quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in columns],
                    quote_char="`",
                )
                base = f"SELECT {col_list} FROM {table_ref}"
            else:
                base = f"SELECT * FROM {table_ref}"
            pk = (cursor_primary_key or "").strip()
            pk_q = (
                quote_sql_identifier(require_safe_identifier(pk, preserve_case=True), "`")
                if pk and pk != cursor_column
                else ""
            )
            if cursor_after:
                if pk_q:
                    query = (
                        f"{base} WHERE ({cursor_q}, {pk_q}) > (%s, %s) "
                        f"ORDER BY {cursor_q}, {pk_q} LIMIT %s"
                    )
                    if "|" in str(cursor_after):
                        cur_val, pk_val = str(cursor_after).split("|", 1)
                    else:
                        cur_val, pk_val = cursor_after, ""
                    cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = f"{base} WHERE {cursor_q} > %s ORDER BY {cursor_q} LIMIT %s"
                    cur.execute(query, (cursor_after, limit))
            else:
                if pk_q:
                    query = f"{base} ORDER BY {cursor_q}, {pk_q} LIMIT %s"
                else:
                    query = f"{base} ORDER BY {cursor_q} LIMIT %s"
                cur.execute(query, (limit,))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
            # Keyset pages are not a cardinality bound — page length must never
            # trip stream early-stop (fetch_offset >= total_rows).
            return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        conn.close()
