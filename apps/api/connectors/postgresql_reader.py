"""PostgreSQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.driver_guard import require_driver
from connectors.postgresql_conn import get_connection

_api_root = Path(__file__).resolve().parents[1]

logger = logging.getLogger(__name__)
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services import reflection_cache
from services.value_serializer import cell_to_string


def _ensure_psycopg2() -> None:
    try:
        import psycopg2  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(require_driver("psycopg2", "psycopg2-binary")) from exc


def _cell(value: Any) -> str:
    return cell_to_string(value, preserve_sql_null=True)



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
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return None


def _order_by_clause(
    cur, schema: str, table: str, columns: list[str] | None, identity: str = ""
) -> str:
    """Return a deterministic ORDER BY for stable LIMIT/OFFSET pagination.

    The primary key lookup — a two-way join across ``information_schema`` — is
    cached per table, because a chunked read asks for it once per chunk and the
    answer cannot change under a running transfer without breaking the load
    anyway.
    """
    from psycopg2 import sql
    if identity:
        pk = reflection_cache.get_or_load_by_identity(
            identity,
            schema,
            table,
            "pk_columns",
            lambda: _primary_key_columns(cur, schema, table),
        )
    else:
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
    conn: Any | None = None,
) -> ReadBatch:
    from psycopg2 import sql

    from services.source_snapshot import get_source_snapshot_conn

    schema = schema or "public"
    # Prefer an explicit conn, then a transfer-bound RR snapshot (Property 3).
    shared = conn if conn is not None else get_source_snapshot_conn()
    close_conn = shared is None
    if shared is None:
        shared = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
    try:
        with shared.cursor() as cur:
            if known_total_rows is not None:
                total = known_total_rows
            else:
                # COUNT on the SAME connection so cardinality matches the
                # MVCC snapshot used for page reads (never a second conn).
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                    )
                )
                total = int(cur.fetchone()[0])
            order_by = _order_by_clause(
                cur,
                schema,
                table,
                columns,
                identity=reflection_cache.dsn_identity(
                    driver="postgresql",
                    host=host,
                    port=port,
                    database=database,
                    username=username,
                    connection_string=connection_string,
                ),
            )
            order_sql = sql.SQL(order_by)
            if columns:
                col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
                query = sql.SQL("SELECT {} FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    order_sql,
                )
            else:
                query = sql.SQL("SELECT * FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    order_sql,
                )
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
            return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        if close_conn:
            shared.close()


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
    cursor_primary_key: str | None = None,
    conn: Any | None = None,
) -> ReadBatch:
    """Read rows with cursor_column > watermark — for incremental sync.

    When ``cursor_primary_key`` is set, uses lexicographic ``(cursor, pk)`` so
    rows sharing a timestamp watermark are not skipped forever.
    """
    from psycopg2 import sql

    from services.source_snapshot import get_source_snapshot_conn

    schema = schema or "public"
    shared = conn if conn is not None else get_source_snapshot_conn()
    close_conn = shared is None
    if shared is None:
        shared = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
    try:
        with shared.cursor() as cur:
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
                # Composite order: cursor then primary key when provided so tied
                # watermarks do not skip peer rows (timestamp-cursor Airbyte trap).
                pk = (cursor_primary_key or "").strip()
                if pk and pk != cursor_column:
                    query = sql.SQL(
                        "{} WHERE ({}, {}) > (%s, %s) ORDER BY {}, {} LIMIT %s"
                    ).format(
                        base,
                        sql.Identifier(cursor_column),
                        sql.Identifier(pk),
                        sql.Identifier(cursor_column),
                        sql.Identifier(pk),
                    )
                    # cursor_after may be "value|pk" composite or bare cursor.
                    if "|" in str(cursor_after):
                        cur_val, pk_val = str(cursor_after).split("|", 1)
                    else:
                        cur_val, pk_val = cursor_after, ""
                    cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = sql.SQL("{} WHERE {} > %s ORDER BY {} LIMIT %s").format(
                        base,
                        sql.Identifier(cursor_column),
                        sql.Identifier(cursor_column),
                    )
                    cur.execute(query, (cursor_after, limit))
            else:
                pk = (cursor_primary_key or "").strip()
                if pk and pk != cursor_column:
                    query = sql.SQL("{} ORDER BY {}, {} LIMIT %s").format(
                        base,
                        sql.Identifier(cursor_column),
                        sql.Identifier(pk),
                    )
                else:
                    query = sql.SQL("{} ORDER BY {} LIMIT %s").format(
                        base,
                        sql.Identifier(cursor_column),
                    )
                cur.execute(query, (limit,))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            rows = [[_cell(v) for v in row] for row in fetched]
            # Keyset pages are not a cardinality bound — page length must never
            # trip stream early-stop (fetch_offset >= total_rows).
            return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        if close_conn:
            shared.close()
