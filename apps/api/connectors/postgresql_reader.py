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
from services.json_polarity import is_json_catalog_type
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


def _json_column_names(cur, schema: str, table: str) -> frozenset[str]:
    """Columns whose catalog type is json/jsonb — they must travel as engine text."""
    cur.execute(
        """
        SELECT column_name, data_type, udt_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return frozenset(
        str(name)
        for name, data_type, udt_name in cur.fetchall()
        if is_json_catalog_type(str(data_type or ""), str(udt_name or ""))
    )


def _ordered_column_names(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [str(r[0]) for r in cur.fetchall()]


def _select_list(cur, schema: str, table: str, columns: list[str] | None, identity: str):
    """Project JSON/JSONB as engine text so ``\"1\"`` and ``1`` stay distinct.

    ``SELECT *`` lets psycopg2 decode jsonb into Python, after which a JSON
    string ``\"1\"`` is the str ``'1'`` and ``json.loads`` makes it a number.
    ``col::text`` is the engine's own JSON spelling — SQL NULL stays NULL,
    JSON null stays the text ``null``.
    """
    from psycopg2 import sql

    if identity:
        json_cols = reflection_cache.get_or_load_by_identity(
            identity,
            schema,
            table,
            "json_columns",
            lambda: _json_column_names(cur, schema, table),
        )
    else:
        json_cols = _json_column_names(cur, schema, table)
    if not json_cols and not columns:
        return None
    names = columns or _ordered_column_names(cur, schema, table)
    parts = []
    for name in names:
        ident = sql.Identifier(name)
        if name in json_cols:
            parts.append(
                sql.SQL("CASE WHEN {c} IS NULL THEN NULL ELSE {c}::text END AS {c}").format(
                    c=ident
                )
            )
        else:
            parts.append(ident)
    return sql.SQL(", ").join(parts)


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
            identity = reflection_cache.dsn_identity(
                driver="postgresql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            order_by = _order_by_clause(
                cur,
                schema,
                table,
                columns,
                identity=identity,
            )
            order_sql = sql.SQL(order_by)
            col_sql = _select_list(cur, schema, table, columns, identity)
            if col_sql is None:
                query = sql.SQL("SELECT * FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    order_sql,
                )
            else:
                query = sql.SQL("SELECT {} FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
                    col_sql,
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


def read_table_scan_batch(
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
    scan_state: dict[str, Any],
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY`` with ``fetchmany`` — no OFFSET, one session."""
    from psycopg2 import sql

    from connectors.sql_snapshot_scan import close_table_scan
    from services.source_snapshot import get_source_snapshot_conn

    schema = schema or "public"
    if not scan_state.get("started"):
        shared = get_source_snapshot_conn()
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
        cur = shared.cursor()
        try:
            if known_total_rows is not None:
                total = known_total_rows
            else:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                    )
                )
                total = int(cur.fetchone()[0])
            identity = reflection_cache.dsn_identity(
                driver="postgresql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            order_by = _order_by_clause(
                cur, schema, table, columns, identity=identity
            )
            order_sql = sql.SQL(order_by)
            col_sql = _select_list(cur, schema, table, columns, identity)
            if col_sql is None:
                query = sql.SQL("SELECT * FROM {}.{} ORDER BY {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    order_sql,
                )
            else:
                query = sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    order_sql,
                )
            cur.execute(query)
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
        except Exception:
            try:
                cur.close()
            except Exception:
                pass
            if close_conn:
                try:
                    shared.close()
                except Exception:
                    pass
            raise
        scan_state.update(
            started=True,
            conn=shared if close_conn else None,
            cur=cur,
            headers=headers,
            total=total,
        )
    cur = scan_state["cur"]
    raw = cur.fetchmany(max(1, int(limit)))
    headers = list(scan_state.get("headers") or [])
    total = scan_state.get("total")
    if not raw:
        close_table_scan(scan_state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    rows = [[_cell(v) for v in row] for row in raw]
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


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

    from services.keyset_pagination import split_cursor_bookmark
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
            identity = reflection_cache.dsn_identity(
                driver="postgresql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            col_sql = _select_list(cur, schema, table, columns, identity)
            if col_sql is None:
                base = sql.SQL("SELECT * FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            else:
                base = sql.SQL("SELECT {} FROM {}.{}").format(
                    col_sql,
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
                    cur_val, pk_val = split_cursor_bookmark(
                        cursor_after, has_tiebreak=True
                    )
                    cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = sql.SQL("{} WHERE {} > %s ORDER BY {} LIMIT %s").format(
                        base,
                        sql.Identifier(cursor_column),
                        sql.Identifier(cursor_column),
                    )
                    cur_val, _ = split_cursor_bookmark(cursor_after, has_tiebreak=False)
                    cur.execute(query, (cur_val, limit))
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
