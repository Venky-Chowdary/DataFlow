"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.mysql_conn import get_connection
from connectors.sql_identifiers import (
    quote_sql_identifier,
    quote_table_ref,
    require_safe_identifier,
)

logger = logging.getLogger(__name__)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services import reflection_cache
from services.json_polarity import is_json_catalog_type
from services.timezone_policy import (
    is_mysql_timestamp_data_type,
    mysql_timestamp_instant_wire,
)
from services.value_serializer import cell_to_string


def _cell(value: Any, *, instant: bool = False) -> str:
    if instant:
        value = mysql_timestamp_instant_wire(value)
    return cell_to_string(value, preserve_sql_null=True)


def _mysql_column_types(cur, table: str) -> list[tuple[str, str]]:
    """Ordered (name, data_type) from the session's own database."""
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        (table,),
    )
    return [(str(name), str(data_type or "").lower()) for name, data_type in cur.fetchall()]


def _column_types(cur, table: str, *, identity: str = "") -> list[tuple[str, str]]:
    if identity:
        return reflection_cache.get_or_load_by_identity(
            identity,
            "",
            table,
            "column_data_types",
            lambda: _mysql_column_types(cur, table),
        )
    return _mysql_column_types(cur, table)


def _instant_set_from_types(types: list[tuple[str, str]]) -> frozenset[str]:
    return frozenset(
        name for name, data_type in types if is_mysql_timestamp_data_type(data_type)
    )


def _json_names_from_types(types: list[tuple[str, str]]) -> frozenset[str]:
    return frozenset(
        name for name, data_type in types if is_json_catalog_type(data_type)
    )


def _mysql_select_list(
    columns: list[str] | None, types: list[tuple[str, str]]
) -> str | None:
    """Project JSON as CHAR so the engine spelling (not a decoded tree) is the wire."""
    json_cols = _json_names_from_types(types)
    if not json_cols and not columns:
        return None
    names = list(columns) if columns else [name for name, _ in types]
    parts: list[str] = []
    for name in names:
        q = quote_sql_identifier(require_safe_identifier(name, preserve_case=True), "`")
        if name in json_cols:
            parts.append(
                f"CASE WHEN {q} IS NULL THEN NULL ELSE "
                f"CAST({q} AS CHAR CHARACTER SET utf8mb4) END AS {q}"
            )
        else:
            parts.append(q)
    return ", ".join(parts)


def _wire_rows(fetched, headers: list[str], instant_cols: frozenset[str]) -> list[list[str]]:
    return [
        [
            _cell(v, instant=headers[i] in instant_cols) if i < len(headers) else _cell(v)
            for i, v in enumerate(row)
        ]
        for row in fetched
    ]


def _primary_key_columns(cur, table: str) -> list[str] | None:
    """Return the ordered list of PRIMARY KEY columns for ``table`` if one exists.

    Scoped to the session's own database. Without that predicate a table name
    that also exists in another schema on the same server returns both primary
    keys interleaved by ordinal position, which yields a bogus ORDER BY — and a
    non-deterministic ORDER BY is how LIMIT/OFFSET pagination silently drops
    and duplicates rows between chunks.
    """
    try:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "AND CONSTRAINT_NAME = 'PRIMARY' "
            "ORDER BY ORDINAL_POSITION",
            (table,),
        )
        rows = cur.fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return None


def _order_by_clause(cur, table: str, columns: list[str] | None, identity: str = "") -> str:
    """Build a deterministic ORDER BY clause for stable pagination.

    Uses the primary key when available; otherwise falls back to the first column
    so LIMIT/OFFSET batches are reproducible and do not drop or duplicate rows.

    The primary key lookup is cached per table: a chunked read calls this once
    per chunk and a table's key does not change underneath a running transfer.
    """
    if identity:
        pk = reflection_cache.get_or_load_by_identity(
            identity, "", table, "pk_columns", lambda: _primary_key_columns(cur, table)
        )
    else:
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
    conn: Any | None = None,
) -> ReadBatch:
    from connectors.sql_identifiers import split_qualified_table

    _schema, table = split_qualified_table(table, schema)
    del schema, _schema
    table_ref = quote_table_ref(table, dialect="mysql")
    safe_table = require_safe_identifier(table, preserve_case=True)
    close_conn = conn is None
    if conn is None:
        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
            purpose="read",
        )
    try:
        with conn.cursor() as cur:
            if known_total_rows is not None:
                total = known_total_rows
            else:
                cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
                total = int(cur.fetchone()[0])
            identity = reflection_cache.dsn_identity(
                driver="mysql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            order_by = _order_by_clause(
                cur,
                safe_table,
                columns,
                identity=identity,
            )
            types = _column_types(cur, safe_table, identity=identity)
            col_list = _mysql_select_list(columns, types)
            if col_list is None:
                query = f"SELECT * FROM {table_ref} ORDER BY {order_by} LIMIT %s OFFSET %s"  # nosec B608
            else:
                query = f"SELECT {col_list} FROM {table_ref} ORDER BY {order_by} LIMIT %s OFFSET %s"  # nosec B608
            cur.execute(query, (limit, offset))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            instant_cols = _instant_set_from_types(types)
            rows = _wire_rows(fetched, headers, instant_cols)
            return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        if close_conn:
            conn.close()


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
    conn: Any | None = None,
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY`` with ``fetchmany`` — no OFFSET, one login."""
    from connectors.sql_snapshot_scan import close_table_scan
    from connectors.sql_identifiers import split_qualified_table

    _schema, table = split_qualified_table(table, schema)
    del schema, _schema
    if not scan_state.get("started"):
        table_ref = quote_table_ref(table, dialect="mysql")
        safe_table = require_safe_identifier(table, preserve_case=True)
        close_conn = conn is None
        if conn is None:
            conn = get_connection(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                purpose="read",
            )
        cur = conn.cursor()
        try:
            if known_total_rows is not None:
                total = known_total_rows
            else:
                cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
                total = int(cur.fetchone()[0])
            identity = reflection_cache.dsn_identity(
                driver="mysql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            order_by = _order_by_clause(cur, safe_table, columns, identity=identity)
            types = _column_types(cur, safe_table, identity=identity)
            col_list = _mysql_select_list(columns, types)
            if col_list is None:
                cur.execute(f"SELECT * FROM {table_ref} ORDER BY {order_by}")  # nosec B608
            else:
                cur.execute(f"SELECT {col_list} FROM {table_ref} ORDER BY {order_by}")  # nosec B608
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
        except Exception:
            try:
                cur.close()
            except Exception:
                pass
            try:
                if close_conn:
                    conn.close()
            except Exception:
                pass
            raise
        scan_state.update(
            started=True,
            conn=conn if close_conn else None,
            cur=cur,
            headers=headers,
            total=total,
            types=types,
        )
    cur = scan_state["cur"]
    raw = cur.fetchmany(max(1, int(limit)))
    headers = list(scan_state.get("headers") or [])
    total = scan_state.get("total")
    if not raw:
        close_table_scan(scan_state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    types = list(scan_state.get("types") or [])
    instant_cols = _instant_set_from_types(types)
    rows = _wire_rows(raw, headers, instant_cols)
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


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
    """Read rows where cursor_column > watermark — for incremental sync.

    Optional ``cursor_primary_key`` enables lexicographic ``(cursor, pk)`` so
    timestamp ties are not skipped forever. Optional ``conn`` reuses a locked
    CDC snapshot session (LOCK TABLES is connection-scoped).
    """
    from connectors.sql_identifiers import split_qualified_table
    from services.keyset_pagination import split_cursor_bookmark

    _schema, table = split_qualified_table(table, schema)
    del schema, _schema
    table_ref = quote_table_ref(table, dialect="mysql")
    cursor_q = quote_sql_identifier(require_safe_identifier(cursor_column, preserve_case=True), "`")
    close_conn = conn is None
    if conn is None:
        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
            purpose="read",
        )
    try:
        with conn.cursor() as cur:
            identity = reflection_cache.dsn_identity(
                driver="mysql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            types = _column_types(
                cur, require_safe_identifier(table, preserve_case=True), identity=identity
            )
            col_list = _mysql_select_list(columns, types)
            if col_list is None:
                base = f"SELECT * FROM {table_ref}"  # nosec B608
            else:
                base = f"SELECT {col_list} FROM {table_ref}"  # nosec B608
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
                    cur_val, pk_val = split_cursor_bookmark(
                        cursor_after, has_tiebreak=True
                    )
                    cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = f"{base} WHERE {cursor_q} > %s ORDER BY {cursor_q} LIMIT %s"
                    cur_val, _ = split_cursor_bookmark(cursor_after, has_tiebreak=False)
                    cur.execute(query, (cur_val, limit))
            else:
                if pk_q:
                    query = f"{base} ORDER BY {cursor_q}, {pk_q} LIMIT %s"
                else:
                    query = f"{base} ORDER BY {cursor_q} LIMIT %s"
                cur.execute(query, (limit,))
            fetched = cur.fetchall()
            headers = [desc[0] for desc in cur.description] if cur.description else (columns or [])
            instant_cols = _instant_set_from_types(types)
            rows = _wire_rows(fetched, headers, instant_cols)
            # Keyset pages are not a cardinality bound — page length must never
            # trip stream early-stop (fetch_offset >= total_rows).
            return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        if close_conn:
            conn.close()
