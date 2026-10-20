"""MySQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import logging
import re
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
        # X-52: COLUMN_TYPE carries the bit width ('bit(8)') — DATA_TYPE is bare
        # 'bit' so BIT(1) and BIT(64) are indistinguishable downstream.
        "SELECT COLUMN_NAME, CASE WHEN DATA_TYPE = 'bit' THEN COLUMN_TYPE "
        "ELSE DATA_TYPE END FROM information_schema.COLUMNS "
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


def mysql_bit_projection(quoted_col: str, width: int) -> str:
    """X-52: BIT(n) arrives as opaque driver bytes that base64 onto the wire
    ('BQ=='). Project the n-char '0'/'1' bit string (LPAD keeps the declared
    width — 0 lands as '00000000', not '0') so PG bit/varbit and digit-VARCHAR
    destinations bind exact values. Shared by every MySQL SELECT producer.
    """
    return (
        f"CASE WHEN {quoted_col} IS NULL THEN NULL ELSE "
        f"LPAD(BIN({quoted_col}), {int(width)}, '0') END AS {quoted_col}"
    )


def mysql_bit_widths(cur, table: str) -> dict[str, int]:
    """``{column: bit width}`` for a MySQL table — see ``mysql_bit_projection``."""
    return _bit_widths_from_types(_mysql_column_types(cur, table))


def mysql_tinyint1_wide_columns(cur, table: str, columns: list[str]) -> set[str]:
    """tinyint(1) columns whose data proves they are NOT boolean (X-59).

    ``tinyint(1)`` is a display width, not a type: MySQL stores -128..127.
    The DDL mapper stamps it BOOLEAN off the width alone, which rejected
    valid rows ('Invalid boolean: 127') or crashed COPY on PG. Evidence
    decides instead: one short-circuit probe per tinyint(1) column returns
    the set holding any value outside {0,1} — those must plan a small-int
    carrier; the rest keep boolean.
    """
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    types = {str(n): str(t or "").lower() for n, t in cur.fetchall()}
    table_q = quote_table_ref(table, dialect="mysql")
    wide: set[str] = set()
    for col in columns or []:
        if str(types.get(col) or "").lower() != "tinyint(1)":
            continue
        col_q = quote_sql_identifier(require_safe_identifier(col, preserve_case=True), "`")
        cur.execute(
            f"SELECT 1 FROM {table_q} WHERE {col_q} NOT IN (0, 1) "
            f"AND {col_q} IS NOT NULL LIMIT 1"
        )
        if cur.fetchone() is not None:
            wide.add(col)
    return wide


def widen_tinyint1_ddls(
    live_types: dict[str, str],
    pairs: list[tuple[str, str]],
    ddls: list[str],
    smallint_ddl: str,
) -> tuple[list[str], list[str]]:
    """Rewrite BOOLEAN-stamped ddls to ``smallint_ddl`` for evidence-wide
    tinyint(1) source columns (X-59). ``live_types`` maps source column →
    COLUMN_TYPE ('tinyint(1)'); only those columns are candidates, and only
    when their current ddl is boolean-carrier. Returns (ddls, widened_cols).
    """
    out = list(ddls)
    widened: list[str] = []
    live_l = {k.lower(): v for k, v in (live_types or {}).items()}
    for idx, (src_col, _dest_col) in enumerate(pairs or []):
        if str(live_l.get(str(src_col).lower()) or "").lower() != "tinyint(1)":
            continue
        if idx >= len(out):
            break
        if str(out[idx]).strip().lower() not in {"boolean", "bool", "bit"}:
            continue
        out[idx] = smallint_ddl
        widened.append(str(src_col))
    return out, widened


def _bit_names_from_types(types: list[tuple[str, str]]) -> frozenset[str]:
    return frozenset(_bit_widths_from_types(types))


def _bit_widths_from_types(types: list[tuple[str, str]]) -> dict[str, int]:
    widths: dict[str, int] = {}
    for name, data_type in types:
        m = re.fullmatch(r"bit\((\d{1,2})\)", data_type)
        if m:
            widths[name] = max(1, int(m.group(1)))
        elif data_type == "bit":
            widths[name] = 1
    return widths


def _mysql_select_list(
    columns: list[str] | None, types: list[tuple[str, str]]
) -> str | None:
    """Project JSON as CHAR so the engine spelling (not a decoded tree) is the wire."""
    json_cols = _json_names_from_types(types)
    bit_cols = _bit_names_from_types(types)
    bits_widths = _bit_widths_from_types(types)
    if not json_cols and not bit_cols and not columns:
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
        elif name in bit_cols:
            parts.append(mysql_bit_projection(q, bits_widths.get(name, 64)))
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


def _order_by_columns(
    cur, table: str, columns: list[str] | None, identity: str = ""
) -> list[str]:
    """Columns a stable page/scan orders by: the primary key, else the first column.

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
        return list(pk)
    if columns:
        return [columns[0]]
    return []


def _order_by_sql(order_cols: list[str]) -> str:
    if not order_cols:
        return "1"
    return ", ".join(
        quote_sql_identifier(require_safe_identifier(c, preserve_case=True), "`")
        for c in order_cols
    )


def _order_by_clause(cur, table: str, columns: list[str] | None, identity: str = "") -> str:
    """Build a deterministic ORDER BY clause for stable pagination.

    Uses the primary key when available; otherwise falls back to the first column
    so LIMIT/OFFSET batches are reproducible and do not drop or duplicate rows.
    """
    return _order_by_sql(_order_by_columns(cur, table, columns, identity=identity))


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
    filter_column: str = "",
    filter_after: str | None = None,
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY`` with ``fetchmany`` — no OFFSET, one login.

    ``filter_column`` / ``filter_after`` bound the scan to ``cursor > watermark``
    (COUNT and SELECT alike) and order it by the cursor first — the incremental
    read for a table with no unique tie-break to seek on.
    """
    from connectors.sql_snapshot_scan import (
        close_table_scan,
        publish_scan_order,
        scan_filter_value,
    )
    from connectors.sql_identifiers import split_qualified_table

    _schema, table = split_qualified_table(table, schema)
    del schema, _schema
    filter_value = scan_filter_value(filter_column, filter_after)
    where_sql = ""
    params: tuple[Any, ...] = ()
    filter_q = ""
    if filter_value is not None:
        filter_q = quote_sql_identifier(
            require_safe_identifier(filter_column, preserve_case=True), "`"
        )
        where_sql = f" WHERE {filter_q} > %s"
        params = (filter_value,)
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
                cur.execute(f"SELECT COUNT(*) FROM {table_ref}{where_sql}", params)  # nosec B608
                total = int(cur.fetchone()[0])
            identity = reflection_cache.dsn_identity(
                driver="mysql",
                host=host,
                port=port,
                database=database,
                username=username,
                connection_string=connection_string,
            )
            order_cols = _order_by_columns(cur, safe_table, columns, identity=identity)
            order_by = _order_by_sql(order_cols)
            if filter_q:
                order_by = f"{filter_q}, {order_by}"
                order_cols = [filter_column] + order_cols
            types = _column_types(cur, safe_table, identity=identity)
            col_list = _mysql_select_list(columns, types)
            if col_list is None:
                cur.execute(
                    f"SELECT * FROM {table_ref}{where_sql} ORDER BY {order_by}",  # nosec B608
                    params,
                )
            else:
                cur.execute(
                    f"SELECT {col_list} FROM {table_ref}{where_sql} ORDER BY {order_by}",  # nosec B608
                    params,
                )
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
        publish_scan_order(scan_state, order_cols)
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
    from services.keyset_pagination import present_cursor_bookmark, split_cursor_bookmark

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
            bookmark = present_cursor_bookmark(cursor_after)
            if bookmark is not None:
                if pk_q:
                    cur_val, pk_val = split_cursor_bookmark(
                        bookmark, has_tiebreak=True
                    )
                    if pk_val == "":
                        # Cursor-only watermark with a tie-break: seek on the
                        # cursor, order on both for a deterministic page edge.
                        query = (
                            f"{base} WHERE {cursor_q} > %s "
                            f"ORDER BY {cursor_q}, {pk_q} LIMIT %s"
                        )
                        cur.execute(query, (cur_val, limit))
                    else:
                        query = (
                            f"{base} WHERE ({cursor_q}, {pk_q}) > (%s, %s) "
                            f"ORDER BY {cursor_q}, {pk_q} LIMIT %s"
                        )
                        cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = f"{base} WHERE {cursor_q} > %s ORDER BY {cursor_q} LIMIT %s"
                    cur_val, _ = split_cursor_bookmark(bookmark, has_tiebreak=False)
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
