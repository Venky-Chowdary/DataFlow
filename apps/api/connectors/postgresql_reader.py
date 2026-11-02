"""PostgreSQL table reader — batched extraction for DB→DB migration."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.driver_guard import require_driver
from connectors.postgresql_conn import get_connection
from connectors.sql_identifiers import split_qualified_table

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


def _bind(schema: str | None, table: str) -> tuple[str, str]:
    """Studio ``public.t`` plus connector schema=public must address ``public.t``."""
    sch, tbl = split_qualified_table(table, schema or "public")
    return sch or "public", tbl


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

    schema, table = _bind(schema, table)
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


def _text_carry_column_names(cur, schema: str, table: str) -> frozenset[str]:
    """Columns psycopg2 would decode into a lossy Python type — carry engine text.

    ``col::text`` gives the engine's own canonical spelling, so the wire keeps
    exactly what Postgres stores and a same-family destination round-trips
    verbatim (C08):

    - json/jsonb — ``'null'::jsonb`` ≠ SQL NULL; ``"1"`` ≠ ``1`` (D75 class).
    - interval — timedelta cannot hold months/years: ``1 mon 1 day`` decayed
      to ``744:00:00`` and ``1 day 03:00:04.5`` re-labelled itself
      ``27:00:04.5`` (D50).
    - float4/real — Python float64 repr invents digits the float32 never had
      (``0.33333334`` reads back ``0.3333333432674408``).
    - inet/cidr/macaddr* — ipaddress objects re-render in a different form.
    - bit/varbit — native decode reaches the binary route and quarantines a
      legal ``B'1'`` (D79).
    - ranges/multiranges — ``psycopg2`` Range renders ``(None, 5)`` instead of
      the stored ``(,5)`` and the string cannot rebind (D80).
    """
    cur.execute(
        """
        SELECT column_name, data_type, udt_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
        """,
        (schema, table),
    )
    def _text_carry_row(row) -> bool:
        if len(row) < 2:
            return False
        data_type = str(row[1] or "").strip().lower()
        udt_name = str(row[2] or "").strip().lower() if len(row) > 2 else ""
        if is_json_catalog_type(data_type, udt_name):
            return True
        tokens = {data_type, udt_name}
        if tokens & {
            "interval", "real", "float4",
            "inet", "cidr", "macaddr", "macaddr8",
            "bit", "varbit", "bit varying",
        }:
            return True
        # numrange/int4range/tsrange/… + their multiranges share udt suffixes.
        return udt_name.endswith("range") or udt_name.endswith("multirange")

    return frozenset(str(row[0]) for row in cur.fetchall() if _text_carry_row(row))


def text_carry_select_sql(
    cur: Any, schema: str, table_name: str, cols: list[str] | None = None
) -> str | None:
    """Select list for a read-back that must see what this reader saw, or None.

    This reader carries the lossy-decode families as ``col::text`` (see
    :func:`_text_carry_column_names`). A verification read through psycopg2's
    decoders instead compared ``[1,)`` with the Range repr ``[1, None)``
    (D80) and JSON ``null`` with SQL NULL (D75); both sides of a compare must
    follow one rule. ``None`` means plain columns are already faithful.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    sch, table = split_qualified_table(str(table_name or ""), schema or "public")
    sch = sch or "public"
    carry = _text_carry_column_names(cur, sch, table)
    if not carry:
        return None
    names = _ordered_column_names(cur, sch, table) if not cols or cols == ["*"] else cols
    parts = []
    for name in names:
        q = quote_sql_identifier(str(name))
        parts.append(
            f"CASE WHEN {q} IS NULL THEN NULL ELSE {q}::text END AS {q}"
            if name in carry
            else q
        )
    return ", ".join(parts)


def _json_column_names(cur, schema: str, table: str) -> frozenset[str]:
    """Back-compat alias — JSON is one of the text-carry families."""
    return _text_carry_column_names(cur, schema, table)


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


def _order_by_columns(
    cur, schema: str, table: str, columns: list[str] | None, identity: str = ""
) -> list[str]:
    """Columns a stable page/scan orders by: the primary key, else the first column.

    The primary key lookup — a two-way join across ``information_schema`` — is
    cached per table, because a chunked read asks for it once per chunk and the
    answer cannot change under a running transfer without breaking the load
    anyway.
    """
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
        return list(pk)
    if columns:
        return [columns[0]]
    return []


def _order_by_clause(
    cur, schema: str, table: str, columns: list[str] | None, identity: str = ""
) -> str:
    """Return a deterministic ORDER BY for stable LIMIT/OFFSET pagination."""
    from psycopg2 import sql

    cols = _order_by_columns(cur, schema, table, columns, identity=identity)
    if cols:
        return ", ".join(sql.Identifier(c).as_string(cur) for c in cols)
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

    schema, table = _bind(schema, table)
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
            from src.ai.copilot.tool_deadline import remaining_seconds

            if known_total_rows is not None:
                total = known_total_rows
            elif remaining_seconds() is not None:
                # A planning tool walks the table only as far as its budget
                # allows; the total is a progress figure, and an exact COUNT(*)
                # on a big table cost a full scan before the walk began (D187).
                total = None
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
    filter_column: str = "",
    filter_after: str | None = None,
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY`` with ``fetchmany`` — no OFFSET, one session.

    ``filter_column`` / ``filter_after`` bound the scan to ``cursor > watermark``
    (COUNT and SELECT alike) and order it by the cursor first — the incremental
    read for a table with no unique tie-break to seek on.
    """
    from psycopg2 import sql

    from connectors.sql_snapshot_scan import close_table_scan, publish_scan_order, scan_filter_value
    from services.source_snapshot import get_source_snapshot_conn

    schema, table = _bind(schema, table)
    filter_value = scan_filter_value(filter_column, filter_after)
    if filter_value is not None:
        where_sql = sql.SQL(" WHERE {} >= %s").format(sql.Identifier(filter_column))
        params: tuple[Any, ...] = (filter_value,)
    else:
        where_sql = sql.SQL("")
        params = ()
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
            from src.ai.copilot.tool_deadline import remaining_seconds

            if known_total_rows is not None:
                total = known_total_rows
            elif remaining_seconds() is not None:
                # A planning tool walks the table only as far as its budget
                # allows; the total is a progress figure, and an exact COUNT(*)
                # on a big table cost a full scan before the walk began (D187).
                total = None
            else:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        where_sql,
                    ),
                    params,
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
            order_cols = _order_by_columns(
                cur, schema, table, columns, identity=identity
            )
            order_by = (
                ", ".join(sql.Identifier(c).as_string(cur) for c in order_cols)
                if order_cols
                else "1"
            )
            if filter_value is not None:
                order_by = sql.Identifier(filter_column).as_string(cur) + ", " + order_by
                order_cols = [filter_column] + order_cols
            order_sql = sql.SQL(order_by)
            col_sql = _select_list(cur, schema, table, columns, identity)
            if col_sql is None:
                query = sql.SQL("SELECT * FROM {}.{}{} ORDER BY {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    where_sql,
                    order_sql,
                )
            else:
                query = sql.SQL("SELECT {} FROM {}.{}{} ORDER BY {}").format(
                    col_sql,
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    where_sql,
                    order_sql,
                )
            cur.execute(query, params)
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
        publish_scan_order(scan_state, order_cols)
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
    cursor_inclusive: bool = False,
) -> ReadBatch:
    """Read rows with cursor_column > watermark — for incremental sync.

    When ``cursor_primary_key`` is set, uses lexicographic ``(cursor, pk)`` so
    rows sharing a timestamp watermark are not skipped forever.
    """
    from psycopg2 import sql

    from services.keyset_pagination import present_cursor_bookmark, split_cursor_bookmark
    from services.source_snapshot import get_source_snapshot_conn

    schema, table = _bind(schema, table)
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
            bookmark = present_cursor_bookmark(cursor_after)
            if bookmark is not None:
                # Composite order: cursor then primary key when provided so tied
                # watermarks do not skip peer rows (timestamp-cursor Airbyte trap).
                pk = (cursor_primary_key or "").strip()
                if cursor_inclusive:
                    # D60/D98 — run-seed watermark: re-read the whole tie group
                    # (cursor >= watermark). Key order inside a tie must never
                    # decide what is skipped; page edges after this first seek
                    # still use the composite > seek below.
                    cur_val, _ = split_cursor_bookmark(bookmark, has_tiebreak=True)
                    if pk and pk != cursor_column:
                        query = sql.SQL(
                            "{} WHERE {} >= %s ORDER BY {}, {} LIMIT %s"
                        ).format(
                            base,
                            sql.Identifier(cursor_column),
                            sql.Identifier(cursor_column),
                            sql.Identifier(pk),
                        )
                    else:
                        query = sql.SQL(
                            "{} WHERE {} >= %s ORDER BY {} LIMIT %s"
                        ).format(
                            base,
                            sql.Identifier(cursor_column),
                            sql.Identifier(cursor_column),
                        )
                    cur.execute(query, (cur_val, limit))
                elif pk and pk != cursor_column:
                    cur_val, pk_val = split_cursor_bookmark(
                        bookmark, has_tiebreak=True
                    )
                    if pk_val == "":
                        # Cursor-only watermark with a tie-break column: seek on
                        # the cursor alone but order on both so the page edge
                        # is deterministic for the composite seek that follows.
                        query = sql.SQL(
                            "{} WHERE {} > %s ORDER BY {}, {} LIMIT %s"
                        ).format(
                            base,
                            sql.Identifier(cursor_column),
                            sql.Identifier(cursor_column),
                            sql.Identifier(pk),
                        )
                        cur.execute(query, (cur_val, limit))
                    else:
                        query = sql.SQL(
                            "{} WHERE ({}, {}) > (%s, %s) ORDER BY {}, {} LIMIT %s"
                        ).format(
                            base,
                            sql.Identifier(cursor_column),
                            sql.Identifier(pk),
                            sql.Identifier(cursor_column),
                            sql.Identifier(pk),
                        )
                        cur.execute(query, (cur_val, pk_val, limit))
                else:
                    query = sql.SQL("{} WHERE {} > %s ORDER BY {} LIMIT %s").format(
                        base,
                        sql.Identifier(cursor_column),
                        sql.Identifier(cursor_column),
                    )
                    cur_val, _ = split_cursor_bookmark(bookmark, has_tiebreak=False)
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
