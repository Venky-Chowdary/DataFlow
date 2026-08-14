"""Destination cardinality taken *before* the write.

Append into a non-empty table cannot be proven by whole-table digests, so
Gate-8 falls back to cardinality. ``target_rows >= expected_rows`` is not a
proof there: a table that already held 30 rows satisfies it even if the writer
appended nothing. The only honest cardinality proof for append is the delta

    rows_after - rows_before == expected_rows

which requires the count taken before the writer runs. This module owns that
one query, so every destination family answers it the same way and
``reconcile()`` can tell "delta proven" apart from "delta unknown" instead of
silently reporting the second as the first.

``None`` means the count is unavailable (unsupported engine, missing table, or
an unreachable destination); callers must degrade assurance rather than assume
zero.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

logger = logging.getLogger(__name__)

__all__ = [
    "PRECOUNT_KEY",
    "destination_row_count",
    "destination_key_hits",
    "precount_destination",
    "precount_table",
]

# Dest-engine IN-list chunk. Partitioning the key set (not overlapping) so
# summed COUNT(DISTINCT) equals the full census.
_KEY_HIT_CHUNK = 400

# Key used to carry the pre-write count on the writer's destination summary.
PRECOUNT_KEY = "target_rows_before"


def _count(conn: Any, table_ref: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        cur.close()


def destination_row_count(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Rows already in the destination table, or ``None`` when unknowable.

    A missing table counts as ``0`` — create-on-first-write is a known empty
    destination, which is a proof, not an unknown.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    try:
        from connectors.sql_identifiers import quote_table_ref

        if db_type == "sqlite":
            import sqlite3

            database = str(cfg.get("database") or "")
            if not database:
                return None
            with sqlite3.connect(database) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    return 0
                return _count(conn, quote_table_ref(table, dialect="sqlite"))

        if db_type in {"postgresql", "redshift"}:
            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                sch = schema or "public"
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass(%s)", (f'"{sch}"."{table}"',)
                    )
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        return 0
                return _count(conn, quote_table_ref(table, sch, dialect="postgresql"))
            finally:
                conn.close()

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 3306),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = %s",
                        (table,),
                    )
                    row = cur.fetchone()
                    if not row or not int(row[0]):
                        return 0
                return _count(conn, quote_table_ref(table, dialect="mysql"))
            finally:
                conn.close()

        if db_type == "mongodb":
            from pymongo import MongoClient

            from src.transfer.adapters import mongodb_connection_string

            client: MongoClient = MongoClient(
                mongodb_connection_string(cfg), serverSelectionTimeoutMS=5000
            )
            try:
                database = str(cfg.get("database") or "")
                if not database:
                    return None
                coll = client[database][table]
                # Exact, not estimated: an approximate count cannot prove a delta.
                return int(coll.count_documents({}))
            finally:
                client.close()
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination count failed: %s", exc)
        return None
    return None


def destination_key_hits(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many of these keys dest already holds — dest-engine, not writer ack.

    Upsert/CDC ``records_processed`` counts updates as writes. ``COUNT(*)``
    does not move. The independent split is: keys in this batch that already
    exist on dest (updates) versus keys that do not (inserts). ``None`` means
    the probe could not run; callers must leave keyed conservation unproven.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    table = (table_name or "").strip()
    if not table or not cols:
        return None
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys or []:
        tup = tuple(raw)
        if len(tup) != len(cols) or any(v is None for v in tup):
            continue
        if tup in seen:
            continue
        seen.add(tup)
        unique.append(tup)
    if not unique:
        return 0
    # Missing / empty dest: no hits, and IN against a missing table would error.
    n = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    if n is None:
        return None
    if n == 0:
        return 0
    try:
        return _key_hits_sql(db_type, cfg, schema=schema, table_name=table, cols=cols, keys=unique)
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination key census failed: %s", exc)
        return None


def _key_hits_sql(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    dialect = "mysql" if db_type == "mysql" else db_type
    qchar = "`" if dialect == "mysql" else '"'
    table_ref = quote_table_ref(
        table_name,
        schema if dialect == "postgresql" else None,
        dialect="postgresql" if dialect == "postgresql" else dialect,
    )
    col_sql = ", ".join(quote_sql_identifier(c, qchar) for c in cols)
    ph = "%s" if dialect in {"postgresql", "mysql"} else "?"
    total = 0
    if dialect == "sqlite":
        import sqlite3

        database = str(cfg.get("database") or "")
        if not database:
            return None
        with sqlite3.connect(database) as conn:
            total = _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        return total
    if dialect in {"postgresql", "redshift"}:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    if dialect == "mysql":
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    return None


def _sum_distinct_hits(
    conn: Any,
    table_ref: str,
    col_sql: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
    ph: str,
) -> int:
    total = 0
    width = len(cols)
    for i in range(0, len(keys), _KEY_HIT_CHUNK):
        chunk = keys[i : i + _KEY_HIT_CHUNK]
        if width == 1:
            in_sql = ", ".join(ph for _ in chunk)
            sql = (
                f"SELECT COUNT(DISTINCT {col_sql}) FROM {table_ref} "  # nosec B608
                f"WHERE {col_sql} IN ({in_sql})"
            )
            params: tuple[Any, ...] = tuple(row[0] for row in chunk)
        else:
            row_ph = "(" + ", ".join(ph for _ in cols) + ")"
            in_sql = ", ".join(row_ph for _ in chunk)
            sql = (
                f"SELECT COUNT(*) FROM ("  # nosec B608
                f"SELECT DISTINCT {col_sql} FROM {table_ref} "
                f"WHERE ({col_sql}) IN ({in_sql})"
                f") _df_key_hits"
            )
            params = tuple(v for row in chunk for v in row)
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            total += int(row[0]) if row and row[0] is not None else 0
        finally:
            cur.close()
    return total


def precount_table(db_type: str, cfg: dict[str, Any], table_name: str) -> int | None:
    """Pre-write count for a table the streaming writers already resolved.

    Streaming paths compute the driver type and destination table themselves and
    call the batch writer directly, so they pass those in rather than
    re-resolving from the endpoint.
    """
    from services.dialect_profiles import schema_from_cfg

    return destination_row_count(
        db_type, cfg, schema=schema_from_cfg(db_type, cfg), table_name=table_name
    )


def precount_destination(
    endpoint: EndpointConfig, cfg: dict[str, Any]
) -> int | None:
    """Pre-write count for a resolved destination endpoint.

    Resolves the driver, schema and table exactly the way the writer will, so
    the delta is measured against the object the rows actually land in.
    """
    from src.transfer.adapters import resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
    return precount_table(
        db_type, cfg, resolve_dest_table(db_type, endpoint, "dt_import")
    )
