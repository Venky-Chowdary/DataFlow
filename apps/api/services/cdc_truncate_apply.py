"""Apply a source TRUNCATE to a CDC destination (data audit DATA-1).

A PostgreSQL ``TRUNCATE`` reaches logical decoding as one message, not as row
deletes. It used to be ignored, so the destination kept every pre-truncate row
next to the rows inserted after it while the run reported success and Gate-8
passed. The change batch now carries ``truncated=True`` and the apply empties
the destination table before the changes that follow the truncate.

A destination this module can't empty stops the stream with a named error
(``CDC_SOURCE_TRUNCATED``) instead of silently keeping the old rows.
"""

from __future__ import annotations

from typing import Any


def _pg(cfg: dict[str, Any], table: str, schema: str | None) -> int:
    from psycopg2 import sql

    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=cfg.get("host", "") or "127.0.0.1", port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""), username=cfg.get("username", ""),
        password=cfg.get("password", ""), connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {}.{}").format(
                sql.Identifier(schema or cfg.get("schema") or "public"), sql.Identifier(table)))
            count = int(cur.rowcount or 0)
        conn.commit()
        return count
    finally:
        conn.close()


def _mysql(cfg: dict[str, Any], table: str) -> int:
    from connectors.mysql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    conn = get_connection(
        host=cfg.get("host", "") or "127.0.0.1", port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""), username=cfg.get("username", ""),
        password=cfg.get("password", ""), connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            count = int(cur.execute("DELETE FROM " + quote_table_ref(table, dialect="mysql")) or 0)  # nosec B608
        conn.commit()
        return count
    finally:
        conn.close()


def _sqlite(cfg: dict[str, Any], table: str) -> int:
    import sqlite3

    from connectors.sql_identifiers import quote_sql_identifier
    from connectors.table_manager import _sqlite_path_from_cfg

    conn = sqlite3.connect(_sqlite_path_from_cfg(cfg))
    try:
        with conn:
            cur = conn.execute("DELETE FROM " + quote_sql_identifier(table, '"'))  # nosec B608
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def _generic(cfg: dict[str, Any], table: str, schema: str | None, dt: str) -> int:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_table_ref
    from connectors.table_manager import _generic_sql_cfg
    from services.engine_pool import release_engine

    sa_cfg = _generic_sql_cfg(cfg, dt)
    dialect = str(sa_cfg.get("type") or sa_cfg.get("db_type") or "ansi")
    engine = get_sqlalchemy_engine(sa_cfg)
    try:
        with engine.begin() as conn:
            res = conn.execute(sa.text(
                "DELETE FROM " + quote_table_ref(table, schema or cfg.get("schema"), dialect=dialect)))  # nosec B608
            return int(res.rowcount or 0)
    finally:
        release_engine(engine)


def _mongodb(cfg: dict[str, Any], table: str) -> int:
    from connectors.table_manager import _mongo_db

    return int(_mongo_db(cfg)[table].delete_many({}).deleted_count or 0)


def _redis(cfg: dict[str, Any], prefix: str) -> int:
    from connectors.redis_reader import _redis_client

    client = _redis_client(dict(cfg))
    names = list(client.scan_iter(match=f"{prefix}:*", count=1000))
    removed = 0
    for i in range(0, len(names), 500):
        removed += int(client.delete(*names[i:i + 500]) or 0)
    return removed


def empty_destination_for_source_truncate(
    dest_type: str, cfg: dict[str, Any], table: str, schema: str | None = None
) -> int:
    """Delete every row of the CDC destination table; return how many went."""
    from services.cdc_destination_support import lsn_fetch_route

    dt = str(dest_type or "").strip().lower()
    route = lsn_fetch_route(dt)
    if route == "postgresql":
        return _pg(cfg, table, schema)
    if route == "mysql":
        return _mysql(cfg, table)
    if dt == "sqlite":
        return _sqlite(cfg, table)
    if route == "generic_sql":
        return _generic(cfg, table, schema, dt)
    if route == "mongodb":
        return _mongodb(cfg, table)
    if route == "redis":
        return _redis(cfg, table)
    from services.data_errors import CdcSourceTruncated

    raise CdcSourceTruncated(
        f"The source table was truncated, and CDC can't empty the {dest_type or 'destination'} "
        f"destination {table} to match. Nothing from that point was applied and the "
        "stream position did not move. Empty the destination yourself and reset this "
        "pipeline's CDC position to take a fresh snapshot.",
        details={"dest_type": dest_type, "table": table},
    )


__all__ = ["empty_destination_for_source_truncate"]
