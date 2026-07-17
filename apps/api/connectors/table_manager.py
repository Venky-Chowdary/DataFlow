"""Table lifecycle helpers — drop/reset/delete destination objects."""

from __future__ import annotations

from typing import Any


def drop_table(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None = None,
) -> bool:
    """Drop the destination object if the driver supports it."""
    dt = (db_type or "").lower().strip()
    if dt in ("postgresql", "redshift"):
        return _drop_postgresql(cfg, table_name, schema)
    if dt == "mysql":
        return _drop_mysql(cfg, table_name, schema)
    if dt == "sqlite":
        return _drop_sqlite(cfg, table_name, schema)
    if dt == "generic_sql":
        return _drop_generic_sql(cfg, table_name, schema)
    if dt == "mongodb":
        return _drop_mongodb(cfg, table_name, schema)
    return False


def _drop_postgresql(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    from psycopg2 import sql

    from connectors.postgresql_conn import get_connection

    try:
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            schema_id = sql.Identifier(schema or "public")
            table_id = sql.Identifier(table_name)
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(schema_id, table_id)
            )
        conn.close()
        return True
    except Exception:
        return False


def _drop_mysql(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    from connectors.mysql_conn import get_connection

    try:
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 3306),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.close()
        return True
    except Exception:
        return False


def _drop_sqlite(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    import sqlite3

    try:
        database = cfg.get("database") or cfg.get("connection_string") or ""
        if not database:
            return False
        conn = sqlite3.connect(database)
        conn.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def _drop_generic_sql(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    try:
        from connectors import generic_sql

        return generic_sql.drop_table(cfg, table_name, schema)
    except Exception:
        return False


def _drop_mongodb(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    try:
        from pymongo import MongoClient

        host = cfg.get("host") or "127.0.0.1"
        port = int(cfg.get("port") or 27017)
        database = cfg.get("database") or "test"
        username = cfg.get("username", "")
        password = cfg.get("password", "")
        if username and password:
            client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")
        else:
            client = MongoClient(host, port)
        client[database].drop_collection(table_name)
        client.close()
        return True
    except Exception:
        return False


def delete_by_primary_keys(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None = None,
) -> int:
    """Delete rows from a destination by primary key values.

    Supports SQL engines (PostgreSQL, MySQL, SQLite, generic_sql) and MongoDB.
    Returns the number of rows deleted. Unsupported drivers return 0.
    """
    if not keys:
        return 0
    dt = (db_type or "").lower().strip()
    if dt in ("postgresql", "redshift"):
        return _delete_postgresql(cfg, table_name, primary_key_column, keys, schema)
    if dt == "mysql":
        return _delete_mysql(cfg, table_name, primary_key_column, keys, schema)
    if dt == "sqlite":
        return _delete_sqlite(cfg, table_name, primary_key_column, keys, schema)
    if dt == "generic_sql":
        return _delete_generic_sql(cfg, table_name, primary_key_column, keys, schema)
    if dt == "mongodb":
        return _delete_mongodb(cfg, table_name, primary_key_column, keys)
    return 0


def _delete_postgresql(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    from psycopg2 import sql

    from connectors.postgresql_conn import get_connection

    try:
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        conn.autocommit = True
        schema_id = sql.Identifier(schema or "public")
        table_id = sql.Identifier(table_name)
        col_id = sql.Identifier(pk_col)
        placeholders = sql.SQL(",").join(sql.Placeholder() * len(keys))
        query = sql.SQL("DELETE FROM {}.{} WHERE {} IN ({})").format(
            schema_id, table_id, col_id, placeholders
        )
        with conn.cursor() as cur:
            cur.execute(query, keys)
            deleted = cur.rowcount
        conn.close()
        return deleted
    except Exception:
        return 0


def _delete_mysql(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    from connectors.mysql_conn import get_connection

    try:
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 3306),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        conn.autocommit = True
        placeholders = ",".join(["%s"] * len(keys))
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{table_name}` WHERE `{pk_col}` IN ({placeholders})", keys)
            deleted = cur.rowcount
        conn.close()
        return deleted
    except Exception:
        return 0


def _delete_sqlite(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    import sqlite3

    try:
        database = cfg.get("database") or cfg.get("connection_string") or ""
        if not database:
            return 0
        conn = sqlite3.connect(database)
        placeholders = ",".join(["?"] * len(keys))
        cur = conn.execute(
            f'DELETE FROM "{table_name}" WHERE "{pk_col}" IN ({placeholders})', keys
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        return deleted
    except Exception:
        return 0


def _delete_generic_sql(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    try:
        from connectors import generic_sql

        return generic_sql.delete_by_primary_keys(cfg, table_name, pk_col, keys, schema)
    except Exception:
        return 0


def _delete_mongodb(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str]) -> int:
    try:
        from pymongo import MongoClient

        host = cfg.get("host") or "127.0.0.1"
        port = int(cfg.get("port") or 27017)
        database = cfg.get("database") or "test"
        username = cfg.get("username", "")
        password = cfg.get("password", "")
        if username and password:
            client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")
        else:
            client = MongoClient(host, port)
        col = client[database][table_name]
        result = col.delete_many({pk_col: {"$in": keys}})
        client.close()
        return result.deleted_count
    except Exception:
        return 0
