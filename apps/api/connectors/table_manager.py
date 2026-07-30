"""Table lifecycle helpers — drop/reset/delete destination objects."""

from __future__ import annotations

import logging
from typing import Any

from connectors.mongodb_common import (
    _mongo_client,
    mongodb_database_from_uri,
    normalize_mongodb_connection_string,
)

logger = logging.getLogger(__name__)


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
    if dt == "snowflake":
        return _drop_snowflake(cfg, table_name, schema)
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


def _drop_snowflake(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    from connectors.snowflake_conn import get_connection, normalize_account

    conn = None
    try:
        conn = get_connection(
            account=normalize_account(cfg.get("host", "")),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            database=cfg.get("database", ""),
            schema=schema or cfg.get("schema", "PUBLIC"),
            warehouse=cfg.get("warehouse", ""),
            connection_string=cfg.get("connection_string", ""),
            role=cfg.get("role", ""),
        )
        with conn.cursor() as cur:
            if cfg.get("warehouse"):
                try:
                    cur.execute(f'USE WAREHOUSE "{cfg["warehouse"]}"')
                except Exception as exc:
                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        return True
    except Exception:
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


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
        conn_str = normalize_mongodb_connection_string(
            connection_string=cfg.get("connection_string", ""),
            host=cfg.get("host") or "127.0.0.1",
            port=int(cfg.get("port") or 27017),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            database=cfg.get("database") or "test",
            auth_source=cfg.get("auth_source", ""),
            ssl=bool(cfg.get("ssl")),
        )
        client = _mongo_client(conn_str)
        db_name = cfg.get("database") or mongodb_database_from_uri(conn_str) or "test"
        client[db_name].drop_collection(table_name)
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
    *,
    incoming_lsn: str | None = None,
    lsn_column: str = "_df_lsn",
) -> int:
    """Delete rows from a destination by primary key values.

    Supports SQL engines (PostgreSQL, MySQL, SQLite, generic_sql) and MongoDB.
    Returns the number of rows deleted. Unsupported drivers return 0.

    When ``incoming_lsn`` is set, stale deletes that would wipe a newer
    ``_df_lsn`` row are skipped (at-least-once CDC redelivery safety).
    """
    if not keys:
        return 0
    dt = (db_type or "").lower().strip()
    # Iceberg owns scan + LSN filter + CoW overwrite (filesystem or pyiceberg).
    if dt in {"iceberg", "apache_iceberg"}:
        from connectors.iceberg_writer import delete_by_primary_keys as _iceberg_delete

        return _iceberg_delete(
            cfg,
            table_name,
            primary_key_column,
            list(keys),
            schema=schema,
            incoming_lsn=incoming_lsn,
            lsn_column=lsn_column,
        )
    work_keys = list(keys)
    if incoming_lsn:
        try:
            existing = _fetch_pk_lsn_map(
                db_type,
                cfg,
                table_name,
                primary_key_column,
                work_keys,
                schema,
                lsn_column=lsn_column,
            )
            from services.cdc_effectively_once import filter_keys_for_lsn_delete

            work_keys = filter_keys_for_lsn_delete(work_keys, existing, incoming_lsn)
        except Exception as exc:
            # Fail closed: unconditional delete under at-least-once redelivery can
            # wipe a row recreated at a newer _df_lsn (silent destination regression).
            logger.error(
                "CDC LSN delete guard unavailable — refusing unconditional delete: %s",
                exc,
                exc_info=exc,
            )
            raise RuntimeError(
                f"Cannot apply LSN-guarded CDC delete (fetch {lsn_column!r} failed): {exc}. "
                "Refusing unconditional delete that could wipe newer rows."
            ) from exc
    if not work_keys:
        return 0
    if dt in ("postgresql", "redshift"):
        return _delete_postgresql(cfg, table_name, primary_key_column, work_keys, schema)
    if dt == "mysql":
        return _delete_mysql(cfg, table_name, primary_key_column, work_keys, schema)
    if dt == "sqlite":
        return _delete_sqlite(cfg, table_name, primary_key_column, work_keys, schema)
    if dt == "generic_sql":
        return _delete_generic_sql(cfg, table_name, primary_key_column, work_keys, schema)
    if dt == "mongodb":
        return _delete_mongodb(cfg, table_name, primary_key_column, work_keys)
    if dt in {
        "sqlserver",
        "mssql",
        "oracle",
        "oracle_db",
        "oracle_autonomous_warehouse",
        "snowflake",
        "bigquery",
        "duckdb",
        "databricks",
        "synapse_analytics",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "google_cloud_sql_sql_server",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
    }:
        # Route warehouse/SQL dialects through the generic SQLAlchemy deleter.
        from connectors.generic_sql import delete_by_primary_keys as _generic_delete

        return _generic_delete(
            {**cfg, "db_type": dt if dt != "mssql" else "sqlserver"},
            table_name,
            primary_key_column,
            work_keys,
            schema=schema,
        )
    return 0


def _fetch_pk_lsn_map(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None,
    *,
    lsn_column: str,
) -> dict[str, Any]:
    """Return ``{pk: _df_lsn_or_None}`` for keys (missing rows → None)."""
    existing: dict[str, Any] = {str(k): None for k in keys}
    dt = (db_type or "").lower().strip()
    if dt in ("postgresql", "redshift"):
        from psycopg2 import sql

        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        try:
            placeholders = sql.SQL(",").join(sql.Placeholder() * len(keys))
            query = sql.SQL("SELECT {}, {} FROM {}.{} WHERE {} IN ({})").format(
                sql.Identifier(primary_key_column),
                sql.Identifier(lsn_column),
                sql.Identifier(schema or "public"),
                sql.Identifier(table_name),
                sql.Identifier(primary_key_column),
                placeholders,
            )
            with conn.cursor() as cur:
                cur.execute(query, keys)
                for row in cur.fetchall() or []:
                    existing[str(row[0])] = row[1]
        finally:
            conn.close()
        return existing
    if dt == "mysql":
        from connectors.mysql_conn import get_connection
        from connectors.writer_common import quote_sql_identifier

        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 3306),
            database=cfg.get("database") or schema or "",
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
        )
        try:
            pk_q = quote_sql_identifier(primary_key_column, "`")
            lsn_q = quote_sql_identifier(lsn_column, "`")
            table_q = quote_sql_identifier(table_name, "`")
            placeholders = ", ".join(["%s"] * len(keys))
            query = f"SELECT {pk_q}, {lsn_q} FROM {table_q} WHERE {pk_q} IN ({placeholders})"
            with conn.cursor() as cur:
                cur.execute(query, keys)
                for row in cur.fetchall() or []:
                    existing[str(row[0])] = row[1]
        finally:
            conn.close()
        return existing
    if dt in {
        "generic_sql",
        "sqlserver",
        "mssql",
        "oracle",
        "snowflake",
        "bigquery",
        "sqlite",
        "duckdb",
        "databricks",
        "synapse_analytics",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "google_cloud_sql_sql_server",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
        "oracle_db",
        "oracle_autonomous_warehouse",
    }:
        from connectors.generic_sql import fetch_pk_lsn_map

        sa_cfg = {**cfg, "db_type": dt if dt != "mssql" else "sqlserver"}
        if "type" not in sa_cfg:
            sa_cfg["type"] = sa_cfg.get("db_type") or dt
        return fetch_pk_lsn_map(
            sa_cfg,
            table_name,
            primary_key_column,
            keys,
            schema=schema,
            lsn_column=lsn_column,
        )
    if dt in {"mongodb", "mongo"}:
        return _fetch_pk_lsn_map_mongodb(
            cfg, table_name, primary_key_column, keys, lsn_column=lsn_column
        )
    raise RuntimeError(
        f"LSN delete fetch is not wired for destination type {dt!r}; "
        "refusing to invent existing_lsn=None (would always apply deletes)"
    )


def _fetch_pk_lsn_map_mongodb(
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str,
    keys: list[str],
    *,
    lsn_column: str,
) -> dict[str, Any]:
    existing: dict[str, Any] = {str(k): None for k in keys}
    conn_str = normalize_mongodb_connection_string(
        connection_string=cfg.get("connection_string", ""),
        host=cfg.get("host") or "127.0.0.1",
        port=int(cfg.get("port") or 27017),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        database=cfg.get("database") or "test",
        auth_source=cfg.get("auth_source", ""),
        ssl=bool(cfg.get("ssl")),
    )
    client = _mongo_client(conn_str)
    try:
        db_name = cfg.get("database") or mongodb_database_from_uri(conn_str) or "test"
        coll = client[db_name][table_name]
        # Coerce numeric-looking keys when docs store ints.
        query_keys: list[Any] = []
        for k in keys:
            query_keys.append(k)
            if isinstance(k, str) and k.isdigit():
                query_keys.append(int(k))
        cursor = coll.find(
            {primary_key_column: {"$in": query_keys}},
            {primary_key_column: 1, lsn_column: 1},
        )
        for doc in cursor:
            pk = doc.get(primary_key_column)
            if pk is None and primary_key_column == "_id":
                pk = doc.get("_id")
            if pk is not None:
                existing[str(pk)] = doc.get(lsn_column)
        return existing
    finally:
        client.close()


class UnsupportedCdcDeleteError(RuntimeError):
    """Raised when CDC deletes cannot be applied on the destination."""


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
            cur.execute(f"DELETE FROM `{table_name}` WHERE `{pk_col}` IN ({placeholders})", keys)  # nosec B608
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
            f'DELETE FROM "{table_name}" WHERE "{pk_col}" IN ({placeholders})', keys  # nosec B608
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
        conn_str = normalize_mongodb_connection_string(
            connection_string=cfg.get("connection_string", ""),
            host=cfg.get("host") or "127.0.0.1",
            port=int(cfg.get("port") or 27017),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            database=cfg.get("database") or "test",
            auth_source=cfg.get("auth_source", ""),
            ssl=bool(cfg.get("ssl")),
        )
        client = _mongo_client(conn_str)
        db_name = cfg.get("database") or mongodb_database_from_uri(conn_str) or "test"
        result = client[db_name][table_name].delete_many({pk_col: {"$in": keys}})
        return result.deleted_count
    except Exception:
        return 0
