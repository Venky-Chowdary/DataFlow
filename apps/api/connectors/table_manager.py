"""Table lifecycle helpers — drop/reset/delete destination objects."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from connectors.mongodb_common import (
    _mongo_client,
    mongodb_database_from_uri,
    normalize_mongodb_connection_string,
)
from connectors.sql_identifiers import quote_sql_identifier

logger = logging.getLogger(__name__)

# Destinations whose DELETE / LSN fetch go through SQLAlchemy generic_sql.
# Membership is dialect family (Azure SQL ≡ SQL Server, RDS Oracle ≡ Oracle),
# not a catalog-id allow-list that leftover MERGE can miss.
_GENERIC_SQL_MUTATE_ENGINES = frozenset(
    {
        "generic_sql",
        "snowflake",
        "bigquery",
        "duckdb",
        "databricks",
    }
)


def _generic_sql_cfg(cfg: dict[str, Any], dt: str) -> dict[str, Any]:
    """Engine builder reads ``type``; catalog ``mssql`` quotes as sqlserver."""
    out = {**cfg, "db_type": "sqlserver" if dt == "mssql" else dt}
    if not str(out.get("type") or "").strip():
        out["type"] = out["db_type"]
    return out


def _routes_generic_sql_mutate(dt: str) -> bool:
    from services.dialect_profiles import warehouse_sql_quote_dialect

    if dt in _GENERIC_SQL_MUTATE_ENGINES:
        return True
    return warehouse_sql_quote_dialect(dt) is not None


def _sqlite_path_from_cfg(cfg: dict[str, Any]) -> str:
    """Resolve a SQLite filesystem path from endpoint config.

    Routes through the canonical :func:`sqlite_file_path` so a ``sqlite:///``
    URL is stripped to a real path. Passing the raw URL to ``sqlite3.connect``
    made every URL-configured SQLite destination fail its full-refresh DROP,
    CDC delete, and LSN read-back with "unable to open database file".
    """
    from connectors.sqlite_common import sqlite_file_path

    return sqlite_file_path(
        str(cfg.get("database") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("host") or ""),
    )


class TableDropError(RuntimeError):
    """A destination DROP was attempted and failed.

    This exists because the old contract returned ``False`` for both "this
    driver cannot drop" and "the DROP raised". Callers used the result to decide
    whether a ``full_refresh`` had truly cleared the table, so a permission
    error, lock timeout, or dead connection silently degraded the run to an
    append — the destination kept its old rows, the new rows landed on top, and
    the job reported success with a doubled row count.
    """

    def __init__(self, table_name: str, cause: BaseException) -> None:
        self.table_name = table_name
        self.cause = cause
        super().__init__(
            f"Could not drop destination table '{table_name}': "
            f"{type(cause).__name__}: {cause}"
        )


class DestinationDeleteError(RuntimeError):
    """A destination DELETE by primary key was attempted and failed.

    Same reasoning as :class:`TableDropError`. ``0`` legitimately means "those
    keys were already absent", which is an idempotent success for CDC. A
    swallowed driver error also returned ``0``, so a failed tombstone apply was
    read as success and the CDC cursor advanced past deletes that never
    happened — the deleted source rows lived forever at the destination.
    """

    def __init__(self, table_name: str, cause: BaseException) -> None:
        self.table_name = table_name
        self.cause = cause
        super().__init__(
            f"Could not delete rows from destination table '{table_name}': "
            f"{type(cause).__name__}: {cause}"
        )


def drop_table(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None = None,
) -> bool:
    """Drop the destination object.

    Returns ``True`` on a successful drop and ``False`` only when this driver
    has no drop support at all. A drop that was attempted and failed raises
    :class:`TableDropError` — the two outcomes must stay distinguishable so a
    ``full_refresh`` cannot silently continue as an append.
    """
    dt = (db_type or "").lower().strip()
    if dt in ("postgresql", "redshift"):
        return _drop_postgresql(cfg, table_name, schema)
    if dt == "pgvector":
        return _drop_pgvector(cfg, table_name, schema)
    if dt == "mysql":
        return _drop_mysql(cfg, table_name, schema)
    if dt == "sqlite":
        return _drop_sqlite(cfg, table_name, schema)
    if dt == "generic_sql":
        return _drop_generic_sql(cfg, table_name, schema)
    if dt == "mongodb":
        return _drop_mongodb(cfg, table_name, schema)
    if dt == "elasticsearch":
        return _drop_elasticsearch(cfg, table_name)
    if dt == "qdrant":
        return _drop_qdrant(cfg, table_name)
    if dt == "snowflake":
        return _drop_snowflake(cfg, table_name, schema)
    if dt == "bigquery":
        return _drop_bigquery(cfg, table_name, schema)
    if dt == "clickhouse":
        return _drop_clickhouse(cfg, table_name, schema)
    if _routes_generic_sql_mutate(dt):
        # Oracle / SQL Server / BigQuery / DuckDB / Databricks: callers that
        # still drop (staging, rollback) use this. Overwrite of a relational
        # table does not — see ``empty_existing_for_overwrite``.
        return _drop_generic_sql(_generic_sql_cfg(cfg, dt), table_name, schema)
    return False


# Existing relational tables are emptied, not dropped. DROP+CREATE discarded
# primary key, unique, NOT NULL, check, foreign key and identity, then loaded
# rows that those constraints would have refused (DEF-V7-E1-021).
_EMPTY_IN_PLACE_ENGINES = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlite",
    "oracle",
    "sqlserver",
    "generic_sql",
    "cockroachdb",
    "greenplum",
    "timescaledb",
    "db2",
    "teradata",
})

_MISSING_TABLE_TOKENS = (
    "42p01",
    "does not exist",
    "doesn't exist",
    "no such table",
    "ora-00942",
    "invalid object name",
    "1146",
    "1051",
)


def overwrite_clear_kind(db_type: str) -> str:
    """How an overwrite removes the previous generation.

    ``empty`` keeps the table and its constraints. ``rename_collection``
    parks a Mongo collection aside so a failed load can put it back.
    ``drop`` is only for engines whose overwrite still replaces the object.
    """
    from services.db_type_utils import overwrite_engine_kind

    dt = overwrite_engine_kind(db_type)
    if dt == "mongodb":
        return "rename_collection"
    if dt in _EMPTY_IN_PLACE_ENGINES:
        return "empty"
    return "drop"


def overwrite_empty_statements(dialect: str, quoted: str) -> tuple[str, str]:
    """TRUNCATE (may be empty) then DELETE. DELETE runs when TRUNCATE is blocked.

    A foreign key that references this table rejects TRUNCATE. DELETE removes
    this table's rows and leaves the referencing table, and this table's own
    primary key, unique, check, NOT NULL and identity, in place.
    """
    kind = (dialect or "").lower().strip()
    if kind in {"postgresql", "redshift", "postgres", "cockroachdb", "greenplum"}:
        return (
            f"TRUNCATE TABLE {quoted} RESTART IDENTITY",
            f"DELETE FROM {quoted}",
        )
    if kind == "sqlite":
        return ("", f"DELETE FROM {quoted}")
    return (f"TRUNCATE TABLE {quoted}", f"DELETE FROM {quoted}")


def _missing_table_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(token in text for token in _MISSING_TABLE_TOKENS)


def _execute_empty_statements(run: Any, dialect: str, quoted: str) -> str:
    """Run TRUNCATE, then DELETE if TRUNCATE is refused. ``run`` executes SQL.

    Returns ``emptied``. A missing table raises the driver's error so the
    caller can treat create-new as already clear.
    """
    truncate_sql, delete_sql = overwrite_empty_statements(dialect, quoted)
    if truncate_sql:
        try:
            run(truncate_sql)
            return "emptied"
        except Exception as exc:
            if _missing_table_error(exc):
                raise
            logger.info(
                "Overwrite TRUNCATE refused for %s (%s); deleting rows in place",
                quoted,
                exc,
            )
    run(delete_sql)
    return "emptied"


def empty_existing_for_overwrite(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None = None,
) -> str:
    """Remove rows from an existing table. The table and its constraints stay.

    Returns ``absent`` when the object is already gone (create-new may CREATE)
    and ``emptied`` when rows were removed. A permission or lock failure raises
    :class:`TableDropError` so the overwrite cannot continue as an append.
    """
    from services.db_type_utils import overwrite_engine_kind

    dt = overwrite_engine_kind(db_type)
    try:
        if dt in ("postgresql", "redshift"):
            return _empty_postgresql(cfg, table_name, schema, dialect=dt)
        if dt in ("mysql", "mariadb"):
            return _empty_mysql(cfg, table_name)
        if dt == "sqlite":
            return _empty_sqlite(cfg, table_name)
        if dt == "generic_sql" or _routes_generic_sql_mutate(dt):
            return _empty_generic_sql(_generic_sql_cfg(cfg, dt), table_name, schema)
    except TableDropError:
        raise
    except Exception as exc:
        if _missing_table_error(exc):
            return "absent"
        raise TableDropError(table_name, exc) from exc
    raise TableDropError(
        table_name,
        RuntimeError(f"{dt or 'unknown'} has no in-place empty"),
    )


def _empty_postgresql(
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None,
    *,
    dialect: str,
) -> str:
    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    quoted = quote_table_ref(table_name, schema or cfg.get("schema"), dialect=dialect)
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
    try:
        def _run(sql: str) -> None:
            with conn.cursor() as cur:
                cur.execute(sql)

        return _execute_empty_statements(_run, dialect, quoted)
    except Exception as exc:
        if _missing_table_error(exc):
            return "absent"
        raise TableDropError(table_name, exc) from exc
    finally:
        conn.close()


def _empty_mysql(cfg: dict[str, Any], table_name: str) -> str:
    from connectors.mysql_conn import enable_autocommit, get_connection
    from connectors.sql_identifiers import quote_table_ref

    quoted = quote_table_ref(table_name, dialect="mysql")
    conn = get_connection(
        host=cfg.get("host", "") or "127.0.0.1",
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
        purpose="ddl",
    )
    enable_autocommit(conn)
    try:
        def _run(sql: str) -> None:
            with conn.cursor() as cur:
                cur.execute(sql)

        return _execute_empty_statements(_run, "mysql", quoted)
    except Exception as exc:
        if _missing_table_error(exc):
            return "absent"
        raise TableDropError(table_name, exc) from exc
    finally:
        try:
            conn.close()
        except Exception:
            logger.warning("MySQL empty connection close failed", exc_info=True)


def _empty_sqlite(cfg: dict[str, Any], table_name: str) -> str:
    """Do not delete the live rows.

    SQLite overwrite writes a stage table and publishes it in one transaction
    (``atomic_overwrite`` on the sqlite writer). A DELETE here is what made
    readers see a partial table mid-load (QA R2-035). ``deferred`` means the
    previous snapshot stays until that publish.
    """
    import sqlite3

    database = _sqlite_path_from_cfg(cfg)
    if not database:
        return "absent"
    conn = sqlite3.connect(database)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
    except Exception as exc:
        if _missing_table_error(exc):
            return "absent"
        raise TableDropError(table_name, exc) from exc
    finally:
        conn.close()
    if row is None:
        return "absent"
    return "deferred"


def _empty_generic_sql(
    cfg: dict[str, Any], table_name: str, schema: str | None
) -> str:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_table_ref
    from services.engine_pool import release_engine

    dialect = str(cfg.get("type") or cfg.get("db_type") or "ansi")
    quoted = quote_table_ref(
        table_name, schema or cfg.get("schema"), dialect=dialect
    )
    engine = get_sqlalchemy_engine(cfg)
    try:
        with engine.connect() as conn:
            def _run(sql: str) -> None:
                try:
                    conn.execute(sa.text(sql))
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise

            try:
                return _execute_empty_statements(_run, dialect, quoted)
            except Exception as exc:
                if _missing_table_error(exc):
                    return "absent"
                raise TableDropError(table_name, exc) from exc
    finally:
        release_engine(engine)


def postgres_columns_to_keep(
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None,
    mappings: list[Any] | None,
) -> list[dict[str, str]]:
    """Destination columns the overwrite CREATE must still declare."""
    from connectors.postgresql_conn import get_connection
    from services.overwrite_keep import columns_to_keep

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
        sch = schema or cfg.get("schema") or "public"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (sch, table_name),
            )
            live = [{"name": row[0], "ddl_type": row[1]} for row in cur.fetchall()]
        return columns_to_keep(live, mappings)
    finally:
        conn.close()


def _drop_postgresql(
    cfg: dict[str, Any],
    table_name: str,
    schema: str | None,
    *,
    after_drop: Callable[[Any], None] | None = None,
) -> bool:
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
            if after_drop is not None:
                after_drop(cur)
        conn.close()
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_pgvector(
    cfg: dict[str, Any], table_name: str, schema: str | None
) -> bool:
    from services.vector_fingerprint import delete_pgvector_fingerprint

    schema_name = schema or str(cfg.get("schema") or "public")
    return _drop_postgresql(
        cfg,
        table_name,
        schema_name,
        after_drop=lambda cursor: delete_pgvector_fingerprint(
            cursor, schema_name, table_name
        ),
    )


def _drop_bigquery(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    """DROP via the BigQuery client. Missing table is already clear.

    SQLAlchemy cannot bind the goccy HTTP emulator (dialect ``http``). Inventing
    a generic_sql DROP would fail-closed create-new overwrite as an append risk.
    """
    from connectors.bigquery_conn import get_client
    from connectors.google_emulator import (
        google_emulator_retry,
        google_emulator_timeout,
        looks_like_google_emulator,
    )

    project = str(cfg.get("database") or cfg.get("project_id") or "").strip()
    dataset = str(schema or cfg.get("schema") or "").strip()
    if not project or not dataset or not table_name:
        raise TableDropError(
            table_name or "unknown",
            ValueError("BigQuery drop needs project, dataset, and table"),
        )
    try:
        client = get_client(
            project_id=project,
            service_account=str(cfg.get("service_account") or ""),
            location=str(cfg.get("location") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
            connection_string=str(cfg.get("connection_string") or ""),
        )
        table_id = f"{project}.{dataset}.{table_name}"
        extra: dict[str, Any] = {}
        if looks_like_google_emulator(
            endpoint=str(cfg.get("connection_string") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
        ):
            extra["retry"] = google_emulator_retry()
            extra["timeout"] = google_emulator_timeout()
        client.delete_table(table_id, not_found_ok=True, **extra)
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


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
                    warehouse_q = quote_sql_identifier(cfg["warehouse"], '"')
                    cur.execute(f"USE WAREHOUSE {warehouse_q}")
                except Exception as exc:
                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
            # A bare name is case-sensitive once quoted ("foo_df_quarantine" !=
            # FOO_DF_QUARANTINE) — resolve the stored casing first so FAIL_JOB
            # undo actually drops the DLQ table it created (D1).
            from connectors.snowflake_conn import resolve_snowflake_table_name

            resolved = resolve_snowflake_table_name(
                cur, schema or cfg.get("schema", "PUBLIC"), table_name
            )
            target = resolved or table_name
            target_q = quote_sql_identifier(target, '"')
            cur.execute(f"DROP TABLE IF EXISTS {target_q}")
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def mysql_overwrite_backup_name(table_name: str) -> str:
    """Backup identifier, at most 64 characters (MySQL's limit)."""
    suffix = "__df_bak"
    base = str(table_name or "t")[: 64 - len(suffix)]
    return f"{base}{suffix}"


def _mysql_tables_present(cur: Any, names: list[str]) -> set[str]:
    if not names:
        return set()
    placeholders = ", ".join(["%s"] * len(names))
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = DATABASE() AND table_name IN ({placeholders})",
        tuple(names),
    )
    return {str(row[0]) for row in cur.fetchall()}


MYSQL_ROW_BACKUP_MARK = "dataflow:overwrite-row-backup"


def _mysql_ddl_connection(cfg: dict[str, Any]) -> Any:
    from connectors.mysql_conn import enable_autocommit, get_connection

    conn = get_connection(
        host=cfg.get("host", "") or "127.0.0.1",
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl")),
        purpose="ddl",
    )
    enable_autocommit(conn)
    return conn


def _mysql_table_comment(cur: Any, name: str) -> str:
    cur.execute(
        "SELECT TABLE_COMMENT FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (name,),
    )
    row = cur.fetchone()
    return str(row[0] or "") if row else ""


def _mysql_stored_columns(cur: Any, name: str) -> list[str]:
    """Columns a row copy may write (generated columns compute themselves)."""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
        "AND EXTRA NOT LIKE %s ORDER BY ORDINAL_POSITION",
        (name, "%GENERATED%"),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _mysql_copy_rows_back(cur: Any, table_name: str, backup: str) -> None:
    """Put the backup's rows into the live table in one transaction."""
    from connectors.sql_identifiers import quote_sql_identifier

    cols = ", ".join(quote_sql_identifier(c, "`") for c in _mysql_stored_columns(cur, backup))
    src_q = quote_sql_identifier(table_name, "`")
    bak_q = quote_sql_identifier(backup, "`")
    cur.execute("START TRANSACTION")
    try:
        cur.execute(f"DELETE FROM {src_q}")  # nosec B608
        cur.execute(f"INSERT INTO {src_q} ({cols}) SELECT {cols} FROM {bak_q}")  # nosec B608
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise


def retire_mysql_overwrite(
    cfg: dict[str, Any],
    table_name: str,
    mappings: list[Any] | None = None,
) -> tuple[str | None, list[dict[str, str]]]:
    """Copy the live rows aside, then empty the live table in place (D195).

    The live table object stays: its CHECK, FOREIGN KEY, NOT NULL, DEFAULT,
    indexes, comment and triggers are the destination's, not DataFlow's.
    Renaming it aside and letting the writer CREATE a replacement from
    DataFlow's DDL lost all of them. MySQL ``TRUNCATE`` commits on its own,
    so the pre-run rows are kept in ``<table>__df_bak`` (a ``CREATE TABLE
    LIKE`` copy marked :data:`MYSQL_ROW_BACKUP_MARK`) until the load finishes;
    a failed load copies them back.

    Returns ``(backup_name or None, columns the load must keep)``.
    """
    from connectors.sql_identifiers import quote_sql_identifier
    from services.overwrite_keep import columns_to_keep

    backup = mysql_overwrite_backup_name(table_name)
    conn = None
    try:
        conn = _mysql_ddl_connection(cfg)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (table_name,),
            )
            live = [{"name": row[0], "ddl_type": row[1]} for row in cur.fetchall()]
            kept = columns_to_keep(live, mappings)
            present = _mysql_tables_present(cur, [table_name, backup])
            src_q = quote_sql_identifier(table_name, "`")
            bak_q = quote_sql_identifier(backup, "`")
            if backup in present:
                row_backup = _mysql_table_comment(cur, backup) == MYSQL_ROW_BACKUP_MARK
                if row_backup and table_name in present:
                    # The previous run emptied the live table and died before
                    # settling. Its backup holds the pre-run rows: put them
                    # back, then take this run's backup from them.
                    _mysql_copy_rows_back(cur, table_name, backup)
                    cur.execute(f"DROP TABLE {bak_q}")
                elif table_name in present:
                    # A rename-aside backup from an older build: the live
                    # name is that run's replacement. Put the original back.
                    cur.execute(f"DROP TABLE {src_q}")
                    cur.execute(f"RENAME TABLE {bak_q} TO {src_q}")
                elif not row_backup:
                    cur.execute(f"RENAME TABLE {bak_q} TO {src_q}")
                else:
                    raise TableDropError(
                        table_name,
                        f"backup {backup} holds the pre-run rows but {table_name} is missing",
                    )
                present = _mysql_tables_present(cur, [table_name, backup])
            if table_name not in present:
                return None, kept
            if backup in present:
                logger.error(
                    "MySQL overwrite refused to drop backup %s while %s is live",
                    backup,
                    table_name,
                )
                raise TableDropError(
                    table_name,
                    f"backup {backup} still holds the pre-run rows",
                )
            cols = ", ".join(
                quote_sql_identifier(c, "`") for c in _mysql_stored_columns(cur, table_name)
            )
            cur.execute(f"CREATE TABLE {bak_q} LIKE {src_q}")
            cur.execute(f"ALTER TABLE {bak_q} COMMENT = %s", (MYSQL_ROW_BACKUP_MARK,))
            cur.execute(f"INSERT INTO {bak_q} ({cols}) SELECT {cols} FROM {src_q}")  # nosec B608

            def _run(sql: str) -> None:
                cur.execute(sql)

            _execute_empty_statements(_run, "mysql", src_q)
        return backup, kept
    except TableDropError:
        raise
    except Exception as exc:  # noqa: BLE001 - any DDL failure must fail the overwrite
        raise TableDropError(table_name, exc) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def restore_mysql_overwrite(cfg: dict[str, Any], table_name: str, backup: str) -> None:
    """Put the pre-run rows back after a failed load.

    A row backup (:data:`MYSQL_ROW_BACKUP_MARK`) is copied back into the live
    table, which never left. A rename-aside backup from an older build is
    renamed back over the partial replacement.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    conn = None
    try:
        conn = _mysql_ddl_connection(cfg)
        with conn.cursor() as cur:
            present = _mysql_tables_present(cur, [table_name, backup])
            src_q = quote_sql_identifier(table_name, "`")
            bak_q = quote_sql_identifier(backup, "`")
            if backup not in present:
                return
            if (
                table_name in present
                and _mysql_table_comment(cur, backup) == MYSQL_ROW_BACKUP_MARK
            ):
                _mysql_copy_rows_back(cur, table_name, backup)
                cur.execute(f"DROP TABLE {bak_q}")
                return
            if table_name in present:
                cur.execute(f"DROP TABLE {src_q}")
            cur.execute(f"RENAME TABLE {bak_q} TO {src_q}")
    except Exception as exc:  # noqa: BLE001 - restore must surface every DDL failure
        raise TableDropError(table_name, exc) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def discard_mysql_overwrite(cfg: dict[str, Any], backup: str) -> None:
    """Drop the renamed original after the replacement load has committed."""
    from connectors.mysql_conn import enable_autocommit, get_connection
    from connectors.sql_identifiers import quote_sql_identifier

    conn = None
    try:
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 3306),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
            purpose="ddl",
        )
        enable_autocommit(conn)
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {quote_sql_identifier(backup, '`')}")
    except Exception as exc:  # noqa: BLE001 - backup drop must surface every DDL failure
        raise TableDropError(backup, exc) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def _drop_mysql(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    from connectors.mysql_conn import enable_autocommit, get_connection

    try:
        # DDL purpose: short lock wait so full_refresh overwrite cannot hang a
        # tiny demo behind a metadata lock from an idle Validate/probe session.
        conn = get_connection(
            host=cfg.get("host", "") or "127.0.0.1",
            port=int(cfg.get("port") or 3306),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl")),
            purpose="ddl",
        )
        enable_autocommit(conn)
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {quote_sql_identifier(table_name, '`')}")
        conn.close()
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_sqlite(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    import sqlite3

    try:
        database = _sqlite_path_from_cfg(cfg)
        if not database:
            return False
        conn = sqlite3.connect(database)
        conn.execute(f"DROP TABLE IF EXISTS {quote_sql_identifier(table_name, '`')}")
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_generic_sql(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    try:
        from connectors import generic_sql

        return generic_sql.drop_table(cfg, table_name, schema)
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_clickhouse(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    """Overwrite DROP — leftover MERGE cannot undo insert-append duplicates."""
    import sqlalchemy as sa
    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_table_ref
    from services.engine_pool import release_engine

    sch = (schema or cfg.get("schema") or "").strip() or None
    table_ref = quote_table_ref(table_name, sch, dialect="clickhouse")
    engine_cfg = {**cfg, "type": cfg.get("type") or "clickhouse"}
    engine = get_sqlalchemy_engine(engine_cfg)
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_ref}"))  # nosec B608
            try:
                conn.commit()
            except Exception:
                pass
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc
    finally:
        release_engine(engine)


def _mongo_db(cfg: dict[str, Any]) -> Any:
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
    return client[db_name]


def _collection_create_options(db: Any, name: str) -> dict[str, Any]:
    """Validator and validation level, so the replacement rejects the same docs."""
    try:
        listed = db.command("listCollections", filter={"name": name})
    except Exception:
        return {}
    batch = (listed or {}).get("cursor", {}).get("firstBatch") or []
    if not batch:
        return {}
    opts = dict(batch[0].get("options") or {})
    kept: dict[str, Any] = {}
    for key in ("validator", "validationLevel", "validationAction"):
        if key in opts and opts[key] not in (None, {}):
            kept[key] = opts[key]
    return kept


def _copy_collection_indexes(source: Any, dest: Any) -> None:
    try:
        indexes = list(source.list_indexes())
    except Exception:
        logger.warning("Mongo overwrite could not read indexes", exc_info=True)
        return
    for spec in indexes:
        if str(spec.get("name") or "") == "_id_":
            continue
        keys = list((spec.get("key") or {}).items())
        if not keys:
            continue
        kwargs: dict[str, Any] = {}
        if spec.get("name"):
            kwargs["name"] = spec["name"]
        if spec.get("unique"):
            kwargs["unique"] = True
        if spec.get("sparse"):
            kwargs["sparse"] = True
        try:
            dest.create_index(keys, **kwargs)
        except Exception:
            logger.warning(
                "Mongo overwrite could not copy index %s", spec.get("name"), exc_info=True
            )


def retire_mongodb_overwrite(cfg: dict[str, Any], table_name: str) -> str | None:
    """Rename the live collection aside and recreate it empty.

    ``drop_collection`` deleted the collection before the load. A failed
    overwrite then had nothing to restore (E1-025). The backup keeps the
    documents. The new collection keeps the validator and indexes, so a
    document that breaks them is rejected instead of loaded.
    """
    backup = mysql_overwrite_backup_name(table_name)
    try:
        db = _mongo_db(cfg)
        names = set(db.list_collection_names())
        if table_name not in names and backup not in names:
            return None
        if table_name in names and backup in names:
            db.drop_collection(table_name)
            db[backup].rename(table_name)
            names.discard(backup)
            names.add(table_name)
        elif table_name not in names and backup in names:
            db[backup].rename(table_name)
            names.add(table_name)
        options = _collection_create_options(db, table_name)
        db[table_name].rename(backup)
        db.create_collection(table_name, **options)
        _copy_collection_indexes(db[backup], db[table_name])
        return backup
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def restore_mongodb_overwrite(cfg: dict[str, Any], table_name: str, backup: str) -> None:
    """Drop a partial replacement and rename the backup back to the live name."""
    try:
        db = _mongo_db(cfg)
        names = set(db.list_collection_names())
        if table_name in names:
            db.drop_collection(table_name)
        if backup in names:
            db[backup].rename(table_name)
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def discard_mongodb_overwrite(cfg: dict[str, Any], backup: str) -> None:
    """Drop the renamed original after the replacement load has committed."""
    try:
        db = _mongo_db(cfg)
        if backup in set(db.list_collection_names()):
            db.drop_collection(backup)
    except Exception as exc:
        raise TableDropError(backup, exc) from exc


def _drop_mongodb(cfg: dict[str, Any], table_name: str, schema: str | None) -> bool:
    try:
        db = _mongo_db(cfg)
        db.drop_collection(table_name)
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_elasticsearch(cfg: dict[str, Any], table_name: str) -> bool:
    """Delete the destination index so a full refresh starts empty.

    Without it an overwrite reached the bulk writer with the index intact and
    every document came back a version conflict (``op_type=create`` deliberately
    refuses to clobber existing docs), so a second pass was unrunnable instead
    of being an overwrite.
    """
    from connectors.elasticsearch_reader import _client

    try:
        client = _client(cfg)
        client.indices.delete(index=table_name, ignore_unavailable=True)
        return True
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc


def _drop_qdrant(cfg: dict[str, Any], table_name: str) -> bool:
    """Delete the destination collection so overwrite cannot append points."""
    from connectors.qdrant_writer import qdrant_rest

    session = None
    try:
        session, base_url, headers = qdrant_rest(cfg)
        resp = session.delete(
            f"{base_url}/collections/{table_name}", headers=headers, timeout=10
        )
        if resp.status_code in {200, 201, 404}:
            from services.vector_fingerprint import delete_qdrant_fingerprint

            delete_qdrant_fingerprint(session, base_url, headers, table_name)
            return True
        raise RuntimeError(
            f"Qdrant drop failed: {resp.status_code} {resp.text[:300]}"
        )
    except TableDropError:
        raise
    except Exception as exc:
        raise TableDropError(table_name, exc) from exc
    finally:
        if session is not None:
            try:
                session.close()
            except Exception as exc:
                logger.warning("Failed to close Qdrant drop session: %s", exc)


def delete_by_primary_keys(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str | list[str],
    keys: list[str],
    schema: str | None = None,
    *,
    incoming_lsn: str | None = None,
    lsn_column: str = "_df_lsn",
) -> int:
    """Delete rows from a destination by primary key values.

    Supports SQL engines (PostgreSQL, MySQL, SQLite, generic_sql), MongoDB,
    Redis, Elasticsearch, DynamoDB, Iceberg, and object-store leftovers.

    ``primary_key_column`` may be a single column or a list / comma-joined
    composite. Composite keys arrive already joined with the unit separator
    used by ``services.cdc_snapshot_window._pk_value``; they are split back
    into per-column predicates so a multi-column PK cannot silently no-op.

    Returns the number of rows deleted, where ``0`` means either "this driver
    has no delete support" or "those keys were already absent" — both genuine
    idempotent outcomes. A delete that was attempted and failed raises
    :class:`DestinationDeleteError` rather than returning ``0``, so CDC cannot
    mistake a driver error for a successful tombstone apply and advance its
    cursor past deletes that never happened.

    When ``incoming_lsn`` is set, stale deletes that would wipe a newer
    ``_df_lsn`` row are skipped (at-least-once CDC redelivery safety).
    """
    if not keys:
        return 0
    from services.cdc_snapshot_window import _pk_columns

    pk_cols = _pk_columns(primary_key_column)
    dt = (db_type or "").lower().strip()
    from services.dest_precount import _object_store_kind

    store = _object_store_kind(dt)
    if store in {"s3", "gcs", "adls"}:
        from connectors.object_store_leftover import (
            delete_by_primary_keys as _object_store_delete,
        )

        return _object_store_delete(
            dt,
            cfg,
            table_name,
            pk_cols,
            list(keys),
            schema=schema,
        )
    if dt == "snowflake":
        from services.dest_precount import _snowflake_delete_keys

        try:
            return _snowflake_delete_keys(
                cfg,
                schema=str(schema or cfg.get("schema") or ""),
                table_name=table_name,
                cols=pk_cols,
                keys=list(keys),
            )
        except Exception as exc:
            raise DestinationDeleteError(table_name, exc) from exc
    if dt == "bigquery":
        from services.dest_precount import _bigquery_delete_keys

        try:
            return _bigquery_delete_keys(
                cfg,
                schema=str(schema or cfg.get("schema") or ""),
                table_name=table_name,
                cols=pk_cols,
                keys=list(keys),
            )
        except Exception as exc:
            raise DestinationDeleteError(table_name, exc) from exc
    # Iceberg owns scan + LSN filter + CoW overwrite (filesystem or pyiceberg).
    if dt in {"iceberg", "apache_iceberg"}:
        from connectors.iceberg_writer import delete_by_primary_keys as _iceberg_delete

        return _iceberg_delete(
            cfg,
            table_name,
            pk_cols,
            list(keys),
            schema=schema,
            incoming_lsn=incoming_lsn,
            lsn_column=lsn_column,
        )
    if dt in {"pgvector", "qdrant", "weaviate", "pinecone"}:
        # Vector stores have no _df_lsn; deletes remain honest at-least-once.
        if incoming_lsn:
            logger.debug("Ignoring incoming_lsn for vector destination %s", dt)
        from services.row_conservation import parse_delete_keys
        from services.vector_sync import (
            pgvector_delete_doc_keys,
            qdrant_delete_doc_keys,
            vector_doc_key,
            vector_engine_delete_doc_keys,
        )

        doc_keys = [
            vector_doc_key(values)
            for values in parse_delete_keys(list(keys), len(pk_cols))
        ]
        if dt == "pgvector":
            return pgvector_delete_doc_keys(cfg, table_name, schema, doc_keys)
        if dt == "qdrant":
            return qdrant_delete_doc_keys(cfg, table_name, doc_keys)
        return vector_engine_delete_doc_keys(dt, cfg, table_name, doc_keys)

    work_keys = list(keys)
    if incoming_lsn:
        try:
            existing = _fetch_pk_lsn_map(
                db_type,
                cfg,
                table_name,
                pk_cols,
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
    if dt == "redis":
        from services.dest_precount import _redis_delete_keys

        return _redis_delete_keys(
            cfg, prefix=table_name, cols=pk_cols, keys=work_keys
        )
    if dt in {"elasticsearch", "opensearch"}:
        from services.dest_precount import _elasticsearch_delete_keys

        return _elasticsearch_delete_keys(
            cfg, index=table_name, cols=pk_cols, keys=work_keys
        )
    if dt == "dynamodb":
        from services.dest_precount import _dynamodb_delete_keys

        return _dynamodb_delete_keys(
            cfg, table_name=table_name, cols=pk_cols, keys=work_keys
        )
    if len(pk_cols) > 1:
        return _delete_composite(dt, cfg, table_name, pk_cols, work_keys, schema)
    if dt in ("postgresql", "redshift"):
        return _delete_postgresql(cfg, table_name, pk_cols[0], work_keys, schema)
    if dt == "mysql":
        return _delete_mysql(cfg, table_name, pk_cols[0], work_keys, schema)
    if dt == "sqlite":
        return _delete_sqlite(cfg, table_name, pk_cols[0], work_keys, schema)
    if dt == "generic_sql":
        return _delete_generic_sql(cfg, table_name, pk_cols[0], work_keys, schema)
    if dt == "mongodb":
        return _delete_mongodb(cfg, table_name, pk_cols[0], work_keys)
    if dt == "clickhouse":
        return _delete_clickhouse(cfg, table_name, pk_cols, work_keys, schema)
    if _routes_generic_sql_mutate(dt):
        # Route warehouse/SQL dialects through the generic SQLAlchemy deleter.
        from connectors.generic_sql import delete_by_primary_keys as _generic_delete

        return _generic_delete(
            _generic_sql_cfg(cfg, dt),
            table_name,
            pk_cols[0],
            work_keys,
            schema=schema,
        )
    return 0


def _fetch_pk_lsn_map(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str | list[str],
    keys: list[str],
    schema: str | None,
    *,
    lsn_column: str,
) -> dict[str, Any]:
    """Return ``{pk: _df_lsn_or_None}`` for keys (missing rows → None).

    Composite keys are addressed with the same unit-separator join the CDC
    readers emit, so the LSN guard and the delete path share one key space.
    """
    from services.cdc_snapshot_window import _pk_columns

    pk_cols = _pk_columns(primary_key_column)
    existing: dict[str, Any] = {str(k): None for k in keys}
    dt = (db_type or "").lower().strip()
    if len(pk_cols) > 1:
        return _fetch_composite_pk_lsn_map(
            dt, cfg, table_name, pk_cols, keys, schema, lsn_column=lsn_column
        )
    primary_key_column = pk_cols[0]
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
    if dt == "sqlite" or _routes_generic_sql_mutate(dt):
        from connectors.generic_sql import fetch_pk_lsn_map

        sa_cfg = _generic_sql_cfg(cfg, dt)
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
        # Writer may have stored int / Decimal / ObjectId. isdigit() missed
        # locale money and did not refuse Auto grouping.
        from services.target_sample import mongo_query_key_variants

        query_keys: list[Any] = []
        for k in keys:
            query_keys.extend(mongo_query_key_variants(k))
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


def _fetch_composite_pk_lsn_map(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    keys: list[str],
    schema: str | None,
    *,
    lsn_column: str,
) -> dict[str, Any]:
    """LSN map keyed by the unit-separator join the CDC readers emit."""
    from services.cdc_snapshot_window import _pk_row_dict, _pk_value

    existing: dict[str, Any] = {str(k): None for k in keys}
    wanted = set(existing)
    tuples = [tuple(_pk_row_dict(pk_cols, k)[c] for c in pk_cols) for k in keys]
    dt = (db_type or "").lower().strip()

    def _absorb(row_values: tuple[Any, ...], lsn: Any) -> None:
        row = {pk_cols[i]: row_values[i] for i in range(len(pk_cols))}
        key = _pk_value(row, pk_cols)
        if key is not None and key in wanted:
            existing[key] = lsn

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
            per_key = sql.SQL("({})").format(
                sql.SQL(" AND ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder())
                    for c in pk_cols
                )
            )
            where = sql.SQL(" OR ").join(per_key for _ in tuples)
            cols = sql.SQL(", ").join(
                sql.Identifier(c) for c in [*pk_cols, lsn_column]
            )
            query = sql.SQL("SELECT {} FROM {}.{} WHERE {}").format(
                cols,
                sql.Identifier(schema or "public"),
                sql.Identifier(table_name),
                where,
            )
            binds: list[Any] = [v for tup in tuples for v in tup]
            with conn.cursor() as cur:
                cur.execute(query, binds)
                for row in cur.fetchall() or []:
                    _absorb(tuple(row[: len(pk_cols)]), row[len(pk_cols)])
        finally:
            conn.close()
        return existing

    if dt == "mysql":
        from connectors.mysql_conn import get_connection

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
            where, _ = _composite_or_and_clause(
                pk_cols, len(tuples), quote_char="`", placeholder="%s"
            )
            from connectors.sql_identifiers import quote_sql_identifier

            col_sql = ", ".join(
                quote_sql_identifier(c, "`") for c in [*pk_cols, lsn_column]
            )
            table_q = quote_sql_identifier(table_name, "`")
            binds = [v for tup in tuples for v in tup]
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {col_sql} FROM {table_q} WHERE {where}",  # nosec B608
                    binds,
                )
                for row in cur.fetchall() or []:
                    _absorb(tuple(row[: len(pk_cols)]), row[len(pk_cols)])
        finally:
            conn.close()
        return existing

    if dt == "sqlite":
        import sqlite3

        database = _sqlite_path_from_cfg(cfg)
        if not database:
            raise RuntimeError(
                "SQLite destination path could not be resolved for the CDC LSN "
                "read-back; refusing to report an empty LSN map (a stale change "
                "would then overwrite newer destination rows)"
            )
        conn = sqlite3.connect(database)
        try:
            where, _ = _composite_or_and_clause(
                pk_cols, len(tuples), quote_char='"', placeholder="?"
            )
            from connectors.sql_identifiers import quote_sql_identifier

            col_sql = ", ".join(
                quote_sql_identifier(c, '"') for c in [*pk_cols, lsn_column]
            )
            table_q = quote_sql_identifier(table_name, '"')
            binds = [v for tup in tuples for v in tup]
            for row in conn.execute(
                f"SELECT {col_sql} FROM {table_q} WHERE {where}",  # nosec B608
                binds,
            ):
                _absorb(tuple(row[: len(pk_cols)]), row[len(pk_cols)])
        finally:
            conn.close()
        return existing

    # Warehouse / generic SQLAlchemy path.
    from connectors.generic_sql import _engine
    import sqlalchemy as sa

    engine = _engine({**cfg, "db_type": dt if dt != "mssql" else "sqlserver"})
    try:
        meta = sa.MetaData()
        table = sa.Table(
            table_name,
            meta,
            *[sa.Column(c, sa.String) for c in [*pk_cols, lsn_column]],
            schema=schema or None,
            keep_existing=True,
        )
        clauses = [
            sa.and_(*[table.c[c] == tup[i] for i, c in enumerate(pk_cols)])
            for tup in tuples
        ]
        with engine.connect() as conn:
            rows = conn.execute(
                sa.select(*[table.c[c] for c in [*pk_cols, lsn_column]]).where(
                    sa.or_(*clauses)
                )
            ).fetchall()
            for row in rows:
                _absorb(tuple(row[: len(pk_cols)]), row[len(pk_cols)])
    finally:
        from services.engine_pool import release_engine

        release_engine(engine)
    return existing


def _delete_composite(
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    keys: list[str],
    schema: str | None,
) -> int:
    """Delete rows addressed by a composite primary key.

    Keys arrive joined with the unit separator from
    ``services.cdc_snapshot_window._pk_value``. Each key expands to an
    ``(col1 = ? AND col2 = ? AND …)`` clause; the clauses are OR'd so one
    statement covers the whole batch. Engines that support row-value
    constructors get the same semantics via that form when available, but the
    AND/OR expansion is portable and exact.
    """
    from services.cdc_snapshot_window import _pk_row_dict

    if not pk_cols or not keys:
        return 0
    dt = (db_type or "").lower().strip()
    tuples: list[tuple[Any, ...]] = []
    for key in keys:
        parts = _pk_row_dict(pk_cols, key)
        tuples.append(tuple(parts[c] for c in pk_cols))

    if dt in ("postgresql", "redshift"):
        return _delete_postgresql_composite(cfg, table_name, pk_cols, tuples, schema)
    if dt == "mysql":
        return _delete_mysql_composite(cfg, table_name, pk_cols, tuples, schema)
    if dt == "sqlite":
        return _delete_sqlite_composite(cfg, table_name, pk_cols, tuples, schema)
    if dt == "clickhouse":
        return _delete_clickhouse(cfg, table_name, pk_cols, keys, schema)
    if _routes_generic_sql_mutate(dt):
        return _delete_generic_sql_composite(
            _generic_sql_cfg(cfg, dt),
            table_name,
            pk_cols,
            tuples,
            schema,
        )
    raise DestinationDeleteError(
        table_name,
        RuntimeError(
            f"Composite primary-key CDC deletes are not wired for destination "
            f"type {dt!r}; refusing to drop the trailing key columns"
        ),
    )


def _composite_or_and_clause(
    pk_cols: list[str],
    n_keys: int,
    *,
    quote_char: str,
    placeholder: str,
) -> tuple[str, list[Any]]:
    """Build ``(a=? AND b=?) OR (a=? AND b=?)`` and the matching bind list shape.

    The bind values are filled by the caller; this returns only the SQL and an
    empty list sized so callers can see the arity.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    quoted = [quote_sql_identifier(c, quote_char) for c in pk_cols]
    per_key = "(" + " AND ".join(f"{c} = {placeholder}" for c in quoted) + ")"
    sql = " OR ".join(per_key for _ in range(n_keys))
    return sql, [None] * (n_keys * len(pk_cols))


def _delete_postgresql_composite(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    tuples: list[tuple[Any, ...]],
    schema: str | None,
) -> int:
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
        per_key = sql.SQL("({})").format(
            sql.SQL(" AND ").join(
                sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder())
                for c in pk_cols
            )
        )
        where = sql.SQL(" OR ").join(per_key for _ in tuples)
        query = sql.SQL("DELETE FROM {}.{} WHERE {}").format(schema_id, table_id, where)
        binds: list[Any] = [v for tup in tuples for v in tup]
        with conn.cursor() as cur:
            cur.execute(query, binds)
            deleted = cur.rowcount
        conn.close()
        return deleted
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_mysql_composite(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    tuples: list[tuple[Any, ...]],
    schema: str | None,
) -> int:
    from connectors.mysql_conn import enable_autocommit, get_connection

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
        enable_autocommit(conn)
        where, _ = _composite_or_and_clause(
            pk_cols, len(tuples), quote_char="`", placeholder="%s"
        )
        binds: list[Any] = [v for tup in tuples for v in tup]
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {quote_sql_identifier(table_name, '`')} WHERE {where}",  # nosec B608
                binds,
            )
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return deleted
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_sqlite_composite(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    tuples: list[tuple[Any, ...]],
    schema: str | None,
) -> int:
    import sqlite3

    try:
        database = _sqlite_path_from_cfg(cfg)
        if not database:
            raise RuntimeError(
                "SQLite destination path could not be resolved; refusing to "
                "report 0 composite deletes as an idempotent success"
            )
        conn = sqlite3.connect(database)
        where, _ = _composite_or_and_clause(
            pk_cols, len(tuples), quote_char='"', placeholder="?"
        )
        table_q = quote_sql_identifier(table_name, '"')
        binds: list[Any] = [v for tup in tuples for v in tup]
        cur = conn.execute(
            f"DELETE FROM {table_q} WHERE {where}",  # nosec B608
            binds,
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        return deleted
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_generic_sql_composite(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    tuples: list[tuple[Any, ...]],
    schema: str | None,
) -> int:
    """Composite delete via SQLAlchemy Core for warehouse dialects."""
    try:
        from connectors.generic_sql import _engine
        import sqlalchemy as sa

        engine = _engine(cfg)
        try:
            meta = sa.MetaData()
            table = sa.Table(
                table_name,
                meta,
                *[sa.Column(c, sa.String) for c in pk_cols],
                schema=schema or None,
                keep_existing=True,
            )
            # OR of AND equality predicates — portable across dialects that
            # reject row-value IN lists (Oracle, older SQL Server).
            clauses = [
                sa.and_(*[table.c[c] == tup[i] for i, c in enumerate(pk_cols)])
                for tup in tuples
            ]
            with engine.begin() as conn:
                result = conn.execute(sa.delete(table).where(sa.or_(*clauses)))
                return int(result.rowcount or 0)
        finally:
            from services.engine_pool import release_engine

            release_engine(engine)
    except DestinationDeleteError:
        raise
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


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
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_mysql(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    from connectors.mysql_conn import enable_autocommit, get_connection

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
        enable_autocommit(conn)
        placeholders = ",".join(["%s"] * len(keys))
        with conn.cursor() as cur:
            table_q = quote_sql_identifier(table_name, "`")
            pk_q = quote_sql_identifier(pk_col, "`")
            cur.execute(f"DELETE FROM {table_q} WHERE {pk_q} IN ({placeholders})", keys)  # nosec B608
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return deleted
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_sqlite(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    import sqlite3

    try:
        database = _sqlite_path_from_cfg(cfg)
        if not database:
            raise RuntimeError(
                "SQLite destination path could not be resolved; refusing to "
                "report 0 deletes as an idempotent success (the CDC cursor "
                "would advance past tombstones that were never applied)"
            )
        conn = sqlite3.connect(database)
        placeholders = ",".join(["?"] * len(keys))
        table_q = quote_sql_identifier(table_name, '"')
        pk_q = quote_sql_identifier(pk_col, '"')
        cur = conn.execute(
            f"DELETE FROM {table_q} WHERE {pk_q} IN ({placeholders})", keys  # nosec B608
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        return deleted
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _delete_generic_sql(cfg: dict[str, Any], table_name: str, pk_col: str, keys: list[str], schema: str | None) -> int:
    try:
        from connectors import generic_sql

        return generic_sql.delete_by_primary_keys(cfg, table_name, pk_col, keys, schema)
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


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
        coll = client[db_name][table_name]
        bound = _mongo_bind_keys(coll, pk_col, keys)
        result = coll.delete_many({pk_col: {"$in": bound}})
        return result.deleted_count
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc


def _mongo_bind_keys(coll: Any, pk_col: str, keys: list[str]) -> list[Any]:
    """Bind leftover/CDC keys to the collection's stored PK type.

    Digit strings against ObjectId hex stay ObjectId. Digit strings against
    int stay int. String PKs stay strings — same Iceberg leftover typing.
    """
    sample = None
    try:
        doc = coll.find_one({pk_col: {"$exists": True}}, {pk_col: 1})
        if isinstance(doc, dict):
            sample = doc.get(pk_col)
    except Exception:
        sample = None
    out: list[Any] = []
    for raw in keys:
        out.append(_mongo_typed_key(raw, sample))
    return out


def _mongo_typed_key(raw: Any, sample: Any) -> Any:
    """Bind one leftover/CDC key to the stored PK type — write-path, no invent.

    Informal ``yes`` is not True. Auto ``1.234`` is not a float PK. Locale
    money the write path stores still binds. Dest-canonical storage text
    (``1.234`` on a float PK) uses ``Decimal(text)`` first so leftover
    delete still finds the row.
    """
    if sample is None:
        return raw
    type_name = type(sample).__name__
    if type_name == "ObjectId":
        from bson import ObjectId

        return ObjectId(str(raw))
    if isinstance(sample, bool):
        if isinstance(raw, bool):
            return raw
        from connectors.sql_bind import coerce_boolean_wire

        parsed = coerce_boolean_wire(raw)
        if isinstance(parsed, bool):
            return parsed
        raise ValueError(
            f"Mongo leftover key {raw!r} is not a write-path boolean "
            "(true/t/1/false/f/0) — refuse invent from yes/on"
        )
    if isinstance(sample, int) and not isinstance(sample, bool):
        if isinstance(raw, int) and not isinstance(raw, bool):
            return raw
        from connectors.sql_bind import coerce_integer_wire

        try:
            parsed = coerce_integer_wire(raw, ddl_type="INTEGER")
        except ValueError as exc:
            raise ValueError(
                f"Mongo leftover key {raw!r} is not an integer — refuse invent"
            ) from exc
        if parsed is None:
            raise ValueError(
                f"Mongo leftover key {raw!r} is not an integer — refuse invent"
            )
        return int(parsed)
    if isinstance(sample, float):
        if isinstance(raw, bool):
            raise ValueError(
                f"Mongo leftover key {raw!r} is not a number — refuse invent"
            )
        from decimal import Decimal, InvalidOperation
        from services.transform_engine import decimal_wire_value, float_carrier_or_refuse

        parsed: Decimal | None = None
        if isinstance(raw, float):
            if raw != raw or raw in (float("inf"), float("-inf")):
                return raw
            parsed = Decimal(str(raw))
        elif isinstance(raw, int):
            parsed = Decimal(raw)
        else:
            text = str(raw).strip()
            if not text:
                raise ValueError(
                    f"Mongo leftover key {raw!r} is not a number — refuse invent"
                )
            try:
                dest = Decimal(text)
                if dest.is_finite():
                    parsed = dest
            except (InvalidOperation, ValueError, ArithmeticError):
                parsed = decimal_wire_value(text)
        if parsed is None or not parsed.is_finite():
            raise ValueError(
                f"Mongo leftover key {raw!r} is not a number — refuse invent"
            )
        try:
            return float_carrier_or_refuse(parsed)
        except ValueError as exc:
            raise ValueError(
                f"Mongo leftover key {raw!r} is not a number — refuse invent"
            ) from exc
    return str(raw) if raw is not None else raw


def _delete_clickhouse(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
    keys: list[str],
    schema: str | None,
) -> int:
    """Leftover MERGE delete: ALTER DELETE + wait until COUNT(*) FINAL can see it.

    ClickHouse mutations are async. Stamping leftover_deleted before
    ``SYSTEM WAIT MUTATIONS`` would claim extra=0 while FINAL still holds
    the leftover row. Wait failure is fail-closed, not leftover_deleted=0.
    """
    import sqlalchemy as sa
    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
    from services.engine_pool import release_engine
    from services.row_conservation import coerce_pk_part, parse_delete_keys

    sch = (schema or cfg.get("schema") or "").strip() or None
    table_ref = quote_table_ref(table_name, sch, dialect="clickhouse")
    quoted = [quote_sql_identifier(c, "`") for c in pk_cols]
    tuples = parse_delete_keys(keys, len(pk_cols))
    if not tuples:
        return 0
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, tup in enumerate(tuples):
        parts: list[str] = []
        for j, col in enumerate(quoted):
            name = f"k{i}_{j}"
            parts.append(f"{col} = :{name}")
            params[name] = coerce_pk_part(tup[j])
        clauses.append("(" + " AND ".join(parts) + ")")
    where_sql = " OR ".join(clauses)
    mutate = f"ALTER TABLE {table_ref} DELETE WHERE {where_sql}"  # nosec B608
    engine_cfg = {**cfg, "type": cfg.get("type") or "clickhouse"}
    engine = get_sqlalchemy_engine(engine_cfg)
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(mutate), params)
            try:
                conn.execute(sa.text("SYSTEM WAIT MUTATIONS"))
            except Exception as wait_exc:
                raise DestinationDeleteError(table_name, wait_exc) from wait_exc
            try:
                conn.commit()
            except Exception:
                pass
        return len(tuples)
    except DestinationDeleteError:
        raise
    except Exception as exc:
        raise DestinationDeleteError(table_name, exc) from exc
    finally:
        release_engine(engine)


def _pg_route_backup_connection(cfg: dict[str, Any]) -> Any:
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
    conn.autocommit = True
    return conn


class PgBackupRestoreRefused(RuntimeError):
    """A strict keyed write whose pre-run rows could not be restored exactly.

    Raised before any write, so the run is refused instead of risking a failed
    verification that leaves its rejected batch in place (D122).
    """


#: ``pg_trigger.tgenabled`` -> the ALTER TABLE clause that restores it.
_PG_TRIGGER_ENABLE: dict[str, str] = {
    "O": "ENABLE TRIGGER",
    "A": "ENABLE ALWAYS TRIGGER",
    "R": "ENABLE REPLICA TRIGGER",
}


def _pg_active_user_triggers(cur: Any, schema: str, table: str) -> list[tuple[str, str]]:
    """``[(trigger_name, tgenabled)]`` for the table's enabled user triggers."""
    cur.execute(
        "SELECT t.tgname, t.tgenabled FROM pg_trigger t "
        "JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND NOT t.tgisinternal AND t.tgenabled <> 'D' ORDER BY t.tgname",
        (schema, table),
    )
    return [(str(name), str(mode)) for name, mode in cur.fetchall() or []]


def _pg_refuse_unrestorable_triggers(cur: Any, schema: str, table: str) -> None:
    """Refuse up front when a restore would have to replay rows through triggers.

    Suspending a table's triggers needs its owner (or a superuser). Any other
    role restoring the backup fires them again, the verify sees the rewrite,
    and the failed batch stays live with the originals only in ``__dfbak``.
    """
    triggers = _pg_active_user_triggers(cur, schema, table)
    if not triggers:
        return
    cur.execute(
        "SELECT pg_has_role(current_user, c.relowner, 'USAGE') "
        "OR (SELECT rolsuper FROM pg_roles WHERE rolname = current_user), "
        "current_user FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema, table),
    )
    row = cur.fetchone() or (False, "")
    if row[0]:
        return
    names = ", ".join(name for name, _mode in triggers)
    raise PgBackupRestoreRefused(
        f"Strict keyed write into {schema}.{table} refused before any write: "
        f"the table has user trigger(s) {names} that role {row[1]!r} cannot "
        "suspend, so a failed verification could not restore the pre-run rows "
        "exactly. Run as the table's owner, or choose a validation mode that "
        "keeps rows on a failed proof."
    )


def _pg_suspend_user_triggers(
    cur: Any, tbl_ref: str, schema: str, table: str
) -> list[tuple[str, str]]:
    """Disable the table's enabled user triggers inside the restore transaction.

    ``session_replication_role = replica`` (superuser only) also skips FK
    enforcement as before, but it neither exists for an ordinary owner nor
    silences ``ENABLE ALWAYS`` triggers; disabling each trigger by name covers
    both, and the transaction (or :func:`_pg_resume_user_triggers`) puts every
    trigger back in its own mode.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    cur.execute("SAVEPOINT route_backup_role")
    try:
        cur.execute("SET LOCAL session_replication_role = 'replica'")
        cur.execute("RELEASE SAVEPOINT route_backup_role")
    except Exception:  # noqa: BLE001 - superuser only; per-trigger disable follows
        cur.execute("ROLLBACK TO SAVEPOINT route_backup_role")
        cur.execute("RELEASE SAVEPOINT route_backup_role")
    triggers = _pg_active_user_triggers(cur, schema, table)
    for name, _mode in triggers:
        cur.execute(f"ALTER TABLE {tbl_ref} DISABLE TRIGGER {quote_sql_identifier(name)}")
    return triggers


def _pg_resume_user_triggers(
    cur: Any, tbl_ref: str, triggers: list[tuple[str, str]]
) -> None:
    from connectors.sql_identifiers import quote_sql_identifier

    for name, mode in triggers:
        clause = _PG_TRIGGER_ENABLE.get(mode, "ENABLE TRIGGER")
        cur.execute(f"ALTER TABLE {tbl_ref} {clause} {quote_sql_identifier(name)}")


def create_pg_table_backup(
    cfg: dict[str, Any],
    *,
    schema: str,
    table: str,
    job_id: str,
) -> dict[str, str] | None:
    """Row-copy a live table to ``<table>__dfbak_<job8>`` before a strict
    keyed write on a populated destination (D111).

    The live table object is never renamed (P0-G1 — DDL/indexes/grants stay
    bound); a post-write verification failure restores by DELETE + INSERT
    FROM the backup via :func:`restore_pg_route_backups`, the exact pre-run
    state. Returns the ``{table, backup, schema}`` pair for the
    ``route_backups`` stamp, or None when the backup could not be taken
    (the run proceeds; undo then reports honestly that no rollback exists).

    Raises :class:`PgBackupRestoreRefused` when the table carries user
    triggers this role cannot suspend — that restore could never be exact.
    """
    from connectors.sql_identifiers import quote_table_ref, split_qualified_table

    schema, table = split_qualified_table(str(table or ""), str(schema or "public"))
    if not table:
        return None
    backup = pg_route_backup_name(table, job_id)
    conn = None
    try:
        conn = _pg_route_backup_connection(cfg)
        live_ref = quote_table_ref(table, schema, dialect="postgresql")
        bak_ref = quote_table_ref(backup, schema, dialect="postgresql")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            if not cur.fetchone():
                return None
            _pg_refuse_unrestorable_triggers(cur, schema, table)
            cur.execute(f"DROP TABLE IF EXISTS {bak_ref}")
            cur.execute(f"CREATE TABLE {bak_ref} (LIKE {live_ref} INCLUDING ALL)")
            cur.execute(f"INSERT INTO {bak_ref} SELECT * FROM {live_ref}")
    except PgBackupRestoreRefused:
        raise
    except Exception as exc:  # noqa: BLE001 - a missing backup must not abort the write
        logger.warning(
            "Strict-run backup copy of %s.%s skipped (%s); a post-write "
            "proof failure cannot restore prior rows",
            schema,
            table,
            exc,
        )
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return {"table": table, "backup": backup, "schema": schema}


def pg_route_backup_name(table_name: str, job_id: str) -> str:
    """Backup identifier for a routed run, at most 63 bytes (PostgreSQL limit)."""
    suffix = f"__dfbak_{str(job_id or '')[:8]}"
    base = str(table_name or "t")[: 63 - len(suffix)]
    return f"{base}{suffix}"


def _pg_route_backup_parts(
    pair: dict[str, Any], default_schema: str
) -> tuple[str, str, str, str]:
    """(schema, table, backup_schema, backup) — schema-qualified for every
    lookup; a same-named table in another schema must never match (P0-G2)."""
    from connectors.sql_identifiers import split_qualified_table

    schema, table = split_qualified_table(
        str(pair.get("table") or ""), str(pair.get("schema") or default_schema)
    )
    bak_schema, backup = split_qualified_table(
        str(pair.get("backup") or ""), schema
    )
    return schema, table, bak_schema, backup


def restore_pg_route_backups(
    cfg: dict[str, Any],
    backups: dict[str, dict[str, str]],
) -> list[str]:
    """Restore every occupied route table's pre-run rows after a failed run.

    ``backups`` maps route name -> {"table": live_name, "backup": bak_name,
    "schema": schema}. The live table object was never touched (P0-G1 — a
    rename-aside steals the customer's DDL, indexes, grants and re-binds
    dependent views to the backup), so restore is DELETE + INSERT FROM the
    backup copy, then DROP the backup — all inside one transaction with a
    savepoint per route so a single unrestorable table does not abort the
    others.
    """
    from connectors.sql_identifiers import quote_table_ref

    restored: list[str] = []
    conn = None
    try:
        conn = _pg_route_backup_connection(cfg)
        conn.autocommit = False
        default_schema = str(cfg.get("schema") or "public")
        with conn.cursor() as cur:
            for route, pair in (backups or {}).items():
                schema, table, bak_schema, backup = _pg_route_backup_parts(
                    pair, default_schema
                )
                if not table or not backup:
                    continue
                tbl_ref = quote_table_ref(
                    table, schema, dialect="postgresql"
                )
                bak_ref = quote_table_ref(
                    backup, bak_schema, dialect="postgresql"
                )
                cur.execute("SAVEPOINT route_backup_restore")
                try:
                    cur.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_name = %s",
                        (bak_schema, backup),
                    )
                    if not cur.fetchone():
                        cur.execute("RELEASE SAVEPOINT route_backup_restore")
                        continue
                    # D114/D122: replaying the pre-run rows through user
                    # triggers mutates them (a BEFORE INSERT amt:=amt+0.01
                    # fired again on restore), so they are suspended for this
                    # transaction — by name, which an owner may do, not only
                    # via the superuser-only session_replication_role.
                    suspended = _pg_suspend_user_triggers(cur, tbl_ref, schema, table)
                    cur.execute(f"DELETE FROM {tbl_ref}")
                    cur.execute(f"INSERT INTO {tbl_ref} SELECT * FROM {bak_ref}")
                    # Verify the live table equals the backup as a multiset —
                    # EXCEPT ALL, so a lost or doubled duplicate row counts —
                    # over each row's text image, since json / point / xml
                    # have no equality operator for EXCEPT to use.
                    cur.execute(
                        f"SELECT COUNT(*) FROM ("
                        f"(SELECT _t::text FROM {tbl_ref} _t"
                        f" EXCEPT ALL SELECT _b::text FROM {bak_ref} _b)"
                        f" UNION ALL "
                        f"(SELECT _b::text FROM {bak_ref} _b"
                        f" EXCEPT ALL SELECT _t::text FROM {tbl_ref} _t)"
                        f") _df_restore_diff"
                    )
                    diff = int((cur.fetchone() or [0])[0] or 0)
                    if diff:
                        logger.warning(
                            "Route %s restore verify failed: %d differing row(s) "
                            "vs backup %s — backup kept for inspection",
                            route, diff, backup,
                        )
                        cur.execute("ROLLBACK TO SAVEPOINT route_backup_restore")
                        cur.execute("RELEASE SAVEPOINT route_backup_restore")
                        continue
                    _pg_resume_user_triggers(cur, tbl_ref, suspended)
                    cur.execute(f"DROP TABLE {bak_ref}")
                    cur.execute("RELEASE SAVEPOINT route_backup_restore")
                    restored.append(str(route))
                except Exception as exc:  # noqa: BLE001 - report, keep others
                    logger.warning(
                        "Route %s backup restore failed: %s", route, exc
                    )
                    cur.execute("ROLLBACK TO SAVEPOINT route_backup_restore")
                    cur.execute("RELEASE SAVEPOINT route_backup_restore")
            conn.commit()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return restored


def undo_pg_route_run(
    cfg: dict[str, Any],
    txids: dict[str, int | str],
    tables: list[str],
) -> list[str]:
    """Delete exactly the rows one committed routed run wrote (P2 undo).

    Each route table's write commits under its own transaction id, so every
    row this run inserted into ``t`` carries ``xmin = txids[t]``. Deleting
    by ``xmin`` removes exactly this run's copies — a byte-identical row an
    earlier run landed has a different ``xmin`` and survives — and never
    touches rows that pre-date the run. One transaction with a savepoint
    per table so a single missing table does not abort the others. Returns
    the table names it removed this run's rows from.
    """
    from connectors.sql_identifiers import quote_table_ref, split_qualified_table

    if not txids:
        return []
    restored: list[str] = []
    conn = None
    try:
        conn = _pg_route_backup_connection(cfg)
        conn.autocommit = False
        default_schema = str(cfg.get("schema") or "public")
        with conn.cursor() as cur:
            for t in tables or []:
                schema, table = split_qualified_table(str(t), default_schema)
                if not table:
                    continue
                txid = txids.get(t) or txids.get(table)
                if not txid:
                    continue
                tbl_ref = quote_table_ref(table, schema, dialect="postgresql")
                cur.execute("SAVEPOINT route_run_undo")
                try:
                    cur.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_name = %s",
                        (schema, table),
                    )
                    if not cur.fetchone():
                        cur.execute("RELEASE SAVEPOINT route_run_undo")
                        continue
                    cur.execute(
                        f"DELETE FROM {tbl_ref} WHERE xmin = %s",  # nosec B608
                        (str(txid),),
                    )
                    cur.execute("RELEASE SAVEPOINT route_run_undo")
                    restored.append(table)
                except Exception as exc:  # noqa: BLE001 - report, keep others
                    logger.warning(
                        "Route run undo failed for %s: %s", table, exc
                    )
                    cur.execute("ROLLBACK TO SAVEPOINT route_run_undo")
                    cur.execute("RELEASE SAVEPOINT route_run_undo")
            conn.commit()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return restored


def discard_pg_route_backups(
    cfg: dict[str, Any],
    backups: dict[str, dict[str, str]],
) -> None:
    """Drop the backup copies after a Gate-8-verified run."""
    from connectors.sql_identifiers import quote_table_ref

    conn = None
    try:
        conn = _pg_route_backup_connection(cfg)
        default_schema = str(cfg.get("schema") or "public")
        with conn.cursor() as cur:
            for pair in (backups or {}).values():
                _, _, bak_schema, backup = _pg_route_backup_parts(
                    pair, default_schema
                )
                if backup:
                    cur.execute(
                        "DROP TABLE IF EXISTS "
                        + quote_table_ref(
                            backup, bak_schema, dialect="postgresql"
                        )
                    )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001 - close is best-effort
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
