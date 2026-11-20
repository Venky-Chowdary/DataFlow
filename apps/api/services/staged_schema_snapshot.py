"""The destination schema snapshot behind a staged preflight's version (D308).

Staging and Confirm hash the same catalog rows, read with the same SQL text
through the same driver, so an unchanged table hashes the same at both ends.

Where the rows come from differs, and that is the point of this module:

* Inside a tool call (staging) the endpoint-session scope already holds the
  session the preflight's own introspection logged in with. PostgreSQL and
  MySQL borrow it: the snapshot is not a second login per staging. SQL Server
  introspects through the pooled SQLAlchemy engine, so the snapshot uses the
  same engine identity and checks out the connection that read returned.
* Outside a scope (Confirm, before the run's own sessions open) the read dials
  the guarded engine, the path a transfer uses: egress guard and the
  connector's TLS keywords apply, under the caller's connect deadline.
"""

from __future__ import annotations

from typing import Any

PG_KINDS = frozenset({"postgresql", "postgres"})
MYSQL_KINDS = frozenset({"mysql", "mariadb"})
MSSQL_KINDS = frozenset({"sqlserver", "mssql", "azure_sql"})
SNAPSHOT_KINDS = PG_KINDS | MYSQL_KINDS | MSSQL_KINDS

# pyformat (psycopg2 and PyMySQL both take it): one SQL text for the borrowed
# session and the engine's driver connection, so both read identical rows.
_PG_SQL = (
    (
        "columns",
        "SELECT column_name, data_type, character_maximum_length, "
        "numeric_precision, numeric_scale, collation_name, is_nullable, "
        "column_default, is_identity, identity_generation "
        "FROM information_schema.columns "
        "WHERE table_schema = %(s)s AND table_name = %(t)s "
        "ORDER BY ordinal_position",
    ),
    (
        "indexes",
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = %(s)s AND tablename = %(t)s ORDER BY indexname",
    ),
    (
        "constraints",
        "SELECT con.conname, con.contype::text, pg_get_constraintdef(con.oid) "
        "FROM pg_constraint con "
        "JOIN pg_class rel ON rel.oid = con.conrelid "
        "JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
        "WHERE nsp.nspname = %(s)s AND rel.relname = %(t)s "
        "ORDER BY con.conname",
    ),
)
_MYSQL_SQL = (
    (
        "columns",
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
        "EXTRA, COLUMN_KEY, GENERATION_EXPRESSION, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %(t)s "
        "ORDER BY ORDINAL_POSITION",
    ),
    (
        "indexes",
        "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %(t)s "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
    ),
    (
        "constraints",
        "SELECT tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, cc.CHECK_CLAUSE "
        "FROM information_schema.TABLE_CONSTRAINTS tc "
        "LEFT JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
        "AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
        "WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = %(t)s "
        "ORDER BY tc.CONSTRAINT_NAME",
    ),
)
_MSSQL_SQL = (
    (
        "columns",
        "SELECT c.name, ty.name, c.max_length, c.precision, c.scale, "
        "c.collation_name, c.is_nullable, c.is_identity, "
        "OBJECT_DEFINITION(c.default_object_id) "
        "FROM sys.columns c "
        "JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
        "JOIN sys.tables t ON c.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = :s AND t.name = :t ORDER BY c.column_id",
    ),
    (
        "indexes",
        "SELECT i.name, i.is_unique, i.is_primary_key, col.name "
        "FROM sys.indexes i "
        "JOIN sys.tables t ON i.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id "
        "WHERE s.name = :s AND t.name = :t AND i.name IS NOT NULL "
        "ORDER BY i.name, ic.key_ordinal",
    ),
    (
        "constraints",
        "SELECT name, definition FROM sys.check_constraints "
        "WHERE parent_object_id = OBJECT_ID(:qualified) ORDER BY name",
    ),
)


def _scope_engine(kind: str) -> str:
    return "postgresql" if kind in PG_KINDS else "mysql"


def _borrowed_session(kind: str, cfg: dict[str, Any]) -> Any:
    """The preflight introspection's own session, when a tool-call scope holds one.

    Same arguments as ``schema_introspect`` passes, so the endpoint-session
    key is the same and ``borrow`` hands back the idle session instead of
    logging in again. ``None`` outside a scope.
    """
    from services.endpoint_session import in_scope

    if kind not in PG_KINDS | MYSQL_KINDS or not in_scope(_scope_engine(kind)):
        return None
    common = {
        "host": str(cfg.get("host") or ""),
        "database": str(cfg.get("database") or ""),
        "username": str(cfg.get("username") or ""),
        "password": str(cfg.get("password") or ""),
        "connection_string": str(cfg.get("connection_string") or ""),
        "ssl": bool(cfg.get("ssl")),
    }
    if kind in PG_KINDS:
        from connectors.postgresql_conn import get_connection

        return get_connection(port=int(cfg.get("port") or 5432), **common)
    from connectors.mysql_conn import get_connection as mysql_connection

    return mysql_connection(
        port=int(cfg.get("port") or 3306), purpose="introspect", **common
    )


def _dbapi_rows(conn: Any, statements: Any, params: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    cur = conn.cursor()
    try:
        for name, sql in statements:
            cur.execute(sql, params)
            out[name] = [tuple(row) for row in cur.fetchall()]
    finally:
        cur.close()
    return out


def _engine_cfg(kind: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """The cfg SQL Server introspection pools under, so both share one engine."""
    if kind not in MSSQL_KINDS:
        return cfg
    from connectors.generic_sql import connection_options

    return {
        **connection_options({**(cfg.get("extra") or {}), **cfg}),
        "type": "sqlserver",
        "host": str(cfg.get("host") or ""),
        "port": int(cfg.get("port") or 1433),
        "database": str(cfg.get("database") or ""),
        "username": str(cfg.get("username") or ""),
        "password": str(cfg.get("password") or ""),
        "schema": str(cfg.get("schema") or "dbo"),
        "connection_string": str(cfg.get("connection_string") or ""),
        "ssl": bool(cfg.get("ssl")),
    }


def read_schema_snapshot(cfg: dict[str, Any]) -> dict[str, Any]:
    """Columns, keys, indexes, foreign keys, checks, defaults and identity."""
    kind = str(cfg.get("type") or "").lower()
    schema = str(cfg.get("schema") or "")
    table = str(cfg.get("table") or "")
    if kind not in SNAPSHOT_KINDS:
        # No catalog read for this engine: never log in only to fail. A remote
        # Snowflake login is the slowest step of a plan (D187); "unread" keeps
        # the staged result from being reused.
        raise RuntimeError(f"schema version is not read for {kind or 'this engine'}")
    if kind in PG_KINDS | MYSQL_KINDS:
        statements = _PG_SQL if kind in PG_KINDS else _MYSQL_SQL
        params = {"s": schema or "public", "t": table}
        session = _borrowed_session(kind, cfg)
        if session is not None:
            try:
                return _dbapi_rows(session, statements, params)
            finally:
                session.close()  # hands the session back to the scope
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from services.engine_pool import release_engine

    engine = get_sqlalchemy_engine(_engine_cfg(kind, cfg))
    try:
        with engine.connect() as conn:
            if kind not in MSSQL_KINDS:
                return _dbapi_rows(conn.connection, statements, params)
            mssql_params = {
                "s": schema or "dbo",
                "t": table,
                "qualified": f"{schema or 'dbo'}.{table}",
            }
            return {
                name: [
                    tuple(row)
                    for row in conn.execute(sa.text(sql), mssql_params).fetchall()
                ]
                for name, sql in _MSSQL_SQL
            }
    finally:
        release_engine(engine)
