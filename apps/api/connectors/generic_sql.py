"""Universal SQLAlchemy connector for any SQL database with a Python DBAPI.

This connector lets Datawrap treat SQLAlchemy-supported engines as first-class
sources and destinations. The user provides the catalog type (e.g. mssql,
oracle, db2, trino, h2) or a full connection_string; we build the SQLAlchemy
URL and driver name from the catalog. This is the fastest path to 100+
real, working catalog IDs without needing a dedicated connector for every
engine.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Final

from connectors.base import ReadBatch
from connectors.merge_dialects import (  # noqa: F401  (re-exported for callers)
    _clickhouse_replacing_upsert,
    _db2_merge_upsert,
    _duckdb_merge_upsert,
    _firebird_merge_upsert,
    _firebird_qualified_table,
    _firebird_quote,
    _generic_apply_sparse_upsert,
    _hana_merge_upsert,
    _informix_merge_upsert,
    _mssql_bracket,
    _mssql_merge_upsert,
    _mssql_qualified_table,
    _netezza_merge_upsert,
    _oracle_merge_upsert,
    _oracle_qualified_table,
    _oracle_quote,
    _sybase_merge_upsert,
    _teradata_merge_on,
    _teradata_merge_upsert,
    _trino_merge_upsert,
    _trino_qualified_table,
    _vertica_merge_upsert,
    clickhouse_final_table_sql,
)
from connectors.schema_drift import (
    _build_widen_ddl,
    add_missing_columns,
    is_wider_type,
    raise_widen_refusal,
)
from connectors.sql_temporal import (
    bind_time_clock,
    coerce_sql_temporal,
    extract_column_from_sql_error,
    is_sql_data_error,
    logical_to_temporal_ddl,
)
from connectors.write_resilience import (
    build_write_batch_key,
    ensure_sqlalchemy_write_ledger,
    mark_sqlalchemy_chunk_committed,
    sqlalchemy_chunk_rows_written,
)
from services import reflection_cache
from services.decision_kernel import (
    ddl_type,
    materialize_dest_ddl,
    normalize_logical_type,
)
from services.engine_pool import release_engine
from services.identity_carry import identity_seed_step
from services.type_system import parse_numeric_precision_scale
from services.value_serializer import cell_to_string, json_default

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.dialects import mssql, mysql, oracle, postgresql
    from sqlalchemy.exc import NoSuchModuleError

    SQLALCHEMY_AVAILABLE = True

    try:
        from clickhouse_sqlalchemy import engines as ch_engines
        from clickhouse_sqlalchemy.types import DateTime64 as ChDateTime64
        from clickhouse_sqlalchemy.types import Nullable as ChNullable
    except (ImportError, AttributeError):  # pragma: no cover
        ch_engines = None
        ChDateTime64 = None
        ChNullable = None

    try:
        from trino.sqlalchemy.datatype import TIMESTAMP as TrinoTimestamp
    except (ImportError, AttributeError):  # pragma: no cover
        TrinoTimestamp = None

    class _DialectNativeType(sa.types.UserDefinedType):
        """Compile to an exact dialect DDL token (SDO_GEOMETRY, INTERVAL, …)."""

        cache_ok = True

        def __init__(self, col_spec: str) -> None:
            self._col_spec = col_spec

        def get_col_spec(self, **_kw: Any) -> str:
            return self._col_spec

except (ImportError, AttributeError):  # pragma: no cover
    SQLALCHEMY_AVAILABLE = False
    mssql = None  # type: ignore[assignment]
    mysql = None  # type: ignore[assignment]
    oracle = None  # type: ignore[assignment]
    ch_engines = None
    ChDateTime64 = None
    ChNullable = None
    TrinoTimestamp = None
    _DialectNativeType = None  # type: ignore[misc, assignment]

from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _conflict_key_identity,
    _is_nullish_conflict_key,
    _rejected_row_count,
    assert_sparse_upsert_has_pk,
    compare_lsn,
    flush_normalized_child_batches,
    materialize_missing_as_null_for_dense_write,
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_booleans,
    quarantine_unfit_decimals,
    quarantine_unfit_enum_set,
    quarantine_unfit_integers,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
    quote_sql_identifier,
    reject_on_strict_policy,
    resolve_conflict_targets,
    resolve_target_columns,
    row_checksum,
    split_dense_sparse_rows,
    multi_row_insert_written,
    stamp_is_operator_ceiling,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "sqlalchemy"


# Catalog type -> SQLAlchemy drivername.  If a type is missing we attempt to
# use the catalog type as the drivername, which works for engines where the
# DBAPI/dialect package already installed a SQLAlchemy dialect.
_DRIVERNAME_MAP: dict[str, str] = {
    "mssql": "mssql+pyodbc",
    "sql_server": "mssql+pyodbc",
    "sqlserver": "mssql+pyodbc",
    "microsoft_sql_server": "mssql+pyodbc",
    "azure_sql_database": "mssql+pyodbc",
    "google_cloud_sql_sql_server": "mssql+pyodbc",
    "amazon_rds_sql_server": "mssql+pyodbc",
    "synapse_analytics": "mssql+pyodbc",
    "azure_synapse_dedicated": "mssql+pyodbc",
    "azure_synapse_serverless": "mssql+pyodbc",
    "oracle": "oracle+oracledb",
    "oracle_db": "oracle+oracledb",
    "oracle_autonomous_warehouse": "oracle+oracledb",
    "amazon_rds_oracle": "oracle+oracledb",
    "db2": "ibm_db_sa",
    "ibm_db2": "ibm_db_sa",
    "ibm_db2_warehouse": "ibm_db_sa",
    "sybase_ase": "sybase+pyodbc",
    "sap_ase": "sybase+pyodbc",
    "sap_iq": "sybase+pyodbc",
    "teradata": "teradatasql",
    "teradata_vantage": "teradatasql",
    "netezza": "nzpsql",
    "vertica": "vertica+vertica_python",
    "exasol": "exasol+pyodbc",
    "firebird": "firebird+fdb",
    "h2": "h2",
    "clickhouse": "clickhouse+native",
    "druid": "druid",
    "pinot": "pinot",
    "presto": "presto",
    "trino": "trino",
    "apache_hive": "hive",
    "apache_impala": "impala",
    "sparksql": "spark",
    "spark": "spark",
    "apache_spark": "spark",
    "phoenix": "phoenix",
    "sap_hana": "hana",
    "hana": "hana",
    "duckdb": "duckdb",
    "databricks": "databricks",
    "sqlite": "sqlite",
    # PostgreSQL-wire compatible engines
    "greenplum": "postgresql+psycopg2",
    "cratedb": "postgresql+psycopg2",
    "yugabytedb": "postgresql+psycopg2",
    "cockroachdb": "postgresql+psycopg2",
    "timescaledb": "postgresql+psycopg2",
    "alloydb": "postgresql+psycopg2",
    "supabase": "postgresql+psycopg2",
    "neon": "postgresql+psycopg2",
    "citus": "postgresql+psycopg2",
    "citusdb": "postgresql+psycopg2",
    "citus_db": "postgresql+psycopg2",
    "amazon_rds_postgresql": "postgresql+psycopg2",
    "google_cloud_sql_postgresql": "postgresql+psycopg2",
    "azure_database_for_postgresql": "postgresql+psycopg2",
    "questdb": "postgresql+psycopg2",
    # MySQL-wire compatible engines handled by generic SQL too if not routed to mysql
    "singlestore": "mysql+pymysql",
    "doris": "mysql+pymysql",
    "starrocks": "mysql+pymysql",
    "oceanbase": "mysql+pymysql",
    "tidb": "mysql+pymysql",
    "polardb": "mysql+pymysql",
    "gaussdb": "mysql+pymysql",
    "goldendb": "mysql+pymysql",
    "vitess": "mysql+pymysql",
    "planetscale": "mysql+pymysql",
    "amazon_rds_mysql": "mysql+pymysql",
    "google_cloud_sql_mysql": "mysql+pymysql",
    "azure_database_for_mysql": "mysql+pymysql",
    "amazon_aurora": "mysql+pymysql",
    "mariadb": "mysql+pymysql",
    # Additional SQL engines reached via generic SQL driver
    "dremio": "dremio+flight",
    "dremio_flight": "dremio+flight",
    "firebolt": "firebolt",
    "risingwave": "postgresql+psycopg2",
    "materialize": "postgresql+psycopg2",
    "yellowbrick": "postgresql+psycopg2",
    "actian_avalanche": "postgresql+psycopg2",
    "actian": "postgresql+psycopg2",
    "informix": "informix+pyodbc",
    "athena": "awsathena+rest",
    "amazon_athena": "awsathena+rest",
    "synapse": "mssql+pyodbc",
    "azure_synapse": "mssql+pyodbc",
    "amazon_emr": "hive",
    "cloudera_data_platform": "impala",
    "sap_bw_4hana": "hana",
    "motherduck": "duckdb",
    # First-class RDBMS — never fall back to bare dialect without a DBAPI.
    "mysql": "mysql+pymysql",
    "postgresql": "postgresql+psycopg2",
    "postgres": "postgresql+psycopg2",
    "redshift": "postgresql+psycopg2",
    "amazon_redshift": "postgresql+psycopg2",
    "snowflake": "snowflake",
}

_DEFAULT_PORT_MAP: dict[str, int] = {
    "mssql": 1433,
    "sql_server": 1433,
    "sqlserver": 1433,
    "microsoft_sql_server": 1433,
    "azure_sql_database": 1433,
    "google_cloud_sql_sql_server": 1433,
    "amazon_rds_sql_server": 1433,
    "synapse_analytics": 1433,
    "azure_synapse_dedicated": 1433,
    "azure_synapse_serverless": 1433,
    "oracle": 1521,
    "oracle_db": 1521,
    "oracle_autonomous_warehouse": 1521,
    "amazon_rds_oracle": 1521,
    "db2": 50000,
    "ibm_db2": 50000,
    "ibm_db2_warehouse": 50000,
    "sybase_ase": 5000,
    "sap_ase": 5000,
    "sap_iq": 2638,
    "teradata": 1025,
    "teradata_vantage": 1025,
    "netezza": 5480,
    "vertica": 5433,
    "exasol": 8563,
    "firebird": 3050,
    "h2": 9092,
    "clickhouse": 9000,  # native TCP port for clickhouse+native
    "druid": 8082,
    "pinot": 8099,
    "presto": 8080,
    "trino": 8080,
    "apache_hive": 10000,
    "apache_impala": 21000,
    "sparksql": 10000,
    "spark": 10000,
    "apache_spark": 10000,
    "phoenix": 8765,
    "sap_hana": 30015,
    "hana": 30015,
    "duckdb": 0,
    "databricks": 443,
    "sqlite": 0,
    "greenplum": 5432,
    "cratedb": 5432,
    "yugabytedb": 5433,
    "cockroachdb": 26257,
    "timescaledb": 5432,
    "alloydb": 5432,
    "supabase": 5432,
    "neon": 5432,
    "citus": 5432,
    "citusdb": 5432,
    "citus_db": 5432,
    "amazon_rds_postgresql": 5432,
    "google_cloud_sql_postgresql": 5432,
    "azure_database_for_postgresql": 5432,
    "questdb": 8812,
    "singlestore": 3306,
    "doris": 9030,
    "starrocks": 9030,
    "oceanbase": 2881,
    "tidb": 4000,
    "polardb": 3306,
    "gaussdb": 3306,
    "goldendb": 3306,
    "vitess": 3306,
    "planetscale": 3306,
    "amazon_rds_mysql": 3306,
    "google_cloud_sql_mysql": 3306,
    "azure_database_for_mysql": 3306,
    "amazon_aurora": 3306,
    "mariadb": 3306,
    "mysql": 3306,
    "postgresql": 5432,
    "postgres": 5432,
    "redshift": 5439,
    "amazon_redshift": 5439,
    "dremio": 32010,
    "dremio_flight": 32010,
    "firebolt": 443,
    "risingwave": 4566,
    "materialize": 6875,
    "yellowbrick": 5432,
    "actian_avalanche": 5432,
    "actian": 5432,
    "informix": 9088,
    "athena": 443,
    "amazon_athena": 443,
    "synapse": 1433,
    "azure_synapse": 1433,
    "amazon_emr": 10000,
    "cloudera_data_platform": 21000,
    "sap_bw_4hana": 30015,
    "motherduck": 0,
}


def _drivername(db_type: str) -> str:
    return _DRIVERNAME_MAP.get(db_type, db_type)


def _mssql_odbc_driver() -> str | None:
    """Return an installed Microsoft ODBC driver name, or None.

    Importing ``pyodbc`` is not enough — the unixODBC driver manager still
    fails when ``libmsodbcsql-17.so`` is absent. Probe ``pyodbc.drivers()``.
    """
    try:
        import pyodbc
    except Exception:
        return None
    try:
        installed = {str(d) for d in (pyodbc.drivers() or [])}
    except Exception:
        return None
    for name in (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ):
        if name in installed:
            return name
    return None


def _mssql_drivername() -> str:
    """Pick an installed SQL Server DBAPI: pyodbc+driver preferred, pymssql fallback."""
    if _mssql_odbc_driver():
        return "mssql+pyodbc"
    try:
        import pymssql  # noqa: F401
        return "mssql+pymssql"
    except Exception:
        pass
    return "mssql+pyodbc"


def adapt_mssql_sql(sql: str) -> str:
    """Rewrite ``%s`` placeholders for the live SQL Server DBAPI.

    Native CDC / CT SQL is written pymssql-style (``%s``). pyodbc is qmark
    (``?``). Importing pyodbc without adapting SQL used to fail closed as
    ``0 parameter markers`` and hide a live CDC capture.
    """
    if not isinstance(sql, str) or "%s" not in sql:
        return sql
    if _mssql_drivername() != "mssql+pyodbc":
        return sql
    return sql.replace("%s", "?")


class _MssqlQmarkCursor:
    def __init__(self, cur: Any):
        self._cur = cur

    def execute(self, sql, *args, **kwargs):
        return self._cur.execute(adapt_mssql_sql(sql), *args, **kwargs)

    def executemany(self, sql, *args, **kwargs):
        return self._cur.executemany(adapt_mssql_sql(sql), *args, **kwargs)

    def __enter__(self):
        entered = self._cur.__enter__() if hasattr(self._cur, "__enter__") else self._cur
        if entered is self._cur:
            return self
        return _MssqlQmarkCursor(entered)

    def __exit__(self, *exc):
        if hasattr(self._cur, "__exit__"):
            return self._cur.__exit__(*exc)
        return False

    def __getattr__(self, name):
        return getattr(self._cur, name)


class _MssqlQmarkConnection:
    """DBAPI connection whose cursors adapt ``%s`` → ``?`` for pyodbc."""

    def __init__(self, conn: Any):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        return _MssqlQmarkCursor(self._conn.cursor(*args, **kwargs))

    def __enter__(self):
        entered = self._conn.__enter__() if hasattr(self._conn, "__enter__") else self._conn
        if entered is self._conn:
            return self
        return _MssqlQmarkConnection(entered)

    def __exit__(self, *exc):
        if hasattr(self._conn, "__exit__"):
            return self._conn.__exit__(*exc)
        return False

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _default_port(db_type: str) -> int:
    return _DEFAULT_PORT_MAP.get(db_type, 0)


def _normalize_sqlite_url(url: str) -> str:
    """Ensure absolute SQLite file paths use the SQLAlchemy-correct slash count.

    SQLAlchemy rules:
    * ``sqlite:///relative.db`` — relative path
    * ``sqlite:////absolute/path.db`` — Unix absolute (four slashes)
    * ``sqlite:///C:/windows/path.db`` — Windows drive absolute (three slashes)

    Users often paste ``sqlite:///C:/…`` or ``sqlite:////var/…``; only Unix
    absolute paths missing the fourth slash are rewritten. Windows drive
    letters must keep three slashes — adding a fourth makes sqlite3 fail with
    ``unable to open database file``.
    """
    if url.startswith("sqlite:///") and not url.startswith("sqlite:////"):
        path = url[len("sqlite:///") :]
        # Windows drive letter — already absolute with three slashes.
        if len(path) >= 2 and path[1] == ":":
            return url
        # Unix absolute path written with three slashes → four.
        if path.startswith("/"):
            return f"sqlite:////{path}"
    return url


def _normalize_sqlalchemy_url_string(url: str, db_type: str = "") -> str:
    """Rewrite bare dialect schemes to installed DBAPI dialects.

    Users and saved connectors often store ``mysql://…`` / ``postgresql://…``.
    SQLAlchemy 2 requires an explicit DBAPI (``mysql+pymysql``, ``postgresql+psycopg2``).
    Only the scheme prefix is rewritten — userinfo/password material is preserved.
    """
    raw = (url or "").strip()
    if not raw:
        return raw
    lower = raw.lower()

    # Longest-first so postgresql+psycopg2 is not mistreated as postgresql://.
    replacements: list[tuple[str, str]] = [
        ("mysql+pymysql://", "mysql+pymysql://"),
        ("mariadb+pymysql://", "mysql+pymysql://"),
        ("mysql://", "mysql+pymysql://"),
        ("mariadb://", "mysql+pymysql://"),
        ("postgresql+psycopg2://", "postgresql+psycopg2://"),
        ("postgresql://", "postgresql+psycopg2://"),
        ("postgres://", "postgresql+psycopg2://"),
        ("pgsql://", "postgresql+psycopg2://"),
        ("redshift://", "postgresql+psycopg2://"),
    ]
    for src, dst in replacements:
        if not lower.startswith(src):
            continue
        if src == dst:
            return raw
        return dst + raw[len(src) :]
    return raw


def _build_url(cfg: dict[str, Any]) -> str | sa.URL:
    """Build a SQLAlchemy URL from host/port or use the explicit connection string."""
    connection_string = cfg.get("connection_string") or ""
    db_type = (cfg.get("type") or "").lower().strip()

    if connection_string:
        if connection_string.startswith(("duckdb:", "sqlite:")):
            if connection_string.startswith("sqlite:"):
                return _normalize_sqlite_url(connection_string)
            return connection_string
        if db_type == "duckdb":
            return (
                f"duckdb:////{connection_string}"
                if connection_string.startswith("/")
                else f"duckdb:///{connection_string}"
            )
        if db_type == "sqlite":
            return _normalize_sqlite_url(f"sqlite:///{connection_string}")
        from connectors.sql_dsn import sync_credentials_into_connection_string
        from connectors.url_authority import parse_url_authority, rebuild_url

        sync_credentials_into_connection_string(cfg)
        raw = (cfg.get("connection_string") or connection_string).strip()
        parsed = parse_url_authority(raw)
        if parsed.host:
            form_user = str(cfg.get("username") or "").strip()
            form_password = str(cfg.get("password") or "")
            user = form_user or parsed.user
            password = form_password if form_password.strip() else parsed.password
            raw = rebuild_url(parsed, user=user, password=password)
        return _normalize_sqlalchemy_url_string(raw, db_type)

    if not db_type:
        raise ValueError("A database type or connection_string is required")

    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError(
            "SQLAlchemy is not installed in this API environment. "
            "Install sqlalchemy and the DBAPI for your engine (e.g. pymysql for MySQL)."
        )

    # MotherDuck is DuckDB cloud: the database token/DB is addressed as md:<database>.
    if db_type == "motherduck":
        database = (cfg.get("database") or "").strip() or "my_db"
        if not database.startswith("md:"):
            database = f"md:{database}"
        return f"duckdb:///{database}"

    drivername = _drivername(db_type)

    if drivername == "sqlite":
        database = cfg.get("database") or ""
        return f"sqlite:///{database or ':memory:'}"

    if drivername == "duckdb":
        database = cfg.get("database") or ""
        return (
            f"duckdb:////{database}"
            if database.startswith("/")
            else f"duckdb:///{database or ':memory:'}"
        )

    if drivername in ("presto", "trino"):
        # Trino/Presto URLs require catalog/schema in the path.
        schema = _schema_name(cfg) or ""
        database = cfg.get("database") or "default"
        host = cfg.get("host") or "localhost"
        port = int(cfg.get("port") or 0) or _default_port(db_type)
        user = cfg.get("username") or ""
        auth = f"{user}@" if user else ""
        path = f"/{database}/{schema}" if schema else f"/{database}"
        return f"{drivername}://{auth}{host}:{port}{path}"

    port = int(cfg.get("port") or 0)
    if not port:
        port = _default_port(db_type)

    query: dict[str, str] | None = None
    if drivername.startswith("mssql"):
        drivername = _mssql_drivername()
        query = {}
        if drivername == "mssql+pyodbc":
            query["driver"] = _mssql_odbc_driver() or "ODBC Driver 17 for SQL Server"
        # Always On listener: MultiSubnetFailover speeds AG failover reconnect.
        multi = cfg.get("multi_subnet_failover")
        if multi is None:
            multi = cfg.get("MultiSubnetFailover")
        if multi in (True, 1, "1", "true", "True", "yes", "Yes", "YES"):
            query["MultiSubnetFailover"] = "Yes"
        intent = str(
            cfg.get("application_intent") or cfg.get("ApplicationIntent") or ""
        ).strip()
        if intent:
            # ReadOnly routes to a readable secondary when the AG allows it.
            query["ApplicationIntent"] = intent
        if not query:
            query = None

    database = cfg.get("database") or None
    if drivername.startswith("oracle"):
        # ``oracle+oracledb://u:p@host:port/NAME`` builds a *SID* DSN, and SIDs
        # are legacy: every pluggable database, RAC service and Autonomous
        # instance is reached by service name, so the connect fails with
        # ORA-12505 / DPY-6003 on anything newer than a bare single instance.
        # Honour an explicit ``sid`` when the operator sets one; otherwise the
        # database name is a service name.
        sid = str(cfg.get("sid") or "").strip()
        service = str(cfg.get("service_name") or "").strip() or (database or "")
        if sid:
            database = sid
        elif service:
            database = None
            query = {**(query or {}), "service_name": service}

    return sa.URL.create(
        drivername,
        username=cfg.get("username") or None,
        password=cfg.get("password") or None,
        host=cfg.get("host") or "localhost",
        port=port if port else None,
        database=database,
        query=query,
    )


def _engine(cfg: dict[str, Any]) -> Any:
    """Engine for ``cfg``, reused across calls.

    An Engine owns a connection pool and is designed to be long-lived and
    thread-safe. This used to build a new one on every read chunk, write chunk
    and checksum re-read — roughly ``3N`` pools per N-chunk transfer, each used
    for exactly one connection. :mod:`services.engine_pool` keeps one per
    distinct connection target so the pool can actually pool, and so
    SQLAlchemy's reflection cache stays warm between chunks.

    Callers must pair this with ``release_engine`` rather than ``dispose()``:
    disposing a shared engine would tear the pool out from under every other
    chunk in flight.
    """
    from services.engine_pool import get_pooled_engine

    return get_pooled_engine(cfg, _build_engine)


def _build_engine(cfg: dict[str, Any]) -> Any:
    """Construct a brand-new Engine. Called once per distinct target."""
    url = _build_url(cfg)
    # Fast, safe defaults for local and network databases.
    db_type = (cfg.get("type") or "").lower()
    connection_string = (cfg.get("connection_string") or "").lower()
    # DuckDB and SQLite are file-based; use NullPool so the file lock is released
    # after each operation and external readers can open the database.
    try:
        if (
            db_type in ("duckdb", "sqlite")
            or "duckdb" in connection_string
            or "sqlite://" in connection_string
        ):
            from sqlalchemy.pool import NullPool

            engine = create_engine(url, poolclass=NullPool)
            # DuckDB's SQLAlchemy dialect reports supports_native_decimal=False by
            # default, which silently rounds Decimal binds through float and corrupts
            # money/numeric fidelity.  Decimal is native in DuckDB, so enable it.
            if db_type == "duckdb" or "duckdb" in connection_string:
                engine.dialect.supports_native_decimal = True
            # SQLite has no Decimal affinity. Register dest-canonical text bind so
            # SQLAlchemy/sqlite3 accept Python Decimal (apply_transform decimal
            # wire) instead of ProgrammingError or IEEE float invent.
            if db_type == "sqlite" or "sqlite://" in connection_string:
                from connectors.sqlite_common import register_sqlite_decimal_adapter

                register_sqlite_decimal_adapter()
            return engine
        from services.engine_pool import pool_settings

        engine = create_engine(url, pool_pre_ping=True, **pool_settings())
        from sqlalchemy import event

        from services.dest_dialect_facts import _normalize_dest_db

        driver = str(getattr(url, "drivername", "")).lower()
        # SQL Server: refuse silent VARCHAR truncation at the session level.
        if (
            db_type in {
                "mssql",
                "sql_server",
                "sqlserver",
                "microsoft_sql_server",
                "azure_sql_database",
                "amazon_rds_sql_server",
                "google_cloud_sql_sql_server",
                "synapse_analytics",
                "azure_synapse_dedicated",
                "azure_synapse_serverless",
            }
            or "mssql" in driver
        ):
            from connectors.write_resilience import apply_mssql_session_guards

            @event.listens_for(engine, "connect")
            def _mssql_fail_closed_session(dbapi_conn, _connection_record):  # noqa: ANN001
                apply_mssql_session_guards(dbapi_conn)

        # MySQL TIMESTAMP is converted with session time_zone on read and write.
        # Pin every pooled connection (source and dest) so the wire is the UTC
        # instant — the AWS DMS failure is inheriting the server zone.
        if (
            _normalize_dest_db(db_type) == "mysql"
            or "mysql" in driver
            or "mariadb" in driver
        ):
            from services.timezone_policy import pin_mysql_session_utc

            @event.listens_for(engine, "connect")
            def _mysql_utc_session(dbapi_conn, _connection_record):  # noqa: ANN001
                pin_mysql_session_utc(dbapi_conn)

        return engine
    except (NoSuchModuleError, ImportError) as exc:
        # SQLAlchemy raises NoSuchModuleError when the dialect is not installed.
        # Convert it to a clear RuntimeError so callers can surface a 4xx/5xx
        # response instead of an unhandled ExceptionGroup crashing the worker.
        driver = getattr(url, "drivername", None) or str(url).split("://", 1)[0]
        dialect_key = str(db_type or str(driver).split("+", 1)[0]).lower()
        driver_s = str(driver).lower()
        hint_by_dialect = {
            "mysql": "pymysql (scheme mysql+pymysql://)",
            "mariadb": "pymysql (scheme mysql+pymysql://)",
            "postgresql": "psycopg2-binary (scheme postgresql+psycopg2://)",
            "postgres": "psycopg2-binary (scheme postgresql+psycopg2://)",
            "redshift": "psycopg2-binary (scheme postgresql+psycopg2://)",
            "snowflake": "snowflake-sqlalchemy",
            "databricks": "databricks-sqlalchemy",
        }
        hint = hint_by_dialect.get(dialect_key)
        if not hint:
            if "mysql" in driver_s or "mariadb" in driver_s:
                hint = "pymysql (scheme mysql+pymysql://)"
            elif "postgres" in driver_s or "redshift" in driver_s:
                hint = "psycopg2-binary (scheme postgresql+psycopg2://)"
        detail = (
            f"SQLAlchemy dialect/driver for '{db_type or driver}' is not available "
            f"(tried '{driver}')."
        )
        if hint:
            detail += f" Install/enable {hint}."
        else:
            detail += (
                " Install the matching driver package "
                "(e.g. pymysql, psycopg2-binary, snowflake-sqlalchemy, databricks-sqlalchemy)."
            )
        raise RuntimeError(detail) from exc


def get_sqlalchemy_engine(cfg: dict[str, Any]) -> Any:
    """Public accessor for a configured SQLAlchemy engine."""
    return _engine(cfg)


def get_connection(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    db_type: str = "",
    **kwargs: Any,
) -> Any:
    """Return a raw DBAPI connection for the configured SQL engine.

    The context manager yields a DBAPI connection with a ``.cursor()`` method so
    callers (e.g. CDC readers) can execute raw SQL and parameterised queries.
    """
    cfg = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
        "type": db_type or kwargs.get("type") or "",
    }
    conn = _engine(cfg).raw_connection()
    db_type = (cfg.get("type") or "").lower()
    if db_type in {
        "sqlserver",
        "mssql",
        "microsoft_sql_server",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "google_cloud_sql_sql_server",
    }:
        return _MssqlQmarkConnection(conn)
    return conn


def _schema_name(cfg: dict[str, Any]) -> str | None:
    from services.dialect_profiles import normalize_schema

    db_type = (cfg.get("type") or "").lower()
    connection_string = (cfg.get("connection_string") or "").lower()
    # Infer dialect from URL when type is generic_sql / blank.
    if not db_type or db_type == "generic_sql":
        if connection_string.startswith("mysql") or "mariadb" in connection_string:
            db_type = "mysql"
        elif connection_string.startswith(("postgresql", "postgres")):
            db_type = "postgresql"
        elif "sqlserver" in connection_string or "mssql" in connection_string:
            db_type = "sqlserver"
        elif connection_string.startswith("oracle"):
            db_type = "oracle"
        elif connection_string.startswith("sqlite"):
            db_type = "sqlite"
        elif connection_string.startswith("duckdb"):
            db_type = "duckdb"
    return normalize_schema(db_type, cfg.get("schema"), username=cfg.get("username"))


def get_sql_schema(cfg: dict[str, Any]) -> str | None:
    """Public accessor for the SQL schema name implied by a connector config."""
    return _schema_name(cfg)


def _dialect_key(cfg: dict[str, Any]) -> str:
    db_type = (cfg.get("type") or "").lower()
    if db_type and db_type != "generic_sql":
        return db_type
    connection_string = (cfg.get("connection_string") or "").lower()
    if connection_string.startswith("mysql") or "mariadb" in connection_string:
        return "mysql"
    if connection_string.startswith(("postgresql", "postgres")):
        return "postgresql"
    if "sqlserver" in connection_string or "mssql" in connection_string:
        return "sqlserver"
    if connection_string.startswith("oracle"):
        return "oracle"
    if connection_string.startswith("sqlite"):
        return "sqlite"
    if connection_string.startswith("duckdb"):
        return "duckdb"
    return db_type or "ansi"


def _qualified_table_ref(cfg: dict[str, Any], table: str, schema: str | None) -> str:
    """Dialect-aware schema.table quoting (brackets, backticks, fold)."""
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(table, schema, dialect=_dialect_key(cfg))


def _type_repr(type_obj: Any) -> str:
    try:
        return str(type_obj).lower()
    except (TypeError, ValueError):
        return ""


def _logical_type_from_sa(col_type: Any) -> str:
    """Map a SQLAlchemy type instance to a Datawrap logical type."""
    from services.type_system import normalize_logical_type

    if col_type is None:
        return "string"

    repr_ = _type_repr(col_type)

    # Direct dialect UUID types
    if "uuid" in repr_:
        return "uuid"
    if isinstance(col_type, (sa.UUID,)):
        return "uuid"
    if isinstance(col_type, postgresql.UUID):
        return "uuid"

    if isinstance(col_type, (sa.ARRAY,)):
        return "array"

    if isinstance(col_type, (sa.JSON,)):
        return "json"

    if isinstance(col_type, (sa.LargeBinary, sa.BINARY)):
        return "binary"

    if isinstance(col_type, (sa.Boolean,)):
        return "boolean"

    # MySQL-style TINYINT(1) is conventionally boolean.
    if "tinyint" in repr_ and getattr(col_type, "display_width", 0) == 1:
        return "boolean"

    # IEEE floats must stay FLOAT — never collapse into DECIMAL/NUMBER.
    if isinstance(col_type, (sa.Float, sa.Double, sa.REAL)):
        return "float"
    if any(
        tok in repr_
        for tok in ("float", "double", "real", "binary_float", "binary_double")
    ) and "decimal" not in repr_ and "numeric" not in repr_ and "number" not in repr_:
        return "float"

    # Fixed-point before integer: ``oracle.NUMBER`` subclasses *both* Numeric and
    # Integer, so an Integer-first check read NUMBER(12,2) money as ``integer``
    # and dropped the scale — every Oracle source column came back integral and
    # the create-new target then collapsed to TEXT.
    if isinstance(col_type, (sa.Numeric,)):
        from services.type_system import (
            zero_scale_fits_signed_bigint,
            zero_scale_numeric_carrier,
        )

        precision = getattr(col_type, "precision", None)
        scale = getattr(col_type, "scale", None)
        if precision is not None and scale is not None:
            if int(scale) == 0:
                # Wide NUMERIC(38,0) must stay DECIMAL — never signed BIGINT overflow.
                if zero_scale_fits_signed_bigint(int(precision)):
                    return "integer"
                return zero_scale_numeric_carrier(int(precision))
            return f"DECIMAL({int(precision)},{int(scale)})"
        if precision is not None:
            return f"DECIMAL({int(precision)})"
        if isinstance(col_type, (sa.Integer,)):
            # Unconstrained NUMBER — integral wire, no declared scale to keep.
            return "integer"
        return "decimal"

    if isinstance(col_type, (sa.Integer, sa.BigInteger, sa.SmallInteger)):
        return "integer"

    if isinstance(col_type, (sa.DateTime,)):
        # Preserve TIMESTAMPTZ vs NTZ — collapsing both to "datetime" loses TZ polarity
        # on generic Postgres/Trino/warehouse reflection (Airbyte-class honesty gap).
        tz = getattr(col_type, "timezone", None)
        if tz is True:
            return "timestamptz"
        if tz is False:
            return "timestamp_ntz"
        return "datetime"

    if isinstance(col_type, (sa.Date,)):
        return "date"

    if isinstance(col_type, (sa.Time,)):
        return "time"

    if isinstance(col_type, (sa.String, sa.Text, sa.CHAR)):
        # Preserve VARCHAR(n)/CHAR(n)/NVARCHAR(n) — bare "string" invents
        # unbounded capacity over live typmod on existing-table overlay.
        length = getattr(col_type, "length", None)
        try:
            n = int(length) if length is not None else None
        except (TypeError, ValueError):
            n = None
        type_name = getattr(getattr(col_type, "__class__", None), "__name__", "").lower()
        module = getattr(getattr(col_type, "__class__", None), "__module__", "").lower()
        national = "nvarchar" in type_name or "nchar" in type_name or (
            "mssql" in module and "national" in repr(col_type).lower()
        )
        if isinstance(col_type, sa.Text) and n is None:
            return "TEXT"
        if n is not None and n > 0:
            if national:
                prefix = "NCHAR" if "char" in type_name and "var" not in type_name else "NVARCHAR"
                return f"{prefix}({n})"
            if isinstance(col_type, sa.CHAR) or (
                "char" in type_name and "var" not in type_name
            ):
                return f"CHAR({n})"
            return f"VARCHAR({n})"
        return "string"

    # Fallback text matching for dialect-specific types not captured above
    # SQL Server specifics BEFORE broad timestamp/datetime matching.
    if "datetimeoffset" in repr_:
        return "timestamptz"
    type_name = getattr(getattr(col_type, "__class__", None), "__name__", "").lower()
    module = getattr(getattr(col_type, "__class__", None), "__module__", "").lower()
    # Specialty BEFORE broad "variant" → JSON (sql_variant must not invent JSON).
    if "hierarchyid" in repr_ or type_name == "hierarchyid":
        return "HIERARCHYID"
    if "sql_variant" in repr_ or type_name in {"sql_variant", "sqlvariant"}:
        return "SQL_VARIANT"
    if type_name == "xml" or (repr_ == "xml" or repr_.endswith(".xml")):
        return "XML"
    if "geography" in repr_ or type_name == "geography":
        return "GEOGRAPHY"
    if "geometry" in repr_ or type_name == "geometry":
        return "GEOMETRY"
    if "rowversion" in repr_ or (
        "mssql" in module and type_name in {"timestamp", "rowversion"}
    ):
        # SQL Server TIMESTAMP is rowversion (binary), not a datetime.
        return "ROWVERSION"
    if "json" in repr_ or "super" in repr_:
        return "json"
    # Snowflake/Databricks VARIANT (not SQL Server sql_variant — handled above).
    if "variant" in repr_ and "sql_variant" not in repr_:
        return "json"
    if "array" in repr_:
        return "array"
    if "uuid" in repr_ or "guid" in repr_ or "uniqueidentifier" in repr_:
        return "uuid"
    if any(
        x in repr_
        for x in ("binary", "blob", "bytea", "varbinary", "image", "raw", "rowversion")
    ):
        return "binary"
    # FLOAT before DECIMAL — "float" must not fall into the numeric/decimal bucket.
    if any(
        x in repr_ for x in ("binary_float", "binary_double", "float", "double", "real")
    ) and not any(x in repr_ for x in ("numeric", "decimal", "number(", "money")):
        return "float"
    if any(x in repr_ for x in ("numeric", "decimal", "number", "money", "smallmoney")):
        return "decimal"
    if any(x in repr_ for x in ("int", "serial", "smallint", "tinyint", "bigint")):
        return "integer"
    if "bool" in repr_ or "bit" in repr_:
        return "boolean"
    if "datetimeoffset" in repr_ or "timestamptz" in repr_ or "with time zone" in repr_:
        return "timestamptz"
    if "without time zone" in repr_ or "timestamp_ntz" in repr_:
        return "timestamp_ntz"
    # ClickHouse DateTime64 / IPv4 / Tuple — preserve carriers (not bare string).
    if any(
        tok in repr_
        for tok in ("datetime64", "ipv4", "ipv6", "tuple(", "array(", "map(")
    ) or (
        "clickhouse" in module
        and any(tok in repr_ for tok in ("datetime", "date", "uuid", "decimal"))
    ):
        from services.schema_introspect import _ch_to_logical

        try:
            original = str(col_type)
        except (TypeError, ValueError):
            original = repr_
        return _ch_to_logical(original)
    if "datetime" in repr_ or "timestamp" in repr_:
        return "datetime"
    if "date" in repr_:
        return "date"
    if "time" in repr_:
        return "time"
    if any(x in repr_ for x in ("char", "varchar", "text", "clob", "string")):
        return "string"

    return normalize_logical_type(repr_)


class _ExactJSON(sa.JSON):
    """JSON type that stores canonical JSON text (no stdlib re-parse).

    SQLAlchemy's default ``sa.JSON`` re-parses with ``json.loads`` (IEEE invent)
    and binds Python ``None`` as the JSON literal ``null``. This subclass uses
    ``json_document_wire``: valid JSON text keeps its digits and polarity
    (``\"1\"`` stays a string). Python trees dump compact. ``None`` is SQL NULL.
    Used for DuckDB and every leftover generic-SQL JSON DDL site that would
    otherwise emit bare ``sa.JSON()`` (MySQL / SQLite / MSSQL / …).
    """

    __visit_name__ = "JSON"

    def _gen_dialect_impl(self, dialect: Any) -> Any:
        """Keep these processors — a dialect JSON impl re-serializes the text.

        SQLAlchemy swaps ``sa.JSON`` for the dialect's own JSON type via
        ``colspecs`` (``sqlite.JSON``, ``mysql.JSON``, …), and that impl dumps
        the canonical text we already produced, so every JSON column landed as
        a quoted JSON *string*: ``"{\\"tier\\":\\"gold\\"}"``. DDL still
        compiles through ``visit_JSON`` on the dialect, so the column type is
        unchanged.
        """
        return self

    def bind_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            from services.value_serializer import (
                absent_sql_bind,
                is_missing_sentinel,
            )

            # Missing stays a raise in json_document_wire (omit upstream).
            # Reader-null is SQL/JSON NULL — never the extract token as text.
            if not is_missing_sentinel(value):
                handled, bound = absent_sql_bind(value)
                if handled:
                    return bound
            from services.json_polarity import json_document_wire

            return json_document_wire(value)

        return process

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Callable[[Any], Any] | None:
        # Drivers that still return JSON as text stay text. A second
        # ``json.loads`` here would IEEE-collapse long fractions before
        # ``json_document_wire`` could keep the engine spelling.
        return lambda value: value


# Backward name — DuckDB was the first engine on this wire.
_DuckDBJSON = _ExactJSON


#: Dialects whose catalogs fold unquoted identifiers to a single case.
_FOLDING_DIALECTS: Final = {"oracle", "snowflake", "db2", "ibm_db_sa"}

_ORACLE_WIRES: Final = {"oracle", "oracledb", "oracle_autonomous", "oracle_adw", "oracle_atp"}


_MSSQL_WIRES: Final = {"sqlserver", "mssql", "azure_sql", "synapse", "azure_synapse"}
_MYSQL_WIRES: Final = {"mysql", "mariadb", "tidb", "singlestore"}


def physical_table_spelling(
    cfg: dict[str, Any], table: str, schema: str | None = None
) -> str:
    """Spelling the catalog stores for ``table``, or ``""`` when it is absent.

    Callers that are about to drop and recreate a destination need the spelling
    *before* the drop: afterwards the table is gone and the recreate can only
    guess the engine's folding convention.
    """
    if not SQLALCHEMY_AVAILABLE or not table:
        return ""
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    try:
        ident = resolve_object_identity(engine, table, schema or _schema_name(cfg))
        return ident.table if (ident.resolved and ident.exists) else ""
    except Exception as exc:  # noqa: BLE001 — unreadable catalog: caller decides
        logger.debug("physical spelling probe failed for %s: %s", table, exc)
        return ""
    finally:
        release_engine(engine)


def _resolve_physical_table_ident(
    engine: Any, table: str, schema: str | None, prior_spelling: str = ""
) -> tuple[str, str | None]:
    """Physical spelling of a destination on case-folding engines.

    Oracle/Snowflake/DB2 fold unquoted identifiers to upper case, but every
    CREATE here is emitted quoted, so a lower-case name typed in Studio landed a
    lower-case-quoted table that read-back (which folds, like every other client)
    could not see: ``ORA-00942`` on a table the write had just reported written.
    An existing table keeps its own spelling; only a table that does not exist
    yet is created under the folded name the catalog will hand back.
    """
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "").lower()
    if dialect not in _FOLDING_DIALECTS:
        return table, schema
    from services.dialect_profiles import fold_identifier
    from services.sql_object_identity import resolve_object_identity

    folded = fold_identifier(dialect, table)
    folded_schema = fold_identifier(dialect, schema) if schema else schema
    if folded == table and folded_schema == schema:
        return table, schema
    # ``has_table`` folds like every other client, so a quoted lower-case table
    # read as absent and the write created a second, folded one beside it.
    ident = resolve_object_identity(engine, table, schema)
    if not ident.resolved:
        # Catalog unreadable: keep the operator's spelling rather than guess.
        return table, schema
    if ident.exists:
        return ident.table, ident.schema
    if prior_spelling and prior_spelling.casefold() == table.casefold():
        # full_refresh just dropped this table: recreating it folded would move
        # the destination to a different identifier than the one the operator
        # (and everything reading it) points at.
        return prior_spelling, schema
    return folded, folded_schema


def _resolve_physical_column_idents(
    engine: Any, table: str, schema: str | None, columns: list[str]
) -> dict[str, str]:
    """Stored spelling of each destination column on case-folding engines.

    Every statement here quotes its identifiers, so on Oracle/Snowflake/DB2 a
    mapped ``label`` is asked for as ``"label"`` — a *different* column from the
    ``LABEL`` an ordinary ``CREATE TABLE`` produced. Appending into a table the
    client created therefore failed with ORA-00904 on a column that is plainly
    there, and drift widening emitted ``MODIFY ("name" CLOB)`` against ``NAME``.

    Only columns whose stored spelling actually differs are returned; a column
    the catalog does not have is left alone so ADD COLUMN still creates it.
    """
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "").lower()
    if dialect not in _FOLDING_DIALECTS or not columns:
        return {}
    stored = _stored_column_spellings(engine, engine, table, schema)
    if not stored:
        return {}
    from services.dialect_profiles import fold_identifier

    # A column drift is about to add must follow the convention of the table it
    # joins: adding a quoted lower-case column beside folded ones leaves a
    # column the client's own SELECT cannot see.
    folded_table = all(name == name.upper() for name in stored.values())
    renames: dict[str, str] = {}
    for col in columns:
        hit = stored.get(str(col).casefold())
        if hit is None and folded_table:
            hit = fold_identifier(dialect, col)
        if hit and hit != col:
            renames[col] = hit
    return renames


def _stored_column_spellings(
    engine: Any, bind: Any, table: str, schema: str | None
) -> dict[str, str]:
    """``{folded name: spelling the catalog stores}`` for one table.

    Reflection hands back *normalised* names (Oracle's ``LABEL`` arrives as
    ``label``), so comparing or quoting them directly addresses a column that
    does not exist. ``denormalize_name`` is the dialect's own inverse and keeps
    a deliberately quoted lower-case column quoted.
    """
    from sqlalchemy.sql import quoted_name

    inspector = inspect(bind)
    try:
        cols = inspector.get_columns(table, schema=schema)
    except Exception as exc:  # noqa: BLE001 — unreadable catalog: keep Map names
        logger.debug("column spelling probe failed for %s: %s", table, exc)
        cols = []
    if not cols:
        # The probe folds the *table* name too, so a deliberately quoted
        # lower-case table (``"scn_dst"``) read as absent and every column kept
        # its Map spelling: appending into it asked Oracle for ``"email"`` beside
        # the stored ``EMAIL`` and failed with ORA-00904 on a column plainly there.
        try:
            cols = inspector.get_columns(quoted_name(table, True), schema=schema)
        except Exception as exc:  # noqa: BLE001 — unreadable catalog: keep Map names
            logger.debug("quoted column spelling probe failed for %s: %s", table, exc)
            return {}
    denormalize = getattr(
        getattr(engine, "dialect", None), "denormalize_name", lambda n: n
    )
    out: dict[str, str] = {}
    for col in cols:
        name = col.get("name")
        if not name:
            continue
        out[str(name).casefold()] = str(denormalize(name) or name)
    return out


def _is_oracle_wire(dialect_name: str, db_type: str) -> bool:
    """True when DDL compiles through the Oracle dialect."""
    if oracle is None:
        return False
    return (dialect_name or "").lower() == "oracle" or (db_type or "").lower() in _ORACLE_WIRES


def _sub_second_naive_wire(dialect_name: str, db_type: str) -> Any:
    """Naive-datetime carrier that keeps sub-second precision, else ``None``.

    ``sa.DateTime()`` compiles to Oracle ``DATE`` (whole seconds, no fraction),
    SQL Server ``DATETIME`` (rounded to 1/300 s) and MySQL ``DATETIME``
    (fsp 0 — the fraction is dropped), so a PostgreSQL microsecond stamp lands
    altered and a row checksum can only match by luck. On MySQL that also
    collapsed SCD2 version boundaries: two versions written in the same second
    got one instant, so ``valid_from == valid_to`` and no as-of query could see
    the closed version. ``None`` means the dialect's own default already
    carries fractions.
    """
    if _is_oracle_wire(dialect_name, db_type):
        return oracle.TIMESTAMP()
    if mssql is not None and (
        (dialect_name or "").lower() == "mssql" or (db_type or "").lower() in _MSSQL_WIRES
    ):
        return mssql.DATETIME2()
    if mysql is not None and _MYSQL_WIRES & {
        (dialect_name or "").lower(), (db_type or "").lower()
    }:
        return mysql.DATETIME(fsp=6)
    return None


def _sa_type_for_logical(
    logical: str,
    dialect_name: str,
    db_type: str = "",
    *,
    nullable: bool = True,
) -> Any:
    """Map a Datawrap logical type to a SQLAlchemy type that compiles for the engine.

    Accepts carriers like ``DECIMAL(12,4)`` / ``NUMERIC(38,10)`` — bare
    ``t == "decimal"`` matching used to fall through to TEXT and strip scale
    (SQL Server / Oracle / DuckDB greenfield fidelity bug).
    """
    from services.decision_kernel import ddl_type, normalize_logical_type
    from services.type_system import (
        LOGICAL_ARRAY,
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_GEOGRAPHY,
        LOGICAL_INTEGER,
        LOGICAL_INTERVAL,
        LOGICAL_JSON,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_TIME,
        LOGICAL_UUID,
        parse_numeric_precision_scale,
    )

    raw = (logical or "string").strip()
    raw_lower = raw.lower()
    t = normalize_logical_type(raw)

    def _maybe_nullable(sa_type: Any) -> Any:
        # ClickHouse: only Nullable(T) when the column is nullable. PK / ORDER BY
        # identity cols must stay bare Int64/DateTime — wrapping everything made
        # empty→NULL legal at DDL and weakened ReplacingMergeTree contracts.
        if dialect_name == "clickhouse" and ChNullable is not None and nullable:
            return ChNullable(sa_type)
        return sa_type

    # TZ polarity from introspection carriers — check before LOGICAL_DATETIME collapse.
    if (
        "timestamptz" in raw_lower
        or "timestamp_tz" in raw_lower
        or "timestamp_ltz" in raw_lower
        or "timestamp with time zone" in raw_lower
        or "timestamp with local time zone" in raw_lower
        or "datetimeoffset" in raw_lower
        or raw_lower.endswith(" with time zone")
        or raw_lower.endswith(" local time zone")
    ):
        # SQL Server DATETIMEOFFSET is TZ-aware — never bind as naive DATETIME2.
        if db_type == "questdb":
            return _maybe_nullable(sa.DateTime())
        if db_type == "trino" and TrinoTimestamp is not None:
            return TrinoTimestamp(precision=3, timezone=True)
        if _is_oracle_wire(dialect_name, db_type):
            # sa.DateTime(timezone=True) compiles to Oracle DATE — second
            # granularity with no zone at all, so a live postgresql->oracle run
            # created DATE for TIMESTAMPTZ and reconciled green only because the
            # fixture held whole-second UTC values.
            local = "local time zone" in raw_lower or "_ltz" in raw_lower
            return _maybe_nullable(
                oracle.TIMESTAMP(timezone=not local, local_timezone=local)
            )
        if mysql is not None and _MYSQL_WIRES & {
            (dialect_name or "").lower(), (db_type or "").lower()
        }:
            # No MySQL carrier holds an offset, so the zone decision is made
            # upstream. sa.DateTime(timezone=True) compiles to fsp-0 DATETIME,
            # dropping the fraction too — a second loss for nothing.
            return _maybe_nullable(mysql.DATETIME(fsp=6))
        return sa.DateTime(timezone=True)
    if (
        "timestamp_ntz" in raw_lower
        or "timestamp without time zone" in raw_lower
        or "datetime_ntz" in raw_lower
        or " without time zone" in raw_lower
    ):
        sub_second = _sub_second_naive_wire(dialect_name, db_type)
        return _maybe_nullable(sub_second if sub_second is not None else sa.DateTime())

    if t == LOGICAL_INTEGER:
        # Width SSOT = decision_kernel integer_width_carrier / ddl_type.
        # Bare logical ``integer`` must invent 64-bit (never sa.Integer INT32).
        # Explicit INTEGER/INT/INT32 stay 32-bit; BIGINT stays 64-bit.
        from services.decision_kernel import integer_width_carrier
        from services.type_system import integer_bit_width

        int_u = raw.upper().split("(", 1)[0].strip().replace(" ", "")
        carrier = integer_width_carrier(raw) or ""
        width = integer_bit_width(carrier) if carrier else None
        if int_u in {"INT", "INTEGER"} and db_type:
            # The bare keyword is ambiguous across engines, so the carrier
            # widens it to 64-bit. Once the destination engine is named that
            # ambiguity is gone: SQL Server / PostgreSQL / MySQL INT is int32,
            # and binding it as BIGINT re-widens a column the operator declared
            # 32-bit. Oracle/Snowflake report unbounded and keep the widen.
            from services.numeric_fit import integer_storage_bounds

            bounds = integer_storage_bounds(int_u, dest_db=db_type)
            if bounds and bounds[1] == 2147483647:
                return _maybe_nullable(sa.Integer())
        if int_u in {
            "BIGINT",
            "INT64",
            "LONG",
            "UBIGINT",
            "UINT64",
            "BIGSERIAL",
        } or width == 64 or (carrier.upper() in {"BIGINT", "INT64", "LONG"}):
            return _maybe_nullable(sa.BigInteger())
        # Oracle NUMBER(p,0) normalized as integer — width from precision.
        if int_u == "NUMBER":
            from services.type_system import parse_numeric_precision_scale as _pnps

            np, _ns = _pnps(raw)
            if np is not None and int(np) > 9:
                return _maybe_nullable(sa.BigInteger())
            if np is not None and int(np) <= 4:
                return _maybe_nullable(sa.SmallInteger())
            return _maybe_nullable(sa.Integer())
        if int_u in {"SMALLINT", "INT2", "SMALLSERIAL", "SHORT", "INT16"} or width == 16:
            return _maybe_nullable(sa.SmallInteger())
        if int_u in {"TINYINT", "INT1", "UINT8", "BYTE"} or (width is not None and width <= 8):
            return _maybe_nullable(sa.SmallInteger())
        if int_u in {"INTEGER", "INT", "INT32", "MEDIUMINT", "SERIAL", "SIGNED"} or width == 32:
            return _maybe_nullable(sa.Integer())
        # Bare / unknown integer invent — never-narrower 64-bit (audit ITEM 1).
        return _maybe_nullable(sa.BigInteger())
    if t == LOGICAL_DECIMAL:
        precision, scale = parse_numeric_precision_scale(raw)
        if db_type == "risingwave":
            return sa.Numeric()
        # QuestDB lacks true DECIMAL — CREATE uses DOUBLE; ddl_type stamps
        # DOUBLE so Map/Validate match physical (never silent DECIMAL invent).
        if db_type == "questdb":
            return sa.Double()
        # Preserve source DECIMAL(p,s) when present — never invent TEXT.
        if precision is not None:
            out_scale = 0 if scale is None else int(scale)
            if db_type == "presto":
                return sa.DECIMAL(int(precision), out_scale)
            return _maybe_nullable(sa.Numeric(int(precision), out_scale))
        # Bare DECIMAL/NUMBER — Map≡CREATE must match type_system ddl_type SSOT.
        # Never invent Numeric(38,15) when the destination default is (38,10)
        # (SQL Server / Oracle / Databricks / Synapse).
        dest = (db_type or dialect_name or "").strip() or "generic_sql"
        wire = ddl_type(dest, raw)
        wp, ws = parse_numeric_precision_scale(wire)
        if wp is not None:
            out_scale = 0 if ws is None else int(ws)
            if (db_type or "").lower() == "presto" or dialect_name == "presto":
                return sa.DECIMAL(int(wp), out_scale)
            return _maybe_nullable(sa.Numeric(int(wp), out_scale))
        # PostgreSQL-wire: bare NUMERIC (arbitrary scale, no invent).
        if dialect_name == "postgresql" or (db_type or "").lower() in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "yugabytedb",
            "timescale",
            "supabase",
            "neon",
            "risingwave",
        }:
            return sa.Numeric()
        # Destination has no fixed-point wire (e.g. SQLite TEXT) — do not invent
        # Numeric(38,15); follow ddl_type collapse.
        wire_logical = normalize_logical_type(wire)
        if wire_logical in {LOGICAL_TEXT, LOGICAL_STRING}:
            if "TEXT" in (wire or "").upper():
                return _maybe_nullable(sa.Text())
            return _maybe_nullable(sa.String())
        return _maybe_nullable(sa.Numeric())
    if t == LOGICAL_FLOAT:
        # Approximate IEEE float — never rewrite to fixed-point Numeric.
        # Exact lowercase logical ``float`` → Double (never-narrower invent).
        # Honor Map REAL/FLOAT4/FLOAT stamps (sa.Double invents mantissa widen).
        if (raw or "").strip() == LOGICAL_FLOAT:
            return _maybe_nullable(sa.Double())
        float_u = raw.upper().split("(", 1)[0].strip()
        if float_u in {"REAL", "FLOAT4", "HALF", "FLOAT16", "BINARY_FLOAT", "FLOAT32"}:
            if dialect_name == "postgresql" and hasattr(postgresql, "REAL"):
                return _maybe_nullable(postgresql.REAL())
            return _maybe_nullable(sa.Float())
        if float_u == "FLOAT" and (db_type or "").lower() in {
            "mysql",
            "mariadb",
            "tidb",
            "sqlserver",
            "mssql",
            "databricks",
            "spark",
            "delta",
            "delta_lake",
            "databricks_sql",
        }:
            return _maybe_nullable(sa.Float())
        return _maybe_nullable(sa.Double())
    if t == LOGICAL_BOOLEAN:
        return _maybe_nullable(sa.Boolean())
    if t == LOGICAL_DATE:
        return _maybe_nullable(sa.Date())
    if t == LOGICAL_DATETIME:
        if db_type == "questdb":
            return sa.DateTime()
        # Preserve timezone metadata when the target dialect supports it.
        if dialect_name == "clickhouse":
            return _maybe_nullable(
                ChDateTime64(3) if ChDateTime64 is not None else sa.DateTime()
            )
        if db_type == "trino" and TrinoTimestamp is not None:
            # Trino bare timestamp path — TZ-aware stamps already returned above.
            return TrinoTimestamp(precision=3, timezone=False)
        if db_type == "presto":
            return sa.TIMESTAMP()
        sub_second = _sub_second_naive_wire(dialect_name, db_type)
        if sub_second is not None:
            return _maybe_nullable(sub_second)
        # Map≡CREATE: LOGICAL_DATETIME without TZ markers is NTZ wall-clock on
        # Oracle/DuckDB/PG/SQL Server. Databricks TIMESTAMP is session-TZ aware
        # (TIMESTAMP_NTZ already returned naive above) — never invent the wrong
        # polarity. TZ-aware carriers (TIMESTAMPTZ, DATETIMEOFFSET, Oracle
        # WITH [LOCAL] TIME ZONE) already returned timezone=True above.
        db_l = (db_type or "").strip().lower()
        if db_l in {
            "databricks",
            "spark",
            "delta",
            "delta_lake",
            "databricks_sql",
        }:
            base = raw_lower.split("(", 1)[0].strip()
            if base in {"timestamp", "timestamptz"}:
                return sa.DateTime(timezone=True)
        return _maybe_nullable(sa.DateTime())
    if t == LOGICAL_TIME:
        # ClickHouse, QuestDB and Presto (PyHive) do not bind Python time objects
        # reliably; store as string in these engines.
        if dialect_name == "clickhouse" or db_type in (
            "clickhouse",
            "questdb",
            "presto",
        ):
            return _maybe_nullable(sa.String())
        return _maybe_nullable(sa.Time())
    if t == LOGICAL_UUID:
        if db_type == "questdb":
            return sa.Text()
        if db_type == "risingwave":
            return sa.String()
        # ClickHouse stores UUIDs as variable-length String to avoid
        # FixedString(36) padding/failure for non-canonical UUIDs.
        if dialect_name == "clickhouse":
            return _maybe_nullable(sa.String())
        if dialect_name == "postgresql":
            return postgresql.UUID()
        return _maybe_nullable(sa.String(36))
    if t in (LOGICAL_JSON, LOGICAL_ARRAY):
        # DuckDB: use a custom JSON type that stores compact text and binds
        # Python None as SQL NULL.  Typed ``ARRAY<...>`` carriers still map to
        # ``sa.ARRAY`` for callers that introspect the SQLAlchemy type.
        if db_type == "duckdb":
            if t == LOGICAL_ARRAY:
                match = re.match(r"^(?:ARRAY|LIST)<(.+)>$", raw, re.IGNORECASE)
                if match:
                    element = match.group(1).strip()
                    return sa.ARRAY(
                        _sa_type_for_logical(element, dialect_name, db_type)
                    )
            return _ExactJSON(none_as_null=True)
        if db_type in ("oracle", "clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        if dialect_name == "postgresql":
            if t == LOGICAL_ARRAY:
                elem = None
                match = re.match(r"^(?:ARRAY|LIST)<(.+)>$", raw, re.IGNORECASE)
                postfix = re.match(r"^(.+?)\s*\[\s*\]\s*$", raw.strip(), re.IGNORECASE)
                if match:
                    elem = match.group(1).strip()
                elif postfix:
                    elem = postfix.group(1).strip()
                if elem:
                    return sa.ARRAY(
                        _sa_type_for_logical(elem, dialect_name, db_type)
                    )
            return postgresql.JSONB()
        return _ExactJSON(none_as_null=True)
    if t == LOGICAL_BINARY:
        if db_type in ("clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        return sa.LargeBinary()
    if t in (LOGICAL_GEOGRAPHY, LOGICAL_INTERVAL):
        # Typed specialty DDL — never invent TEXT for Oracle SDO_GEOMETRY / PG INTERVAL.
        # String carriers (Databricks/Iceberg/MySQL interval→TEXT) stay Text honestly.
        engine_key = (db_type or dialect_name or "").lower()
        logical_name = "geography" if t == LOGICAL_GEOGRAPHY else "interval"
        native = ddl_type(engine_key, logical_name) if engine_key else ""
        native_logical = normalize_logical_type(native)
        if (
            not native
            or native_logical in {LOGICAL_STRING, LOGICAL_TEXT}
            or native.upper() in {"STRING", "TEXT", "VARCHAR", "NVARCHAR", "VARCHAR2"}
        ):
            return _maybe_nullable(sa.Text())
        if _DialectNativeType is None:
            return _maybe_nullable(sa.Text())
        return _maybe_nullable(_DialectNativeType(native))
    # Specialty carriers that normalize to STRING (HIERARCHYID, SQL_VARIANT, XML,
    # ROWVERSION, …) — DialectNativeType / typed wire via ddl_type; never invent
    # bare Text that rematerializes as Map VARCHAR over live specialty DDL.
    from services.type_system import (
        is_fixed_char_carrier,
        is_national_string_carrier,
        specialty_carrier_base,
        string_carrier_length,
    )

    # Unicode polarity and declared width survive to DDL. Collapsing every string
    # carrier to sa.Text() made SQL Server CREATE a code-page VARCHAR(MAX) for an
    # invented NVARCHAR(64), and a live postgresql->mssql read-back came back with
    # ``中`` rewritten to ``?``. sa.Unicode/UnicodeText are the dialect-neutral
    # national wires (NVARCHAR on SQL Server, NVARCHAR2 on Oracle, VARCHAR on
    # engines that are Unicode-only anyway).
    if is_national_string_carrier(raw):
        width = string_carrier_length(raw)
        if width is not None:
            if is_fixed_char_carrier(raw):
                return _maybe_nullable(sa.NCHAR(width))
            return _maybe_nullable(sa.Unicode(width))
        if dialect_name == "mssql" or db_type in {"sqlserver", "mssql", "azure_sql"}:
            # sa.UnicodeText compiles to deprecated NTEXT; NVARCHAR(max) is the
            # only supported SQL Server Unicode LOB wire.
            return _maybe_nullable(sa.Unicode())
        return _maybe_nullable(sa.UnicodeText())

    # Declared width survives to DDL for the non-national carriers too: every
    # bounded VARCHAR(n)/CHAR(n) used to land as sa.Text(), so a live
    # postgresql->oracle CREATE turned VARCHAR(64) into CLOB — unindexable, and
    # the destination no longer enforces the source's length contract.
    width = string_carrier_length(raw)
    if width is not None:
        if is_fixed_char_carrier(raw):
            return _maybe_nullable(sa.CHAR(width))
        return _maybe_nullable(sa.String(width))

    spec = specialty_carrier_base(raw)
    if spec:
        engine_key = (db_type or dialect_name or "").lower()
        native = ddl_type(engine_key, raw) if engine_key else spec
        native_logical = normalize_logical_type(native or "")
        native_base = (native or "").upper().split("(", 1)[0].strip()
        # Same-token or remapped specialty (HIERARCHYID, LTREE, SQL_VARIANT, …)
        # before LOGICAL_STRING collapse invents bare Text.
        native_spec = specialty_carrier_base(native)
        if native_spec:
            if _DialectNativeType is None:
                return _maybe_nullable(sa.Text())
            return _maybe_nullable(_DialectNativeType(native))
        stringish = {
            "STRING",
            "TEXT",
            "VARCHAR",
            "NVARCHAR",
            "VARCHAR2",
            "NVARCHAR2",
            "CLOB",
            "NCLOB",
            "CHAR",
            "NCHAR",
        }
        if (
            not native
            or native_logical in {LOGICAL_STRING, LOGICAL_TEXT}
            or native_base in stringish
        ):
            m = re.match(
                r"^(?:N?VAR)?CHAR2?\s*\(\s*(\d+)\s*\)$",
                (native or "").strip(),
                re.IGNORECASE,
            )
            if m:
                return _maybe_nullable(sa.String(int(m.group(1))))
            return _maybe_nullable(sa.Text())
        if native_logical == LOGICAL_BINARY:
            return sa.LargeBinary()
        if native_logical == LOGICAL_UUID:
            if dialect_name == "postgresql":
                return postgresql.UUID()
            return _maybe_nullable(sa.String(36))
        if native_logical == LOGICAL_JSON:
            if dialect_name == "postgresql":
                return postgresql.JSONB()
            return _ExactJSON(none_as_null=True)
        if _DialectNativeType is None:
            return _maybe_nullable(sa.Text())
        return _maybe_nullable(_DialectNativeType(native))
    if mssql is not None and (
        dialect_name == "mssql" or (db_type or "").lower() in _MSSQL_WIRES
    ):
        # sa.Text() compiles to SQL Server TEXT, deprecated since 2005 and
        # unusable with most string predicates. VARCHAR(max) is the wire.
        return _maybe_nullable(mssql.VARCHAR(None))
    return _maybe_nullable(sa.Text())


def _is_string_type(sa_type: Any) -> bool:
    if sa_type is None:
        return False
    if isinstance(sa_type, (sa.String, sa.Text, sa.CHAR)):
        return True
    # Handle ClickHouse Nullable(String) / Nullable(TEXT)
    nested = getattr(sa_type, "nested_type", None)
    return bool(nested is not None and isinstance(nested, (sa.String, sa.Text, sa.CHAR)))


def _to_sa_value(
    value: Any,
    logical: str,
    sa_type: Any = None,
    dialect_name: str = "",
    db_type: str = "",
) -> Any:
    """Convert transform-engine output values to Python objects SQLAlchemy accepts."""
    from services.value_serializer import absent_sql_bind

    # Reader-null is SQL NULL. Missing stays Missing (sparse omit, never wipe).
    handled, bound = absent_sql_bind(value)
    if handled:
        return bound

    # Specialty carriers (INET, PG_LSN, geometric, OID, snapshots, …) must use
    # the shared sql_bind SSOT before LOGICAL_* collapse invents string/int/geo.
    # Airbyte maps these to string; we validate + canonicalize then bind.
    eng = (db_type or dialect_name or "").strip().lower()
    raw_carrier = (logical or "").strip()
    if raw_carrier:
        from connectors.sql_bind import normalize_sql_bind_value
        from connectors.sql_temporal import sql_base_type
        from services.type_system import parse_enum_or_set_ordered_members

        # MySQL ENUM/SET domains — coerce ordinals/bitmasks before INTEGER collapse.
        if parse_enum_or_set_ordered_members(raw_carrier) is not None:
            return normalize_sql_bind_value(value, raw_carrier, engine=eng)

        base = sql_base_type(raw_carrier)
        specialty = {
            "INET",
            "CIDR",
            "MACADDR",
            "MACADDR8",
            "POINT",
            "LINE",
            "LSEG",
            "BOX",
            "PATH",
            "POLYGON",
            "CIRCLE",
            "PG_LSN",
            "LSN",
            "OID",
            "TID",
            "CTID",
            "XID",
            "XID8",
            "CID",
            "HSTORE",
            "XML",
            "XMLTYPE",
            "CITEXT",
            "LTREE",
            "TSVECTOR",
            "TSQUERY",
            "TXID_SNAPSHOT",
            "PG_SNAPSHOT",
            "UUID",
            "UNIQUEIDENTIFIER",
            "GUID",
            "ROWVERSION",
            "HIERARCHYID",
            "GEOGRAPHY",
            "GEOMETRY",
            "SDO_GEOMETRY",
            "INTERVAL",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "FLOAT16",
            "FLOAT32",
            "FLOAT64",
            "HALF",
            "HALFFLOAT",
            "REAL",
            "DOUBLE",
            "DOUBLE PRECISION",
            "BINARY_FLOAT",
            "BINARY_DOUBLE",
            "SQL_VARIANT",
            "ROWID",
            "UROWID",
        }
        if (
            base in specialty
            or "MULTIRANGE" in base
            or (base.endswith("RANGE") and base != "RANGE")
            or base == "RANGE"
        ):
            return normalize_sql_bind_value(value, raw_carrier, engine=eng)

    from services.type_system import (
        LOGICAL_ARRAY,
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        LOGICAL_JSON,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        normalize_logical_type,
    )

    t = normalize_logical_type(logical)
    # Oracle write-location: '' → NULL for string carriers (VARCHAR2 semantics).
    if (
        isinstance(value, str)
        and value == ""
        and t in {LOGICAL_STRING, LOGICAL_TEXT, ""}
        and (eng in {"oracle", "oracledb", "oracle_autonomous"} or eng.startswith("oracle"))
    ):
        return None

    if t in (LOGICAL_JSON, LOGICAL_ARRAY):
        from connectors.sql_bind import coerce_json_wire

        # Empty JSON wire raises — quarantine upstream (never invent SQL NULL wipe).
        # _ExactJSON bind keeps engine text; parsing here then dumping would
        # stringify non-IEEE Decimals (number → string polarity invent).
        as_text = _is_string_type(sa_type) or isinstance(sa_type, _ExactJSON)
        bound = coerce_json_wire(value, as_text=as_text)
        if as_text:
            return bound
        if isinstance(bound, str) and not _is_string_type(sa_type):
            # Valid JSON text → native for JSONB; wrap leftovers stay text.
            # json_loads_exact keeps digits that binary64 cannot hold.
            from services.value_serializer import json_loads_exact

            try:
                return json_loads_exact(bound)
            except (json.JSONDecodeError, ValueError, TypeError):
                return bound
        return bound

    if t == LOGICAL_BOOLEAN:
        from connectors.sql_bind import coerce_boolean_wire

        # MySQL TINYINT(1) expects 0/1; Postgres/others accept native bool.
        as_int = (db_type or dialect_name or "").strip().lower() in {
            "mysql",
            "mariadb",
            "tidb",
        }
        coerced = coerce_boolean_wire(value, as_int=as_int)
        if as_int:
            if coerced is not None and coerced not in (0, 1):
                raise ValueError(
                    f"generic SQL BOOLEAN refused {value!r} "
                    "(refuse invent via pass-through)"
                )
        elif coerced is not None and not isinstance(coerced, bool):
            raise ValueError(
                f"generic SQL BOOLEAN refused {value!r} "
                "(refuse invent via pass-through)"
            )
        return coerced

    if t == LOGICAL_BINARY:
        from connectors.sql_bind import coerce_binary_wire

        if isinstance(value, bytes):
            if _is_string_type(sa_type):
                return base64.b64encode(value).decode("ascii")
            return value
        if isinstance(value, str):
            if _is_string_type(sa_type):
                return value
            return coerce_binary_wire(value)
        return coerce_binary_wire(value)

    # Temporal: same parse/coerce path as MySQL/Postgres writers (ISO-Z → bind).
    # Prefer the *carrier* spelling (TIMESTAMPTZ) before the collapsed logical
    # family (datetime) — otherwise SQL Server TIMESTAMPTZ invents DATETIME NTZ
    # and the naive-datetime guard fires on aware UTC (audit §2.6).
    ddl_type = logical_to_temporal_ddl(logical) or logical_to_temporal_ddl(t)
    if ddl_type:
        # SA timezone=True (from physical TIMESTAMP WITH TIME ZONE) must win over
        # a collapsed Map logical of "datetime" — otherwise coerce_sql_temporal
        # strips Z to naive DATETIME and the TZ guard refuses the value.
        from services.dest_dialect_facts import _normalize_dest_db

        sa_tz = bool(getattr(sa_type, "timezone", False))
        coerce_ddl = ddl_type
        if sa_tz and str(ddl_type).upper() in {
            "DATETIME",
            "TIMESTAMP",
            "DATETIME2",
            "SMALLDATETIME",
        }:
            coerce_ddl = "TIMESTAMPTZ"
        elif (
            type(sa_type).__name__.upper() == "TIMESTAMP"
            and _normalize_dest_db(eng) == "mysql"
        ):
            # Collapsed Map logical "datetime" must not strip a MySQL TIMESTAMP
            # instant into DATETIME wall-clock digits.
            coerce_ddl = "TIMESTAMP"
        coerced = coerce_sql_temporal(
            value, coerce_ddl, engine=str(db_type or dialect_name or "")
        )
        base = str(coerce_ddl).upper()

        if base == "DATE":
            if coerced is None:
                return None
            if isinstance(coerced, datetime):
                return coerced.date()
            if isinstance(coerced, date):
                return coerced
            return value

        if base == "TIME":
            clock = bind_time_clock(value)
            if clock is None:
                return None
            if (
                _is_string_type(sa_type)
                or db_type == "presto"
                or dialect_name == "presto"
            ):
                return clock.isoformat()
            return clock

        # DATETIME2 (SQL Server) / QuestDB / Oracle TIMESTAMP / ClickHouse DateTime
        # are naive wall clocks. Never invent tzinfo=UTC on naive values — that
        # silently shifts polarity for every generic_sql destination.
        # Instant-only dests bind UTC. Offset-storing dests (DATETIMEOFFSET)
        # keep the originating label — UTC-normalizing here is the DMS hole.
        raw_lower = f"{logical or ''} {coerce_ddl or ''} {ddl_type or ''}".lower()
        is_tz_aware = sa_tz or (
            "timestamptz" in raw_lower
            or "datetimeoffset" in raw_lower
            or "timestamp_tz" in raw_lower
            or "timestamp with time zone" in raw_lower
            or "with local time zone" in raw_lower
        )
        if is_tz_aware:
            from services.offset_label import bind_aware_datetime

            if coerced is None:
                return None
            if isinstance(coerced, datetime):
                if coerced.tzinfo is None:
                    raise ValueError(
                        f"generic SQL {ddl_type} refused naive datetime — provide "
                        "offset/Z (refuse silent UTC invent)"
                    )
                return bind_aware_datetime(
                    coerced,
                    engine=str(db_type or dialect_name or ""),
                    dest_type=str(coerce_ddl or ddl_type or ""),
                    original=value,
                )
            if isinstance(coerced, date) and not isinstance(coerced, datetime):
                raise ValueError(
                    f"generic SQL {ddl_type} refused date-only value — provide "
                    "a timezone-aware timestamp"
                )
            return value
        # NTZ / DATETIME: keep civil digits; strip offset without astimezone.
        if coerced is None:
            return None
        if isinstance(coerced, datetime):
            if coerced.tzinfo is not None:
                return coerced.replace(tzinfo=None)
            return coerced
        if isinstance(coerced, date) and not isinstance(coerced, datetime):
            return datetime.combine(coerced, time())
        return value

    if t == LOGICAL_DECIMAL:
        from connectors.sql_bind import coerce_decimal_wire

        # Never bind Decimal('NaN')/Inf — coerce_decimal_wire refuses non-finite.
        # Pass engine so PG-family scale-round matches quarantine (quarantine≡bind).
        return coerce_decimal_wire(
            value,
            ddl_type=str(sa_type or "DECIMAL"),
            engine=str(db_type or dialect_name or ""),
        )

    if t == LOGICAL_INTEGER:
        from connectors.sql_bind import coerce_integer_wire

        # Never pass through unparseable strings — that invents VARCHAR in an
        # INTEGER bind under dialects that coerce silently.
        return coerce_integer_wire(
            value,
            ddl_type=str(sa_type or logical or "INTEGER"),
            engine=str(db_type or dialect_name or ""),
        )

    if t == LOGICAL_FLOAT:
        from connectors.sql_bind import coerce_float_wire

        # Empty / non-numeric must refuse — never invent 0.0 or pass '' through.
        return coerce_float_wire(
            value,
            ddl_type=str(sa_type or logical or "FLOAT"),
        )

    if t in (LOGICAL_STRING, LOGICAL_TEXT) or _is_string_type(sa_type):
        from services.encoding_capacity import bind_unicode_text

        # CESU-8 / surrogate leaks become Unicode scalars. Dest that cannot
        # encode a scalar raises — quarantine holds the cell, never '?'.
        return bind_unicode_text(
            value,
            engine=str(db_type or dialect_name or ""),
            dest_type=str(logical or ""),
        )

    # uuid leftover; string/text already bound above
    return value


def _cfg_from_params(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    type: str = "",
    **_: Any,
) -> dict[str, Any]:
    cfg = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "schema": schema,
        "connection_string": connection_string,
        "ssl": ssl,
        "type": type,
    }
    return cfg


def test_generic_sql(**kwargs: Any) -> tuple[bool, str]:
    """Probe connectivity using a lightweight SELECT 1 equivalent."""
    if not SQLALCHEMY_AVAILABLE:
        return False, "SQLAlchemy is not installed"
    cfg = _cfg_from_params(**kwargs)
    try:
        engine = _engine(cfg)
        with engine.connect() as conn:
            conn.execute(sa.select(sa.literal(1)))
        return True, "SQLAlchemy connection successful"
    except (sa.exc.SQLAlchemyError, OSError, RuntimeError) as exc:
        return False, str(exc)


def _reflect_table(
    engine: Any,
    table: str,
    schema: str | None,
    columns: list[str] | None = None,
    include_pk: bool = False,
) -> sa.Table:
    """Reflect or build a Table object for reading/writing.

    The ``autoload_with`` reflection is the expensive half and is cached per
    table, because a chunked read calls this once per chunk for a shape that
    only changes when we ourselves run DDL. The column-subset projection below
    is pure Python and is rebuilt per call, so callers still get their own
    object to hold and the shared reflected table is never mutated.
    """
    from services.reflection_cache import get_or_load

    def _reflect() -> sa.Table:
        # Quote identifiers for safety with reserved words and case-sensitive engines.
        try:
            return sa.Table(
                table,
                sa.MetaData(),
                schema=schema,
                quote=True,
                quote_schema=True,
                autoload_with=engine,
            )
        except sa.exc.NoSuchTableError:
            # Oracle/DB2/Snowflake fold unquoted identifiers to upper case and
            # SQLAlchemy expects the *normalised* (lower-case) spelling; forcing
            # quote=True looks for a literal name, misses, and the caller
            # degrades to an untyped ``SELECT *`` that loses every column type
            # (an Oracle NUMBER(12,2) then scored off sampled values instead).
            attempts: list[tuple[str, str | None]] = [(table, schema)]
            if str(getattr(engine.dialect, "name", "")).lower() in _FOLDING_DIALECTS:
                # Only folding dialects may retry a case-changed name: on
                # PostgreSQL "Foo" and "foo" are different tables and guessing
                # would read the wrong one.
                attempts.append((table.lower(), (schema or "").lower() or None))
            last: Exception | None = None
            for name, sch in attempts:
                try:
                    return sa.Table(
                        name, sa.MetaData(), schema=sch, autoload_with=engine
                    )
                except sa.exc.NoSuchTableError as retry_exc:
                    last = retry_exc
            raise last if last is not None else sa.exc.NoSuchTableError(table)

    table_obj = get_or_load(engine, schema, table, "reflect", _reflect)
    if columns is None:
        return table_obj

    # Restrict to requested columns but keep the full table for ordering/cursor.
    selected = []
    for c in columns:
        if c in table_obj.c:
            selected.append(table_obj.c[c])
        else:
            raise ValueError(f"Column '{c}' not found in table {table}")
    # Return a subselect proxy with those columns only so we can still use .c.
    new_meta = sa.MetaData()
    new_table = sa.Table(table, new_meta, schema=schema, quote=True, quote_schema=True)
    for col in selected:
        new_table.append_column(sa.Column(col.name, col.type, quote=True))
    return new_table


def _fidelity_dialect(db_type: str, dialect_name: str) -> str:
    """Canonical dialect name for the fidelity planner (never a driver name)."""
    d = (db_type or "").strip().lower() or (dialect_name or "").strip().lower()
    aliases = {
        "mssql": "sqlserver",
        "azure_sql": "sqlserver",
        "mariadb": "mysql",
        "postgres": "postgresql",
        "cockroachdb": "postgresql",
        "yugabytedb": "postgresql",
        "oracledb": "oracle",
    }
    return aliases.get(d, d)


def _with_placement_suffix(ddl: str, suffix: str) -> str:
    """Append the planned PARTITION BY / TABLESPACE clause to a compiled CREATE."""
    clause = suffix.strip()
    if not clause:
        return ddl
    return f"{ddl.rstrip().rstrip(';')} {clause}"


def _build_table_for_write(
    engine: Any,
    table_name: str,
    schema: str | None,
    columns: list[str],
    column_types: dict[str, str],
    db_type: str = "",
    conflict_columns: list[str] | None = None,
    fidelity_plan: Any = None,
) -> sa.Table:
    """Build an explicit Table definition for CREATE/INSERT using the target schema.

    When ``conflict_columns`` are supplied for upsert, add a PRIMARY KEY over them
    so native ``ON CONFLICT`` / ``ON DUPLICATE KEY`` upsert has the required
    unique constraint and retries are truly idempotent.
    """
    metadata = sa.MetaData()
    dialect_name = engine.dialect.name if engine.dialect else ""
    from connectors.writer_common import resolve_conflict_targets

    try:
        conflict_cols = resolve_conflict_targets(
            conflict_columns, columns, strict=True
        )
    except ValueError as exc:
        # Never CREATE a table with a silently degraded / empty PK when the
        # operator configured conflict columns — clients cannot trust that schema.
        raise ValueError(
            f"Cannot CREATE TABLE: conflict/PK columns do not resolve "
            f"against the planned schema ({exc})."
        ) from exc
    pk_set = set()
    if conflict_cols:
        pk_set = set(conflict_cols)

    # Source semantics come from the one canonical planner, never from a second
    # opinion invented here: a CHECK the certificate calls carried must be the
    # CHECK this CREATE TABLE emits.
    plan_not_null: set[str] = set()
    plan_defaults: dict[str, str] = {}
    plan_uniques: list[list[str]] = []
    plan_checks: list[tuple[str, str]] = []
    if fidelity_plan is not None:
        plan_not_null = set(getattr(fidelity_plan, "not_null_columns", []) or [])
        plan_defaults = dict(getattr(fidelity_plan, "column_defaults", {}) or {})
        plan_uniques = [
            list(u) for u in (getattr(fidelity_plan, "unique_constraints", []) or [])
        ]
        plan_checks = [
            (str(n), str(p))
            for n, p in (getattr(fidelity_plan, "check_predicates", []) or [])
        ]
        # Resolve the plan's names against the columns actually being created.
        # An exact-match-only lookup silently dropped the PRIMARY KEY when the
        # two spellings differed only by case, and the certificate still said
        # "carried" — a mismatch must resolve or be visible, never vanish.
        by_fold = {str(c).casefold(): c for c in columns}
        plan_pk = [
            by_fold[str(c).casefold()]
            for c in (getattr(fidelity_plan, "primary_key", []) or [])
            if str(c).casefold() in by_fold
        ]
        plan_not_null = {
            by_fold[c.casefold()] for c in plan_not_null if c.casefold() in by_fold
        }
        plan_defaults = {
            by_fold[c.casefold()]: v
            for c, v in plan_defaults.items()
            if c.casefold() in by_fold
        }
        plan_uniques = [
            [by_fold[c.casefold()] for c in u if c.casefold() in by_fold]
            for u in plan_uniques
        ]
        if not pk_set and plan_pk and len(plan_pk) == len(
            getattr(fidelity_plan, "primary_key", []) or []
        ):
            pk_set = set(plan_pk)

    # dest column -> the generator clause the planner decided, which carries the
    # source's own seed and increment.
    plan_identity: dict[str, str] = {}
    if fidelity_plan is not None:
        by_fold_ident = {str(c).casefold(): c for c in columns}
        plan_identity = {
            by_fold_ident[str(c).casefold()]: str(clause or "")
            for c, clause in (getattr(fidelity_plan, "identity_columns", {}) or {}).items()
            if str(c).casefold() in by_fold_ident
        }

    cols = []
    for col in columns:
        logical = column_types.get(col, "string")
        is_pk = col in pk_set
        # Setting autoincrement=False prevents SQLAlchemy from fabricating a
        # backing sequence for dialects (e.g. DuckDB) that do not create it
        # automatically.  The PK exists purely for upsert semantics, not identity.
        autoincrement = False if is_pk else None
        nullable = not (is_pk or col in plan_not_null)
        # A key generator the source declared. MySQL spells it AUTO_INCREMENT,
        # which SQLAlchemy renders from ``autoincrement``; every other engine
        # here takes the standard IDENTITY construct. ``always=False`` is
        # deliberate — the load writes the source's own key values, and the
        # certificate reports the relaxation.
        identity_arg: list[Any] = []
        if col in plan_identity:
            nullable = False
            if dialect_name in {"mysql", "mariadb"}:
                autoincrement = True
            else:
                # SQLAlchemy rejects an explicit autoincrement=False beside an
                # Identity object; the Identity is the generator here.
                autoincrement = None
                seed, step = identity_seed_step(plan_identity[col])
                identity_arg.append(
                    sa.Identity(always=False, start=seed, increment=step)
                )
        cols.append(
            sa.Column(
                col,
                _sa_type_for_logical(
                    logical, dialect_name, db_type, nullable=nullable
                ),
                *identity_arg,
                primary_key=is_pk,
                nullable=nullable,
                autoincrement=autoincrement,
                server_default=(
                    sa.text(plan_defaults[col]) if col in plan_defaults else None
                ),
                quote=True,
            )
        )

    constraints: list[Any] = []
    if conflict_cols and not pk_set.issubset(set(columns)):
        # ``quote=`` is a Column kwarg; on a constraint SQLAlchemy rejects it
        # as an unknown dialect argument and the whole CREATE fails.
        constraints.append(sa.UniqueConstraint(*conflict_cols))
    for unique_cols in plan_uniques:
        if all(c in columns for c in unique_cols) and set(unique_cols) != pk_set:
            constraints.append(sa.UniqueConstraint(*unique_cols))
    for check_name, predicate in plan_checks:
        constraints.append(
            sa.CheckConstraint(sa.text(predicate), name=check_name or None)
        )

    if dialect_name == "clickhouse" and ch_engines is not None:
        # Airbyte-class: upsert identity is ORDER BY on ReplacingMergeTree, not
        # a SQL PRIMARY KEY. Plain MergeTree + delete+insert is the wrong algorithm.
        if conflict_cols:
            order_by = tuple(sa.column(c) for c in conflict_cols)
            replacing = getattr(ch_engines, "ReplacingMergeTree", None)
            if replacing is not None:
                if DF_LSN_COL in columns:
                    # Higher ``_df_lsn`` wins on background merge (at-least-once CDC).
                    ch_engine = replacing(DF_LSN_COL, order_by=order_by)
                else:
                    ch_engine = replacing(order_by=order_by)
            else:
                ch_engine = ch_engines.MergeTree(order_by=order_by)
        else:
            ch_engine = ch_engines.MergeTree(order_by=sa.text("tuple()"))
        return sa.Table(
            table_name,
            metadata,
            *cols,
            *constraints,
            ch_engine,
            schema=schema,
            quote=True,
            quote_schema=True,
        )

    return sa.Table(
        table_name,
        metadata,
        *cols,
        *constraints,
        schema=schema,
        quote=True,
        quote_schema=True,
    )


def _type_has_params(type_name: str | None) -> bool:
    """True when a DDL string carries a length, precision, or dimension."""
    return bool(re.search(r"\(\s*\d", type_name or ""))


def _source_ddl_for_widen(
    mapping_source: str | None,
    catalog_source: str | None,
) -> str | None:
    """Choose the best source DDL for schema-drift widening.

    ``mapping_source`` is the inferred source type attached to the mapping
    (e.g. DECIMAL); ``catalog_source`` is the raw source catalog type (e.g. TEXT
    for file formats or NUMERIC(12,2) for SQL introspection).  Prefer the
    mapping when it is concrete and the catalog is a generic string, but upgrade
    to the catalog type when it is wider in the same logical family.
    """
    if not mapping_source and not catalog_source:
        return None
    if not mapping_source:
        return catalog_source
    if not catalog_source:
        return mapping_source

    from services.type_system import normalize_logical_type

    mapping_logical = normalize_logical_type(mapping_source)
    catalog_logical = normalize_logical_type(catalog_source)

    if catalog_logical in {"string", "text"} and mapping_logical not in {"string", "text"}:
        return mapping_source
    if mapping_logical in {"string", "text"} and catalog_logical not in {"string", "text"}:
        return catalog_source
    if mapping_logical == catalog_logical:
        mapping_params = _type_has_params(mapping_source)
        catalog_params = _type_has_params(catalog_source)
        # If one side is bare (e.g. DECIMAL) and the other carries precision
        # (e.g. NUMERIC(12,2)), prefer the concrete side. Bare logicals are
        # not wider than a typed carrier; they are just less specific.
        if mapping_params and not catalog_params:
            return mapping_source
        if catalog_params and not mapping_params:
            return catalog_source
        return catalog_source if is_wider_type(mapping_source, catalog_source) else mapping_source
    return mapping_source


def _widen_existing_columns_sa(
    conn: Any,
    engine: Any,
    dialect_name: str,
    schema: str | None,
    table_name: str,
    target_cols: list[str],
    target_column_types: dict[str, str],
    conflict_columns: list[str] | None = None,
    *,
    stamp_ceiling_by_col: dict[str, str] | None = None,
    refusals_out: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Issue ALTER COLUMN / MODIFY COLUMN to widen any columns that drifted wider.

    Uses SQLAlchemy reflection and text execution so the same helper works for
    every SQLAlchemy-backed dialect (DuckDB, SQL Server, Oracle, SQLite skipped).
    Primary-key / conflict columns are skipped because most engines cannot
    ALTER the type of a key column in place.

    Map≡ALTER: when ``stamp_ceiling_by_col`` is set, never ALTER past the
    approved Map stamp — refuse-closed with audit evidence in ``refusals_out``.
    """
    if not target_cols or not target_column_types:
        return []

    dialect_name = (dialect_name or "").lower()
    if dialect_name == "sqlite":
        return []

    skip_cols = set(conflict_columns or [])
    ceilings = {
        str(k): str(v)
        for k, v in (stamp_ceiling_by_col or {}).items()
        if k and v
    }

    try:
        inspector = sa.inspect(conn)
        existing_cols = inspector.get_columns(table_name, schema=schema)
    except Exception as exc:
        logger.debug("Could not reflect columns for widen: %s", exc, exc_info=exc)
        return []

    from connectors.schema_drift import existing_column_index

    catalog_names = existing_column_index(
        dialect_name, (c["name"] for c in existing_cols)
    )
    log: list[str] = []
    for col in target_cols:
        if col in skip_cols:
            continue
        # Reflection normalises Oracle's ``LABEL`` to ``label``; an exact match
        # skipped the widen and the row it was needed for was truncated or
        # refused with no ALTER ever attempted.
        catalog_name = catalog_names.get(col) or catalog_names.get(col.casefold())
        existing = next(
            (c for c in existing_cols if c["name"] == catalog_name), None
        )
        if not existing:
            continue
        existing_type = str(existing["type"].compile(dialect=engine.dialect))
        proposed = target_column_types.get(col, existing_type)
        stamp = ceilings.get(col)
        desired_type = proposed
        if stamp:
            # Hard ceiling: align live up to stamp only; never past it.
            if proposed and is_wider_type(stamp, proposed):
                refusal = {
                    "column": col,
                    "mapped_type": stamp,
                    "refused_wider": proposed,
                    "reason": "explicit_map_stamp_ceiling",
                }
                if refusals_out is not None:
                    refusals_out.append(refusal)
                else:
                    logger.info(
                        "generic_sql Map≡ALTER refusal (stamp ceiling): %s", refusal
                    )
            desired_type = stamp
        if not is_wider_type(existing_type, desired_type):
            continue
        try:
            ddl = _build_widen_ddl(
                dialect_name, schema, table_name, col, desired_type, existing_type
            )
            conn.execute(sa.text(ddl))
            conn.commit()
            log.append(ddl)
            logger.debug(
                "Widened %s.%s from %s to %s",
                table_name,
                col,
                existing_type,
                desired_type,
            )
        except Exception as exc:
            err = str(exc).lower()
            if any(
                phrase in err for phrase in ("already", "cannot alter", "not supported")
            ):
                logger.debug(
                    "Widen skipped for %s.%s: %s", table_name, col, exc, exc_info=exc
                )
                continue
            logger.warning(
                "Widen failed for %s.%s: %s", table_name, col, exc, exc_info=exc
            )
            raise_widen_refusal(col, existing_type, desired_type, exc)
            raise
    return log


def _infer_logical_from_samples(values: list[Any], field_name: str = "") -> str | None:
    """Use Datawrap value inference to narrow generic SQL String columns.

    We intentionally do NOT narrow string columns to INTEGER or DECIMAL: a
    string column may contain codes, identifiers, bit strings, or formatted
    values (e.g. $1,000.00, 1010) that would be corrupted by numeric coercion.
    Structural/representational types (JSON, UUID, BINARY, DATE, TIME, etc.)
    are still recovered safely.
    """
    try:
        from services.schema_inference import infer_column

        mapped = {
            "JSON": "json",
            "BINARY": "binary",
            "UUID": "uuid",
            "DATE": "date",
            "TIMESTAMP": "datetime",
            "TIME": "time",
            "BOOLEAN": "boolean",
            "VARCHAR": "string",
            "TEXT": "string",
        }
        samples = [cell_to_string(v, preserve_sql_null=True) for v in values]
        return mapped.get(
            str(infer_column(samples, field_name=field_name)["logical_type"])
        )
    except Exception:
        return None


def _sample_raw_table(
    conn: Any,
    table: str,
    schema: str | None,
    *,
    dialect: str = "ansi",
) -> tuple[list[str], list[Any]]:
    from connectors.sql_identifiers import quote_table_ref
    from services.dialect_profiles import uses_fetch_first_pagination

    qualified = quote_table_ref(table, schema, dialect=dialect)
    dialect_l = (dialect or "ansi").lower()
    # SQL Server / Sybase reject LIMIT — use TOP.
    if dialect_l in {
        "mssql",
        "sqlserver",
        "microsoft_sql_server",
        "azure_sql_database",
    }:
        stmt = f"SELECT TOP 200 * FROM {qualified}"  # nosec B608
    elif uses_fetch_first_pagination(dialect):
        # Oracle/DB2 reject LIMIT; FETCH FIRST needs no ORDER BY for a sample.
        stmt = f"SELECT * FROM {qualified} FETCH FIRST 200 ROWS ONLY"  # nosec B608
    else:
        stmt = f"SELECT * FROM {qualified} LIMIT 200"  # nosec B608
    result = conn.execute(sa.text(stmt))
    headers = list(result.keys())
    rows = result.fetchall()
    return headers, rows


def introspect_table_schema(
    cfg: dict[str, Any],
    table: str,
) -> dict[str, Any]:
    """Return schema metadata for the table using SQLAlchemy reflection."""
    if not SQLALCHEMY_AVAILABLE:
        return {
            "ok": False,
            "error": "SQLAlchemy is not installed",
            "columns": [],
            "tables": [],
        }
    engine = _engine(cfg)
    try:
        schema = _schema_name(cfg)
        inspector = inspect(engine)
        try:
            columns = inspector.get_columns(table, schema=schema)
        except Exception:
            # Engines like RisingWave/QuestDB expose a SQL endpoint but not full pg_catalog.
            # Try information_schema.columns before falling back to raw value sampling.
            try:
                with engine.connect() as conn:
                    from services.schema_introspect import _ch_to_logical
                    from services.type_system import normalize_logical_type

                    dialect_key = _dialect_key(cfg)
                    params: dict = {"table": table}
                    if schema is None:
                        sql = sa.text(
                            "SELECT column_name, data_type, is_nullable "
                            "FROM information_schema.columns "
                            "WHERE table_name = :table AND table_schema = current_schema() "
                            "ORDER BY ordinal_position"
                        )
                    else:
                        params["schema"] = schema
                        sql = sa.text(
                            "SELECT column_name, data_type, is_nullable "
                            "FROM information_schema.columns "
                            "WHERE table_name = :table AND table_schema = :schema "
                            "ORDER BY ordinal_position"
                        )
                    rows = conn.execute(sql, params).fetchall()
                    if rows:

                        def _dtype_to_inferred(data_type: Any) -> str:
                            text = str(data_type or "").strip()
                            if dialect_key == "clickhouse" or text.upper().startswith(
                                (
                                    "DATETIME64",
                                    "DATETIME(",
                                    "IPV4",
                                    "IPV6",
                                    "TUPLE(",
                                    "ARRAY(",
                                    "MAP(",
                                )
                            ):
                                return _ch_to_logical(text)
                            # Prefer physical dtype tokens over stripped logical names.
                            from services.type_system import ddl_carrier_type

                            carrier = ddl_carrier_type(text)
                            if carrier and carrier.upper() not in {
                                "VARCHAR",
                                "STRING",
                            }:
                                return carrier
                            return normalize_logical_type(text)

                        result = [
                            {
                                "name": name,
                                "inferred_type": _dtype_to_inferred(data_type),
                                "nullable": str(nullable).upper() != "NO",
                            }
                            for name, data_type, nullable in rows
                        ]
                        # Refine text columns from a sample to recover JSON, UUID, BINARY, etc.
                        headers, sample_rows = _sample_raw_table(
                            conn, table, schema, dialect=_dialect_key(cfg)
                        )
                        if sample_rows and headers:
                            name_to_idx = {n: i for i, n in enumerate(headers)}
                            for col in result:
                                if col["inferred_type"] == "string":
                                    idx = name_to_idx.get(col["name"])
                                    if idx is None:
                                        continue
                                    values = [
                                        row[idx]
                                        for row in sample_rows
                                        if idx < len(row)
                                    ]
                                    inferred = _infer_logical_from_samples(
                                        values, field_name=col["name"]
                                    )
                                    if inferred and inferred != "string":
                                        col["inferred_type"] = inferred
                        return {
                            "ok": True,
                            "columns": result,
                            "tables": [table],
                            "schema": schema or "",
                        }
            except Exception:
                logger.warning(
                    "generic_sql inspector path failed for %s.%s; falling back to sample",
                    schema,
                    table,
                    exc_info=True,
                )

            with engine.connect() as conn:
                headers, sample_rows = _sample_raw_table(
                    conn, table, schema, dialect=_dialect_key(cfg)
                )
                result = [
                    {
                        "name": name,
                        "inferred_type": "string",
                        "nullable": True,
                    }
                    for name in headers
                ]
                if sample_rows:
                    for idx, col in enumerate(result):
                        values = [row[idx] for row in sample_rows if idx < len(row)]
                        inferred = _infer_logical_from_samples(
                            values, field_name=col["name"]
                        )
                        if inferred:
                            col["inferred_type"] = inferred
                return {
                    "ok": True,
                    "columns": result,
                    "tables": [table],
                    "schema": schema or "",
                }

        result = []
        for col in columns:
            result.append(
                {
                    "name": col["name"],
                    "inferred_type": _logical_type_from_sa(col.get("type")),
                    "nullable": col.get("nullable", True),
                }
            )

        # Sample the table to narrow generic String columns to JSON, UUID, BINARY, etc.
        try:
            with engine.connect() as conn:
                headers, sample_rows = _sample_raw_table(
                    conn, table, schema, dialect=_dialect_key(cfg)
                )
                if sample_rows:
                    for idx, col in enumerate(result):
                        if col["inferred_type"] == "string":
                            values = [row[idx] for row in sample_rows if idx < len(row)]
                            inferred = _infer_logical_from_samples(
                                values, field_name=col["name"]
                            )
                            if inferred and inferred != "string":
                                col["inferred_type"] = inferred
        except Exception:
            logger.warning(
                "generic_sql sample refine failed for %s.%s",
                schema,
                table,
                exc_info=True,
            )

        return {
            "ok": True,
            "columns": result,
            "tables": [table],
            "schema": schema or "",
        }
    except Exception as exc:
        logger.warning("generic_sql introspect failed", exc_info=True)
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: SQL schema introspection failed",
            "columns": [],
            "tables": [],
        }
    finally:
        release_engine(engine)


def _drop_table_sql(engine: Any, qualified: str, table: str) -> str:
    """Conditional DROP in the dialect's own spelling.

    ``DROP TABLE IF EXISTS`` is a syntax error on Oracle (ORA-00933) and did not
    exist before SQL Server 2016, so the statement failed and the drop fell
    through to a fallback that could no-op silently.
    """
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "").lower()
    if dialect == "oracle":
        return (
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {qualified}'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
    if dialect == "mssql":
        return f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {qualified}"
    return f"DROP TABLE IF EXISTS {qualified}"


def _require_table_gone(engine: Any, table: str, schema: str | None) -> None:
    """Prove the drop happened; a no-op drop is silent data corruption.

    ``full_refresh`` treats a successful drop as "the destination is empty" and
    then loads. When the DROP addressed a name the engine folded differently it
    reported success while every row stayed, so the refresh appended onto rows it
    had declared cleared — duplicated keys at best, doubled data at worst.
    """
    from services.sql_object_identity import resolve_object_identity

    ident = resolve_object_identity(engine, table, schema)
    if ident.resolved and ident.exists:
        raise RuntimeError(
            f"DROP TABLE reported success but {schema or ''}.{table} is still in "
            "the catalog — refusing to treat the destination as cleared."
        )


def drop_table(cfg: dict[str, Any], table: str, schema: str | None = None) -> bool:
    """Drop a table using SQLAlchemy dialect-aware DDL with a raw fallback.

    Raises on a failed drop rather than returning ``False``. A caller deciding
    whether a ``full_refresh`` actually cleared the destination cannot tell a
    swallowed permission error from "nothing to drop", and guessing wrong means
    appending onto rows that were supposed to be gone.
    """
    if not SQLALCHEMY_AVAILABLE:
        return False
    engine = _engine(cfg)
    try:
        schema = schema or _schema_name(cfg)
        # A folding engine holding a lower-case-quoted table must be dropped
        # under that spelling: dropping the folded name is a no-op, and a
        # full_refresh would then append onto rows it reported as cleared.
        table, schema = _resolve_physical_table_ident(engine, table, schema)
        qualified = _qualified_table_ref(cfg, table, schema)
        with engine.connect() as conn:
            conn.execute(sa.text(_drop_table_sql(engine, qualified, table)))
            conn.commit()
        _require_table_gone(engine, table, schema)
        return True
    except Exception as primary_exc:
        # Some dialects reject the raw IF EXISTS form; retry via dialect DDL
        # before giving up, but surface the original error if that also fails.
        try:
            from sqlalchemy.sql import quoted_name

            # Quoted: an unquoted Table() name is case-insensitive, so on Oracle
            # the fallback compiled ``DROP TABLE scn_dst``, checkfirst read the
            # folded SCN_DST as absent, and the drop became a silent no-op.
            table_obj = sa.Table(
                quoted_name(table, True), sa.MetaData(), schema=schema
            )
            table_obj.drop(engine, checkfirst=True)
            _require_table_gone(engine, table, schema)
            return True
        except Exception as fallback_exc:
            logger.error(
                "Failed to drop table %s (primary: %s; dialect DDL fallback: %s)",
                table,
                primary_exc,
                fallback_exc,
            )
            raise primary_exc
    finally:
        # Whether the drop succeeded or not, any reflected shape for this table
        # is no longer trustworthy.
        reflection_cache.invalidate_table(engine, schema, table)
        release_engine(engine)


def delete_by_primary_keys(
    cfg: dict[str, Any],
    table: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None = None,
) -> int:
    """Delete rows by primary key using a dialect-aware parameterized statement.

    Raises on driver failure. Returning ``0`` here made a failed DELETE
    indistinguishable from "those keys were already absent", so CDC read the
    failure as an idempotent success and advanced its cursor past tombstones
    that were never applied.
    """
    if not SQLALCHEMY_AVAILABLE or not keys:
        return 0
    engine = _engine(cfg)
    try:
        schema = schema or _schema_name(cfg)
        qualified = _qualified_table_ref(cfg, table, schema)
        from services.dialect_profiles import quote_char_for

        q = quote_char_for(_dialect_key(cfg)) or '"'
        if q == "[":
            pk_quoted = f"[{str(primary_key_column).replace(']', ']]')}]"
        else:
            pk_quoted = quote_sql_identifier(primary_key_column, q)
        placeholders = ",".join([f":k{i}" for i in range(len(keys))])
        params = {f"k{i}": k for i, k in enumerate(keys)}
        stmt = f"DELETE FROM {qualified} WHERE {pk_quoted} IN ({placeholders})"  # nosec B608
        with engine.connect() as conn:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            n = result.rowcount
            return len(keys) if n is None or int(n) < 0 else int(n)
    except Exception as exc:
        from connectors.table_manager import DestinationDeleteError

        logger.error("Delete by primary key failed on %s: %s", table, exc, exc_info=exc)
        raise DestinationDeleteError(table, exc) from exc
    finally:
        release_engine(engine)


def fetch_pk_lsn_map(
    cfg: dict[str, Any],
    table: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None = None,
    *,
    lsn_column: str = "_df_lsn",
) -> dict[str, Any]:
    """Return ``{pk: _df_lsn}`` for SQLAlchemy destinations (missing rows → None)."""
    existing: dict[str, Any] = {str(k): None for k in keys}
    if not SQLALCHEMY_AVAILABLE or not keys:
        return existing
    engine = _engine(cfg)
    try:
        schema = schema or _schema_name(cfg)
        qualified = _qualified_table_ref(cfg, table, schema)
        from services.dialect_profiles import quote_char_for

        q = quote_char_for(_dialect_key(cfg)) or '"'
        if q == "[":
            pk_quoted = f"[{str(primary_key_column).replace(']', ']]')}]"
            lsn_quoted = f"[{str(lsn_column).replace(']', ']]')}]"
        else:
            pk_quoted = quote_sql_identifier(primary_key_column, q)
            lsn_quoted = quote_sql_identifier(lsn_column, q)
        placeholders = ",".join([f":k{i}" for i in range(len(keys))])
        params = {f"k{i}": k for i, k in enumerate(keys)}
        stmt = (
            f"SELECT {pk_quoted}, {lsn_quoted} FROM {qualified} "
            f"WHERE {pk_quoted} IN ({placeholders})"
        )  # nosec B608
        with engine.connect() as conn:
            result = conn.execute(sa.text(stmt), params)
            for row in result.fetchall() or []:
                existing[str(row[0])] = row[1]
        return existing
    finally:
        release_engine(engine)


_ORACLE_TSTZ_READ_FORMAT: Final = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'


def _tz_safe_projection(cfg: dict[str, Any], cols: list[Any]) -> list[Any]:
    """Select list that keeps an Oracle TIMESTAMP WITH TIME ZONE offset.

    python-oracledb hands back a *naive* ``datetime`` for TIMESTAMP WITH
    [LOCAL] TIME ZONE — a ``08:31+05:30`` row arrives as bare ``08:31`` and the
    instant is silently rewritten. Rendering those columns server side with
    ``TO_CHAR`` keeps the offset on the wire; every other dialect and column is
    projected unchanged.
    """
    if not _is_oracle_wire(_dialect_key(cfg), str(cfg.get("type") or "")):
        return list(cols)
    out: list[Any] = []
    for col in cols:
        col_type = getattr(col, "type", None)
        tz_carrier = bool(
            getattr(col_type, "timezone", False)
            or getattr(col_type, "local_timezone", False)
        )
        if tz_carrier and isinstance(col_type, sa.DateTime):
            out.append(
                sa.func.to_char(col, _ORACLE_TSTZ_READ_FORMAT).label(col.name)
            )
        else:
            out.append(col)
    return out


def _is_mysql_timestamp_sa(col: Any) -> bool:
    """True for a reflected MySQL TIMESTAMP (instant), not DATETIME (wall-clock)."""
    col_type = getattr(col, "type", None)
    if col_type is None:
        return False
    return type(col_type).__name__.upper() == "TIMESTAMP"


def _serialize_source_cell(value: Any, col: Any, dialect: str) -> str:
    """Transfer-wire spelling. MySQL TIMESTAMP keeps UTC polarity; DATETIME does not."""
    from services.dest_dialect_facts import _normalize_dest_db
    from services.timezone_policy import mysql_timestamp_instant_wire

    if _normalize_dest_db(dialect) == "mysql" and _is_mysql_timestamp_sa(col):
        value = mysql_timestamp_instant_wire(value)
    if _is_json_sa(col):
        from services.json_polarity import json_document_wire

        return json_document_wire(value)
    return cell_to_string(value, preserve_sql_null=True)


def _is_json_sa(col: Any) -> bool:
    col_type = getattr(col, "type", None)
    if col_type is None:
        return False
    if isinstance(col_type, sa.JSON):
        return True
    name = type(col_type).__name__.upper()
    return name in {"JSON", "JSONB"} or name.endswith("JSON") or name.endswith("JSONB")


def _serialize_source_row(row: Any, cols: list[Any], dialect: str) -> list[str]:
    return [
        _serialize_source_cell(value, col, dialect)
        for value, col in zip(row, cols)
    ]


def _read_table_raw(
    conn: Any,
    table: str,
    schema: str | None,
    offset: int,
    limit: int,
    *,
    dialect: str = "ansi",
) -> tuple[list[str], list[list[Any]]]:
    """Fallback read for engines whose SQLAlchemy reflection is incomplete."""
    from connectors.sql_identifiers import quote_table_ref
    from services.dialect_profiles import (
        denormalize_result_key,
        page_clause,
        quote_char_for,
        zero_row_probe_sql,
    )

    qualified = quote_table_ref(table, schema, dialect=dialect)
    base = f"SELECT * FROM {qualified}"  # nosec B608
    # Discover columns so we can ORDER BY the first one — bare LIMIT/OFFSET is
    # non-deterministic and silently duplicates/skips rows across pages.
    probe = conn.execute(sa.text(zero_row_probe_sql(dialect, qualified)))
    headers = list(probe.keys())
    if not headers:
        return [], []
    q = quote_char_for(dialect) or '"'
    # Oracle/DB2/Snowflake store case-insensitive names folded upper and the
    # driver hands them back lowercased; quoting that literal yields ORA-00904.
    # Restore the physical spelling before quoting.
    order_header = _orderable_header(dialect, headers, probe)
    if order_header is None:
        # Oracle refuses ORDER BY on a LOB (ORA-22848); an all-CLOB table would
        # otherwise be unreadable instead of merely unsorted.
        order_col = "ROWID"
    else:
        order_name = denormalize_result_key(dialect, str(order_header))
        if q == "[":
            order_col = f"[{order_name.replace(']', ']]')}]"
        else:
            order_col = quote_sql_identifier(order_name, q)
    sql = f"{base} ORDER BY {order_col} {page_clause(dialect, offset, limit)}"  # nosec B608
    result = conn.execute(sa.text(sql))
    headers = list(result.keys())
    rows = [
        [cell_to_string(value, preserve_sql_null=True) for value in row]
        for row in result.fetchall()
    ]
    return headers, rows


#: Driver type names that cannot appear in an ORDER BY on Oracle/DB2.
_UNORDERABLE_TYPE_TOKENS: Final = ("CLOB", "NCLOB", "BLOB", "LONG", "XMLTYPE")


def _orderable_header(dialect: str, headers: list[str], probe: Any) -> str | None:
    """First column usable as a deterministic page order, or ``None``.

    LOB columns are not sortable on Oracle/DB2, so paging a table whose first
    column is a CLOB raised ORA-22848 and the whole read failed. Prefer the
    first non-LOB column; callers fall back to a pseudo-column when every
    column is a LOB.
    """
    if not headers:
        return None
    if (dialect or "").lower() not in {"oracle", "db2", "ibm_db_sa"}:
        return headers[0]
    description = getattr(getattr(probe, "cursor", None), "description", None)
    if not description:
        return headers[0]
    for idx, header in enumerate(headers):
        if idx >= len(description):
            # No type evidence for this column: keep the ordinary first-column
            # order rather than silently switching the page order to a
            # pseudo-column.
            return header
        type_name = str(getattr(description[idx][1], "name", description[idx][1]))
        if not any(tok in type_name.upper() for tok in _UNORDERABLE_TYPE_TOKENS):
            return header
    return None


def _count_table_raw(
    conn: Any,
    table: str,
    schema: str | None,
    *,
    dialect: str = "ansi",
) -> int | None:
    from connectors.sql_identifiers import quote_table_ref

    qualified = quote_table_ref(table, schema, dialect=dialect)
    try:
        return conn.execute(sa.text(f"SELECT COUNT(*) FROM {qualified}")).scalar()  # nosec B608
    except Exception:
        # Never fabricate len(rows) as cardinality — that stops streaming after page one.
        return None


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
    type: str = "",
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 100_000,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Read a batch of rows from any SQLAlchemy-supported database."""
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("SQLAlchemy is not installed")

    cfg = _cfg_from_params(
        host,
        port,
        database,
        username,
        password,
        schema,
        connection_string,
        ssl,
        type=type,
    )
    engine = _engine(cfg)
    schema_name = _schema_name(cfg)

    try:
        with engine.connect() as conn:
            # RisingWave streams writes through a barrier; issue a FLUSH so the
            # subsequent SELECT observes rows written by a just-finished ingest.
            if (cfg.get("type") or "").lower() == "risingwave":
                with contextlib.suppress(Exception):
                    conn.execute(sa.text("FLUSH"))
            try:
                table_obj = _reflect_table(engine, table, schema_name, columns)
                selected_cols = list(table_obj.c)
                if columns:
                    selected_cols = [
                        table_obj.c[c] for c in columns if c in table_obj.c
                    ]
                else:
                    columns = selected_cols = list(table_obj.c)

                stmt = sa.select(*_tz_safe_projection(cfg, selected_cols))
                # Stable order from page 0 — unordered OFFSET pages skip/duplicate under concurrent writes.
                pk_cols = (
                    [c for c in table_obj.primary_key.columns]
                    if table_obj.primary_key is not None
                    else []
                )
                order_cols = (
                    list(pk_cols)
                    if pk_cols
                    else [selected_cols[0]]
                    if selected_cols
                    else []
                )
                if order_cols:
                    stmt = stmt.order_by(*order_cols)
                stmt = stmt.offset(offset).limit(limit)

                fetched = conn.execute(stmt).fetchall()
                headers = [c.name for c in selected_cols]
                dialect = _dialect_key(cfg)
                rows = [_serialize_source_row(row, selected_cols, dialect) for row in fetched]

                if known_total_rows is not None:
                    total = known_total_rows
                else:
                    try:
                        total = conn.execute(
                            sa.select(sa.func.count()).select_from(table_obj)
                        ).scalar()
                    except Exception:
                        total = None
            except Exception:
                # Engines like RisingWave/QuestDB have incomplete pg_catalog reflection.
                headers, rows = _read_table_raw(
                    conn, table, schema_name, offset, limit, dialect=_dialect_key(cfg)
                )
                if known_total_rows is not None:
                    total = known_total_rows
                else:
                    total = _count_table_raw(
                        conn, table, schema_name, dialect=_dialect_key(cfg)
                    )

        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        release_engine(engine)


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
    type: str = "",
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 100_000,
    known_total_rows: int | None = None,
    scan_state: dict[str, Any],
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY`` with ``fetchmany`` — no OFFSET, one login.

    Covers SQL Server, Oracle, Databricks, and other SQLAlchemy dialects that
    previously opened a new connection and OFFSET-paged every chunk.
    """
    from connectors.sql_snapshot_scan import close_table_scan

    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("SQLAlchemy is not installed")

    if not scan_state.get("started"):
        cfg = _cfg_from_params(
            host,
            port,
            database,
            username,
            password,
            schema,
            connection_string,
            ssl,
            type=type,
        )
        engine = _engine(cfg)
        schema_name = _schema_name(cfg)
        dialect = _dialect_key(cfg)
        conn = engine.connect()
        try:
            if (cfg.get("type") or "").lower() == "risingwave":
                with contextlib.suppress(Exception):
                    conn.execute(sa.text("FLUSH"))
            try:
                table_obj = _reflect_table(engine, table, schema_name, columns)
                selected_cols = list(table_obj.c)
                if columns:
                    selected_cols = [
                        table_obj.c[c] for c in columns if c in table_obj.c
                    ]
                pk_cols = (
                    [c for c in table_obj.primary_key.columns]
                    if table_obj.primary_key is not None
                    else []
                )
                order_cols = (
                    list(pk_cols)
                    if pk_cols
                    else [selected_cols[0]]
                    if selected_cols
                    else []
                )
                stmt = sa.select(*_tz_safe_projection(cfg, selected_cols))
                if order_cols:
                    stmt = stmt.order_by(*order_cols)
                # Statement-scoped: on the connection it leaks into later DDL,
                # which then compiles as DECLARE CURSOR FOR <ddl> and fails.
                result = conn.execute(stmt.execution_options(stream_results=True))
                headers = [c.name for c in selected_cols]
                serialize = True
            except Exception:
                headers, result = _open_raw_table_scan(
                    conn, table, schema_name, dialect=dialect
                )
                selected_cols = []
                serialize = False
            if known_total_rows is not None:
                total = known_total_rows
            else:
                total = _count_table_raw(
                    conn, table, schema_name, dialect=dialect
                )
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            release_engine(engine)
            raise
        scan_state.update(
            started=True,
            engine=engine,
            conn=conn,
            result=result,
            headers=headers,
            total=total,
            selected_cols=selected_cols,
            dialect=dialect,
            serialize=serialize,
        )

    result = scan_state.get("result")
    headers = list(scan_state.get("headers") or [])
    total = scan_state.get("total")
    raw = result.fetchmany(max(1, int(limit))) if result is not None else []
    if not raw:
        close_table_scan(scan_state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    if scan_state.get("serialize"):
        selected_cols = list(scan_state.get("selected_cols") or [])
        dialect = str(scan_state.get("dialect") or "ansi")
        rows = [_serialize_source_row(row, selected_cols, dialect) for row in raw]
    else:
        rows = [
            [cell_to_string(value, preserve_sql_null=True) for value in row]
            for row in raw
        ]
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


def _open_raw_table_scan(
    conn: Any,
    table: str,
    schema: str | None,
    *,
    dialect: str = "ansi",
) -> tuple[list[str], Any]:
    """Open one OFFSET-free SELECT for engines with thin reflection."""
    from connectors.sql_identifiers import quote_table_ref
    from services.dialect_profiles import (
        denormalize_result_key,
        quote_char_for,
        zero_row_probe_sql,
    )

    qualified = quote_table_ref(table, schema, dialect=dialect)
    base = f"SELECT * FROM {qualified}"  # nosec B608
    probe = conn.execute(sa.text(zero_row_probe_sql(dialect, qualified)))
    headers = list(probe.keys())
    if not headers:
        return [], None
    q = quote_char_for(dialect) or '"'
    order_header = _orderable_header(dialect, headers, probe)
    if order_header is None:
        order_col = "ROWID"
    else:
        order_name = denormalize_result_key(dialect, str(order_header))
        if q == "[":
            order_col = f"[{order_name.replace(']', ']]')}]"
        else:
            order_col = quote_sql_identifier(order_name, q)
    sql = f"{base} ORDER BY {order_col}"  # nosec B608
    result = conn.execute(sa.text(sql))
    return list(result.keys()), result


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
    cursor_after: str | None,
    type: str = "",
    columns: list[str] | None = None,
    limit: int = 20_000,
    cursor_primary_key: str | None = None,
    cursor_key_columns: list[str] | None = None,
) -> ReadBatch:
    """Cursor/keyset pagination for incremental and streaming transfers.

    ``cursor_key_columns`` (Phase F2) is the ordered composite key for seek
    pagination — N-col OR/AND, portable to SQL Server / Oracle. When omitted,
    ``cursor_column`` + optional ``cursor_primary_key`` keep the legacy 1-/2-col
    path (including ``cursor|pk`` bookmarks).
    """
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("SQLAlchemy is not installed")

    from services.keyset_pagination import present_cursor_bookmark, sqlalchemy_keyset_clause

    cfg = _cfg_from_params(
        host,
        port,
        database,
        username,
        password,
        schema,
        connection_string,
        ssl,
        type=type,
    )
    engine = _engine(cfg)
    schema_name = _schema_name(cfg)

    try:
        with engine.connect() as conn:
            if (cfg.get("type") or "").lower() == "risingwave":
                with contextlib.suppress(Exception):
                    conn.execute(sa.text("FLUSH"))
            table_obj = _reflect_table(engine, table, schema_name, columns)
            selected_cols = list(table_obj.c)
            if columns:
                selected_cols = [table_obj.c[c] for c in columns if c in table_obj.c]
            else:
                columns = [c.name for c in selected_cols]

            # Resolve ordered key columns for seek.
            key_names: list[str] = []
            if cursor_key_columns:
                key_names = [c for c in cursor_key_columns if c and c in table_obj.c]
            else:
                if cursor_column and cursor_column in table_obj.c:
                    key_names = [cursor_column]
                pk = (cursor_primary_key or "").strip()
                if pk and pk != cursor_column and pk in table_obj.c:
                    key_names.append(pk)
            if not key_names:
                raise ValueError(
                    f"Cursor/keyset columns not found in table {table} "
                    f"(cursor_column={cursor_column!r}, "
                    f"cursor_key_columns={cursor_key_columns!r})"
                )

            key_cols = [table_obj.c[n] for n in key_names]
            stmt = sa.select(*_tz_safe_projection(cfg, selected_cols))
            bookmark = present_cursor_bookmark(cursor_after)
            if bookmark is not None:
                stmt = stmt.where(
                    sqlalchemy_keyset_clause(sa, key_cols, bookmark)
                )
            stmt = stmt.order_by(*key_cols).limit(limit)

            fetched = conn.execute(stmt).fetchall()
            headers = [c.name for c in selected_cols]
            dialect = _dialect_key(cfg)
            rows = [_serialize_source_row(row, selected_cols, dialect) for row in fetched]

        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        release_engine(engine)


def _delete_by_keys(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    chunk_size: int = 1000,
) -> None:
    """Delete existing rows that match the provided conflict keys.

    Uses equality ``OR (a=1 AND b=2)`` clauses instead of ``(a,b) IN (...)`` so
    dialects with limited tuple-IN support still work. Deletions are chunked to
    avoid statement-length limits.

    Null/empty conflict keys are refused — ``col IS NULL`` would mass-delete
    every destination row with a null key (silent data loss).
    """
    if not rows:
        return
    for row in rows:
        for c in conflict_cols:
            val = row.get(c) if isinstance(row, dict) else None
            if _is_nullish_conflict_key(val):
                raise ValueError(
                    f"upsert delete-by-keys refused null/empty conflict key {c!r} — "
                    "IS NULL predicates would mass-delete destination rows"
                )
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        clauses = [
            sa.and_(
                *[table_obj.c[c] == row[c] for c in conflict_cols]
            )
            for row in chunk
        ]
        conn.execute(sa.delete(table_obj).where(sa.or_(*clauses)))


def _prefetch_existing_lsn(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
) -> dict[tuple[Any, ...], Any]:
    """Return mapping from conflict key tuple to the existing ``_df_lsn`` value.

    Fail closed: if the lookup cannot run, raise rather than assuming no
    prior LSN (which would let stale CDC redelivery overwrite the sink).
    """
    existing: dict[tuple[Any, ...], Any] = {}
    if not rows or not conflict_cols:
        return existing
    clauses = []
    for row in rows:
        clauses.append(
            sa.and_(
                *[
                    table_obj.c[c].is_(None)
                    if _is_nullish_conflict_key(row.get(c))
                    else table_obj.c[c] == row[c]
                    for c in conflict_cols
                ]
            )
        )
    if not clauses:
        return existing
    stmt = sa.select(
        *[table_obj.c[c] for c in conflict_cols],
        table_obj.c[DF_LSN_COL],
    ).where(sa.or_(*clauses))
    for found in conn.execute(stmt):
        # Use positional indices because some dialects (DuckDB, SQLite raw) return
        # plain tuples rather than key-addressable Row objects.
        key = tuple(_conflict_key_identity(found[i]) for i in range(len(conflict_cols)))
        existing[key] = found[len(conflict_cols)]
    return existing




def _upsert_batch(
    conn: Any,
    table_obj: sa.Table,
    batch: list[dict[str, Any]],
    conflict_columns: list[str],
    target_cols: list[str],
    dialect_name: str,
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> int:
    """Write a batch idempotently using the best native upsert available.

    Deduplicates the batch on the conflict key, then:
      * PostgreSQL, SQLite, MySQL/MariaDB: native ``ON CONFLICT`` /
        ``ON DUPLICATE KEY`` upsert.
      * SQL Server (mssql): native ``MERGE … WITH (HOLDLOCK)`` + NULL-safe ON;
        falls back to delete+insert if MERGE fails (missing unique key, etc.).
      * Oracle: native ``MERGE INTO`` + NULL-safe ON via private/global temp
        stage; falls back to delete+insert if MERGE fails.
      * DuckDB: native ``MERGE INTO`` + NULL-safe ON (no unique index required);
        falls back to delete+insert if MERGE fails.
      * ClickHouse: ``INSERT`` into ``ReplacingMergeTree`` (engine-level lazy
        dedupe — Airbyte class). Never delete+insert.
      * DB2 (``ibm_db_sa``): native ``MERGE INTO`` + NULL-safe ON via DGTT;
        falls back to delete+insert if MERGE fails.
      * Teradata (``teradatasql``): native ``MERGE INTO`` via VOLATILE stage;
        PI equality ON only (engine forbids NULL equate); never UPDATE PI cols.
      * Trino/Presto/Athena/Hive/Impala: native ``MERGE INTO`` + NULL-safe ON
        via VALUES chunks (Hive ACID / Impala Iceberg / Athena MoR). Query
        engines fall back to append-only INSERT — never invent delete+insert.
      * SAP HANA (``hana``): native ``MERGE INTO`` + NULL-safe ON via local
        temporary column table; falls back to delete+insert if MERGE fails.
      * Vertica: native ``MERGE INTO`` + NULL-safe ON via local temp stage;
        falls back to delete+insert if MERGE fails.
      * Netezza / IBM NPS (``nzpsql``, 7.2.1+): native ``MERGE INTO`` +
        NULL-safe ON via TEMP stage; falls back to delete+insert if MERGE fails.
      * Informix: native ``MERGE INTO`` + NULL-safe ON via TEMP ``WITH NO LOG``;
        falls back to delete+insert if MERGE fails.
      * Firebird: native ``MERGE INTO`` + NULL-safe ON via ``RDB$DATABASE``
        row stage; falls back to delete+insert if MERGE fails.
      * Sybase ASE / SAP ASE (15.7+): native ``MERGE`` + NULL-safe ON via
        ``#temp`` stage; falls back to delete+insert if MERGE fails.
      * Everyone else: chunked DELETE by equality keys followed by INSERT,
        except when dest-owned lattice columns (mirror ``_deleted``) are
        present — then portable UPDATE+INSERT (never DELETE).

    Returns the number of destination rows actually written in this batch.
    """
    from connectors.writer_common import resolve_conflict_targets

    try:
        conflict_cols = resolve_conflict_targets(
            conflict_columns, target_cols, strict=True
        )
    except ValueError:
        raise
    if not conflict_cols:
        result = conn.execute(table_obj.insert(), batch)
        return multi_row_insert_written(result, len(batch))

    from connectors.writer_common import partition_dense_upsert_rows

    # Quarantine null/empty keys — never abort the whole MERGE chunk.
    batch = partition_dense_upsert_rows(
        batch,
        conflict_cols,
        rejected_details=rejected_details,
        policy=policy,
    )
    if not batch:
        return 0

    update_cols = [c for c in target_cols if c not in conflict_cols]
    lsn_guarded = DF_LSN_COL in target_cols

    # Keep the highest-LSN row per conflict key so CDC redelivery inside one
    # batch is deterministic; fall back to last-wins when no LSN column.
    if lsn_guarded:
        best: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in batch:
            key = tuple(_conflict_key_identity(row[c]) for c in conflict_cols)
            prev = best.get(key)
            if prev is None or compare_lsn(row.get(DF_LSN_COL), prev.get(DF_LSN_COL)) >= 0:
                best[key] = row
        rows = list(best.values())
        # Prefetch existing LSNs and drop stale rows before any write so
        # ``rows_skipped`` accounting is exact and redelivery cannot regress.
        existing_lsn = _prefetch_existing_lsn(conn, table_obj, rows, conflict_cols)
        filtered: list[dict[str, Any]] = []
        for row in rows:
            key = tuple(_conflict_key_identity(row.get(c)) for c in conflict_cols)
            prior = existing_lsn.get(key)
            incoming = row.get(DF_LSN_COL)
            if incoming is not None and compare_lsn(incoming, prior) <= 0:
                continue
            filtered.append(row)
        rows = filtered
    else:
        deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in batch:
            key = tuple(_conflict_key_identity(row[c]) for c in conflict_cols)
            deduped[key] = row
        rows = list(deduped.values())

    from services.mirror_engine import lattice_columns_on_table, strip_lattice_from_upsert

    lattice = lattice_columns_on_table(conn, table_obj)
    rows, update_cols, target_cols = strip_lattice_from_upsert(
        rows, update_cols, target_cols, lattice
    )

    def _native_upsert() -> int | None:
        try:
            if dialect_name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                from connectors.writer_common import (
                    DF_LSN_COL,
                    postgres_lsn_update_guard_sql,
                )

                stmt = pg_insert(table_obj).values(rows)
                if update_cols:
                    kwargs_pg: dict[str, Any] = {
                        "index_elements": conflict_cols,
                        "set_": {c: stmt.excluded[c] for c in update_cols},
                    }
                    # At-least-once guard: only apply when incoming _df_lsn is newer.
                    if DF_LSN_COL in target_cols and DF_LSN_COL in update_cols:
                        kwargs_pg["where"] = sa.text(
                            postgres_lsn_update_guard_sql(table_obj.name)
                        )
                    stmt = stmt.on_conflict_do_update(**kwargs_pg)
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                conn.execute(stmt)
                if lsn_guarded:
                    # Rows were pre-filtered to those with a strictly newer LSN,
                    # so every row in ``rows`` is applied. Rowcount counts
                    # conflicts as inserted for PG, which would swallow skips.
                    return len(rows)
                return len(rows)

            if dialect_name == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                from connectors.writer_common import (
                    DF_LSN_COL,
                    sqlite_lsn_update_guard_sql,
                )

                stmt = sqlite_insert(table_obj).values(rows)
                if update_cols:
                    kwargs_sqlite: dict[str, Any] = {
                        "index_elements": conflict_cols,
                        "set_": {c: stmt.excluded[c] for c in update_cols},
                    }
                    if DF_LSN_COL in target_cols and DF_LSN_COL in update_cols:
                        kwargs_sqlite["where"] = sa.text(
                            sqlite_lsn_update_guard_sql(table_obj.name)
                        )
                    stmt = stmt.on_conflict_do_update(**kwargs_sqlite)
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                conn.execute(stmt)
                if lsn_guarded:
                    return len(rows)
                return len(rows)

            if dialect_name in ("mysql", "mariadb"):
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                from connectors.writer_common import (
                    DF_LSN_COL,
                    mysql_lsn_values_newer_sql,
                )

                # MySQL ON DUPLICATE KEY UPDATE reports affected_rows as 2 for
                # updates and 1 for inserts, so rowcount cannot be trusted for
                # per-source-row accounting. Use the delete+insert fallback when
                # an LSN guard is required so the skip count is exact.
                if lsn_guarded:
                    return None

                stmt = mysql_insert(table_obj).values(rows)
                if update_cols:
                    if DF_LSN_COL in target_cols and DF_LSN_COL in update_cols:
                        newer = mysql_lsn_values_newer_sql(DF_LSN_COL, quote="`")
                        set_map = {
                            c: sa.text(f"IF({newer}, VALUES(`{c}`), `{c}`)")
                            for c in update_cols
                        }
                        stmt = stmt.on_duplicate_key_update(set_map)
                    else:
                        stmt = stmt.on_duplicate_key_update(
                            {c: stmt.inserted[c] for c in update_cols}
                        )
                else:
                    stmt = stmt.prefix_with("IGNORE")
                conn.execute(stmt)
                return len(rows)

            if dialect_name == "mssql":
                # Rows already LSN-filtered above when ``_df_lsn`` is present.
                return _mssql_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name == "oracle":
                return _oracle_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name == "duckdb":
                return _duckdb_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name == "clickhouse" or str(dialect_name).startswith("clickhouse"):
                return _clickhouse_replacing_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"ibm_db_sa", "db2"}:
                return _db2_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"teradatasql", "teradata"}:
                return _teradata_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {
                "trino",
                "presto",
                "awsathena",
                "athena",
                "hive",
                "impala",
            }:
                return _trino_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"hana", "sap_hana"}:
                return _hana_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"vertica", "vertica_python"}:
                return _vertica_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"netezza", "nzpsql", "nzpy"}:
                return _netezza_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"informix", "ifx"}:
                return _informix_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"firebird", "firebird2", "fdb"}:
                return _firebird_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )

            if dialect_name in {"sybase", "ase", "sap_ase", "sybase_ase"}:
                return _sybase_merge_upsert(
                    conn,
                    table_obj,
                    rows,
                    conflict_cols,
                    target_cols,
                    update_cols,
                )
        except (sa.exc.SQLAlchemyError, OSError, ValueError):
            # Native upsert can fail if the table lacks the required unique
            # index/constraint.  Roll back the aborted transaction so the
            # delete+insert fallback can run cleanly.
            logger.debug(
                "native upsert unavailable; using delete+insert fallback", exc_info=True
            )
            try:
                conn.rollback()
            except (sa.exc.SQLAlchemyError, OSError) as exc:
                logger.warning("chunk rollback failed: %s", exc, exc_info=exc)
            return None
        return None

    native_count = _native_upsert()
    if native_count is not None:
        return native_count

    # delete+insert: drop stale CDC rows before deleting so redelivery
    # cannot regress state when ``_df_lsn`` is present.
    apply_rows = rows
    if lsn_guarded:
        filtered: list[dict[str, Any]] = []
        for row in rows:
            incoming_lsn = row.get(DF_LSN_COL)
            key_clause = sa.and_(
                *[table_obj.c[c] == row[c] for c in conflict_cols]
            )
            existing = conn.execute(
                sa.select(table_obj.c[DF_LSN_COL]).where(key_clause).limit(1)
            ).fetchone()
            if existing is not None and compare_lsn(incoming_lsn, existing[0]) <= 0:
                continue
            filtered.append(row)
        apply_rows = filtered

    # ClickHouse must never DELETE+INSERT even if native path failed — mutations
    # race ReplacingMergeTree merges (Airbyte destination AGENTS.md). Dedup is
    # therefore deferred to the table engine: Datawrap creates ReplacingMergeTree,
    # but an operator-owned plain MergeTree keeps every duplicate. Say so rather
    # than let the run look like a clean upsert.
    if dialect_name == "clickhouse" or str(dialect_name).startswith("clickhouse"):
        if apply_rows:
            logger.warning(
                "ClickHouse upsert into %s fell back to plain INSERT — dedup relies "
                "on a ReplacingMergeTree engine with ORDER BY %s. On a plain "
                "MergeTree this is at-least-once and keeps duplicate keys.",
                getattr(table_obj, "name", "table"),
                ", ".join(conflict_cols) or "<key>",
            )
            result = conn.execute(table_obj.insert(), apply_rows)
            return max(0, getattr(result, "rowcount", None) or 0) or len(apply_rows)
        return 0

    # Athena / Hive / Impala: MERGE is ACID/Iceberg-only. DELETE+INSERT invents
    # a non-transactional upsert on HDFS and fails on many table formats —
    # append-only INSERT preserves at-least-once honesty (duplicates possible).
    if (
        dialect_name in {"awsathena", "athena", "hive", "impala"}
        or str(dialect_name).startswith("athena")
        or str(dialect_name).startswith("hive")
        or str(dialect_name).startswith("impala")
    ):
        if apply_rows:
            result = conn.execute(table_obj.insert(), apply_rows)
            return max(0, getattr(result, "rowcount", None) or 0) or len(apply_rows)
        return 0

    if apply_rows:
        if lattice:
            from connectors.merge_dialects import update_insert_upsert

            return update_insert_upsert(
                conn, table_obj, apply_rows, conflict_cols, target_cols
            )
        _delete_by_keys(conn, table_obj, apply_rows, conflict_cols)
        result = conn.execute(table_obj.insert(), apply_rows)
        return max(0, getattr(result, "rowcount", None) or 0) or len(apply_rows)
    return 0


def _generic_sql_engine_label(dest_db: str) -> str:
    return {
        "databricks": "Databricks",
        "duckdb": "DuckDB",
        "clickhouse": "ClickHouse",
        "trino": "Trino",
        "presto": "Presto",
        "athena": "Athena",
        "synapse": "Synapse",
        "motherduck": "MotherDuck",
    }.get(dest_db, dest_db.title() if dest_db else "SQL")


def _generic_sql_map_kwargs(
    *,
    headers: list[str],
    data_rows: list,
    mappings: list,
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str],
    policy: Any,
    dest_kind: str,
    destination_pk_columns: list[str] | None,
    destination_column_nullability: Any,
    records: list[dict[str, Any]] | None,
    source_spool: Any,
    extra: dict[str, Any] | None,
    materialize_batch: int | None,
) -> dict[str, Any]:
    return {
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "target_cols": target_cols,
        "column_types": column_types,
        "dest_types": dest_types,
        "error_policy": policy,
        "preserve_case": True,
        "dest_kind": dest_kind,
        "destination_pk_columns": list(destination_pk_columns or []) or None,
        "destination_column_nullability": destination_column_nullability,
        "records": records,
        "source_spool": source_spool,
        "extra": extra,
        "batch_size": materialize_batch,
    }


def _generic_promote_oracle_empties(
    finished: Any,
    *,
    write_mode: str,
    conflict_columns: list[str] | None,
    dest_db: str,
) -> Any:
    """Oracle VARCHAR2 ''→NULL on dense MERGE SET is a wipe — promote to sparse omit."""
    _db = str(dest_db or "").strip().lower()
    if not (
        write_mode == "upsert"
        and conflict_columns
        and (_db in {"oracle", "oracledb", "oracle_autonomous"} or _db.startswith("oracle"))
    ):
        return finished
    from services.value_serializer import DF_MISSING_SENTINEL

    kept_dense: list[tuple] = []
    kept_nums: list[int] = []
    nums = list(finished.dense_row_numbers or [])
    for i, row in enumerate(finished.dense_rows):
        if any(isinstance(v, str) and v == "" for v in row):
            finished.sparse_rows.append(
                tuple(
                    DF_MISSING_SENTINEL if (isinstance(v, str) and v == "") else v
                    for v in row
                )
            )
            if i < len(nums):
                finished.sparse_row_numbers.append(nums[i])
        else:
            kept_dense.append(row)
            if i < len(nums):
                kept_nums.append(nums[i])
    finished.dense_rows = kept_dense
    finished.dense_row_numbers = kept_nums
    return finished


def _generic_finish_mapped_bundle(
    bundle: Any,
    *,
    target_cols: list[str],
    dest_types: dict[str, str],
    policy: Any,
    conflict_columns: list[str] | None,
    write_mode: str,
    mappings: list,
    dest_db: str,
    dialect_label: str,
    sa_col_types: dict[str, Any] | None = None,
    dialect_name: str = "",
    db_type: str = "",
) -> Any:
    """Quarantine + in-bundle dedupe + optional SA bind. Peak RAM is this bundle."""
    from connectors.sql_write_materialize import finish_sql_mapped_bundle
    from connectors.writer_common import (
        combined_mapped_rows_for_checksum,
        materialize_missing_as_null_for_dense_write,
    )

    target_types = [str(dest_types.get(c, "") or "") for c in target_cols]
    finished = finish_sql_mapped_bundle(
        bundle,
        target_cols=target_cols,
        target_types=target_types,
        policy=policy,
        dialect_label=dialect_label,
        dest_db=dest_db or "",
        mappings=mappings,
        write_mode=write_mode,
        conflict_columns=conflict_columns,
    )
    finished = _generic_promote_oracle_empties(
        finished,
        write_mode=write_mode,
        conflict_columns=conflict_columns,
        dest_db=dest_db,
    )
    if not (write_mode == "upsert" and conflict_columns):
        finished.dense_rows = materialize_missing_as_null_for_dense_write(
            finished.dense_rows
        )
    finished.dense_dicts: list[dict[str, Any]] = []
    finished.sparse_dicts: list[dict[str, Any]] = []
    if sa_col_types is not None:
        oracle_omit = write_mode == "upsert" and (
            dialect_name in {"oracle", "oracledb", "oracle_autonomous"}
            or str(dialect_name).startswith("oracle")
            or str(db_type or "").lower().startswith("oracle")
        )
        finished.dense_rows, finished.dense_dicts, finished.dense_row_numbers = (
            _generic_bind_tuple_rows(
                finished.dense_rows,
                finished.dense_row_numbers,
                target_cols=target_cols,
                dest_types=dest_types,
                sa_col_types=sa_col_types,
                dialect_name=dialect_name,
                db_type=db_type,
                rejected_details=finished.rejected_details,
                policy=policy,
                mappings=mappings,
            )
        )
        finished.sparse_rows, finished.sparse_dicts, finished.sparse_row_numbers = (
            _generic_bind_tuple_rows(
                finished.sparse_rows,
                finished.sparse_row_numbers,
                target_cols=target_cols,
                dest_types=dest_types,
                sa_col_types=sa_col_types,
                dialect_name=dialect_name,
                db_type=db_type,
                rejected_details=finished.rejected_details,
                policy=policy,
                mappings=mappings,
                sparse=True,
                oracle_upsert_omit_empty=oracle_omit,
            )
        )
    finished.checksum_rows = combined_mapped_rows_for_checksum(
        finished.dense_rows, finished.sparse_rows
    )
    finished.target_types = target_types
    return finished


def _generic_bind_tuple_rows(
    rows: list[tuple],
    row_numbers: list[int] | None,
    *,
    target_cols: list[str],
    dest_types: dict[str, str],
    sa_col_types: dict[str, Any],
    dialect_name: str,
    db_type: str,
    rejected_details: list,
    policy: Any,
    mappings: list,
    sparse: bool = False,
    oracle_upsert_omit_empty: bool = False,
) -> tuple[list[tuple], list[dict[str, Any]], list[int]]:
    """Bind one bundle to SQLAlchemy cells. Surviving tuples stay for checksum."""
    from connectors.writer_common import append_write_quarantine_detail
    from services.value_serializer import (
        DF_MISSING_SENTINEL,
        cell_to_string,
        is_missing_sentinel,
    )

    kept_rows: list[tuple] = []
    kept_dicts: list[dict[str, Any]] = []
    kept_nums: list[int] = []
    nums = list(row_numbers or [])
    for idx, row in enumerate(rows):
        cells: dict[str, Any] = {}
        hold_out = False
        src_row = nums[idx] if idx < len(nums) else idx + 1
        for i in range(len(target_cols)):
            col = target_cols[i]
            raw = row[i] if i < len(row) else None
            if sparse and is_missing_sentinel(raw):
                cells[col] = DF_MISSING_SENTINEL
                continue
            if (
                sparse
                and oracle_upsert_omit_empty
                and isinstance(raw, str)
                and raw == ""
            ):
                cells[col] = DF_MISSING_SENTINEL
                continue
            try:
                cells[col] = _to_sa_value(
                    raw,
                    str(dest_types.get(col) or "string"),
                    sa_col_types.get(col),
                    dialect_name,
                    db_type,
                )
            except ValueError as exc:
                sample = cell_to_string(raw)[:120]
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": src_row,
                        "column": col,
                        "target": col,
                        "value": sample,
                        "reason": (
                            f"generic SQL bind refused {sample!r}: {exc} "
                            "— quarantined (refuse silent NULL invent)"
                        ),
                        "policy": (
                            "coerce_null" if policy == "coerce_null" else "write_quarantine"
                        ),
                        "chars": [],
                    },
                    mapped_row=row,
                    target_cols=target_cols,
                    mappings=mappings,
                )
                if policy == "coerce_null":
                    cells[col] = DF_MISSING_SENTINEL if sparse else None
                else:
                    hold_out = True
                    break
        if hold_out:
            continue
        kept_rows.append(row)
        kept_dicts.append(cells)
        kept_nums.append(src_row)
    return kept_rows, kept_dicts, kept_nums


def iter_generic_sql_finished_bundles(
    *,
    headers: list[str],
    data_rows: list,
    mappings: list,
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str],
    policy: Any,
    conflict_columns: list[str] | None,
    write_mode: str,
    dest_db: str,
    dialect_label: str | None = None,
    destination_pk_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    records: list[dict[str, Any]] | None = None,
    source_spool: Any = None,
    extra: dict[str, Any] | None = None,
    materialize_batch: int | None = None,
    sa_col_types: dict[str, Any] | None = None,
    dialect_name: str = "",
    db_type: str = "",
) -> Any:
    from connectors.sql_write_materialize import iter_finished_sql_bundles

    label = dialect_label or _generic_sql_engine_label(dest_db)

    def _finish(bundle):
        return _generic_finish_mapped_bundle(
            bundle,
            target_cols=target_cols,
            dest_types=dest_types,
            policy=policy,
            conflict_columns=conflict_columns,
            write_mode=write_mode,
            mappings=mappings,
            dest_db=dest_db,
            dialect_label=label,
            sa_col_types=sa_col_types,
            dialect_name=dialect_name,
            db_type=db_type,
        )

    yield from iter_finished_sql_bundles(
        finish=_finish,
        **_generic_sql_map_kwargs(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            policy=policy,
            dest_kind=str(dest_db or "sql").lower(),
            destination_pk_columns=destination_pk_columns,
            destination_column_nullability=destination_column_nullability,
            records=records,
            source_spool=source_spool,
            extra=extra,
            materialize_batch=materialize_batch,
        ),
    )


def _generic_scan_finished_bundles(**kwargs: Any) -> Any:
    from connectors.sql_write_materialize import SqlWriteAccumulator

    dest_db = str(kwargs.get("dest_db") or "sql")
    label = kwargs.get("dialect_label") or _generic_sql_engine_label(dest_db)
    acc = SqlWriteAccumulator(
        target_cols=kwargs["target_cols"],
        dest_db_type=dest_db,
        dest_types=kwargs.get("dest_types") if isinstance(kwargs.get("dest_types"), dict) else {},
        dialect_label=label,
    )
    source_row_count = 0
    for finished in iter_generic_sql_finished_bundles(**kwargs):
        acc.note_rejects(finished.rejected_details, finished.transform_errors)
        source_row_count = finished.source_row_count
        del finished
    acc.stop_writing()
    return acc, source_row_count


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    type: str = "",
    **_kwargs: Any,
) -> WriteResult:
    """Write mapped rows to any SQLAlchemy-supported destination."""
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
    if not SQLALCHEMY_AVAILABLE:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or database,
            checksum="",
            chunks_completed=0,
            error="SQLAlchemy is not installed",
        )

    cfg = _cfg_from_params(
        host,
        port,
        database,
        username,
        password,
        schema,
        connection_string,
        ssl,
        type=type,
    )
    engine = _engine(cfg)
    schema_name = _schema_name(cfg)
    table_name, schema_name = _resolve_physical_table_ident(
        engine,
        table_name,
        schema_name,
        prior_spelling=str(
            _kwargs.get("dest_table_prior_spelling")
            or cfg.get("dest_table_prior_spelling")
            or ""
        ),
    )

    # SQL Server writes must fail closed on ANSI_WARNINGS — engine connect soft-applies
    # for introspect; re-assert require=True on the write connection.
    dest_for_guards = str(cfg.get("type") or type or "").lower()
    if dest_for_guards in {
        "mssql",
        "sql_server",
        "sqlserver",
        "microsoft_sql_server",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "google_cloud_sql_sql_server",
        "synapse_analytics",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
    }:
        from connectors.write_resilience import apply_mssql_session_guards

        with engine.connect() as _guard_conn:
            apply_mssql_session_guards(
                getattr(_guard_conn, "connection", None) or _guard_conn,
                require_ansi_warnings=True,
            )

    # Durable chunk ledger: without it, a transient failure after chunk k
    # committed makes the outer retry re-run this write from chunk 0 and
    # duplicate every already-landed row. Only meaningful when the caller
    # supplies a job_id to scope the ledger to this attempt chain.
    ledger_job_id = str(_kwargs.get("job_id") or "").strip()
    ledger_batch_key = str(
        _kwargs.get("write_batch_key") or ""
    ).strip() or build_write_batch_key(
        table_name=table_name,
        file_batch_idx=_kwargs.get("file_batch_idx"),
    )
    ledger_chunks_skipped = 0
    ledger_unavailable = False

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    # Bind every downstream statement to the spelling the destination stores,
    # before mapped rows, types and the Table object are keyed by these names.
    _stored_names = _resolve_physical_column_idents(
        engine, table_name, schema_name, list(target_cols)
    )
    if _stored_names:
        target_cols = [_stored_names.get(c, c) for c in target_cols]
        mappings = [
            ({**m, "target": _stored_names[str(m.get("target"))]}
             if str(m.get("target")) in _stored_names else m)
            for m in (mappings or [])
        ]
        if conflict_columns:
            conflict_columns = [_stored_names.get(c, c) for c in conflict_columns]
    if conflict_columns:
        try:
            from connectors.writer_common import resolve_conflict_targets

            conflict_columns = resolve_conflict_targets(
                conflict_columns, target_cols, strict=True
            )
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema_name or "",
                checksum="",
                chunks_completed=0,
                error=str(exc),
            )
    from services.mapping_constraints import write_mappings
    from services.type_system import is_generated_always_column

    # Omit GENERATED ALWAYS from INSERT projection by target column — never
    # index-zip mappings[i] (omits/reorder mis-stamp invents wrong DDL).
    keep_idx = [
        i for i, typ in enumerate(logical_types) if not is_generated_always_column(typ)
    ]
    if len(keep_idx) < len(logical_types):
        target_cols = [target_cols[i] for i in keep_idx]
        logical_types = [logical_types[i] for i in keep_idx]
    dest_db = (cfg.get("type") or "").lower()
    by_tgt: dict[str, dict] = {}
    for mapping in write_mappings(list(mappings or [])):
        tgt = str(mapping.get("target") or "").strip()
        if tgt and tgt not in by_tgt:
            by_tgt[tgt] = mapping
            by_tgt.setdefault(tgt.lower(), mapping)
    target_column_types = {}
    explicit_stamps: set[str] = set()
    # Studio-probed live DDL beats Map stamps for existing tables (invent cliff).
    live_dest = _kwargs.get("destination_column_types")
    live_fold = {
        str(k).lower(): str(v)
        for k, v in (live_dest.items() if isinstance(live_dest, dict) else [])
        if k and v
    }
    studio_err: str | None = None
    if isinstance(live_dest, dict) and live_dest:
        from connectors.saas_common import merge_saas_live_types

        _studio_only, studio_err = merge_saas_live_types(
            {
                str(k): str(v).strip()
                for k, v in live_dest.items()
                if k and str(v or "").strip()
            },
            list(target_cols or []),
            studio_types=None,
            product=(cfg.get("type") or "SQL").strip() or "SQL",
        )
        del _studio_only
    live_locked: set[str] = set()
    for i, col in enumerate(target_cols):
        live_hit = live_fold.get(str(col).lower())
        if live_hit:
            derived = (
                materialize_dest_ddl(dest_db, live_hit) if dest_db else str(live_hit)
            )
            target_column_types[col] = derived
            live_locked.add(col)
            continue
        # Partial Studio: do not Map-fill gaps — rematerialize or create-new refuse.
        if studio_err:
            continue
        mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
        explicit = str(
            mapping.get("target_type") or mapping.get("dest_type") or ""
        ).strip() or None
        source_type = (
            column_types.get(str(mapping.get("source") or ""))
            or (logical_types[i] if i < len(logical_types) else "string")
        )
        # Map stamps through materialize_dest_ddl so CREATE cannot invent
        # REAL→DOUBLE or BQ TIMESTAMP→DATETIME after Map stamped. Missing stamp
        # uses Decision Kernel invent_dest_type (same CREATE_NEW as Validate) —
        # never a second materialize(source) invent authority.
        if explicit:
            derived = (
                materialize_dest_ddl(dest_db, explicit, source_type=source_type)
                if dest_db
                else str(explicit)
            )
            # A stamp that only echoes the destination catalog is not an
            # operator ceiling; under backfill it may widen to the source.
            if stamp_is_operator_ceiling(mapping) or not backfill_new_fields:
                explicit_stamps.add(col)
        elif dest_db:
            from services.decision_kernel import InventContext, invent_dest_type

            derived = invent_dest_type(
                str(source_type),
                dest_db=dest_db,
                context=InventContext.CREATE_NEW,
            )
        else:
            derived = source_type
        # Bare DECIMAL/NUMERIC keep DECIMAL(38,15) via materialize_dest_ddl —
        # never invent DOUBLE under skip_preflight (IEEE fidelity cliff).
        target_column_types[col] = derived

    # Partial Studio: probe existence before Map≡ALTER / CREATE invent.
    if studio_err:
        try:
            _insp = sa.inspect(engine)
            _exists = bool(_insp.has_table(table_name, schema=schema_name))
        except Exception:
            _exists = not create_table
        if not _exists:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=studio_err,
            )
        # Existing table: leave gaps empty for rematerialize — never Map-fill.
        for col in target_cols:
            target_column_types.setdefault(col, "")

    # Map≡ALTER: source DDL may propose a wider type; explicit Map stamps are a
    # hard ceiling (same helper as PostgreSQL / MySQL writers). Overflow cells
    # quarantine on write — never silent ALTER past the approved mapping.
    # Live-locked columns stay physical — widen must not erase Studio probe.
    # Skip entirely when partial Studio (gaps must come from live DDL rematerialize).
    from connectors.writer_common import desired_types_honoring_map_stamps

    if not studio_err:
        ceiling_types = [target_column_types[col] for col in target_cols]
        candidate_by_col: dict[str, str] = {}
        # Backfill is the operator's standing approval for additive drift, so a
        # live carrier the source has outgrown may widen to the source's own
        # declared type. Without this the probed live DDL froze the column and
        # every drifted row quarantined as "would truncate on write" while the
        # ALTER that fixes it never ran. An explicit Map stamp still ceilings.
        widenable_live = live_locked if not backfill_new_fields else set()
        for i, col in enumerate(target_cols):
            if col in explicit_stamps or col in widenable_live:
                continue
            mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
            mapping_source = mapping.get("source_type")
            catalog_source = column_types.get(str(mapping.get("source") or ""))
            source_type = _source_ddl_for_widen(mapping_source, catalog_source)
            # Unknown source DDL: do not invent string/VARCHAR widen candidate.
            if not str(source_type or "").strip():
                continue
            source_ddl = (
                materialize_dest_ddl(dest_db, source_type) if dest_db else source_type
            )
            candidate_by_col[col] = source_ddl

        desired_list, alter_refusals = desired_types_honoring_map_stamps(
            target_cols=target_cols,
            current_target_types=ceiling_types,
            mappings=mappings,
            candidate_by_col=candidate_by_col,
            preserve_case=True,
            explicit_columns=explicit_stamps | widenable_live,
        )
        if alter_refusals:
            logger.info(
                "generic_sql Map≡ALTER refusals (stamp ceiling): %s", alter_refusals
            )
        for i, col in enumerate(target_cols):
            if col in widenable_live:
                continue
            new_typ = desired_list[i]
            old_typ = target_column_types[col]
            target_column_types[col] = new_typ
            if col not in explicit_stamps and new_typ != old_typ:
                mapping = by_tgt.get(col) or by_tgt.get(str(col).lower())
                if mapping is not None:
                    updated = {**mapping, "target_type": new_typ}
                    by_tgt[col] = updated
                    by_tgt[str(col).lower()] = updated
                    # Keep write_mappings list in sync for quarantine / bind.
                    for mi, m in enumerate(mappings):
                        if str(m.get("target") or "").strip().lower() == str(col).lower():
                            mappings[mi] = updated
                            break

    policy = transform_error_policy(error_policy)
    # Engine-honest dialect labels (Databricks/Delta via generic_sql share this path).
    _engine_label = _generic_sql_engine_label(dest_db)

    # Partial Studio: defer Map + strict abort until live DDL is settled.
    # Create-new already refused above. The write loop maps once after settle.
    transform_errors: list[str] = []
    rejected_details: list[dict] = []
    _tgt_types: list[str] = [
        str(target_column_types.get(c, "") or "") for c in target_cols
    ]

    # Built after rematerialize when live carriers differ — see rebuild below.
    table_obj = _build_table_for_write(
        engine,
        table_name,
        schema_name,
        target_cols,
        target_column_types,
        db_type=cfg.get("type", ""),
        conflict_columns=conflict_columns,
    )

    dialect_name = engine.dialect.name if engine.dialect else ""
    # Overlay live DDL when Map stamped VARCHAR over typed sinks (empty refuse).
    table_existed = False
    physical: dict[str, str] = {}
    try:
        from connectors.writer_common import (
            overlay_physical_bind_types,
            require_physical_types_for_existing_table,
        )

        # Same entry point the create/exists branch below uses — two spellings
        # of the inspector answered differently under a patched module.
        inspector = inspect(engine)
        existence_known = True
        try:
            table_existed = bool(
                inspector.has_table(table_name, schema=schema_name)
            )
        except Exception:
            # Unknown existence: if create is disabled the table must already
            # exist — fail-closed on empty physical rather than Map VARCHAR invent.
            existence_known = False
            table_existed = not create_table
        existing_cols = []
        cols_probe_failed = False
        if table_existed:
            try:
                existing_cols = inspector.get_columns(
                    table_name, schema=schema_name
                )
            except Exception:
                existing_cols = []
                cols_probe_failed = True
                # Keep table_existed True so require_physical fail-closes.
        if table_existed and not existence_known and not create_table:
            # Fail closed, but do not report an unread catalog as a read one:
            # "empty for an existing table" names a grant problem on a table
            # nobody has seen. The operator's action is the existence probe.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=(
                    f"{_engine_label} could not determine whether "
                    f"{table_name!r} exists and create_table is disabled — "
                    "refuse Map VARCHAR bind (empty→NULL invent risk). "
                    "Re-check grants / information_schema and retry."
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        if table_existed and cols_probe_failed:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=(
                    f"{_engine_label} get_columns failed for existing table "
                    f"{table_name!r} — refuse Map VARCHAR bind (empty→NULL invent "
                    "risk). Re-check grants / information_schema and retry."
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        # Create-new: partial Studio must not soft-bind Map VARCHAR.
        if not table_existed and studio_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=studio_err,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        for col_meta in existing_cols or []:
            name = str(col_meta.get("name") or "")
            if not name:
                continue
            typ = col_meta.get("type")
            # Prefer specialty/logical carriers over raw SA str() (e.g. sql_variant
            # must not soft-bind as Map VARCHAR / invent JSON via "variant").
            ddl = _logical_type_from_sa(typ) if typ is not None else ""
            if not str(ddl or "").strip():
                ddl = str(typ) if typ is not None else ""
            physical[name] = ddl
            physical[name.lower()] = ddl
            physical[name.upper()] = ddl
        overlay_err = require_physical_types_for_existing_table(
            table_existed=table_existed,
            physical=physical,
            dialect_label="SQL",
            # With backfill, ADD COLUMN runs later — only require carriers for
            # columns already on the table (PG/MySQL/SQLite parity).
            target_cols=(
                [
                    c
                    for c in target_cols
                    if c
                    and (
                        c in physical
                        or str(c).lower() in {str(k).lower() for k in physical}
                    )
                ]
                if (table_existed and backfill_new_fields)
                else target_cols
            ),
        )
        if overlay_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=overlay_err,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        if physical:
            from connectors.writer_common import rematerialize_live_dest_types

            # Overlay live carriers for existing columns; additive Map cols keep
            # Map stamps until ALTER ADD COLUMN (schema-evolution parity).
            covered_cols: list[str] = []
            covered_physical: dict[str, str] = {}
            for c in target_cols or []:
                if not c:
                    continue
                hit = (
                    physical.get(c)
                    or physical.get(str(c).lower())
                    or physical.get(str(c).upper())
                )
                if hit and str(hit).strip():
                    planned = str(target_column_types.get(c) or "").strip()
                    # Drift widen is applied by ALTER before the insert, so a
                    # column the source outgrew must be judged against the
                    # carrier it is about to have. Overlaying today's narrow
                    # physical type here quarantined every drifted row and the
                    # ALTER never ran. A failed ALTER still aborts the write.
                    if (
                        backfill_new_fields
                        and planned
                        and c not in explicit_stamps
                        and is_wider_type(str(hit).strip(), planned, dest_db=dest_db)
                    ):
                        continue
                    covered_cols.append(c)
                    covered_physical[c] = str(hit).strip()
            live_partial = (
                rematerialize_live_dest_types(
                    covered_physical, covered_cols, product=_engine_label or "SQL"
                )
                if covered_cols
                else None
            )
            if covered_cols and live_partial is None:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or database,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"{_engine_label} live DDL incomplete for existing mapped "
                        "columns — refuse Map VARCHAR rematerialize invent. "
                        "Re-run destination schema introspect and retry."
                    ),
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )
            live_dest_types = dict(target_column_types or {})
            if live_partial:
                live_dest_types.update(live_partial)
            # Partial Studio + backfill: additive cols need an explicit Map stamp
            # (operator-approved) — never invent from source DDL / bare string.
            # Lookup by target name (mappings need not be index-aligned).
            if studio_err and backfill_new_fields:
                from services.mapping_constraints import write_mappings

                covered_fold = {str(c).lower() for c in covered_cols}
                by_tgt: dict[str, dict] = {}
                for mapping in write_mappings(list(mappings or [])):
                    tgt = str(mapping.get("target") or "").strip()
                    if tgt and tgt not in by_tgt:
                        by_tgt[tgt] = mapping
                        by_tgt.setdefault(tgt.lower(), mapping)
                for col in target_cols:
                    if not col or str(col).lower() in covered_fold:
                        continue
                    if str(live_dest_types.get(col) or "").strip():
                        continue
                    mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
                    explicit = str(
                        mapping.get("target_type") or mapping.get("dest_type") or ""
                    ).strip()
                    if not explicit:
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=table_name,
                            target_schema=schema or database,
                            checksum="",
                            chunks_completed=0,
                            error=(
                                f"{_engine_label} additive column {col!r} lacks "
                                "Studio/live type and Map target_type under partial "
                                "Studio — refuse Map VARCHAR ADD invent. Stamp the "
                                "column on Map or disable backfill_new_fields."
                            ),
                            rejected_details=rejected_details,
                            warnings=transform_errors,
                        )
                    add_source = (
                        column_types.get(str(mapping.get("source") or ""))
                        or mapping.get("source_type")
                    )
                    derived = (
                        materialize_dest_ddl(
                            dest_db, explicit, source_type=add_source
                        )
                        if dest_db
                        else str(explicit)
                    )
                    live_dest_types[col] = derived
            carriers_differ = bool(covered_cols) and any(
                str(target_column_types.get(c) or "").strip().upper()
                != str(live_dest_types.get(c) or "").strip().upper()
                for c in covered_cols
            )
            types_changed = carriers_differ or any(
                str(target_column_types.get(c) or "").strip().upper()
                != str(live_dest_types.get(c) or "").strip().upper()
                for c in target_cols
            )
            target_column_types = live_dest_types
            _tgt_overlaid = [
                str(target_column_types.get(c, "") or "") for c in target_cols
            ]
            # Settle live carriers now. The write loop maps once against this image.
            need_remap = carriers_differ or bool(studio_err)
            if types_changed or need_remap:
                table_obj = _build_table_for_write(
                    engine,
                    table_name,
                    schema_name,
                    target_cols,
                    target_column_types,
                    db_type=cfg.get("type", ""),
                    conflict_columns=conflict_columns,
                )
            _tgt_types = _tgt_overlaid
    except Exception:
        logger.debug(
            "generic_sql physical column introspection failed",
            exc_info=True,
        )
        if table_existed:
            from connectors.writer_common import require_physical_types_for_existing_table

            overlay_err = require_physical_types_for_existing_table(
                table_existed=True,
                physical={},
                dialect_label="SQL",
            )
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=overlay_err or "SQL physical DDL introspection failed",
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

    # Partial Studio: never soft-bind logical "string" for empty carriers —
    # rematerialize / additive Map stamp must have filled every mapped column.
    if studio_err:
        missing_carriers = [
            c
            for c in target_cols
            if c and not str(target_column_types.get(c) or "").strip()
        ]
        if missing_carriers:
            sample = ", ".join(repr(c) for c in missing_carriers[:12])
            more = (
                f" (+{len(missing_carriers) - 12} more)"
                if len(missing_carriers) > 12
                else ""
            )
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=(
                    f"{_engine_label} mapped field(s) {sample}{more} lack live/"
                    "Map carriers under partial Studio — refuse SA string bind "
                    "invent. Re-run destination schema introspect or stamp Map "
                    "target_type for additive columns."
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

    sa_col_types = {
        col: _sa_type_for_logical(
            str(target_column_types.get(col) or "string"),
            dialect_name,
            cfg.get("type", ""),
        )
        for col in target_cols
    }

    from connectors.sql_write_materialize import (
        SqlWriteAccumulator,
        ensure_sql_source_spool,
        sql_source_from_writer,
    )

    extra = (
        _kwargs.get("dest_extra")
        if isinstance(_kwargs.get("dest_extra"), dict)
        else {}
    )
    _sql_src = sql_source_from_writer(_kwargs, extra)
    spool, close_spool = ensure_sql_source_spool(
        headers=headers,
        data_rows=data_rows,
        records=_sql_src["records"],
        mappings=mappings,
        extra=extra,
        source_spool=_sql_src.get("source_spool"),
        spill_max=_sql_src.get("source_spill_max"),
    )
    source_row_count = int(getattr(spool, "row_count", 0) or 0)

    def _cleanup_spool() -> None:
        nonlocal close_spool
        if not close_spool:
            return
        close_spool = False
        try:
            spool.close()
        except Exception:
            logger.debug("generic sql source spool close skipped", exc_info=True)

    write_acc = SqlWriteAccumulator(
        target_cols=target_cols,
        dest_db_type=str(dest_db or type or "sql").lower(),
        dest_types=target_column_types if isinstance(target_column_types, dict) else {},
        dialect_label=_engine_label,
    )
    _generic_finish_kwargs = dict(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=target_column_types,
        policy=policy,
        conflict_columns=conflict_columns,
        write_mode=write_mode,
        dest_db=str(dest_db or type or "sql").lower(),
        dialect_label=_engine_label,
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        records=None,
        source_spool=spool,
        extra=extra,
        materialize_batch=_sql_src["materialize_batch"],
        sa_col_types=sa_col_types,
        dialect_name=str(dialect_name or ""),
        db_type=str(cfg.get("type") or ""),
    )
    if policy == "fail":
        scan_acc, source_row_count = _generic_scan_finished_bundles(
            **_generic_finish_kwargs
        )
        rejected_details = list(scan_acc.rejected_details)
        transform_errors = list(scan_acc.transform_errors)
        _bind_abort = scan_acc.abort_error(policy)
        if _bind_abort:
            _cleanup_spool()
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=0,
                error=_bind_abort,
                rejected_rows=_rejected_row_count(
                    data_rows,
                    [],
                    rejected_details,
                    policy,
                    source_row_count=source_row_count or None,
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

    written = 0
    chunks_completed = 0
    rows_skipped = 0
    identity_session: Any = None
    try:
        with engine.connect() as conn:
            db_type = (cfg.get("type") or "").lower()
            if db_type == "questdb":
                # QuestDB's pg_catalog reflection is incomplete; use idempotent DDL.
                table_exists = False
            else:
                # Cached per table: a chunked load asks this once per chunk, and
                # the answer only changes on DDL we run ourselves below — every
                # such branch invalidates.
                table_exists = reflection_cache.get_or_load(
                    engine,
                    schema_name,
                    table_name,
                    "has_table",
                    lambda: inspect(engine).has_table(table_name, schema=schema_name),
                )

            if write_mode == "replace" and table_exists:
                conn.execute(sa.schema.DropTable(table_obj, if_exists=True))
                conn.commit()
                table_exists = False
                reflection_cache.invalidate_table(engine, schema_name, table_name)

            if not table_exists and not create_table:
                _cleanup_spool()
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema_name or (cfg.get("database") or ""),
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Destination table '{table_name}' is missing and "
                        "create_table is disabled"
                    ),
                )

            if create_table and not table_exists:
                # Until now only postgresql_writer/sqlite_writer consulted the
                # fidelity planner, so MySQL/SQL Server/Oracle create-new landed
                # types and an upsert PK and nothing else — source NOT NULL,
                # DEFAULT, UNIQUE and CHECK were dropped without a certificate.
                fidelity_plan = None
                placement_suffix = ""
                try:
                    from services.physical_placement_ddl import (
                        list_destination_tablespaces,
                    )
                    from services.schema_fidelity import resolve_create_fidelity_plan

                    fidelity_plan = resolve_create_fidelity_plan(
                        source_schema_catalog=_kwargs.get("source_schema_catalog"),
                        mappings=mappings,
                        target_columns=target_cols,
                        target_types=[
                            target_column_types.get(c, "") for c in target_cols
                        ],
                        dest_dialect=_fidelity_dialect(dest_db, dialect_name),
                        table_already_exists=False,
                        dest_table=table_name,
                        dest_schema=schema_name or "",
                        dest_tablespaces=list_destination_tablespaces(
                            _fidelity_dialect(dest_db, dialect_name), conn
                        ),
                    )
                    placement_suffix = fidelity_plan.create_suffix
                    table_obj = _build_table_for_write(
                        engine,
                        table_name,
                        schema_name,
                        target_cols,
                        target_column_types,
                        db_type=cfg.get("type", ""),
                        conflict_columns=conflict_columns,
                        fidelity_plan=fidelity_plan,
                    )
                    _kwargs["_schema_fidelity_report"] = fidelity_plan.report.to_dict()
                except Exception as exc:
                    # A planner failure must not silently become a types-only
                    # CREATE that the certificate then calls faithful.
                    logger.warning(
                        "%s schema fidelity plan failed; create-new carries types "
                        "only and constraints are not certified: %s",
                        _engine_label,
                        exc,
                    )
                    fidelity_plan = None
                    from services.schema_fidelity import empty_unsupported_report

                    _kwargs["_schema_fidelity_report"] = empty_unsupported_report(
                        source_dialect="",
                        dest_dialect=_fidelity_dialect(dest_db, dialect_name),
                        reason=(
                            "Create-new fidelity planning failed; column types were "
                            f"emitted without certified constraints ({exc})."
                        ),
                    ).to_dict()
                try:
                    # Wave 63: PostgreSQL create-new closed ENUM → CREATE TYPE +
                    # column typed as df_enum_* (same SSOT as postgresql_writer).
                    if dest_db in {"postgresql", "postgres", "cockroachdb", "yugabytedb"}:
                        from services.type_system import collect_pg_enum_prerequisites

                        for stmt in collect_pg_enum_prerequisites(logical_types):
                            try:
                                conn.execute(sa.text(stmt))
                            except Exception as enum_exc:
                                err_e = str(enum_exc).lower()
                                if "already exists" not in err_e and "duplicate" not in err_e:
                                    raise
                    if db_type == "questdb":
                        # QuestDB supports TIMESTAMP but not the PG "WITHOUT TIME ZONE" clause.
                        ddl = str(
                            sa.schema.CreateTable(
                                table_obj, if_not_exists=True
                            ).compile(dialect=engine.dialect)
                        )
                        ddl = ddl.replace(
                            "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP"
                        ).replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
                        conn.execute(sa.text(ddl))
                    elif (
                        (dialect_name or "").lower() in {"mssql", "oracle"}
                        or db_type in {
                            "sqlserver",
                            "mssql",
                            "azure_sql",
                            "oracle",
                            "oracledb",
                            "oracle_db",
                            "oracle_autonomous_warehouse",
                            "amazon_rds_oracle",
                        }
                    ):
                        # T-SQL has no CREATE TABLE IF NOT EXISTS. Oracle XE
                        # (21c and earlier) rejects IF NOT EXISTS (ORA-00922);
                        # existence was already probed via inspector.
                        conn.execute(
                            sa.text(
                                _with_placement_suffix(
                                    str(
                                        sa.schema.CreateTable(table_obj).compile(
                                            dialect=engine.dialect
                                        )
                                    ),
                                    placement_suffix,
                                )
                            )
                        )
                    else:
                        create = sa.schema.CreateTable(
                            table_obj, if_not_exists=True
                        )
                        if placement_suffix:
                            # Placement lives inside CREATE: no engine can
                            # partition or relocate a table after the fact.
                            conn.execute(
                                sa.text(
                                    _with_placement_suffix(
                                        str(create.compile(dialect=engine.dialect)),
                                        placement_suffix,
                                    )
                                )
                            )
                        else:
                            conn.execute(create)
                    if fidelity_plan is not None:
                        from services.schema_fidelity import apply_post_create_sql

                        # Executed one statement at a time: a refused CREATE
                        # INDEX downgrades that index in the certificate rather
                        # than failing the table or claiming an index that is
                        # not there.
                        def _run_post_create(stmt: str) -> None:
                            with engine.begin() as index_conn:
                                index_conn.execute(sa.text(stmt))

                        conn.commit()
                        apply_post_create_sql(fidelity_plan, _run_post_create)
                        from services.schema_fidelity import (
                            certify_placement_on_destination,
                        )

                        certify_placement_on_destination(
                            fidelity_plan,
                            dialect=_fidelity_dialect(dest_db, dialect_name),
                            cursor=conn,
                            schema=schema_name or (cfg.get("database") or ""),
                            table=table_name,
                        )
                        from services.identity_carry import sqlalchemy_fetchall
                        from services.schema_fidelity import (
                            certify_identity_on_destination,
                        )

                        certify_identity_on_destination(
                            fidelity_plan,
                            dialect=_fidelity_dialect(dest_db, dialect_name),
                            schema=schema_name or (cfg.get("database") or ""),
                            table=table_name,
                            fetchall=sqlalchemy_fetchall(conn),
                        )
                        from services.schema_fidelity import (
                            certify_structure_on_destination,
                        )

                        certify_structure_on_destination(
                            fidelity_plan,
                            dialect=_fidelity_dialect(dest_db, dialect_name),
                            schema=schema_name or (cfg.get("database") or ""),
                            table=table_name,
                            fetchall=sqlalchemy_fetchall(conn),
                        )
                        _kwargs["_schema_fidelity_report"] = fidelity_plan.report.to_dict()
                    conn.commit()
                except Exception as exc:
                    # If the dialect does not support IF NOT EXISTS and the table
                    # was created concurrently, ignore the error and continue.
                    err = str(exc).lower()
                    if "already exists" in err or "duplicate" in err:
                        conn.rollback()
                    else:
                        raise
                finally:
                    # The table now exists (or a concurrent writer created it).
                    # Either way the cached "missing" answer is stale.
                    reflection_cache.invalidate_table(engine, schema_name, table_name)

            if table_exists and backfill_new_fields:
                add_missing_columns(
                    engine,
                    table_name,
                    schema_name,
                    target_cols,
                    sa_col_types,
                    backfill=True,
                    connection=conn,
                )
                stamp_ceiling_by_col = {
                    col: target_column_types[col]
                    for col in target_cols
                    if col in explicit_stamps and col in target_column_types
                }
                alter_widen_refusals: list[dict[str, Any]] = []
                _widen_existing_columns_sa(
                    conn,
                    engine,
                    dialect_name,
                    schema_name,
                    table_name,
                    target_cols,
                    target_column_types,
                    conflict_columns=conflict_columns,
                    stamp_ceiling_by_col=stamp_ceiling_by_col or None,
                    refusals_out=alter_widen_refusals,
                )
                if alter_widen_refusals:
                    logger.info(
                        "generic_sql Map≡ALTER live refusals: %s",
                        alter_widen_refusals,
                    )
                # Drift backfill may have added or widened columns; anything
                # reflected before this point describes the old shape.
                reflection_cache.invalidate_table(engine, schema_name, table_name)

            # The rows carry the source's own key values; on SQL Server an
            # IDENTITY column rejects them unless the session says so.
            from connectors.writer_common import begin_identity_insert

            identity_session = begin_identity_insert(
                conn,
                dialect_name=dialect_name,
                schema=schema_name or "",
                table=table_name,
                target_cols=list(target_cols),
            )

            collect_map_details = policy != "fail"
            writing = True
            chunk_idx = 0
            row_offset = 0
            chunks = max(1, (source_row_count + CHUNK_SIZE - 1) // CHUNK_SIZE) if source_row_count else 0
            ledger_table = None
            if ledger_job_id and source_row_count and not (
                write_mode == "upsert" and conflict_columns
            ):
                ledger_table = ensure_sqlalchemy_write_ledger(conn, schema=schema_name)
                if ledger_table is not None:
                    conn.commit()
                else:
                    ledger_unavailable = True
            for finished in iter_generic_sql_finished_bundles(**_generic_finish_kwargs):
                if collect_map_details:
                    rejected_details.extend(finished.rejected_details)
                    transform_errors.extend(finished.transform_errors)
                if writing and reject_on_strict_policy(
                    policy, rejected_details, "SQL", transform_errors
                ):
                    writing = False
                    write_acc.stop_writing()
                if not writing:
                    del finished
                    continue
                sparse_converted = list(getattr(finished, "sparse_dicts", None) or [])
                if sparse_converted and write_mode == "upsert" and conflict_columns:
                    sparse_written, sparse_skipped, sparse_checksum = (
                        _generic_apply_sparse_upsert(
                            conn,
                            table_obj,
                            target_cols,
                            conflict_columns,
                            sparse_converted,
                            dialect_name=str(
                                dest_db
                                or getattr(getattr(engine, "dialect", None), "name", "")
                                or ""
                            ).lower(),
                            rejected_details=rejected_details,
                            policy=policy,
                        )
                    )
                    written += sparse_written
                    rows_skipped += sparse_skipped
                    write_acc.add_accepted(list(sparse_checksum))
                    conn.commit()
                dense_dicts = list(getattr(finished, "dense_dicts", None) or [])
                for offset in range(0, len(dense_dicts), CHUNK_SIZE) if dense_dicts else []:
                    batch = dense_dicts[offset : offset + CHUNK_SIZE]
                    start = row_offset + offset
                    try:
                        already = (
                            sqlalchemy_chunk_rows_written(
                                conn,
                                ledger_table,
                                job_id=ledger_job_id,
                                batch_key=ledger_batch_key,
                                chunk_idx=chunk_idx,
                            )
                            if ledger_table is not None
                            else None
                        )
                        if already is not None:
                            written += already
                            ledger_chunks_skipped += 1
                            chunks_completed = chunk_idx + 1
                            if on_checkpoint:
                                on_checkpoint(
                                    chunks_completed, max(chunks, chunk_idx + 1), written
                                )
                            chunk_idx += 1
                            continue
                        chunk_written = 0
                        if write_mode == "upsert" and conflict_columns:
                            chunk_written = _upsert_batch(
                                conn,
                                table_obj,
                                batch,
                                conflict_columns,
                                target_cols,
                                dialect_name,
                                rejected_details=rejected_details,
                                policy=policy,
                            )
                            if DF_LSN_COL in target_cols:
                                rows_skipped += len(batch) - chunk_written
                            else:
                                chunk_written = len(batch)
                        else:
                            result = conn.execute(table_obj.insert(), batch)
                            chunk_written = multi_row_insert_written(
                                result, len(batch)
                            )
                        if ledger_table is not None:
                            mark_sqlalchemy_chunk_committed(
                                conn,
                                ledger_table,
                                job_id=ledger_job_id,
                                batch_key=ledger_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=chunk_written,
                                row_start=start,
                                row_end=start + max(chunk_written - 1, 0),
                                attempt=1,
                            )
                        conn.commit()
                        written += chunk_written
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception as rollback_exc:
                            logger.debug(
                                "chunk rollback failed: %s",
                                rollback_exc,
                                exc_info=rollback_exc,
                            )
                        if is_sql_data_error(chunk_exc) and policy in {
                            "quarantine",
                            "coerce_null",
                        }:
                            chunk_written = 0
                            for row_i, row in enumerate(batch):
                                try:
                                    if write_mode == "upsert" and conflict_columns:
                                        row_written = _upsert_batch(
                                            conn,
                                            table_obj,
                                            [row],
                                            conflict_columns,
                                            target_cols,
                                            dialect_name,
                                            rejected_details=rejected_details,
                                            policy=policy,
                                        )
                                        if DF_LSN_COL in target_cols:
                                            if not row_written:
                                                rows_skipped += 1
                                        else:
                                            row_written = 1
                                    else:
                                        result = conn.execute(table_obj.insert(), [row])
                                        row_written = (
                                            1
                                            if getattr(result, "rowcount", None) is None
                                            else (max(0, result.rowcount or 0) or 1)
                                        )
                                    conn.commit()
                                    chunk_written += row_written
                                except Exception as row_exc:
                                    try:
                                        conn.rollback()
                                    except Exception as rollback_exc:
                                        logger.debug(
                                            "row rollback failed: %s",
                                            rollback_exc,
                                            exc_info=rollback_exc,
                                        )
                                    if not is_sql_data_error(row_exc):
                                        raise
                                    col_name = extract_column_from_sql_error(row_exc) or "*"
                                    sample_val = ""
                                    if col_name != "*" and col_name in row:
                                        sample_val = str(row.get(col_name, ""))[:120]
                                    from connectors.writer_common import (
                                        append_write_quarantine_detail,
                                    )

                                    append_write_quarantine_detail(
                                        rejected_details,
                                        {
                                            "row": start + row_i,
                                            "column": col_name,
                                            "value": sample_val,
                                            "reason": str(row_exc)[:300],
                                            "policy": policy,
                                        },
                                        mapped_row=row,
                                        target_cols=target_cols,
                                        mappings=mappings,
                                    )
                                    transform_errors.append(str(row_exc)[:200])
                            if ledger_table is not None:
                                try:
                                    mark_sqlalchemy_chunk_committed(
                                        conn,
                                        ledger_table,
                                        job_id=ledger_job_id,
                                        batch_key=ledger_batch_key,
                                        chunk_idx=chunk_idx,
                                        rows_written=chunk_written,
                                        row_start=start,
                                        row_end=start + max(chunk_written - 1, 0),
                                        attempt=1,
                                    )
                                    conn.commit()
                                except Exception as ledger_exc:
                                    try:
                                        conn.rollback()
                                    except Exception:
                                        pass
                                    logger.warning(
                                        "write ledger update failed for chunk %s: %s",
                                        chunk_idx,
                                        ledger_exc,
                                    )
                            written += chunk_written
                        elif policy == "fail" or not is_sql_data_error(chunk_exc):
                            raise
                        else:
                            raise
                    chunks_completed = chunk_idx + 1
                    if on_checkpoint:
                        on_checkpoint(
                            chunks_completed, max(chunks, chunk_idx + 1), written
                        )
                    chunk_idx += 1
                write_acc.add_accepted(list(finished.dense_rows))
                row_offset += len(dense_dicts)
                del finished

            if identity_session is not None:
                identity_session.close()
                identity_session = None

        if ledger_chunks_skipped:
            transform_errors.append(
                f"Skipped {ledger_chunks_skipped} chunk(s) already committed by a "
                "previous attempt (write ledger prevented duplicate rows)"
            )
        elif ledger_unavailable:
            transform_errors.append(
                "Could not create the write ledger on this destination; a retry "
                "after an interrupted write may duplicate rows"
            )

        _child_db = str(dest_db or cfg.get("type") or "generic_sql").lower()
        _child_quote = "`" if _child_db in {"mysql", "mariadb", "tidb", "singlestore"} else '"'
        child_flush = flush_normalized_child_batches(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            dest_db=_child_db,
            create_table=create_table,
            sa_conn=conn,
            quote=_child_quote,
            placeholder="%s",
            schema=schema_name or schema or None,
        )
        if not child_flush.get("ok", True):
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error="; ".join(child_flush.get("errors") or ["child table flush failed"]),
                rejected_rows=max(
                    _rejected_row_count(
                        data_rows,
                        [()] * write_acc.accepted_row_count,
                        rejected_details,
                        policy,
                        source_row_count=source_row_count or None,
                    ),
                    len(data_rows) - written - rows_skipped if data_rows else 0,
                ),
                rejected_details=rejected_details,
                coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )
        if child_flush.get("rows_written"):
            try:
                conn.commit()
            except Exception as exc:
                logger.warning("child flush commit failed: %s", exc, exc_info=exc)
            for t in child_flush.get("tables") or []:
                transform_errors.append(f"normalized child table wrote {t}")

        _final_abort = reject_on_strict_policy(policy, rejected_details, "SQL")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=schema or database,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=_final_abort,
                rejected_rows=max(
                    _rejected_row_count(
                        data_rows,
                        [()] * write_acc.accepted_row_count,
                        rejected_details,
                        policy,
                        source_row_count=source_row_count or None,
                    ),
                    len(data_rows) - written - rows_skipped if data_rows else 0,
                ),
                rejected_details=rejected_details,
                coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )

        fid_report = _kwargs.get("_schema_fidelity_report")
        meta_out = write_acc.gate8_meta(conflict_columns=conflict_columns or None)
        if isinstance(fid_report, dict):
            meta_out = dict(meta_out or {})
            meta_out["schema_fidelity"] = fid_report
        _cleanup_spool()
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or database,
            checksum=write_acc.digest(),
            meta=meta_out,
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(
                _rejected_row_count(
                    data_rows,
                    [()] * write_acc.accepted_row_count,
                    rejected_details,
                    policy,
                    source_row_count=source_row_count or None,
                ),
                len(data_rows) - written - rows_skipped if data_rows else 0,
            ),
            rejected_details=rejected_details,
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    except Exception as exc:
        cleanup = locals().get("_cleanup_spool")
        if callable(cleanup):
            cleanup()
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or database,
            checksum=write_acc.digest() if write_acc.accepted_row_count else "",
            chunks_completed=chunks_completed,
            error=str(exc),
            rejected_rows=_rejected_row_count(
                data_rows,
                [()] * write_acc.accepted_row_count,
                rejected_details,
                policy,
                source_row_count=source_row_count or None,
            ),
            rejected_details=rejected_details,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    finally:
        # A pooled connection left with IDENTITY_INSERT ON fails the *next*
        # table's load with an error naming the wrong table, so the release is
        # owed on every exit — success, early return, or exception.
        if identity_session is not None:
            identity_session.close()
        cleanup = locals().get("_cleanup_spool")
        if callable(cleanup):
            cleanup()
        release_engine(engine)
