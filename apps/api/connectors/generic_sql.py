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
from typing import Any

from connectors.base import ReadBatch
from connectors.schema_drift import (
    _build_widen_ddl,
    add_missing_columns,
    is_wider_type,
)
from connectors.sql_temporal import (
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
from services.engine_pool import release_engine
from services.type_system import (
    ddl_type,
    materialize_dest_ddl,
    normalize_logical_type,
    parse_numeric_precision_scale,
)
from services.value_serializer import cell_to_string, json_default

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.dialects import postgresql
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
    ch_engines = None
    ChDateTime64 = None
    ChNullable = None
    TrinoTimestamp = None
    _DialectNativeType = None  # type: ignore[misc, assignment]

from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    assert_sparse_upsert_has_pk,
    build_mapped_rows_with_details,
    compare_lsn,
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
    resolve_conflict_targets,
    resolve_target_columns,
    row_checksum,
    materialize_missing_as_null_for_dense_write,
    reject_on_strict_policy,
    split_dense_sparse_rows,
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


def _mssql_drivername() -> str:
    """Pick an installed SQL Server DBAPI: pyodbc preferred, pymssql fallback."""
    try:
        import pyodbc  # noqa: F401
        return "mssql+pyodbc"
    except Exception:
        pass
    try:
        import pymssql  # noqa: F401
        return "mssql+pymssql"
    except Exception:
        pass
    return "mssql+pyodbc"


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
        return _normalize_sqlalchemy_url_string(connection_string, db_type)

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
            query["driver"] = "ODBC Driver 17 for SQL Server"
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

    return sa.URL.create(
        drivername,
        username=cfg.get("username") or None,
        password=cfg.get("password") or None,
        host=cfg.get("host") or "localhost",
        port=port if port else None,
        database=cfg.get("database") or None,
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
            return engine
        from services.engine_pool import pool_settings

        engine = create_engine(url, pool_pre_ping=True, **pool_settings())
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
            or "mssql" in str(getattr(url, "drivername", "")).lower()
        ):
            from sqlalchemy import event

            from connectors.write_resilience import apply_mssql_session_guards

            @event.listens_for(engine, "connect")
            def _mssql_fail_closed_session(dbapi_conn, _connection_record):  # noqa: ANN001
                apply_mssql_session_guards(dbapi_conn)

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
    return _engine(cfg).raw_connection()


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

    if isinstance(col_type, (sa.Integer, sa.BigInteger, sa.SmallInteger)):
        return "integer"

    # IEEE floats must stay FLOAT — never collapse into DECIMAL/NUMBER.
    if isinstance(col_type, (sa.Float, sa.Double, sa.REAL)):
        return "float"
    if any(
        tok in repr_
        for tok in ("float", "double", "real", "binary_float", "binary_double")
    ) and "decimal" not in repr_ and "numeric" not in repr_ and "number" not in repr_:
        return "float"

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
        return "decimal"

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
        return "string"

    # Fallback text matching for dialect-specific types not captured above
    # SQL Server specifics BEFORE broad timestamp/datetime matching.
    if "datetimeoffset" in repr_:
        return "timestamptz"
    type_name = getattr(getattr(col_type, "__class__", None), "__name__", "").lower()
    module = getattr(getattr(col_type, "__class__", None), "__module__", "").lower()
    if "rowversion" in repr_ or (
        "mssql" in module and type_name in {"timestamp", "rowversion"}
    ):
        # SQL Server TIMESTAMP is rowversion (binary), not a datetime.
        return "binary"
    if "json" in repr_ or "variant" in repr_ or "super" in repr_:
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


class _DuckDBJSON(sa.JSON):
    """JSON type that stores compact, deterministic JSON text in DuckDB.

    SQLAlchemy's default ``sa.JSON`` re-serializes dict/list with spaces and
    binds Python ``None`` as the JSON literal ``null``.  This subclass keeps
    source JSON text compact and treats ``None`` as SQL NULL so round-trips are
    exact and checksums line up.
    """

    __visit_name__ = "JSON"

    def bind_processor(self, dialect: Any) -> Callable[[Any], Any] | None:
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return value
            if isinstance(value, (dict, list, tuple, set, frozenset)):
                return json.dumps(
                    value,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=json_default,
                )
            return value

        return process

    def result_processor(
        self, dialect: Any, coltype: Any
    ) -> Callable[[Any], Any] | None:
        # DuckDB returns JSON values as text.  Keep them as text so the
        # downstream value serializer can apply the same canonical compact JSON.
        return lambda value: value


def _sa_type_for_logical(logical: str, dialect_name: str, db_type: str = "") -> Any:
    """Map a Datawrap logical type to a SQLAlchemy type that compiles for the engine.

    Accepts carriers like ``DECIMAL(12,4)`` / ``NUMERIC(38,10)`` — bare
    ``t == "decimal"`` matching used to fall through to TEXT and strip scale
    (SQL Server / Oracle / DuckDB greenfield fidelity bug).
    """
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
        ddl_type,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    raw = (logical or "string").strip()
    raw_lower = raw.lower()
    t = normalize_logical_type(raw)

    def _maybe_nullable(sa_type: Any) -> Any:
        if dialect_name == "clickhouse" and ChNullable is not None:
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
        return sa.DateTime(timezone=True)
    if (
        "timestamp_ntz" in raw_lower
        or "timestamp without time zone" in raw_lower
        or "datetime_ntz" in raw_lower
        or " without time zone" in raw_lower
    ):
        return _maybe_nullable(sa.DateTime())

    if t == LOGICAL_INTEGER:
        # Honor Map integer width — never invent BIGINT from INTEGER/INT.
        int_u = raw.upper().split("(", 1)[0].strip().replace(" ", "")
        if int_u in {
            "BIGINT",
            "INT64",
            "LONG",
            "UBIGINT",
            "UINT64",
            "BIGSERIAL",
        }:
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
        if int_u in {"SMALLINT", "INT2", "SMALLSERIAL", "SHORT", "INT16"}:
            return _maybe_nullable(sa.SmallInteger())
        if int_u in {"TINYINT", "INT1", "UINT8", "BYTE"}:
            return _maybe_nullable(sa.SmallInteger())
        # INTEGER / INT / MEDIUMINT / INT32 / SERIAL / NUMBER(p,0) mid-range
        return _maybe_nullable(sa.Integer())
    if t == LOGICAL_DECIMAL:
        precision, scale = parse_numeric_precision_scale(raw)
        if db_type == "risingwave":
            return sa.Numeric()
        # QuestDB lacks true DECIMAL — DOUBLE is the platform limit (documented).
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
        # Honor Map REAL/FLOAT4/FLOAT stamps (sa.Double invents mantissa widen).
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
            return _DuckDBJSON(none_as_null=True)
        if db_type in ("oracle", "clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        if dialect_name == "postgresql":
            return postgresql.JSONB()
        return sa.JSON()
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
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    # Sparse CDC: never coerce DF_MISSING → NULL (would wipe present destination cols).
    if is_missing_sentinel(value):
        return value

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
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
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

        # Empty JSON wire → SQL NULL (MySQL 3140 / JSONB empty-string class).
        if isinstance(value, str) and not value.strip():
            return None
        as_text = _is_string_type(sa_type)
        bound = coerce_json_wire(value, as_text=as_text)
        if as_text:
            return bound
        if isinstance(bound, str) and not _is_string_type(sa_type):
            # Valid JSON text → native for JSONB; wrap leftovers stay text.
            try:
                return json.loads(bound)
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
        return coerce_boolean_wire(value, as_int=as_int)

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
    ddl_type = logical_to_temporal_ddl(t) or logical_to_temporal_ddl(logical)
    if ddl_type:
        coerced = coerce_sql_temporal(value, ddl_type)
        base = ddl_type.upper()

        def _ensure_utc(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        def _naive_utc(dt: datetime) -> datetime:
            return _ensure_utc(dt).replace(tzinfo=None)

        if base == "DATE":
            if isinstance(coerced, datetime):
                return coerced.date()
            if isinstance(coerced, date):
                return coerced
            return value

        if base == "TIME":
            if _is_string_type(sa_type):
                if isinstance(coerced, time):
                    return coerced.isoformat()
                if isinstance(coerced, datetime):
                    return coerced.time().isoformat()
                return value if isinstance(value, str) else str(value)
            if isinstance(coerced, time):
                if db_type == "presto" or dialect_name == "presto":
                    return coerced.isoformat()
                return coerced
            if isinstance(coerced, datetime):
                tm = coerced.time()
                if db_type == "presto" or dialect_name == "presto":
                    return tm.isoformat()
                return tm
            return value

        # DATETIME2 (SQL Server) / QuestDB are naive wall clocks; DATETIMEOFFSET /
        # TIMESTAMPTZ carriers must keep aware UTC (never strip offset silently).
        raw_lower = f"{logical or ''} {ddl_type or ''}".lower()
        is_tz_aware = (
            "timestamptz" in raw_lower
            or "datetimeoffset" in raw_lower
            or "timestamp_tz" in raw_lower
            or "timestamp with time zone" in raw_lower
            or "with local time zone" in raw_lower
        )
        use_naive = not is_tz_aware and (
            db_type in {"questdb", "sqlserver", "mssql"} or dialect_name == "mssql"
        )
        if isinstance(coerced, datetime):
            return _naive_utc(coerced) if use_naive else _ensure_utc(coerced)
        if isinstance(coerced, date) and not isinstance(coerced, datetime):
            dt = datetime.combine(coerced, time())
            return _naive_utc(dt) if use_naive else _ensure_utc(dt)
        return value

    if t == LOGICAL_DECIMAL:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        if isinstance(value, str):
            return Decimal(value)
        return value

    if t == LOGICAL_INTEGER:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(
                    f"cannot coerce non-integral float {value!r} to INTEGER "
                    "without truncation"
                )
            return int(value)
        if isinstance(value, Decimal):
            if value != value.to_integral_value():
                raise ValueError(
                    f"cannot coerce non-integral decimal {value!r} to INTEGER "
                    "without truncation"
                )
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except (ValueError, TypeError):
                return value
        return value

    # uuid, string/text are already bound-friendly
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
        metadata = sa.MetaData()
        # Quote identifiers for safety with reserved words and case-sensitive engines.
        return sa.Table(
            table,
            metadata,
            schema=schema,
            quote=True,
            quote_schema=True,
            autoload_with=engine,
        )

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


def _build_table_for_write(
    engine: Any,
    table_name: str,
    schema: str | None,
    columns: list[str],
    column_types: dict[str, str],
    db_type: str = "",
    conflict_columns: list[str] | None = None,
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

    cols = []
    for col in columns:
        logical = column_types.get(col, "string")
        is_pk = col in pk_set
        # Setting autoincrement=False prevents SQLAlchemy from fabricating a
        # backing sequence for dialects (e.g. DuckDB) that do not create it
        # automatically.  The PK exists purely for upsert semantics, not identity.
        autoincrement = False if is_pk else None
        cols.append(
            sa.Column(
                col,
                _sa_type_for_logical(logical, dialect_name, db_type),
                primary_key=is_pk,
                nullable=not is_pk,
                autoincrement=autoincrement,
                quote=True,
            )
        )

    constraints: list[Any] = []
    if conflict_cols and not pk_set.issubset(set(columns)):
        constraints.append(sa.UniqueConstraint(*conflict_cols, quote=True))

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

    log: list[str] = []
    for col in target_cols:
        if col in skip_cols:
            continue
        existing = next((c for c in existing_cols if c["name"] == col), None)
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
        qualified = _qualified_table_ref(cfg, table, schema)
        with engine.connect() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {qualified}"))
            conn.commit()
        return True
    except Exception as primary_exc:
        # Some dialects reject the raw IF EXISTS form; retry via dialect DDL
        # before giving up, but surface the original error if that also fails.
        try:
            table_obj = sa.Table(table, sa.MetaData(), schema=schema)
            table_obj.drop(engine, checkfirst=True)
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
            return result.rowcount or 0
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
    from services.dialect_profiles import quote_char_for

    qualified = quote_table_ref(table, schema, dialect=dialect)
    base = f"SELECT * FROM {qualified}"  # nosec B608
    dialect_l = (dialect or "ansi").lower()
    is_mssql = dialect_l in {
        "mssql",
        "sqlserver",
        "microsoft_sql_server",
        "azure_sql_database",
        "synapse_analytics",
        "azure_synapse_dedicated",
        "azure_synapse_serverless",
        "google_cloud_sql_sql_server",
        "amazon_rds_sql_server",
    }
    # Discover columns so we can ORDER BY the first one — bare LIMIT/OFFSET is
    # non-deterministic and silently duplicates/skips rows across pages.
    probe_sql = f"SELECT TOP 0 * FROM {qualified}" if is_mssql else f"{base} LIMIT 0"  # nosec B608
    probe = conn.execute(sa.text(probe_sql))
    headers = list(probe.keys())
    if not headers:
        return [], []
    q = quote_char_for(dialect) or '"'
    if q == "[":
        order_col = f"[{str(headers[0]).replace(']', ']]')}]"
    else:
        order_col = quote_sql_identifier(headers[0], q)
    if is_mssql:
        sql = (
            f"{base} ORDER BY {order_col} "
            f"OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"
        )
    else:
        sql = f"{base} ORDER BY {order_col} LIMIT {int(limit)} OFFSET {int(offset)}"
    result = conn.execute(sa.text(sql))
    headers = list(result.keys())
    rows = [
        [cell_to_string(value, preserve_sql_null=True) for value in row]
        for row in result.fetchall()
    ]
    return headers, rows


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

                stmt = sa.select(*selected_cols)
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
                rows = [
                    [cell_to_string(value, preserve_sql_null=True) for value in row]
                    for row in fetched
                ]

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
) -> ReadBatch:
    """Cursor/keyset pagination for incremental and streaming transfers.

    Optional ``cursor_primary_key`` enables lexicographic ``(cursor, pk)`` so
    timestamp ties are not skipped forever (parity with PG/MySQL).
    """
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
            if (cfg.get("type") or "").lower() == "risingwave":
                with contextlib.suppress(Exception):
                    conn.execute(sa.text("FLUSH"))
            table_obj = _reflect_table(engine, table, schema_name, columns)
            if cursor_column not in table_obj.c:
                raise ValueError(
                    f"Cursor column '{cursor_column}' not found in table {table}"
                )
            cursor_col = table_obj.c[cursor_column]
            selected_cols = list(table_obj.c)
            if columns:
                selected_cols = [table_obj.c[c] for c in columns if c in table_obj.c]
            else:
                columns = selected_cols = list(table_obj.c)

            pk = (cursor_primary_key or "").strip()
            pk_col = (
                table_obj.c[pk]
                if pk and pk != cursor_column and pk in table_obj.c
                else None
            )

            stmt = sa.select(*selected_cols)
            if cursor_after:
                if pk_col is not None:
                    if "|" in str(cursor_after):
                        cur_val, pk_val = str(cursor_after).split("|", 1)
                    else:
                        cur_val, pk_val = cursor_after, ""
                    cur_marker = sa.cast(sa.literal(cur_val), cursor_col.type)
                    pk_marker = sa.cast(sa.literal(pk_val), pk_col.type)
                    # Row-value ``(a,b) > (x,y)`` is not portable (SQL Server).
                    # Expand to OR/AND so composite watermarks resume correctly.
                    stmt = stmt.where(
                        sa.or_(
                            cursor_col > cur_marker,
                            sa.and_(cursor_col == cur_marker, pk_col > pk_marker),
                        )
                    )
                    stmt = stmt.order_by(cursor_col, pk_col).limit(limit)
                else:
                    marker = sa.cast(sa.literal(cursor_after), cursor_col.type)
                    stmt = stmt.where(cursor_col > marker)
                    stmt = stmt.order_by(cursor_col).limit(limit)
            else:
                if pk_col is not None:
                    stmt = stmt.order_by(cursor_col, pk_col).limit(limit)
                else:
                    stmt = stmt.order_by(cursor_col).limit(limit)

            fetched = conn.execute(stmt).fetchall()
            headers = [c.name for c in selected_cols]
            rows = [
                [cell_to_string(value, preserve_sql_null=True) for value in row]
                for row in fetched
            ]

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
            if val is None or (isinstance(val, str) and str(val).strip() == ""):
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
        if any(row.get(c) in (None, "") for c in conflict_cols):
            continue
        clauses.append(
            sa.and_(
                *[
                    table_obj.c[c].is_(None) if row[c] is None else table_obj.c[c] == row[c]
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
        key = tuple(found[i] for i in range(len(conflict_cols)))
        existing[key] = found[len(conflict_cols)]
    return existing


def _generic_apply_sparse_upsert(
    conn: Any,
    table_obj: sa.Table,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[dict[str, Any]],
    *,
    dialect_name: str = "",
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from connectors.writer_common import run_sparse_cdc_upsert
    from services.value_serializer import is_missing_sentinel

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("sparse SQLAlchemy upsert requires conflict_columns")

    # Normalize dict rows to target_cols tuples for the shared loop.
    from services.value_serializer import DF_MISSING_SENTINEL

    as_tuples: list[tuple] = []
    for row in sparse_rows:
        as_tuples.append(
            tuple(
                (
                    row[c]
                    if c in row and not is_missing_sentinel(row.get(c))
                    else DF_MISSING_SENTINEL
                )
                for c in target_cols
            )
        )

    is_clickhouse = dialect_name == "clickhouse" or str(dialect_name).startswith(
        "clickhouse"
    )

    def fetch_existing(pk_vals: list[Any]) -> tuple | None:
        pk_clause = sa.and_(
            *[table_obj.c[c] == pk_vals[i] for i, c in enumerate(conflict)]
        )
        cols = [table_obj.c[c] for c in target_cols if c in table_obj.c]
        if len(cols) != len(target_cols):
            # Missing physical columns — return None so insert path can run.
            return None
        if is_clickhouse:
            # ReplacingMergeTree without FINAL can miss the current version and
            # fall through to a partial INSERT that NULL-wipes omitted attrs.
            from connectors.writer_common import quote_sql_identifier

            parts = []
            if table_obj.schema:
                parts.append(quote_sql_identifier(table_obj.schema))
            parts.append(quote_sql_identifier(table_obj.name))
            table_ref = clickhouse_final_table_sql(".".join(parts))
            col_sql = ", ".join(quote_sql_identifier(c) for c in target_cols)
            where_sql = " AND ".join(
                f"{quote_sql_identifier(c)} = :p{i}" for i, c in enumerate(conflict)
            )
            params = {f"p{i}": pk_vals[i] for i in range(len(conflict))}
            found = conn.execute(
                sa.text(f"SELECT {col_sql} FROM {table_ref} WHERE {where_sql}"),  # nosec B608
                params,
            ).fetchone()
            return tuple(found) if found is not None else None
        found = conn.execute(sa.select(*cols).where(pk_clause)).fetchone()
        return tuple(found) if found is not None else None

    def update_non_pk(non_pk: dict[str, Any], pk_vals: list[Any]) -> int:
        if is_clickhouse:
            # Mutations are not Airbyte-class upsert; force versioned INSERT path.
            return 0
        pk_clause = sa.and_(
            *[table_obj.c[c] == pk_vals[i] for i, c in enumerate(conflict)]
        )
        result = conn.execute(sa.update(table_obj).where(pk_clause).values(**non_pk))
        return int(getattr(result, "rowcount", 0) or 0)

    def insert_present(present: dict[str, Any]) -> None:
        conn.execute(sa.insert(table_obj).values(**present))

    return run_sparse_cdc_upsert(
        target_cols=target_cols,
        conflict_columns=conflict,
        sparse_rows=as_tuples,
        fetch_existing_row=fetch_existing,
        update_non_pk=update_non_pk,
        insert_present=insert_present,
        hydrate_versioned_insert=is_clickhouse,
    )


def _mssql_bracket(ident: str) -> str:
    """Bracket-quote a SQL Server identifier (escape ``]``)."""
    return "[" + str(ident).replace("]", "]]") + "]"


def _mssql_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_mssql_bracket(table_obj.schema))
    parts.append(_mssql_bracket(table_obj.name))
    return ".".join(parts)


def _mssql_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native T-SQL MERGE with HOLDLOCK + NULL-safe ON; staging temp table.

    Matches Airbyte/Fivetran-class SQL Server upsert: stage → MERGE → drop.
    Caller must fall back to delete+insert when this raises.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    stage = f"#df_mrg_{abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000}"
    target = _mssql_qualified_table(table_obj)
    col_sql = ", ".join(_mssql_bracket(c) for c in target_cols)
    # Clone column shapes from target — never invent VARCHAR widths.
    conn.execute(
        sa.text(f"SELECT TOP 0 {col_sql} INTO {stage} FROM {target}")  # nosec B608
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            params = {c: row.get(c) for c in target_cols}
            conn.execute(insert_sql, params)

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias=_mssql_bracket("t"),
            right_alias=_mssql_bracket("s"),
            quote_column=_mssql_bracket,
        )
        insert_cols = ", ".join(_mssql_bracket(c) for c in target_cols)
        insert_vals = ", ".join(
            f"{_mssql_bracket('s')}.{_mssql_bracket(c)}" for c in target_cols
        )
        if update_cols:
            set_sql = ", ".join(
                f"{_mssql_bracket('t')}.{_mssql_bracket(c)} = "
                f"{_mssql_bracket('s')}.{_mssql_bracket(c)}"
                for c in update_cols
            )
            merge_sql = (
                f"MERGE {target} WITH (HOLDLOCK) AS {_mssql_bracket('t')} "
                f"USING {stage} AS {_mssql_bracket('s')} "
                f"ON {on_sql} "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED BY TARGET THEN "
                f"INSERT ({insert_cols}) VALUES ({insert_vals});"
            )
        else:
            # Conflict-key-only rows: insert missing; leave matched alone.
            merge_sql = (
                f"MERGE {target} WITH (HOLDLOCK) AS {_mssql_bracket('t')} "
                f"USING {stage} AS {_mssql_bracket('s')} "
                f"ON {on_sql} "
                f"WHEN NOT MATCHED BY TARGET THEN "
                f"INSERT ({insert_cols}) VALUES ({insert_vals});"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _oracle_quote(ident: str) -> str:
    """Double-quote an Oracle identifier (escape embedded quotes)."""
    return '"' + str(ident).replace('"', '""') + '"'


def _oracle_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_oracle_quote(table_obj.schema))
    parts.append(_oracle_quote(table_obj.name))
    return ".".join(parts)


def _oracle_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Oracle MERGE with NULL-safe ON via session staging table.

    Stage → MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED (Oracle has no
    ``BY TARGET`` keyword). Prefer PRIVATE TEMPORARY TABLE (18c+); fall back to
    a session GLOBAL TEMPORARY TABLE. Caller falls back to delete+insert on error.
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # Private temp tables require the ORA$PTT_ prefix (Oracle default).
    ptt = f"ORA$PTT_DF_MRG_{suffix}"
    gtt = f"DF_MRG_{suffix}"
    target = _oracle_qualified_table(table_obj)
    col_sql = ", ".join(_oracle_quote(c) for c in target_cols)
    stage_ref = ""
    created: str | None = None
    try:
        try:
            stage_ref = _oracle_quote(ptt)
            conn.execute(
                sa.text(
                    f"CREATE PRIVATE TEMPORARY TABLE {stage_ref} "
                    f"ON COMMIT PRESERVE DEFINITION AS "
                    f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
                )
            )
            created = "ptt"
        except (sa.exc.SQLAlchemyError, OSError, ValueError):
            # Older Oracle / privilege gap — try session GTT (definition may persist).
            with contextlib.suppress(Exception):
                conn.rollback()
            stage_ref = _oracle_quote(gtt)
            conn.execute(
                sa.text(
                    f"CREATE GLOBAL TEMPORARY TABLE {stage_ref} "
                    f"ON COMMIT PRESERVE ROWS AS "
                    f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
                )
            )
            created = "gtt"

        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_oracle_quote,
        )
        insert_cols = ", ".join(_oracle_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_oracle_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_oracle_quote(c)} = s.{_oracle_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_ref} s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_ref} s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        if created == "gtt" and stage_ref:
            # PTT drops with session/definition; GTT definition may linger.
            with contextlib.suppress(Exception):
                conn.execute(sa.text(f"TRUNCATE TABLE {stage_ref}"))
            with contextlib.suppress(Exception):
                conn.execute(sa.text(f"DROP TABLE {stage_ref}"))


def _duckdb_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _duckdb_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_duckdb_quote(table_obj.schema))
    parts.append(_duckdb_quote(table_obj.name))
    return ".".join(parts)


def _duckdb_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native DuckDB MERGE INTO with NULL-safe ON (no PK required).

    DuckDB supports MERGE without a unique index — preferred over delete+insert
    for concurrent readers. Still at-least-once. Caller falls back on error.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = f"df_mrg_{suffix}"
    stage_q = _duckdb_quote(stage)
    target = _duckdb_qualified_table(table_obj)
    col_sql = ", ".join(_duckdb_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage_q} AS "
            f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage_q} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_duckdb_quote,
        )
        insert_cols = ", ".join(_duckdb_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_duckdb_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_duckdb_quote(c)} = s.{_duckdb_quote(c)}" for c in update_cols
            )
            # DuckDB UPDATE SET uses bare column names on the target side.
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_q} s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_q} s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {stage_q}"))


def _clickhouse_replacing_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Airbyte-class ClickHouse upsert: INSERT only into ReplacingMergeTree.

    Dedup is **engine-level and lazy** (background merge / ``SELECT … FINAL``).
    Never DELETE+INSERT — ClickHouse mutations race merges and are not
    Fivetran/Airbyte-class upsert semantics. Still at-least-once.
    """
    del conflict_cols, update_cols  # identity is table ORDER BY / version col
    if not rows:
        return 0
    result = conn.execute(table_obj.insert(), rows)
    return max(0, getattr(result, "rowcount", None) or 0) or len(rows)


def clickhouse_final_table_sql(table_ref: str) -> str:
    """``FROM <table> FINAL`` — Gate-8 must collapse ReplacingMergeTree duplicates.

    Airbyte ClickHouse destination docs: without FINAL (or OPTIMIZE), queries
    may see duplicate keys after at-least-once INSERT upserts.
    """
    ref = (table_ref or "").strip()
    if not ref:
        raise ValueError("clickhouse table ref required for FINAL select")
    # Idempotent if caller already appended FINAL.
    if re.search(r"\bFINAL\b", ref, flags=re.IGNORECASE):
        return ref
    return f"{ref} FINAL"


def _db2_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _db2_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_db2_quote(table_obj.schema))
    parts.append(_db2_quote(table_obj.name))
    return ".".join(parts)


def _db2_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native DB2 ``MERGE INTO`` with NULL-safe ON via session temp stage.

    Matches IBM / Fivetran-class LUW upsert: DECLARE GLOBAL TEMPORARY TABLE →
    INSERT stage → MERGE. Falls back to delete+insert when DECLARE/MERGE fails.
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # SESSION. prefix is required for DGTT identity on LUW.
    stage = f"SESSION.DF_MRG_{suffix}"
    target = _db2_qualified_table(table_obj)
    col_sql = ", ".join(_db2_quote(c) for c in target_cols)
    try:
        conn.execute(
            sa.text(
                f"DECLARE GLOBAL TEMPORARY TABLE {stage} AS "
                f"(SELECT {col_sql} FROM {target} WHERE 1=0) "
                f"WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED"  # nosec B608
            )
        )
    except (sa.exc.SQLAlchemyError, OSError, ValueError):
        # Some DB2 z/OS / privilege profiles reject DGTT — let caller fall back.
        raise

    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_db2_quote,
        )
        insert_cols = ", ".join(_db2_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_db2_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_db2_quote(c)} = s.{_db2_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _teradata_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _teradata_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_teradata_quote(table_obj.schema))
    parts.append(_teradata_quote(table_obj.name))
    return ".".join(parts)


def _teradata_merge_on(conflict_cols: list[str]) -> str:
    """Teradata MERGE ON must be PI equality — cannot equate explicitly with NULL.

    Docs: match_condition cannot equate with NULL and must hash to a single AMP
    on the primary index. Do **not** use null_safe OR-IS-NULL form here.
    """
    return " AND ".join(
        f"t.{_teradata_quote(c)} = s.{_teradata_quote(c)}" for c in conflict_cols
    )


def _teradata_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Teradata ``MERGE INTO`` via VOLATILE stage (Fivetran/Vantage class).

    ON uses PI equality only (Teradata forbids NULL equate in MERGE ON).
    Conflict/PI columns are never UPDATEd. Still at-least-once.
    """
    if not rows:
        return 0
    # Never attempt to UPDATE primary-index columns (Teradata rejects it).
    safe_update = [c for c in update_cols if c not in conflict_cols]
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _teradata_quote(f"DF_MRG_{suffix}")
    target = _teradata_qualified_table(table_obj)
    col_sql = ", ".join(_teradata_quote(c) for c in target_cols)
    pi_sql = ", ".join(_teradata_quote(c) for c in conflict_cols)
    conn.execute(
        sa.text(
            f"CREATE MULTISET VOLATILE TABLE {stage} AS "
            f"(SELECT {col_sql} FROM {target} WHERE 1=0) "
            f"WITH DATA PRIMARY INDEX ({pi_sql}) "
            f"ON COMMIT PRESERVE ROWS"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = _teradata_merge_on(conflict_cols)
        insert_cols = ", ".join(_teradata_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_teradata_quote(c)}" for c in target_cols)
        if safe_update:
            set_sql = ", ".join(
                f"{_teradata_quote(c)} = s.{_teradata_quote(c)}" for c in safe_update
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON {on_sql} "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON {on_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _trino_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _trino_qualified_table(table_obj: sa.Table) -> str:
    parts: list[str] = []
    if table_obj.schema:
        # Trino may embed catalog.schema in Table.schema (e.g. "hive.default").
        for part in str(table_obj.schema).split("."):
            if part:
                parts.append(_trino_quote(part))
    parts.append(_trino_quote(table_obj.name))
    return ".".join(parts)


def _trino_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
    *,
    chunk_size: int = 50,
) -> int:
    """Native Trino/Presto/Athena ``MERGE INTO`` with NULL-safe ON (Iceberg MoR).

    Stages via ``VALUES`` chunks — Trino connector MERGE and Athena engine v3
    Iceberg ``MERGE INTO`` (AWS Big Data Blog / Athena MERGE docs). Falls back
    to delete+insert for Trino/Presto; Athena callers must use append-only
    fallback (see ``_upsert_batch``). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    target = _trino_qualified_table(table_obj)
    on_sql = null_safe_merge_on(
        conflict_cols,
        left_alias="t",
        right_alias="s",
        quote_column=_trino_quote,
    )
    insert_cols = ", ".join(_trino_quote(c) for c in target_cols)
    insert_vals = ", ".join(f"s.{_trino_quote(c)}" for c in target_cols)
    set_sql = ""
    if update_cols:
        set_sql = ", ".join(
            f"{_trino_quote(c)} = s.{_trino_quote(c)}" for c in update_cols
        )
    alias_list = ", ".join(_trino_quote(c) for c in target_cols)
    written = 0
    size = max(1, int(chunk_size))
    for i in range(0, len(rows), size):
        chunk = rows[i : i + size]
        value_rows: list[str] = []
        params: dict[str, Any] = {}
        for ridx, row in enumerate(chunk):
            placeholders = []
            for col in target_cols:
                key = f"r{ridx}_{col}"
                params[key] = row.get(col)
                placeholders.append(f":{key}")
            value_rows.append(f"({', '.join(placeholders)})")
        values_sql = ", ".join(value_rows)
        if set_sql:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING (VALUES {values_sql}) AS s ({alias_list}) "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING (VALUES {values_sql}) AS s ({alias_list}) "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql), params)  # nosec B608
        written += len(chunk)
    return written


def _hana_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _hana_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_hana_quote(table_obj.schema))
    parts.append(_hana_quote(table_obj.name))
    return ".".join(parts)


def _hana_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native SAP HANA ``MERGE INTO`` with NULL-safe ON via local temp stage.

    HANA also offers ``UPSERT … WITH PRIMARY KEY`` for single-row PK paths;
    MERGE is the composite-key / CDC-class algorithm (Airbyte/Fivetran HANA
    destinations). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # HANA local temporary tables are session-scoped and named with #.
    stage = f"#DF_MRG_{suffix}"
    target = _hana_qualified_table(table_obj)
    col_sql = ", ".join(_hana_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE LOCAL TEMPORARY COLUMN TABLE {stage} AS "
            f"(SELECT {col_sql} FROM {target} WHERE 1=0)"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_hana_quote,
        )
        insert_cols = ", ".join(_hana_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_hana_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_hana_quote(c)} = s.{_hana_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _vertica_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _vertica_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_vertica_quote(table_obj.schema))
    parts.append(_vertica_quote(table_obj.name))
    return ".".join(parts)


def _vertica_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Vertica ``MERGE INTO`` with NULL-safe ON via local temp stage.

    Vertica docs: one MERGE upserts matched + unmatched in a single transaction
    (Fivetran Vertica / enterprise warehouse pattern). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _vertica_quote(f"df_mrg_{suffix}")
    target = _vertica_qualified_table(table_obj)
    col_sql = ", ".join(_vertica_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE LOCAL TEMPORARY TABLE {stage} ON COMMIT PRESERVE ROWS AS "
            f"SELECT {col_sql} FROM {target} WHERE FALSE"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_vertica_quote,
        )
        insert_cols = ", ".join(_vertica_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_vertica_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_vertica_quote(c)} = s.{_vertica_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _netezza_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _netezza_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_netezza_quote(table_obj.schema))
    parts.append(_netezza_quote(table_obj.name))
    return ".".join(parts)


def _netezza_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Netezza / IBM NPS ``MERGE INTO`` with NULL-safe ON (7.2.1+).

    Stages via ``CREATE TEMP TABLE … AS SELECT … LIMIT 0`` then MERGE —
    IBM Performance Server / Fivetran Netezza class. Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _netezza_quote(f"df_mrg_{suffix}")
    target = _netezza_qualified_table(table_obj)
    col_sql = ", ".join(_netezza_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage} AS "
            f"SELECT {col_sql} FROM {target} LIMIT 0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_netezza_quote,
        )
        insert_cols = ", ".join(_netezza_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_netezza_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_netezza_quote(c)} = s.{_netezza_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _informix_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _informix_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_informix_quote(table_obj.schema))
    parts.append(_informix_quote(table_obj.name))
    return ".".join(parts)


def _informix_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Informix ``MERGE INTO`` with NULL-safe ON via TEMP stage.

    IBM/HCL docs: TEMP ``WITH NO LOG`` + MERGE join (Fivetran Informix class).
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _informix_quote(f"df_mrg_{suffix}")
    target = _informix_qualified_table(table_obj)
    col_sql = ", ".join(_informix_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage} AS "
            f"SELECT {col_sql} FROM {target} WHERE 1=0 "
            f"WITH NO LOG"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_informix_quote,
        )
        insert_cols = ", ".join(_informix_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_informix_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_informix_quote(c)} = s.{_informix_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _firebird_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _firebird_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_firebird_quote(table_obj.schema))
    parts.append(_firebird_quote(table_obj.name))
    return ".".join(parts)


def _firebird_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Firebird ``MERGE INTO`` with NULL-safe ON via ``RDB$DATABASE``.

    Firebird 2.1+ MERGE; staging each row from ``RDB$DATABASE`` avoids inventing
    GTT DDL without typed columns (Firebird Language Reference). Still
    at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    target = _firebird_qualified_table(table_obj)
    on_sql = null_safe_merge_on(
        conflict_cols,
        left_alias="t",
        right_alias="s",
        quote_column=_firebird_quote,
    )
    insert_cols = ", ".join(_firebird_quote(c) for c in target_cols)
    insert_vals = ", ".join(f"s.{_firebird_quote(c)}" for c in target_cols)
    set_sql = ""
    if update_cols:
        set_sql = ", ".join(
            f"t.{_firebird_quote(c)} = s.{_firebird_quote(c)}" for c in update_cols
        )
    select_list = ", ".join(f":{c} AS {_firebird_quote(c)}" for c in target_cols)
    for row in rows:
        params = {c: row.get(c) for c in target_cols}
        if set_sql:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING (SELECT {select_list} FROM RDB$DATABASE) AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING (SELECT {select_list} FROM RDB$DATABASE) AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql), params)  # nosec B608
    return len(rows)


def _sybase_bracket(ident: str) -> str:
    return "[" + str(ident).replace("]", "]]") + "]"


def _sybase_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_sybase_bracket(table_obj.schema))
    parts.append(_sybase_bracket(table_obj.name))
    return ".".join(parts)


def _sybase_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native SAP ASE / Sybase ``MERGE`` (15.7+) with NULL-safe ON.

    Stages via ``SELECT … INTO #temp WHERE 1=0`` then MERGE — SAP Infocenter
    ASE MERGE class (same family as SQL Server, without HOLDLOCK). Still
    at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    stage = f"#df_mrg_{abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000}"
    target = _sybase_qualified_table(table_obj)
    col_sql = ", ".join(_sybase_bracket(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"SELECT {col_sql} INTO {stage} FROM {target} WHERE 1=0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_sybase_bracket,
        )
        insert_cols = ", ".join(_sybase_bracket(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_sybase_bracket(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_sybase_bracket(c)} = s.{_sybase_bracket(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _upsert_batch(
    conn: Any,
    table_obj: sa.Table,
    batch: list[dict[str, Any]],
    conflict_columns: list[str],
    target_cols: list[str],
    dialect_name: str,
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
      * Everyone else: chunked DELETE by equality keys followed by INSERT.

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
        return max(0, getattr(result, "rowcount", None) or 0) or len(batch)

    update_cols = [c for c in target_cols if c not in conflict_cols]
    lsn_guarded = DF_LSN_COL in target_cols

    # Keep the highest-LSN row per conflict key so CDC redelivery inside one
    # batch is deterministic; fall back to last-wins when no LSN column.
    if lsn_guarded:
        best: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in batch:
            key = tuple(row[c] for c in conflict_cols)
            prev = best.get(key)
            if prev is None or compare_lsn(row.get(DF_LSN_COL), prev.get(DF_LSN_COL)) >= 0:
                best[key] = row
        rows = list(best.values())
        # Prefetch existing LSNs and drop stale rows before any write so
        # ``rows_skipped`` accounting is exact and redelivery cannot regress.
        existing_lsn = _prefetch_existing_lsn(conn, table_obj, rows, conflict_cols)
        filtered: list[dict[str, Any]] = []
        for row in rows:
            key = tuple(row.get(c) for c in conflict_cols)
            prior = existing_lsn.get(key)
            incoming = row.get(DF_LSN_COL)
            if incoming is not None and compare_lsn(incoming, prior) <= 0:
                continue
            filtered.append(row)
        rows = filtered
    else:
        deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in batch:
            key = tuple(row[c] for c in conflict_cols)
            deduped[key] = row
        rows = list(deduped.values())

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
        _delete_by_keys(conn, table_obj, apply_rows, conflict_cols)
        result = conn.execute(table_obj.insert(), apply_rows)
        return max(0, getattr(result, "rowcount", None) or 0) or len(apply_rows)
    return 0


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
    from services.type_system import is_generated_always_column

    # Omit GENERATED ALWAYS from INSERT projection; keep mappings parallel.
    keep_idx = [
        i for i, typ in enumerate(logical_types) if not is_generated_always_column(typ)
    ]
    if len(keep_idx) < len(logical_types):
        target_cols = [target_cols[i] for i in keep_idx]
        logical_types = [logical_types[i] for i in keep_idx]
        mappings = [mappings[i] for i in keep_idx if i < len(mappings)]
    dest_db = (cfg.get("type") or "").lower()
    target_column_types = {}
    explicit_stamps: set[str] = set()
    for i, col in enumerate(target_cols):
        explicit = mappings[i].get("target_type") if i < len(mappings) else None
        source_type = (
            column_types.get(mappings[i]["source"]) if i < len(mappings) else None
        ) or (logical_types[i] if i < len(logical_types) else "string")
        # Map stamps / logicals through materialize_dest_ddl so CREATE cannot
        # invent REAL→DOUBLE or BQ TIMESTAMP→DATETIME after Map stamped.
        if explicit:
            derived = materialize_dest_ddl(dest_db, explicit) if dest_db else str(explicit)
            explicit_stamps.add(col)
        elif dest_db:
            derived = materialize_dest_ddl(dest_db, source_type)
        else:
            derived = source_type
        # DuckDB only: if preflight is skipped and the source DECIMAL has no
        # declared precision/scale (typical for CSV / file inference), fall
        # back to DOUBLE. This avoids inventing a scale that pads values like
        # 3.14 with trailing zeros, while preserving explicit database decimals.
        if (
            dest_db == "duckdb"
            and _kwargs.get("skip_preflight")
            and not mappings[i].get("user_override")
            and normalize_logical_type(derived) == "decimal"
            and not explicit
        ):
            p, s = parse_numeric_precision_scale(source_type)
            if p is None and s is None:
                derived = "DOUBLE"
                mappings[i] = {**mappings[i], "target_type": "DOUBLE"}
        target_column_types[col] = derived

    # Map≡ALTER: source DDL may propose a wider type; explicit Map stamps are a
    # hard ceiling (same helper as PostgreSQL / MySQL writers). Overflow cells
    # quarantine on write — never silent ALTER past the approved mapping.
    from connectors.writer_common import desired_types_honoring_map_stamps

    ceiling_types = [target_column_types[col] for col in target_cols]
    candidate_by_col: dict[str, str] = {}
    for i, col in enumerate(target_cols):
        if col in explicit_stamps:
            continue
        mapping_source = mappings[i].get("source_type") if i < len(mappings) else None
        catalog_source = (
            column_types.get(mappings[i].get("source")) if i < len(mappings) else None
        )
        source_type = _source_ddl_for_widen(mapping_source, catalog_source) or "string"
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
        explicit_columns=explicit_stamps,
    )
    if alter_refusals:
        logger.info(
            "generic_sql Map≡ALTER refusals (stamp ceiling): %s", alter_refusals
        )
    for i, col in enumerate(target_cols):
        new_typ = desired_list[i]
        old_typ = target_column_types[col]
        target_column_types[col] = new_typ
        if col not in explicit_stamps and new_typ != old_typ and i < len(mappings):
            mappings[i] = {**mappings[i], "target_type": new_typ}

    policy = transform_error_policy(error_policy)
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types=target_column_types,
        preserve_case=True,
        dest_kind=str(dest_db or type or "sql").lower(),
        # Upsert conflict cols / dest PK — full composite for quarantine replay identity.
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
    )
    _tgt_types_pre = [str(target_column_types.get(c, "") or "") for c in target_cols]
    # Engine-honest dialect labels (Databricks/Delta via generic_sql share this path).
    _engine_label = {
        "databricks": "Databricks",
        "duckdb": "DuckDB",
        "clickhouse": "ClickHouse",
        "trino": "Trino",
        "presto": "Presto",
        "athena": "Athena",
        "synapse": "Synapse",
        "motherduck": "MotherDuck",
    }.get(dest_db, dest_db.title() if dest_db else "SQL")
    from connectors.writer_common import apply_write_quarantine_matrix

    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        _tgt_types_pre,
        rejected_details,
        policy,
        dialect_label=_engine_label,
        mappings=mappings,
    )
    _tgt_types = _tgt_types_pre
    sparse_rows: list[tuple] = []
    rows_for_checksum: list[tuple] = list(mapped_rows)
    if write_mode == "upsert" and conflict_columns:
        mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
    # Dense INSERT/MERGE: absent schemaless fields → SQL NULL (sparse keeps sentinel).
    mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)

    _map_abort = reject_on_strict_policy(policy, rejected_details, 'SQL')
    if _map_abort or (transform_errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or database,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_rows=_rejected_row_count(
                data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
            ),
            rejected_details=rejected_details,
            warnings=transform_errors,
        )

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
    sa_col_types = {
        col: _sa_type_for_logical(
            target_column_types.get(col, "string"), dialect_name, cfg.get("type", "")
        )
        for col in target_cols
    }

    converted_rows: list[dict] = []
    for row in mapped_rows:
        converted_rows.append(
            {
                target_cols[i]: _to_sa_value(
                    row[i],
                    target_column_types.get(target_cols[i], "string"),
                    sa_col_types.get(target_cols[i]),
                    dialect_name,
                    cfg.get("type", ""),
                )
                for i in range(len(target_cols))
            }
        )
    sparse_converted: list[dict] = []
    for row in sparse_rows:
        sparse_converted.append(
            {
                target_cols[i]: _to_sa_value(
                    row[i],
                    target_column_types.get(target_cols[i], "string"),
                    sa_col_types.get(target_cols[i]),
                    dialect_name,
                    cfg.get("type", ""),
                )
                for i in range(len(target_cols))
            }
        )

    written = 0
    chunks_completed = 0
    rows_skipped = 0
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
                        (dialect_name or "").lower() == "mssql"
                        or db_type in {"sqlserver", "mssql", "azure_sql"}
                    ):
                        # T-SQL has no CREATE TABLE IF NOT EXISTS. Existence was
                        # already probed via inspector — emit plain CREATE TABLE.
                        conn.execute(sa.schema.CreateTable(table_obj))
                    else:
                        conn.execute(
                            sa.schema.CreateTable(table_obj, if_not_exists=True)
                        )
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

            if sparse_converted and write_mode == "upsert" and conflict_columns:
                from connectors.writer_common import row_has_missing_sentinel

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
                    )
                )
                written += sparse_written
                rows_skipped += sparse_skipped
                rows_for_checksum = [
                    r for r in rows_for_checksum if not row_has_missing_sentinel(r)
                ] + list(sparse_checksum)
                conn.commit()

            total = len(converted_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE) if total else 0
            # The ledger is only needed when a replay could duplicate rows. An
            # upsert keyed on conflict columns is already idempotent, so paying
            # for a ledger round-trip per chunk there would be pure overhead.
            ledger_table = None
            if ledger_job_id and total and not (write_mode == "upsert" and conflict_columns):
                ledger_table = ensure_sqlalchemy_write_ledger(conn, schema=schema_name)
                if ledger_table is not None:
                    conn.commit()
                else:
                    ledger_unavailable = True
            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = converted_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break

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
                        # A previous attempt already committed this chunk. Replay
                        # its recorded row count instead of re-inserting the rows.
                        written += already
                        ledger_chunks_skipped += 1
                        chunks_completed = chunk_idx + 1
                        if on_checkpoint:
                            on_checkpoint(chunks_completed, chunks, written)
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
                        )
                        if DF_LSN_COL in target_cols:
                            rows_skipped += len(batch) - chunk_written
                        else:
                            chunk_written = len(batch)
                    else:
                        result = conn.execute(table_obj.insert(), batch)
                        chunk_written = max(
                            0, getattr(result, "rowcount", None) or 0
                        ) or len(batch)
                    if ledger_table is not None:
                        mark_sqlalchemy_chunk_committed(
                            conn,
                            ledger_table,
                            job_id=ledger_job_id,
                            batch_key=ledger_batch_key,
                            chunk_idx=chunk_idx,
                            rows_written=chunk_written,
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
                    # One bad temporal/numeric cell must not abort the whole chunk:
                    # quarantine unfit rows (same contract as MySQL/Postgres writers).
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
                                    )
                                    if DF_LSN_COL in target_cols:
                                        if not row_written:
                                            rows_skipped += 1
                                    else:
                                        row_written = 1
                                else:
                                    result = conn.execute(table_obj.insert(), [row])
                                    row_written = 1 if getattr(result, "rowcount", None) is None else (max(0, result.rowcount or 0) or 1)
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
                                rejected_details.append(
                                    {
                                        "row": start + row_i,
                                        "column": col_name,
                                        "value": sample_val,
                                        "reason": str(row_exc)[:300],
                                        "policy": policy,
                                    }
                                )
                                transform_errors.append(str(row_exc)[:200])
                        if ledger_table is not None:
                            # Row-by-row salvage already committed the good rows.
                            # Record the surviving count so a retry skips them
                            # instead of duplicating them, and so it does not
                            # re-attempt the rows we know are unfit.
                            try:
                                mark_sqlalchemy_chunk_committed(
                                    conn,
                                    ledger_table,
                                    job_id=ledger_job_id,
                                    batch_key=ledger_batch_key,
                                    chunk_idx=chunk_idx,
                                    rows_written=chunk_written,
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
                        # Unknown policy: fail closed — do not land partial bad batch.
                        raise
                chunks_completed = chunk_idx + 1
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)

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
                        data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
                    ),
                    len(data_rows) - written - rows_skipped if data_rows else 0,
                ),
                rejected_details=rejected_details,
                coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )

        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or database,
            checksum=row_checksum(
                rows_for_checksum,
                target_cols,
                dest_db_type=str(cfg.get("type") or "generic_sql"),
                dest_types=target_column_types,
            ),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(
                _rejected_row_count(
                    data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
                ),
                len(data_rows) - written - rows_skipped if data_rows else 0,
            ),
            rejected_details=rejected_details,
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or database,
            checksum=row_checksum(
                rows_for_checksum,
                target_cols,
                dest_db_type=str(cfg.get("type") or "generic_sql"),
                dest_types=target_column_types,
            )
            if rows_for_checksum
            else "",
            chunks_completed=chunks_completed,
            error=str(exc),
            rejected_rows=_rejected_row_count(
                data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
            ),
            rejected_details=rejected_details,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    finally:
        release_engine(engine)
