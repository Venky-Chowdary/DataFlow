"""Universal SQLAlchemy connector for any SQL database with a Python DBAPI.

This connector lets DataFlow treat SQLAlchemy-supported engines as first-class
sources and destinations. The user provides the catalog type (e.g. mssql,
oracle, db2, trino, h2) or a full connection_string; we build the SQLAlchemy
URL and driver name from the catalog. This is the fastest path to 100+
real, working catalog IDs without needing a dedicated connector for every
engine.
"""

from __future__ import annotations

import base64
import contextlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Callable

from connectors.schema_drift import add_missing_columns
from services.value_serializer import cell_to_string

try:
    import sqlalchemy as sa
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.dialects import postgresql

    SQLALCHEMY_AVAILABLE = True

    try:
        from clickhouse_sqlalchemy import engines as ch_engines
        from clickhouse_sqlalchemy.types import DateTime64 as ChDateTime64
        from clickhouse_sqlalchemy.types import Nullable as ChNullable
    except Exception:  # pragma: no cover
        ch_engines = None
        ChDateTime64 = None
        ChNullable = None

    try:
        from trino.sqlalchemy.datatype import TIMESTAMP as TrinoTimestamp
    except Exception:  # pragma: no cover
        TrinoTimestamp = None
except Exception:  # pragma: no cover
    SQLALCHEMY_AVAILABLE = False
    ch_engines = None
    ChDateTime64 = None
    ChNullable = None
    TrinoTimestamp = None

from connectors.writer_common import (
    CHUNK_SIZE,
    _rejected_row_count,
    build_mapped_rows_with_details,
    quote_sql_identifier,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int | None = None


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


def _default_port(db_type: str) -> int:
    return _DEFAULT_PORT_MAP.get(db_type, 0)


def _normalize_sqlite_url(url: str) -> str:
    """Ensure absolute SQLite file paths use four leading slashes.

    SQLAlchemy interprets ``sqlite:///path`` as relative and
    ``sqlite:////absolute/path`` as absolute. Users often supply the former for
    absolute paths, so this normalizes only when the path component is absolute.
    """
    if url.startswith("sqlite:///") and not url.startswith("sqlite:////"):
        path = url[len("sqlite:///"):]
        if path and (path.startswith("/") or (len(path) > 1 and path[1] == ":")):
            return f"sqlite:////{path}"
    return url


def _build_url(cfg: dict[str, Any]) -> str | sa.URL:
    """Build a SQLAlchemy URL from host/port or use the explicit connection string."""
    connection_string = cfg.get("connection_string") or ""
    db_type = (cfg.get("type") or "").lower().strip()

    if connection_string:
        if connection_string.startswith("duckdb:") or connection_string.startswith("sqlite:"):
            if connection_string.startswith("sqlite:"):
                return _normalize_sqlite_url(connection_string)
            return connection_string
        if db_type == "duckdb":
            return f"duckdb:////{connection_string}" if connection_string.startswith("/") else f"duckdb:///{connection_string}"
        if db_type == "sqlite":
            return _normalize_sqlite_url(f"sqlite:///{connection_string}")
        return connection_string

    if not db_type:
        raise ValueError("A database type or connection_string is required")

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
        return f"duckdb:////{database}" if database.startswith("/") else f"duckdb:///{database or ':memory:'}"

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

    query = None
    if drivername.startswith("mssql+pyodbc"):
        query = {"driver": "ODBC Driver 17 for SQL Server"}

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
    url = _build_url(cfg)
    # Fast, safe defaults for local and network databases.
    db_type = (cfg.get("type") or "").lower()
    connection_string = (cfg.get("connection_string") or "").lower()
    # DuckDB and SQLite are file-based; use NullPool so the file lock is released
    # after each operation and external readers can open the database.
    if db_type in ("duckdb", "sqlite") or "duckdb" in connection_string or "sqlite://" in connection_string:
        from sqlalchemy.pool import NullPool

        return create_engine(url, poolclass=NullPool)
    return create_engine(url, pool_pre_ping=True, pool_recycle=600)


def get_sqlalchemy_engine(cfg: dict[str, Any]) -> Any:
    """Public accessor for a configured SQLAlchemy engine."""
    return _engine(cfg)


def _schema_name(cfg: dict[str, Any]) -> str | None:
    schema = cfg.get("schema") or ""
    db_type = (cfg.get("type") or "").lower()
    connection_string = (cfg.get("connection_string") or "").lower()
    # MySQL, MariaDB, SQLite and DuckDB do not use schemas; database is in the URL.
    if (
        db_type == "mysql"
        or db_type == "mariadb"
        or connection_string.startswith("mysql")
        or db_type == "sqlite"
        or connection_string.startswith("sqlite://")
        or db_type == "duckdb"
        or connection_string.startswith("duckdb:")
    ):
        return None
    if not schema:
        if db_type == "presto":
            return "public"
        if db_type == "trino":
            return "default"
    return schema or None


def get_sql_schema(cfg: dict[str, Any]) -> str | None:
    """Public accessor for the SQL schema name implied by a connector config."""
    return _schema_name(cfg)


def _type_repr(type_obj: Any) -> str:
    try:
        return str(type_obj).lower()
    except Exception:
        return ""


def _logical_type_from_sa(col_type: Any) -> str:
    """Map a SQLAlchemy type instance to a DataFlow logical type."""
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

    if isinstance(col_type, (sa.Numeric, sa.Float, sa.Double, sa.REAL)):
        return "decimal"

    if isinstance(col_type, (sa.DateTime,)):
        return "datetime"

    if isinstance(col_type, (sa.Date,)):
        return "date"

    if isinstance(col_type, (sa.Time,)):
        return "time"

    if isinstance(col_type, (sa.String, sa.Text, sa.CHAR)):
        return "string"

    # Fallback text matching for dialect-specific types not captured above
    if "json" in repr_ or "variant" in repr_ or "super" in repr_:
        return "json"
    if "array" in repr_:
        return "array"
    if "uuid" in repr_ or "guid" in repr_ or "uniqueidentifier" in repr_:
        return "uuid"
    if any(x in repr_ for x in ("binary", "blob", "bytea", "varbinary", "image", "raw")):
        return "binary"
    if any(x in repr_ for x in ("numeric", "decimal", "number", "double", "float", "real", "money", "smallmoney")):
        return "decimal"
    if any(x in repr_ for x in ("int", "serial", "smallint", "tinyint", "bigint")):
        return "integer"
    if "bool" in repr_ or "bit" in repr_:
        return "boolean"
    if "datetime" in repr_ or "timestamp" in repr_:
        return "datetime"
    if "date" in repr_:
        return "date"
    if "time" in repr_:
        return "time"
    if any(x in repr_ for x in ("char", "varchar", "text", "clob", "string")):
        return "string"

    return normalize_logical_type(repr_)


def _sa_type_for_logical(logical: str, dialect_name: str, db_type: str = "") -> Any:
    """Map a DataFlow logical type to a SQLAlchemy type that compiles for the engine."""
    t = (logical or "string").lower().strip()

    def _maybe_nullable(sa_type: Any) -> Any:
        if dialect_name == "clickhouse" and ChNullable is not None:
            return ChNullable(sa_type)
        return sa_type

    if t == "integer":
        return _maybe_nullable(sa.BigInteger())
    if t == "decimal":
        if db_type == "risingwave":
            return sa.Numeric()
        if db_type in ("questdb", "duckdb"):
            return sa.Double()
        if db_type == "presto":
            return sa.DECIMAL(38, 15)
        # PostgreSQL-wire engines (Citus, Materialize, CrateDB, etc.) store
        # arbitrary-scale NUMERIC without padding, so avoid fixed scale.
        if dialect_name == "postgresql":
            return sa.Numeric()
        return _maybe_nullable(sa.Numeric(38, 15))
    if t == "boolean":
        return _maybe_nullable(sa.Boolean())
    if t == "date":
        return _maybe_nullable(sa.Date())
    if t in ("datetime", "timestamp"):
        if db_type == "questdb":
            return sa.DateTime()
        # Preserve timezone metadata when the target dialect supports it.
        if dialect_name == "clickhouse":
            return _maybe_nullable(ChDateTime64(3) if ChDateTime64 is not None else sa.DateTime())
        if db_type == "trino" and TrinoTimestamp is not None:
            return TrinoTimestamp(precision=3, timezone=True)
        if db_type == "presto":
            return sa.TIMESTAMP()
        return sa.DateTime(timezone=True)
    if t == "time":
        # ClickHouse, QuestDB and Presto (PyHive) do not bind Python time objects
        # reliably; store as string in these engines.
        if dialect_name == "clickhouse" or db_type in ("clickhouse", "questdb", "presto"):
            return _maybe_nullable(sa.String())
        return _maybe_nullable(sa.Time())
    if t == "uuid":
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
    if t in ("json", "array"):
        if db_type in ("oracle", "clickhouse", "trino", "questdb", "presto", "duckdb"):
            return _maybe_nullable(sa.Text())
        if dialect_name == "postgresql":
            return postgresql.JSONB()
        return sa.JSON()
    if t == "binary":
        if db_type in ("clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        return sa.LargeBinary()
    return _maybe_nullable(sa.Text())


def _is_string_type(sa_type: Any) -> bool:
    if sa_type is None:
        return False
    if isinstance(sa_type, (sa.String, sa.Text, sa.CHAR)):
        return True
    # Handle ClickHouse Nullable(String) / Nullable(TEXT)
    nested = getattr(sa_type, "nested_type", None)
    if nested is not None and isinstance(nested, (sa.String, sa.Text, sa.CHAR)):
        return True
    return False


def _to_sa_value(value: Any, logical: str, sa_type: Any = None, dialect_name: str = "", db_type: str = "") -> Any:
    """Convert transform-engine output values to Python objects SQLAlchemy accepts."""
    if value is None:
        return None

    t = (logical or "string").lower().strip()

    if t in ("json", "array"):
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except Exception:
                parsed = value
        else:
            parsed = value

        if isinstance(parsed, (dict, list)):
            if _is_string_type(sa_type):
                return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"), default=str)
            return parsed
        if isinstance(value, str):
            return value
        return value

    if t == "binary":
        if isinstance(value, bytes):
            if _is_string_type(sa_type):
                try:
                    return base64.b64encode(value).decode("ascii")
                except Exception:
                    return value.decode("utf-8", errors="replace")
            return value
        if isinstance(value, str):
            if _is_string_type(sa_type):
                return value
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return value.encode("utf-8")
        return value

    if t == "date":
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except Exception:
                return value
        return value

    if t in ("datetime", "timestamp"):
        def _ensure_utc(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        def _naive_utc(dt: datetime) -> datetime:
            return _ensure_utc(dt).replace(tzinfo=None)

        if isinstance(value, datetime):
            if db_type == "questdb":
                return _naive_utc(value)
            return _ensure_utc(value)
        if isinstance(value, date):
            dt = datetime.combine(value, time())
            if db_type == "questdb":
                return _naive_utc(dt)
            return _ensure_utc(dt)
        if isinstance(value, str):
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
                if db_type == "questdb":
                    return _naive_utc(dt)
                return _ensure_utc(dt)
            except Exception:
                return value
        return value

    if t == "time":
        if _is_string_type(sa_type):
            if isinstance(value, time):
                return value.isoformat()
            if isinstance(value, datetime):
                return value.time().isoformat()
            if isinstance(value, str):
                return value
            return str(value)
        if isinstance(value, time):
            # PyHive / Presto does not accept Python time objects; send an ISO string.
            if db_type == "presto" or dialect_name == "presto":
                return value.isoformat()
            return value
        if isinstance(value, datetime):
            t = value.time()
            if db_type == "presto" or dialect_name == "presto":
                return t.isoformat()
            return t
        if isinstance(value, str):
            try:
                t = time.fromisoformat(value)
                if db_type == "presto" or dialect_name == "presto":
                    return value
                return t
            except Exception:
                return value
        return value

    if t == "decimal":
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(value)
        if isinstance(value, str):
            return Decimal(value)
        return value

    if t == "integer":
        if isinstance(value, int):
            return value
        if isinstance(value, (float, Decimal)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except Exception:
                return value
        return value

    # integer, decimal, boolean, uuid, string/text are already bound-friendly
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
    except Exception as exc:
        return False, str(exc)


def _reflect_table(
    engine: Any,
    table: str,
    schema: str | None,
    columns: list[str] | None = None,
    include_pk: bool = False,
) -> sa.Table:
    """Reflect or build a Table object for reading/writing."""
    metadata = sa.MetaData()
    # Quote identifiers for safety with reserved words and case-sensitive engines.
    table_obj = sa.Table(
        table,
        metadata,
        schema=schema,
        quote=True,
        quote_schema=True,
        autoload_with=engine,
    )
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
    conflict_cols = [c for c in (conflict_columns or []) if c in columns]
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
        return sa.Table(
            table_name,
            metadata,
            *cols,
            *constraints,
            ch_engines.MergeTree(order_by=sa.text("tuple()")),
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


def _infer_logical_from_samples(values: list[Any], field_name: str = "") -> str | None:
    """Use DataFlow value inference to narrow generic SQL String columns.

    We intentionally do NOT narrow string columns to INTEGER or DECIMAL: a
    string column may contain codes, identifiers, bit strings, or formatted
    values (e.g. $1,000.00, 1010) that would be corrupted by numeric coercion.
    Structural/representational types (JSON, UUID, BINARY, DATE, TIME, etc.)
    are still recovered safely.
    """
    try:
        from services.schema_inference import infer_type

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
        samples = [cell_to_string(v) if v is not None else "" for v in values]
        return mapped.get(infer_type(samples, field_name=field_name))
    except Exception:
        return None


def _sample_raw_table(conn: Any, table: str, schema: str | None) -> tuple[list[str], list[Any]]:
    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    qualified = f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted
    result = conn.execute(sa.text(f"SELECT * FROM {qualified} LIMIT 200"))
    headers = list(result.keys())
    rows = result.fetchall()
    return headers, rows


def introspect_table_schema(
    cfg: dict[str, Any],
    table: str,
) -> dict[str, Any]:
    """Return schema metadata for the table using SQLAlchemy reflection."""
    if not SQLALCHEMY_AVAILABLE:
        return {"ok": False, "error": "SQLAlchemy is not installed", "columns": [], "tables": []}
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
                    from services.type_system import normalize_logical_type
                    schema_expr = "current_schema()" if schema is None else ":schema"
                    params: dict = {"table": table}
                    if schema is not None:
                        params["schema"] = schema
                    sql = (
                        f"SELECT column_name, data_type, is_nullable "
                        f"FROM information_schema.columns "
                        f"WHERE table_name = :table AND table_schema = {schema_expr} "
                        f"ORDER BY ordinal_position"
                    )
                    rows = conn.execute(sa.text(sql), params).fetchall()
                    if rows:
                        result = [
                            {
                                "name": name,
                                "inferred_type": normalize_logical_type(data_type),
                                "nullable": str(nullable).upper() != "NO",
                            }
                            for name, data_type, nullable in rows
                        ]
                        # Refine text columns from a sample to recover JSON, UUID, BINARY, etc.
                        headers, sample_rows = _sample_raw_table(conn, table, schema)
                        if sample_rows and headers:
                            name_to_idx = {n: i for i, n in enumerate(headers)}
                            for col in result:
                                if col["inferred_type"] == "string":
                                    idx = name_to_idx.get(col["name"])
                                    if idx is None:
                                        continue
                                    values = [row[idx] for row in sample_rows if idx < len(row)]
                                    inferred = _infer_logical_from_samples(values, field_name=col["name"])
                                    if inferred and inferred != "string":
                                        col["inferred_type"] = inferred
                        return {"ok": True, "columns": result, "tables": [table], "schema": schema or ""}
            except Exception:
                pass

            with engine.connect() as conn:
                headers, sample_rows = _sample_raw_table(conn, table, schema)
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
                        inferred = _infer_logical_from_samples(values, field_name=col["name"])
                        if inferred:
                            col["inferred_type"] = inferred
                return {"ok": True, "columns": result, "tables": [table], "schema": schema or ""}

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
                headers, sample_rows = _sample_raw_table(conn, table, schema)
                if sample_rows:
                    for idx, col in enumerate(result):
                        if col["inferred_type"] == "string":
                            values = [row[idx] for row in sample_rows if idx < len(row)]
                            inferred = _infer_logical_from_samples(values, field_name=col["name"])
                            if inferred and inferred != "string":
                                col["inferred_type"] = inferred
        except Exception:
            pass

        return {"ok": True, "columns": result, "tables": [table], "schema": schema or ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}
    finally:
        engine.dispose()


def drop_table(cfg: dict[str, Any], table: str, schema: str | None = None) -> bool:
    """Drop a table using SQLAlchemy dialect-aware DDL with a raw fallback."""
    if not SQLALCHEMY_AVAILABLE:
        return False
    engine = _engine(cfg)
    try:
        schema = schema or _schema_name(cfg)
        table_quoted = quote_sql_identifier(table)
        schema_quoted = quote_sql_identifier(schema) if schema else None
        qualified = f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted
        with engine.connect() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {qualified}"))
            conn.commit()
        return True
    except Exception:
        try:
            table_obj = sa.Table(table, sa.MetaData(), schema=schema)
            table_obj.drop(engine, checkfirst=True)
            return True
        except Exception:
            return False
    finally:
        engine.dispose()


def delete_by_primary_keys(
    cfg: dict[str, Any],
    table: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None = None,
) -> int:
    """Delete rows by primary key using a dialect-aware parameterized statement."""
    if not SQLALCHEMY_AVAILABLE or not keys:
        return 0
    engine = _engine(cfg)
    try:
        schema = schema or _schema_name(cfg)
        table_quoted = quote_sql_identifier(table)
        schema_quoted = quote_sql_identifier(schema) if schema else None
        qualified = f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted
        pk_quoted = quote_sql_identifier(primary_key_column)
        placeholders = ",".join([":k{}".format(i) for i in range(len(keys))])
        params = {"k{}".format(i): k for i, k in enumerate(keys)}
        stmt = f"DELETE FROM {qualified} WHERE {pk_quoted} IN ({placeholders})"
        with engine.connect() as conn:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            return result.rowcount or 0
    except Exception:
        return 0
    finally:
        engine.dispose()


def _read_table_raw(
    conn: Any,
    table: str,
    schema: str | None,
    offset: int,
    limit: int,
) -> tuple[list[str], list[list[Any]]]:
    """Fallback read for engines whose SQLAlchemy reflection is incomplete."""
    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    qualified = f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted
    sql = f"SELECT * FROM {qualified}"
    if offset > 0:
        sql += f" LIMIT {limit} OFFSET {offset}"
    else:
        sql += f" LIMIT {limit}"
    result = conn.execute(sa.text(sql))
    headers = list(result.keys())
    rows = [[cell_to_string(value) for value in row] for row in result.fetchall()]
    return headers, rows


def _count_table_raw(
    conn: Any,
    table: str,
    schema: str | None,
) -> int:
    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    qualified = f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted
    try:
        return conn.execute(sa.text(f"SELECT COUNT(*) FROM {qualified}")).scalar() or 0
    except Exception:
        return 0


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
        host, port, database, username, password, schema, connection_string, ssl, type=type
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
                    selected_cols = [table_obj.c[c] for c in columns if c in table_obj.c]
                else:
                    columns = selected_cols = list(table_obj.c)

                stmt = sa.select(*selected_cols)
                if offset > 0:
                    order_col = selected_cols[0]
                    stmt = stmt.order_by(order_col).offset(offset).limit(limit)
                else:
                    stmt = stmt.limit(limit)

                fetched = conn.execute(stmt).fetchall()
                headers = [c.name for c in selected_cols]
                rows = [[cell_to_string(value) for value in row] for row in fetched]

                if known_total_rows is not None:
                    total = known_total_rows
                else:
                    try:
                        total = conn.execute(
                            sa.select(sa.func.count()).select_from(table_obj)
                        ).scalar()
                    except Exception:
                        total = len(rows)
            except Exception:
                # Engines like RisingWave/QuestDB have incomplete pg_catalog reflection.
                headers, rows = _read_table_raw(conn, table, schema_name, offset, limit)
                if known_total_rows is not None:
                    total = known_total_rows
                else:
                    total = _count_table_raw(conn, table, schema_name)
                    if not total:
                        total = len(rows)

        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        engine.dispose()


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
) -> ReadBatch:
    """Cursor/keyset pagination for incremental and streaming transfers."""
    if not SQLALCHEMY_AVAILABLE:
        raise RuntimeError("SQLAlchemy is not installed")

    cfg = _cfg_from_params(
        host, port, database, username, password, schema, connection_string, ssl, type=type
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
                raise ValueError(f"Cursor column '{cursor_column}' not found in table {table}")
            cursor_col = table_obj.c[cursor_column]
            selected_cols = list(table_obj.c)
            if columns:
                selected_cols = [table_obj.c[c] for c in columns if c in table_obj.c]
            else:
                columns = selected_cols = list(table_obj.c)

            stmt = sa.select(*selected_cols)
            if cursor_after:
                # Cast the cursor string to the reflected column type so numeric and
                # date/timestamp cursors compare correctly.
                marker = sa.cast(sa.literal(cursor_after), cursor_col.type)
                stmt = stmt.where(cursor_col > marker)
            stmt = stmt.order_by(cursor_col).limit(limit)

            fetched = conn.execute(stmt).fetchall()
            headers = [c.name for c in selected_cols]
            rows = [[cell_to_string(value) for value in row] for row in fetched]

        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        engine.dispose()


def _delete_by_keys(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    chunk_size: int = 1000,
) -> None:
    """Delete existing rows that match the provided conflict keys.

    Uses equality ``OR (a=1 AND b=2)`` clauses instead of ``(a,b) IN (...)`` so
    that NULL keys match correctly and dialects with limited tuple-IN support
    still work.  Deletions are chunked to avoid generating queries that exceed
    engine statement-length limits.
    """
    if not rows:
        return
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        clauses = [
            sa.and_(*[
                (table_obj.c[c].is_(None) if row[c] is None else table_obj.c[c] == row[c])
                for c in conflict_cols
            ])
            for row in chunk
        ]
        conn.execute(sa.delete(table_obj).where(sa.or_(*clauses)))


def _upsert_batch(
    conn: Any,
    table_obj: sa.Table,
    batch: list[dict[str, Any]],
    conflict_columns: list[str],
    target_cols: list[str],
    dialect_name: str,
) -> None:
    """Write a batch idempotently using the best native upsert available.

    Deduplicates the batch on the conflict key, then:
      * PostgreSQL, SQLite, MySQL/MariaDB: native ``ON CONFLICT`` /
        ``ON DUPLICATE KEY`` upsert.
      * Everyone else: chunked DELETE by equality keys followed by INSERT.
    """
    conflict_cols = [c for c in conflict_columns if c in target_cols]
    if not conflict_cols:
        conn.execute(table_obj.insert(), batch)
        return

    # Last occurrence of each conflict key wins within the batch.
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in batch:
        key = tuple(row[c] for c in conflict_cols)
        deduped[key] = row
    rows = list(deduped.values())

    update_cols = [c for c in target_cols if c not in conflict_cols]

    def _native_upsert() -> bool:
        try:
            if dialect_name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(table_obj).values(rows)
                if update_cols:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_cols,
                        set_={c: stmt.excluded[c] for c in update_cols},
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                conn.execute(stmt)
                return True

            if dialect_name == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                stmt = sqlite_insert(table_obj).values(rows)
                if update_cols:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_cols,
                        set_={c: stmt.excluded[c] for c in update_cols},
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                conn.execute(stmt)
                return True

            if dialect_name in ("mysql", "mariadb"):
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                stmt = mysql_insert(table_obj).values(rows)
                if update_cols:
                    stmt = stmt.on_duplicate_key_update(
                        {c: stmt.inserted[c] for c in update_cols}
                    )
                else:
                    stmt = stmt.prefix_with("IGNORE")
                conn.execute(stmt)
                return True
        except Exception:
            # Native upsert can fail if the table lacks the required unique
            # index/constraint.  Roll back the aborted transaction so the
            # delete+insert fallback can run cleanly.
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        return False

    if not _native_upsert():
        _delete_by_keys(conn, table_obj, rows, conflict_cols)
        conn.execute(table_obj.insert(), rows)


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
        host, port, database, username, password, schema, connection_string, ssl, type=type
    )
    engine = _engine(cfg)
    schema_name = _schema_name(cfg)

    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    target_column_types = {
        target_cols[i]: (
            mappings[i].get("target_type")
            or column_types.get(mappings[i]["source"])
            or "string"
        )
        for i in range(len(target_cols))
    }

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
    )

    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or database,
            checksum="",
            chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_rows=_rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
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
    sa_col_types = {col: _sa_type_for_logical(target_column_types.get(col, "string"), dialect_name, cfg.get("type", "")) for col in target_cols}

    converted_rows: list[dict] = []
    for row in mapped_rows:
        converted_rows.append(
            {target_cols[i]: _to_sa_value(row[i], target_column_types.get(target_cols[i], "string"), sa_col_types.get(target_cols[i]), dialect_name, cfg.get("type", "")) for i in range(len(target_cols))}
        )

    try:
        with engine.connect() as conn:
            db_type = (cfg.get("type") or "").lower()
            if db_type == "questdb":
                # QuestDB's pg_catalog reflection is incomplete; use idempotent DDL.
                table_exists = False
            else:
                inspector = inspect(engine)
                table_exists = inspector.has_table(table_name, schema=schema_name)

            if write_mode == "replace" and table_exists:
                conn.execute(sa.schema.DropTable(table_obj, if_exists=True))
                conn.commit()
                table_exists = False

            if create_table and not table_exists:
                try:
                    if db_type == "questdb":
                        # QuestDB supports TIMESTAMP but not the PG "WITHOUT TIME ZONE" clause.
                        ddl = str(sa.schema.CreateTable(table_obj, if_not_exists=True).compile(dialect=engine.dialect))
                        ddl = ddl.replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP").replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
                        conn.execute(sa.text(ddl))
                    else:
                        conn.execute(sa.schema.CreateTable(table_obj, if_not_exists=True))
                    conn.commit()
                except Exception as exc:
                    # If the dialect does not support IF NOT EXISTS and the table
                    # was created concurrently, ignore the error and continue.
                    err = str(exc).lower()
                    if "already exists" in err or "duplicate" in err:
                        conn.rollback()
                    else:
                        raise

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

            total = len(converted_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
            written = 0
            for chunk_idx in range(chunks):
                batch = converted_rows[chunk_idx * CHUNK_SIZE : (chunk_idx + 1) * CHUNK_SIZE]
                if not batch:
                    break

                if write_mode == "upsert" and conflict_columns:
                    _upsert_batch(conn, table_obj, batch, conflict_columns, target_cols, dialect_name)
                else:
                    conn.execute(table_obj.insert(), batch)
                conn.commit()
                written += len(batch)
                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, chunks, written)

        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or database,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=chunks,
            rejected_rows=_rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
            rejected_details=rejected_details,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or database,
            checksum="",
            chunks_completed=0,
            error=str(exc),
            rejected_rows=_rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
            rejected_details=rejected_details,
            warnings=transform_errors,
        )
    finally:
        engine.dispose()
