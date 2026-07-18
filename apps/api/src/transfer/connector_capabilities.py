"""Connector capability manifest — single source of truth for catalog honesty."""

from __future__ import annotations

import importlib.util
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, FrozenSet

# Driver-level capabilities (implemented in connectors/ + adapters.py)
_DRIVER_CAPS: dict[str, dict[str, bool]] = {
    "postgresql": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "mysql": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "mongodb": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "snowflake": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "bigquery": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "dynamodb": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "redis": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "s3": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "elasticsearch": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "redshift": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "pgvector": {"test": True, "read": False, "write": True, "introspect": False, "preflight": True, "dest_only": True},
    "qdrant": {"test": True, "read": False, "write": True, "introspect": False, "preflight": True, "dest_only": True},
    "gcs": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "adls": {"test": True, "read": True, "write": True, "introspect": False, "preflight": True},
    "sqlite": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "sftp": {"test": True, "read": True, "write": True, "introspect": False, "preflight": False},
    "email": {"test": True, "read": False, "write": True, "introspect": False, "preflight": False, "dest_only": True},
    "salesforce": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "hubspot": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "stripe": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "rest_api": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "influxdb": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "neo4j": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "couchbase": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
    "singer_tap": {"test": True, "read": True, "write": False, "introspect": False, "preflight": False, "source_only": True},
}

# File format capabilities (FileParser + registry)
_FILE_CAPS: dict[str, dict[str, bool]] = {
    "csv": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "tsv": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "json": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "jsonl": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "ndjson": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "excel": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "parquet": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "avro": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "orc": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "xml": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
}

# Catalog marketplace id → driver / format type
CATALOG_ID_ALIASES: dict[str, str] = {
    "csv___tsv": "csv",
    "amazon_s3": "s3",
    "aws_s3": "s3",
    "google_cloud_storage": "gcs",
    "gcs": "gcs",
    "adls": "adls",
    "azure_blob_storage": "adls",
    "azure_data_lake": "adls",
    "azure_data_lake_storage": "adls",
    "minio": "s3",
    "wasabi": "s3",
    "backblaze_b2": "s3",
    "digitalocean_spaces": "s3",
    "cloudflare_r2": "s3",
    "amazon_redshift": "redshift",
    "google_bigquery": "bigquery",
    "opensearch": "elasticsearch",
    "amazon_elasticsearch": "elasticsearch",
    "elastic_cloud": "elasticsearch",
    "amazon_dynamodb": "dynamodb",
    "planetscale": "mysql",
    "mariadb": "mysql",
    "percona_mysql": "mysql",
    "amazon_aurora_mysql": "mysql",
    "amazon_aurora_postgresql": "postgresql",
    "amazon_rds_postgresql": "postgresql",
    "amazon_rds_mysql": "mysql",
    "google_cloud_sql_postgresql": "postgresql",
    "google_cloud_sql_mysql": "mysql",
    "azure_database_postgresql": "postgresql",
    "azure_database_mysql": "mysql",
    "supabase": "postgresql",
    "neon": "postgresql",
    "timescaledb": "postgresql",
    "cockroachdb": "postgresql",
    "alibaba_oss": "s3",
    "alibaba_cloud_object_storage": "s3",
    "azure_cosmos_db": "mongodb",
    "google_biglake": "bigquery",
    "amazon_emr": "generic_sql",
    "cloudera_data_platform": "generic_sql",
    "sap_bw_4hana": "generic_sql",
    "motherduck": "generic_sql",
    "firebase_realtime_db": "rest_api",
    "jsonl": "jsonl",
    "ndjson": "ndjson",
    "sftp": "sftp",
    "ssh": "sftp",
    "email": "email",
    "smtp": "email",
}

# Suggested lists — only connectors users can configure today
SUGGESTED_SOURCES = [
    "postgresql", "mongodb", "mysql", "snowflake", "bigquery", "redshift",
    "csv___tsv", "json", "jsonl", "excel", "parquet",
    "dynamodb", "amazon_s3", "gcs", "google_cloud_storage", "adls", "redis", "elasticsearch",
    "sftp",
    "salesforce", "hubspot", "stripe",
]

# Catalog entry ids that map to implemented drivers — blocks false "Full transfer" on aliases
TRANSFER_READY_CATALOG_IDS = frozenset({
    "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
    "dynamodb", "amazon_s3", "s3", "gcs", "google_cloud_storage", "adls",
    "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage",
    "redis", "elasticsearch", "sqlite", "generic_sql",
    "csv___tsv", "json", "jsonl", "ndjson", "excel", "parquet",
    "sftp", "email",
})

SUGGESTED_DESTINATIONS = [
    "postgresql", "mongodb", "mysql", "snowflake", "bigquery", "redshift",
    "dynamodb", "amazon_s3", "gcs", "google_cloud_storage", "adls", "redis", "elasticsearch",
    "sftp", "email",
]


def default_port(driver_type: str) -> int:
    return {
        "mongodb": 27017,
        "mysql": 3306,
        "postgresql": 5432,
        "redis": 6379,
        "elasticsearch": 9200,
        "snowflake": 443,
        "bigquery": 443,
        "dynamodb": 443,
        "s3": 443,
        "gcs": 443,
        "adls": 443,
        "redshift": 5439,
        "pgvector": 5432,
        "qdrant": 6333,
        "sqlite": 0,
        "generic_sql": 0,
        "sftp": 22,
        "email": 587,
        "salesforce": 443,
        "hubspot": 443,
        "stripe": 443,
        "rest_api": 443,
        "influxdb": 8086,
        "neo4j": 7474,
        "couchbase": 8093,
    }.get((driver_type or "").lower(), 5432)


def resolve_driver_type(catalog_id: str) -> str:
    """Map catalog entry id to implemented driver or file format key."""
    cid = (catalog_id or "").lower().strip()
    if not cid:
        return "unknown"
    if cid == "generic_sql":
        return "generic_sql"
    if cid in CATALOG_ID_ALIASES:
        return CATALOG_ID_ALIASES[cid]
    if cid in _DRIVER_CAPS:
        return cid
    if cid in _FILE_CAPS:
        return cid

    # Generic REST source driver for SaaS/API catalog entries that don't have a
    # dedicated native connector yet. This must run before the generic SQL substring
    # fallback so connectors like athenahealth or oracle_hcm (which contain SQL
    # engine substrings) are routed to the API driver instead of generic SQL.
    if cid in _saas_catalog_ids():
        return "rest_api"

    # Generic SQL fallback handles any SQL engine with a SQLAlchemy dialect.
    def _valid(driver: str) -> bool:
        return driver in _DRIVER_CAPS or driver in _FILE_CAPS or driver == "generic_sql"

    for needle, driver in [
        # Existing first-class drivers
        ("postgres", "postgresql"),
        ("mongo", "mongodb"),
        ("documentdb", "mongodb"),
        ("mysql", "mysql"),
        ("mariadb", "mysql"),
        ("aurora", "mysql"),
        ("planetscale", "mysql"),
        ("vitess", "mysql"),
        ("tidb", "mysql"),
        ("oceanbase", "mysql"),
        ("polardb", "mysql"),
        ("singlestore", "mysql"),
        ("gaussdb", "mysql"),
        ("goldendb", "mysql"),
        ("percona", "mysql"),
        ("snowflake", "snowflake"),
        ("bigquery", "bigquery"),
        ("dynamodb", "dynamodb"),
        ("redis", "redis"),
        ("elastic", "elasticsearch"),
        ("opensearch", "elasticsearch"),
        ("redshift", "redshift"),
        ("timescale", "postgresql"),
        ("cockroach", "postgresql"),
        ("yugabyte", "postgresql"),
        ("alloydb", "postgresql"),
        ("supabase", "postgresql"),
        ("neon", "postgresql"),
        ("minio", "s3"),
        ("wasabi", "s3"),
        ("backblaze", "s3"),
        ("spaces", "s3"),
        ("object_storage", "s3"),
        # Generic SQL engines
        ("mssql", "generic_sql"),
        ("sql_server", "generic_sql"),
        ("sqlserver", "generic_sql"),
        ("microsoft_sql", "generic_sql"),
        ("azure_sql", "generic_sql"),
        ("oracle", "generic_sql"),
        ("db2", "generic_sql"),
        ("ibm_db2", "generic_sql"),
        ("teradata", "generic_sql"),
        ("netezza", "generic_sql"),
        ("vertica", "generic_sql"),
        ("exasol", "generic_sql"),
        ("sybase", "generic_sql"),
        ("sap_ase", "generic_sql"),
        ("sap_iq", "generic_sql"),
        ("sap_hana", "generic_sql"),
        ("hana", "generic_sql"),
        ("firebird", "generic_sql"),
        ("h2", "generic_sql"),
        ("clickhouse", "generic_sql"),
        ("druid", "generic_sql"),
        ("pinot", "generic_sql"),
        ("presto", "generic_sql"),
        ("trino", "generic_sql"),
        ("hive", "generic_sql"),
        ("spark", "generic_sql"),
        ("impala", "generic_sql"),
        ("phoenix", "generic_sql"),
        ("duckdb", "generic_sql"),
        ("databricks", "generic_sql"),
        ("greenplum", "generic_sql"),
        ("cratedb", "generic_sql"),
        ("questdb", "generic_sql"),
        ("doris", "generic_sql"),
        ("starrocks", "generic_sql"),
        ("citus", "generic_sql"),
        ("dremio", "generic_sql"),
        ("firebolt", "generic_sql"),
        ("risingwave", "generic_sql"),
        ("materialize", "generic_sql"),
        ("yellowbrick", "generic_sql"),
        ("actian", "generic_sql"),
        ("informix", "generic_sql"),
        ("athena", "generic_sql"),
        ("synapse", "generic_sql"),
        ("azure_synapse", "generic_sql"),
    ]:
        if needle in cid and _valid(driver):
            return driver
    if "gcs" in cid or "google_cloud_storage" in cid or ("cloud_storage" in cid and "google" in cid):
        return "gcs"
    if "parquet" in cid:
        return "parquet"
    if "s3" in cid or "aws_s3" in cid or "object_storage" in cid:
        return "s3"
    if "azure_blob_storage" in cid or "azure_data_lake" in cid or "adls" in cid:
        return "adls"
    if cid.startswith("jsonl") or cid.endswith("jsonl"):
        return "jsonl"
    if "json" in cid and "jsonl" not in cid:
        return "json"
    if "csv" in cid or "tsv" in cid:
        return "csv"

    base = cid.replace("___", "_").split("_")[0]
    if base in _DRIVER_CAPS or base in _FILE_CAPS or base == "generic_sql":
        return base
    return base


@lru_cache(maxsize=1)
def _saas_catalog_ids() -> FrozenSet[str]:
    """Catalog IDs that are SaaS/API connectors and can use the generic REST driver."""
    try:
        path = Path(__file__).resolve().parents[2] / "data" / "connector_catalog.json"
        with open(path, encoding="utf-8") as f:
            catalog = json.load(f)
        return frozenset(
            c["id"].lower().strip()
            for c in catalog.get("connectors", [])
            if c.get("category") in ("saas", "api", "finance", "marketing", "healthcare", "logistics")
            and c.get("id", "").lower().strip() not in CATALOG_ID_ALIASES
            and c.get("id", "").lower().strip() not in _DRIVER_CAPS
            and c.get("id", "").lower().strip() not in _FILE_CAPS
        )
    except Exception:
        return frozenset()


def _sqlalchemy_available() -> bool:
    try:
        return importlib.util.find_spec("sqlalchemy") is not None
    except Exception:
        return False


# First-class / file drivers and the DBAPI / library modules they need
_DRIVER_MODULE: dict[str, str | None] = {
    "postgresql": "psycopg2",
    "mysql": "pymysql",
    "redshift": "psycopg2",
    "mongodb": "pymongo",
    "redis": "redis",
    "elasticsearch": "elasticsearch",
    "dynamodb": "boto3",
    "s3": "boto3",
    "gcs": "google.cloud.storage",
    "adls": "azure.storage.blob",
    "snowflake": "snowflake.connector",
    "bigquery": "google.cloud.bigquery",
    "sqlite": "sqlite3",
    "sftp": "paramiko",
    "email": None,
    "salesforce": "requests",
    "hubspot": "requests",
    "stripe": "requests",
    "rest_api": "requests",
    "influxdb": "requests",
    "neo4j": "requests",
    "couchbase": "requests",
    "csv": None,
    "tsv": None,
    "json": None,
    "jsonl": None,
    "ndjson": None,
    "excel": "openpyxl",
    "parquet": "pyarrow",
    "avro": "fastavro",
    "orc": "pyarrow",
    "xml": "xmltodict",
}


# Map a generic SQL engine drivername to the DBAPI module it requires
_GENERIC_DRIVERNAME_TO_MODULE: dict[str, str] = {
    "postgresql+psycopg2": "psycopg2",
    "mysql+pymysql": "pymysql",
    "duckdb": "duckdb",
    "sqlite": "sqlite3",
    "mssql+pyodbc": "pyodbc",
    "oracle+oracledb": "oracledb",
    "ibm_db_sa": "ibm_db",
    "teradatasql": "teradatasql",
    "nzpsql": "nzpsql",
    "vertica+vertica_python": "vertica_python",
    "exasol+pyodbc": "pyodbc",
    "sybase+pyodbc": "pyodbc",
    "sap_hana": "hdbcli",
    "hana": "hdbcli",
    "firebird+fdb": "fdb",
    "h2": None,
    "clickhouse+native": "clickhouse_driver",
    "druid": "pydruid",
    "pinot": "pinotdb",
    "presto": "pyhive",
    "trino": "trino",
    "hive": "pyhive",
    "impala": "impala",
    "spark": "pyhive",
    "phoenix": "phoenixdb",
    "databricks": "databricks",
    "databricks+connector": "databricks",
    "dremio+flight": "dremio_sqlalchemy",
    "firebolt": "firebolt",
    "informix+pyodbc": "pyodbc",
    "awsathena+rest": "pyathena",
    "db2": "ibm_db",
}


def _module_is_installed(name: str | None) -> bool:
    if name is None:
        return True
    if not name or not isinstance(name, str):
        return False
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        # find_spec can raise for namespace packages, missing __init__,
        # invalid module paths, or broken parent packages. Treat all as
        # "not installed" rather than crash the catalog.
        return False


@lru_cache(maxsize=1)
def _generic_sql_drivername_map() -> dict[str, str]:
    """Lazy import of generic_sql drivername map to avoid heavy startup import."""
    try:
        from connectors.generic_sql import _DRIVERNAME_MAP

        return _DRIVERNAME_MAP
    except Exception:
        return {}


def _generic_sql_dbapi_module(drivername: str) -> str | None:
    if not drivername:
        return None
    if drivername in _GENERIC_DRIVERNAME_TO_MODULE:
        return _GENERIC_DRIVERNAME_TO_MODULE[drivername]
    # Try to derive from the DBAPI suffix after the "+" separator
    if "+" in drivername:
        return drivername.split("+", 1)[1]
    return None


def driver_available(driver_type: str, catalog_id: str | None = None) -> bool:
    """Check whether the runtime dependencies for a driver are installed.

    For generic SQL this is catalog-id aware: a PostgreSQL-wire alias is only
    ready when psycopg2 is installed, a MySQL-wire alias when pymysql is
    installed, etc. This keeps the catalog honest and avoids claiming "Full
    transfer" for engines whose DBAPI drivers are not present.
    """
    if driver_type == "generic_sql":
        if not _sqlalchemy_available():
            return False
        if not catalog_id:
            return True
        drivername = _generic_sql_drivername_map().get(catalog_id, catalog_id)
        module = _generic_sql_dbapi_module(drivername)
        if module is None or not _module_is_installed(module):
            return False
        # ClickHouse needs the separate SQLAlchemy dialect package too.
        if "clickhouse" in drivername:
            return _module_is_installed("clickhouse_sqlalchemy")
        return True

    module = _DRIVER_MODULE.get(driver_type)
    return _module_is_installed(module)


def get_capabilities(driver_type: str, catalog_id: str | None = None) -> dict[str, bool]:
    try:
        if driver_type == "generic_sql":
            base = {"test": True, "read": True, "write": True, "introspect": True, "preflight": True}
            if not catalog_id:
                return base if driver_available("generic_sql") else {k: False for k in base}
            return base if driver_available("generic_sql", catalog_id) else {k: False for k in base}
        if driver_type in _DRIVER_CAPS:
            caps = dict(_DRIVER_CAPS[driver_type])
            return caps if driver_available(driver_type, catalog_id) else {k: False for k in caps}
        if driver_type in _FILE_CAPS:
            caps = dict(_FILE_CAPS[driver_type])
            return caps if driver_available(driver_type, catalog_id) else {k: False for k in caps}
    except Exception:
        # Capability discovery should never crash the catalog; degrade gracefully.
        pass
    return {"test": False, "read": False, "write": False, "introspect": False, "preflight": False}


def _source_only_ready(caps: dict[str, bool]) -> bool:
    """True for connectors that are read-only sources (SaaS APIs, etc.)."""
    return bool(caps.get("source_only") and caps.get("read"))


def transfer_ready(caps: dict[str, bool]) -> bool:
    """True when connector supports production read+write transfer."""
    if caps.get("file_source"):
        return True
    if caps.get("dest_only") and caps.get("write"):
        return True
    return bool(caps.get("read") and caps.get("write"))


def connect_only(caps: dict[str, bool]) -> bool:
    return bool(caps.get("test") and not (transfer_ready(caps) or _source_only_ready(caps)))


def effective_status(caps: dict[str, bool], catalog_status: str = "") -> str:
    if transfer_ready(caps) or _source_only_ready(caps):
        return "live"
    if connect_only(caps):
        return "connect_only"
    if catalog_status in ("live", "beta") and not caps.get("test"):
        return "planned"
    return "planned"


def capability_label(caps: dict[str, bool]) -> str:
    if transfer_ready(caps):
        if caps.get("dest_only"):
            return "Destination only"
        if caps.get("file_source"):
            return "File transfer"
        return "Full transfer"
    if _source_only_ready(caps):
        return "Source only"
    if connect_only(caps):
        return "Connection test only"
    return "Roadmap"


def _catalog_transfer_ready(catalog_id: str, driver: str, caps: dict[str, bool]) -> bool:
    """True when the resolved driver is implemented and live (aliases inherit readiness)."""
    if not transfer_ready(caps):
        return False
    if driver in TRANSFER_READY_CATALOG_IDS:
        return True
    if driver in _DRIVER_CAPS or driver in _FILE_CAPS:
        return True
    if driver == "generic_sql":
        return True
    return False


def enrich_catalog_entry(entry: dict[str, Any]) -> dict[str, Any]:
    catalog_id = (entry.get("id") or "").lower().strip()
    driver = resolve_driver_type(catalog_id)
    caps = get_capabilities(driver, catalog_id)
    ready = _catalog_transfer_ready(catalog_id, driver, caps)
    eff = effective_status(caps, entry.get("status", "planned"))
    out = dict(entry)
    out["driver_type"] = driver
    out["capabilities"] = caps
    out["effective_status"] = eff
    out["transfer_ready"] = ready
    out["connect_only"] = connect_only(caps) and not ready
    out["capability_label"] = capability_label(caps)
    return out


def source_ready(caps: dict[str, bool]) -> bool:
    """True when connector can act as a transfer source."""
    if caps.get("file_source"):
        return True
    return bool(caps.get("read") and caps.get("write") and not caps.get("dest_only"))


def dest_ready(caps: dict[str, bool]) -> bool:
    """True when connector can act as a transfer destination."""
    if caps.get("file_export"):
        return True
    return bool(caps.get("write") and (caps.get("read") or caps.get("dest_only")))


def transfer_live_driver_types() -> list[str]:
    live = []
    for k, caps in {**_DRIVER_CAPS, **_FILE_CAPS, "generic_sql": get_capabilities("generic_sql")}.items():
        if transfer_ready(caps):
            live.append(k)
    return sorted(set(live))


def source_live_driver_types() -> list[str]:
    live = []
    for k, caps in {**_DRIVER_CAPS, **_FILE_CAPS, "generic_sql": get_capabilities("generic_sql")}.items():
        if source_ready(caps):
            live.append(k)
    return sorted(set(live))


def dest_live_driver_types() -> list[str]:
    live = []
    for k, caps in {**_DRIVER_CAPS, **_FILE_CAPS, "generic_sql": get_capabilities("generic_sql")}.items():
        if dest_ready(caps):
            live.append(k)
    return sorted(set(live))


def manifest_summary() -> dict[str, Any]:
    from .registry import LIVE_MATRIX

    transfer_live = len(transfer_live_driver_types())
    connect_only_count = sum(1 for k in _DRIVER_CAPS if connect_only(_DRIVER_CAPS[k]))
    return {
        "transfer_live_drivers": transfer_live_driver_types(),
        "transfer_live_count": transfer_live,
        "connect_only_count": connect_only_count,
        "live_route_combinations": len(LIVE_MATRIX),
    }
