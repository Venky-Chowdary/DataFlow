"""Canonical connector module registry — probes, readers, writers.

Import this module from adapters, preflight, routers, and tests instead of
duplicating driver→module maps.
"""

from __future__ import annotations

from dataclasses import dataclass
import inspect
import re
from typing import Any

from .connector_capabilities import _DRIVER_CAPS, default_port


@dataclass(frozen=True)
class ConnectorModules:
    """Import paths for a live database / object-store driver."""

    probe: tuple[str, str] | None
    reader: str | None
    reader_fn: str
    writer: str
    writer_fn: str = "write_mapped_rows"


# MongoDB probe uses adapters.probe_mongodb — no standalone test module tuple.
CONNECTOR_MODULES: dict[str, ConnectorModules] = {
    "postgresql": ConnectorModules(
        probe=("connectors.postgresql", "test_postgresql"),
        reader="connectors.postgresql_reader",
        reader_fn="read_table_batch",
        writer="connectors.postgresql_writer",
    ),
    "mysql": ConnectorModules(
        probe=("connectors.mysql", "test_mysql"),
        reader="connectors.mysql_reader",
        reader_fn="read_table_batch",
        writer="connectors.mysql_writer",
    ),
    "mongodb": ConnectorModules(
        probe=None,
        reader="connectors.mongodb_reader",
        reader_fn="read_collection_batch",
        writer="connectors.mongodb_writer",
    ),
    "snowflake": ConnectorModules(
        probe=("connectors.snowflake", "test_snowflake"),
        reader="connectors.snowflake_reader",
        reader_fn="read_table_batch",
        writer="connectors.snowflake_writer",
    ),
    "bigquery": ConnectorModules(
        probe=("connectors.bigquery", "test_bigquery"),
        reader="connectors.bigquery_reader",
        reader_fn="read_table_batch",
        writer="connectors.bigquery_writer",
    ),
    "redshift": ConnectorModules(
        probe=("connectors.redshift", "test_redshift"),
        reader="connectors.postgresql_reader",
        reader_fn="read_table_batch",
        writer="connectors.postgresql_writer",
    ),
    "dynamodb": ConnectorModules(
        probe=("connectors.dynamodb", "test_dynamodb"),
        reader="connectors.dynamodb_reader",
        reader_fn="read_table_batch",
        writer="connectors.dynamodb_writer",
    ),
    "s3": ConnectorModules(
        probe=("connectors.s3", "test_s3"),
        reader="connectors.s3_reader",
        reader_fn="read_object",
        writer="connectors.s3_writer",
    ),
    "gcs": ConnectorModules(
        probe=("connectors.gcs", "test_gcs"),
        reader="connectors.gcs_reader",
        reader_fn="read_object",
        writer="connectors.gcs_writer",
    ),
    "adls": ConnectorModules(
        probe=("connectors.adls", "test_adls"),
        reader="connectors.adls_reader",
        reader_fn="read_object",
        writer="connectors.adls_writer",
    ),
    "redis": ConnectorModules(
        probe=("connectors.redis_kv", "test_redis"),
        reader="connectors.redis_reader",
        reader_fn="read_keys_batch",
        writer="connectors.redis_writer",
    ),
    "elasticsearch": ConnectorModules(
        probe=("connectors.elasticsearch", "test_elasticsearch"),
        reader="connectors.elasticsearch_reader",
        reader_fn="read_index_batch",
        writer="connectors.elasticsearch_writer",
    ),
    "sqlite": ConnectorModules(
        probe=("connectors.sqlite", "test_sqlite"),
        reader="connectors.sqlite_reader",
        reader_fn="read_table_batch",
        writer="connectors.sqlite_writer",
    ),
    "sftp": ConnectorModules(
        probe=("connectors.sftp", "test_sftp"),
        reader="connectors.sftp_reader",
        reader_fn="read_object",
        writer="connectors.sftp_writer",
    ),
    "email": ConnectorModules(
        probe=("connectors.email", "test_email"),
        reader=None,
        reader_fn="",
        writer="connectors.email",
    ),
}


def registered_driver_types() -> list[str]:
    return sorted(CONNECTOR_MODULES.keys())


def assert_registry_matches_capabilities() -> None:
    """Raise if capability manifest and module registry diverge."""
    cap_drivers = {k for k, v in _DRIVER_CAPS.items() if (v.get("read") and v.get("write")) or (v.get("dest_only") and v.get("write"))}
    reg_drivers = set(CONNECTOR_MODULES.keys())
    missing = cap_drivers - reg_drivers
    extra = reg_drivers - cap_drivers
    if missing or extra:
        raise RuntimeError(f"Registry drift — missing={missing}, extra={extra}")


def humanize_connection_error(driver: str, raw: Any) -> str:
    """Convert low-level driver/connection errors into user-friendly messages."""
    text = str(raw).lower()
    driver = (driver or "").lower()

    # Auth / credentials — first because it is the most common and sensitive.
    if re.search(r"authentication|auth|login|credential|password|incorrect|access denied|not authorized|unauthorized|no such user|permission denied|privilege", text):
        if driver == "mongodb":
            return (
                "Authentication failed. Check the username/password and the Auth source field. "
                "Use admin if the user is defined in the admin database, or the database name if the user is defined there."
            )
        if driver == "snowflake":
            return "Authentication failed. Check account name, username, password, role, and that the account is active."
        if driver == "bigquery":
            return "Authentication failed. Check the service account JSON, project ID, and that the account has BigQuery permissions."
        if driver in ("s3", "gcs", "adls"):
            return "Authentication failed. Check access keys / service account, region, bucket/container, and permissions."
        if driver in ("postgresql", "mysql", "redshift", "mariadb", "generic_sql"):
            return "Authentication failed. Check username, password, database, and that the user can log in from this host."
        return "Authentication failed. Check username, password, and permissions."

    # DNS / host unknown
    if re.search(r"name or service not known|nodename|getaddrinfo|dns|unknown host|cannot resolve|not known", text):
        return "Host not found. Check the host/address and that it is reachable from the network."

    # Connection refused / unreachable
    if re.search(r"connection refused|errno 111|network is unreachable|cannot assign|no route|host is down|conn refused", text):
        return "Cannot reach the host on the specified port. Check the host, port, and firewall/security group."

    # Timeouts
    if re.search(r"timed out|timeout|sockettimeout|connecttimeout|operation timed out", text):
        return "Connection timed out. Check the host/port, network, and that the service is running."

    # SSL / TLS
    if re.search(r"ssl|tls|certificate|verify|cert|handshake", text):
        return "SSL/TLS error. Try toggling SSL, check certificates, or verify the host supports the selected SSL mode."

    # Driver not installed
    if re.search(r"no module named|module not found|cannot import|driver not installed|not installed", text):
        return "Connector driver is not installed in this environment. Contact support or install the driver package."

    # Invalid URI / connection string
    if re.search(r"invalid uri|invalid connection string|could not parse|malformed|not a valid uri|bad connection string", text):
        return "The connection string format is invalid. Check the URL, credentials, and query parameters."

    # SFTP-specific
    if driver in ("sftp",) and re.search(r"authentication|auth|login|credential|password|key|private key", text):
        return "SFTP authentication failed. Check username, password, private key, host, and port."
    if driver in ("sftp",) and re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found|no such", text):
        return "SFTP path not found. Check the remote directory and file path."

    # Email-specific
    if driver in ("email",) and re.search(r"authentication|auth|login|credential|password|username", text):
        return "SMTP authentication failed. Check host, port, username, password, and that the account allows SMTP."
    if driver in ("email",) and re.search(r"recipient|to address|to:|invalid address|address", text):
        return "Email recipient is invalid or rejected. Check the 'to' addresses in the SMTP URL or database field."

    # File / path issues
    if re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found", text):
        return "File or path not found. Check the file path and that the volume is mounted."

    # Snowflake-specific
    if driver == "snowflake" and re.search(r"warehouse|role|database|schema", text):
        return "Snowflake connection failed. Check account, warehouse, database, schema, and role."

    # BigQuery-specific
    if driver == "bigquery" and re.search(r"project|dataset|grant|permission|invalid", text):
        return "BigQuery connection failed. Check project ID, dataset, and service account permissions."

    # Object storage
    if driver in ("s3", "gcs", "adls") and re.search(r"bucket|container|region|not found|access|signature", text):
        return "Object storage connection failed. Check bucket/container, region, credentials, and permissions."

    # DynamoDB
    if driver == "dynamodb" and re.search(r"region|resource|table|access|security|token", text):
        return "DynamoDB connection failed. Check region, access keys, and that the table exists."

    # Redis
    if driver == "redis" and re.search(r"auth|password|noauth|wrongpassword", text):
        return "Redis authentication failed. Check the password and that the host/port are correct."

    # Elasticsearch
    if driver == "elasticsearch" and re.search(r"index|cluster|security|license", text):
        return "Elasticsearch connection failed. Check host/port, credentials, and cluster status."

    # Database locked / overloaded
    if re.search(r"database is locked|deadlock|too many connections|max connections|quota exceeded", text):
        return "Database is locked or overloaded. Try again later or reduce concurrency."

    # Resource / table not found
    if re.search(r"table|resource|not found|does not exist|unknown database", text):
        return "Destination or resource not found. Check the database, schema, table, bucket, or index name."

    # Redshift, RDS, cloud Postgres/MySQL share the same auth/host families as their base drivers.
    if driver in ("redshift", "mariadb"):
        if re.search(r"authentication|auth|login|credential|password|incorrect|access denied|not authorized|unauthorized|no such user|permission denied|privilege", text):
            return "Authentication failed. Check username, password, database, and that the user can log in from this host."
        if re.search(r"name or service not known|nodename|getaddrinfo|dns|unknown host|cannot resolve|not known", text):
            return "Host not found. Check the host/address and that it is reachable from the network."
        if re.search(r"connection refused|errno 111|network is unreachable|cannot assign|no route|host is down|conn refused", text):
            return "Cannot reach the host on the specified port. Check the host, port, and firewall/security group."

    # Azure Blob / ADLS
    if driver == "adls":
        if re.search(r"authentication|auth|login|credential|account_key|signature|invalid|unauthorized|access denied|permission", text):
            return "Azure authentication failed. Check the storage account name, account key, or service principal JSON and permissions."
        if re.search(r"container|filesystem|not found|does not exist|resource not found", text):
            return "Container not found. Check the container or filesystem name, or DataFlow will create it during the transfer."

    # GCS
    if driver == "gcs":
        if re.search(r"authentication|auth|credential|service_account|invalid_grant|unauthorized|access denied|permission|forbidden", text):
            return "GCS authentication failed. Check the service account JSON, project ID, and Storage permissions."
        if re.search(r"bucket|not found|does not exist|no such bucket", text):
            return "GCS bucket not found. Check the bucket name and permissions."

    # SFTP-specific (path issues)
    if driver == "sftp" and re.search(r"no such file|is a directory|file not found|could not open|not a directory|path not found|no such", text):
        return "SFTP path not found. Check the remote directory and file path."

    # Fallback: keep the raw message but prefix it clearly.
    return f"Connection failed: {raw}"


def run_probe(db_type: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    """Execute connectivity probe for a driver using resolved config."""
    import importlib

    from .connector_capabilities import resolve_driver_type

    db_type = (db_type or "").lower()
    catalog_id = db_type
    resolved = resolve_driver_type(db_type)

    # Resolve catalog aliases (e.g. cockroachdb -> postgresql) to a registered driver
    db_type = resolved
    spec = CONNECTOR_MODULES.get(db_type)

    if db_type == "mongodb" and spec:
        from .adapters import probe_mongodb

        ok, raw = probe_mongodb(cfg)
        if ok:
            return True, raw
        return False, humanize_connection_error(db_type, raw)

    schema_default = (
        "PUBLIC" if db_type == "snowflake"
        else "dataflow" if db_type == "bigquery"
        else "public"
    )
    probe_kwargs = {
        "host": cfg.get("host", ""),
        "port": int(cfg.get("port") or default_port(db_type or catalog_id)),
        "database": cfg.get("database", ""),
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema", schema_default),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "warehouse": cfg.get("warehouse", ""),
        "table": cfg.get("table", ""),
        "auth_mode": cfg.get("auth_mode", ""),
        "role": cfg.get("role", ""),
        "api_key": cfg.get("api_key", ""),
        "service_account": cfg.get("service_account", ""),
    }

    if resolved == "generic_sql":
        from connectors.generic_sql import test_generic_sql

        # The catalog id (e.g. tidb, clickhouse) must reach the generic SQL engine
        # builder so it can pick the right SQLAlchemy drivername and port.
        engine_type = cfg.get("type") or catalog_id
        ok, raw = test_generic_sql(type=engine_type, **probe_kwargs)
        if ok:
            return True, raw
        return False, humanize_connection_error(engine_type, raw)

    if not spec:
        return False, f"No connectivity probe for {db_type}"

    if not spec.probe:
        return False, f"No probe configured for {db_type}"

    mod_name, fn_name = spec.probe
    mod = importlib.import_module(mod_name)
    probe_fn = getattr(mod, fn_name)
    sig = inspect.signature(probe_fn)
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if not accepts_var_kw:
        probe_kwargs = {k: v for k, v in probe_kwargs.items() if k in sig.parameters}
    result = probe_fn(**probe_kwargs)
    if hasattr(result, "ok"):
        if result.ok:
            return True, str(result.message or "Connection successful")
        return False, humanize_connection_error(db_type, str(result.error or result.message or "unknown error"))
    if isinstance(result, tuple) and len(result) >= 2:
        ok = bool(result[0])
        if ok:
            return True, str(result[1])
        return False, humanize_connection_error(db_type, str(result[1]))
    return bool(result), "OK" if result else humanize_connection_error(db_type, "Probe failed")
