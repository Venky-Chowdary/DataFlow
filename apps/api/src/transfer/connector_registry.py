"""Canonical connector module registry — probes, readers, writers.

Import this module from adapters, preflight, routers, and tests instead of
duplicating driver→module maps.
"""

from __future__ import annotations

from dataclasses import dataclass
import inspect
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
}


def registered_driver_types() -> list[str]:
    return sorted(CONNECTOR_MODULES.keys())


def assert_registry_matches_capabilities() -> None:
    """Raise if capability manifest and module registry diverge."""
    cap_drivers = {k for k, v in _DRIVER_CAPS.items() if v.get("read") and v.get("write")}
    reg_drivers = set(CONNECTOR_MODULES.keys())
    missing = cap_drivers - reg_drivers
    extra = reg_drivers - cap_drivers
    if missing or extra:
        raise RuntimeError(f"Registry drift — missing={missing}, extra={extra}")


def run_probe(db_type: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    """Execute connectivity probe for a driver using resolved config."""
    import importlib

    from .connector_capabilities import resolve_driver_type

    db_type = (db_type or "").lower()
    resolved = resolve_driver_type(db_type)

    if resolved == "generic_sql":
        from connectors.generic_sql import test_generic_sql

        return test_generic_sql(**cfg)

    spec = CONNECTOR_MODULES.get(db_type)
    if not spec:
        return False, f"No connectivity probe for {db_type}"

    if db_type == "mongodb":
        from .adapters import probe_mongodb

        return probe_mongodb(cfg)

    if not spec.probe:
        return False, f"No probe configured for {db_type}"

    mod_name, fn_name = spec.probe
    mod = importlib.import_module(mod_name)
    probe_fn = getattr(mod, fn_name)
    schema_default = (
        "PUBLIC" if db_type == "snowflake"
        else "dataflow" if db_type == "bigquery"
        else "public"
    )
    probe_kwargs = {
        "host": cfg.get("host", ""),
        "port": int(cfg.get("port") or default_port(db_type)),
        "database": cfg.get("database", ""),
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema", schema_default),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "warehouse": cfg.get("warehouse", ""),
        "table": cfg.get("table", ""),
    }
    sig = inspect.signature(probe_fn)
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if not accepts_var_kw:
        probe_kwargs = {k: v for k, v in probe_kwargs.items() if k in sig.parameters}
    result = probe_fn(**probe_kwargs)
    if hasattr(result, "ok"):
        return bool(result.ok), str(result.message if result.ok else (result.error or result.message))
    if isinstance(result, tuple) and len(result) >= 2:
        return bool(result[0]), str(result[1])
    return bool(result), "OK" if result else "Probe failed"
