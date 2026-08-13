"""Registry-driven probe/read/write dispatch — single glue for Transfer Studio + CDC.

Explicit adapter/stream branches remain for legacy drivers. New first-class drivers
(sqlserver, oracle, iceberg, kafka, salesforce, hubspot, …) resolve through
``CONNECTOR_MODULES`` so modules cannot ship without an engine path.
"""

from __future__ import annotations

import importlib
from typing import Any, Callable

from .connector_registry import CONNECTOR_MODULES, ConnectorModules


def _spec(driver: str) -> ConnectorModules | None:
    return CONNECTOR_MODULES.get((driver or "").strip().lower())


def has_writer(driver: str) -> bool:
    spec = _spec(driver)
    return bool(spec and spec.writer and spec.writer_fn)


def has_reader(driver: str) -> bool:
    spec = _spec(driver)
    return bool(spec and spec.reader and spec.reader_fn)


def load_writer(driver: str) -> Callable[..., Any]:
    spec = _spec(driver)
    if not spec or not spec.writer:
        raise ValueError(f"No writer registered for driver '{driver}'")
    mod = importlib.import_module(spec.writer)
    fn = getattr(mod, spec.writer_fn or "write_mapped_rows", None)
    if not callable(fn):
        raise ValueError(f"Writer {spec.writer}.{spec.writer_fn} not callable")
    return fn


def load_reader(driver: str) -> Callable[..., Any]:
    spec = _spec(driver)
    if not spec or not spec.reader:
        raise ValueError(f"No reader registered for driver '{driver}'")
    mod = importlib.import_module(spec.reader)
    fn = getattr(mod, spec.reader_fn, None)
    if not callable(fn):
        raise ValueError(f"Reader {spec.reader}.{spec.reader_fn} not callable")
    return fn


def default_port_for(driver: str) -> int:
    from .connector_capabilities import default_port

    return int(default_port(driver) or 0)


def writer_extra_kwargs(
    driver: str,
    *,
    cfg: dict[str, Any],
    dest: Any = None,
    common: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Driver-specific writer kwargs that the common batch payload does not carry.

    One owner for "what else does *this* writer need", so a connection setting
    cannot reach the adapter path and be dropped by the streaming one — which is
    how SFTP host-key trust came to be verified at Validate and absent at write.
    """
    common = common or {}
    if driver == "sftp":
        from connectors.sftp_common import host_key_settings

        # Host-key trust must ride every path that opens an SFTP connection.
        # Dropping it downgraded the write to "no pinned key" while Validate had
        # just verified against the pinned one.
        extra = dict(host_key_settings(cfg))
        extra["private_key"] = str(cfg.get("private_key") or "")
        return extra
    if driver == "kafka":
        return {
            "schema_registry_url": str(
                (getattr(dest, "extra", None) or {}).get("schema_registry_url")
                or cfg.get("schema_registry_url")
                or ""
            )
        }
    if driver == "iceberg":
        # Forward catalog properties (warehouse, region, catalog_type, token,
        # rest.*, glue.*, …) that are not already part of the common kwargs.
        return {
            k: v for k, v in cfg.items() if k not in common and v not in (None, "")
        }
    return {}


def write_via_registry(
    driver: str,
    *,
    common: dict[str, Any],
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> Any:
    """Invoke the registered writer with Transfer Studio common kwargs."""
    fn = load_writer(driver)
    kwargs = dict(common)
    if extra:
        kwargs.update(extra)
    # Dialect wrappers expect type=; harmless for others via **_kwargs.
    kwargs.setdefault("type", driver)
    # Pass write_mode / conflict_columns when the writer accepts them.
    try:
        return fn(
            **kwargs,
            write_mode=write_mode,
            conflict_columns=conflict_columns or [],
        )
    except TypeError:
        return fn(**kwargs)


def read_via_registry(
    driver: str,
    *,
    cfg: dict[str, Any],
    table: str,
    limit: int = 100_000,
    offset: int = 0,
    columns: list[str] | None = None,
    cursor_column: str = "",
    cursor_after: Any = None,
) -> Any:
    """Invoke the registered batch reader (SQL-style signature or SaaS object)."""
    fn = load_reader(driver)
    # SaaS readers use read_object(cfg=, object=, limit=). Iceberg needs the full
    # resolved config (warehouse, region, extra catalog properties) too.
    if driver in {"salesforce", "hubspot", "stripe", "rest_api", "influxdb", "neo4j", "couchbase", "iceberg"}:
        saas_kwargs: dict[str, Any] = {}
        if cursor_column:
            # Keyset seek for SaaS APIs whose OFFSET paging is capped.
            saas_kwargs["cursor_column"] = cursor_column
            saas_kwargs["cursor_after"] = cursor_after
        return fn(
            cfg=cfg, object=table, limit=limit, offset=offset, columns=columns, **saas_kwargs
        )

    kwargs: dict[str, Any] = {
        "host": cfg.get("host", ""),
        "port": int(cfg.get("port") or default_port_for(driver) or 0),
        "database": cfg.get("database", ""),
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema") or "",
        "connection_string": cfg.get("connection_string", ""),
        "ssl": bool(cfg.get("ssl", False)),
        "table": table,
        "type": cfg.get("type") or driver,
        "offset": offset,
        "limit": limit,
    }
    if columns is not None:
        kwargs["columns"] = columns
    try:
        return fn(**kwargs)
    except TypeError:
        # Some readers take cfg= only
        return fn(cfg={**cfg, "type": cfg.get("type") or driver}, table=table, limit=limit, offset=offset)
