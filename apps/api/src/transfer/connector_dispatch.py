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
) -> Any:
    """Invoke the registered batch reader (SQL-style signature or SaaS object)."""
    fn = load_reader(driver)
    # SaaS readers use read_object(cfg=, object=, limit=)
    if driver in {"salesforce", "hubspot", "stripe", "rest_api", "influxdb", "neo4j", "couchbase"}:
        return fn(cfg=cfg, object=table, limit=limit, offset=offset)

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
