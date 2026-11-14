"""Production health probes — real dependency checks.

The checks open a database connection and touch the data directory, so they
are blocking I/O: the routes call :func:`cached_health` on the thread pool,
never on the event loop, and probes that arrive within
``HEALTH_CACHE_SECONDS`` of each other share one result.
"""

from __future__ import annotations

import contextlib
import copy
import threading
import time
from typing import Any

from services.brand_env import getenv_brand
from services.cdc_lease_store import resolve_backend_name
from services.platform_config import data_dir, upload_dir


def probe_timeout_seconds() -> float:
    """Upper bound for one dependency check (``HEALTH_PROBE_TIMEOUT_SECONDS``, default 2).

    A readiness probe that waits out the driver's own 5-20s timeouts answers
    after the orchestrator has already given up on it.
    """
    try:
        return min(30.0, max(0.2, float(getenv_brand("HEALTH_PROBE_TIMEOUT_SECONDS", "2") or "2")))
    except ValueError:
        return 2.0


def check_mongodb() -> dict[str, Any]:
    """Reachability of the metadata database, proven on a fresh connection.

    Every phase (server selection, connect, the ping itself) is bounded by
    :func:`probe_timeout_seconds`, so a database that accepts the connection
    and goes silent cannot hold the probe.
    """
    client = None
    try:
        from pymongo import MongoClient

        from services.platform_config import mongodb_uri

        timeout_ms = int(probe_timeout_seconds() * 1000)
        client = MongoClient(
            mongodb_uri(),
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
            socketTimeoutMS=timeout_ms,
        )
        client.admin.command("ping")
        return {"status": "up", "detail": "connected"}
    except Exception as exc:
        from pymongo.errors import PyMongoError

        if isinstance(exc, PyMongoError):
            return {"status": "down", "detail": "connection failed"}
        return {"status": "down", "detail": str(exc)[:200]}
    finally:
        if client is not None:
            # Closing a dead client must not fail the probe.
            with contextlib.suppress(Exception):
                client.close()


def check_storage() -> dict[str, Any]:
    try:
        lease_backend = resolve_backend_name()
        d = data_dir()
        u = upload_dir()
        test = d / ".health_probe"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)

        from services.connector_store import connector_persistence_status

        connector_status = connector_persistence_status()
        stores = {
            # True when the active backend (mongo or file) is usable — not merely
            # when connectors.json exists on disk (production uses Mongo).
            "connectors": bool(connector_status.get("ok")),
            "audit_log": (d / "audit_events.jsonl").exists(),
            "mcp_log": (d / "mcp_invocations.jsonl").exists(),
            "transfer_plans": (d / "transfer_plans.json").exists(),
            "schedules": (d / "schedules.json").exists(),
            # The settings document lives in the shared store when one is
            # configured (services.workspace_store); the file only without one.
            "workspace": _workspace_settings_readable(d),
            "integrations": (d / "integrations.json").exists(),
            "upload_registry": (d / "upload_registry.json").exists(),
        }
        return {
            "status": "up",
            "data_dir": str(d),
            "upload_dir": str(u),
            "writable": True,
            "stores": stores,
            "connector_backend": connector_status.get("backend"),
            "cdc_lease_backend": lease_backend,
            "cdc_lease_distributed": lease_backend == "redis",
            "connector_store": connector_status,
        }
    except Exception as exc:
        return {"status": "down", "detail": str(exc)[:200]}


def aggregate_health() -> dict[str, Any]:
    from services.driver_bootstrap import driver_status
    from src.services.catalog_service import catalog_summary

    drivers = driver_status()
    mongo = check_mongodb()
    storage = check_storage()
    catalog = catalog_summary()

    services = {
        "api": "up",
        "mongodb": mongo["status"],
        "storage": storage["status"],
        "connectors": "ready" if drivers.get("ready") else "provisioning",
    }

    all_up = mongo["status"] == "up" and storage["status"] == "up"
    status = "healthy" if all_up and drivers.get("ready") else "degraded"
    if mongo["status"] == "down":
        status = "unhealthy"

    return {
        "status": status,
        "services": services,
        "drivers": drivers,
        "catalog": {
            "total": catalog.get("total"),
            "live": catalog.get("live"),
            "planned": catalog.get("planned"),
            "beta": catalog.get("beta"),
        },
        "mongodb": mongo,
        "storage": storage,
    }


def _workspace_settings_readable(d) -> bool:
    """Whether the general settings document can be read where it lives."""
    try:
        from services.workspace_store import SettingsUnavailable, _shared

        try:
            coll = _shared()
        except SettingsUnavailable:
            return False
        if coll is not None:
            return True
    except Exception:  # noqa: BLE001
        return False
    return (d / "workspace.json").exists()



#: How long one dependency check answers later probes. Kubernetes, the load
#: balancer and the status page probe independently; they need not each open a
#: database connection.
HEALTH_CACHE_SECONDS = 2.0
_health_lock = threading.Lock()
_health_cache: dict[str, Any] = {"at": 0.0, "payload": None, "probe": None}


def cached_health(max_age_seconds: float | None = None) -> dict[str, Any]:
    """:func:`aggregate_health`, shared by probes that arrive close together.

    Blocking: call it from a worker thread (``run_in_threadpool``). Probes that
    arrive while a check is running wait for that one check instead of
    starting their own. The caller gets its own copy.
    """
    ttl = HEALTH_CACHE_SECONDS if max_age_seconds is None else max_age_seconds
    probe = aggregate_health
    with _health_lock:
        fresh = (
            _health_cache["payload"] is not None
            and _health_cache["probe"] is probe
            and time.monotonic() - _health_cache["at"] < ttl
        )
        if not fresh:
            _health_cache.update(payload=probe(), probe=probe, at=time.monotonic())
        return copy.deepcopy(_health_cache["payload"])
