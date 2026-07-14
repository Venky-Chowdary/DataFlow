"""Production health probes — real dependency checks."""

from __future__ import annotations

from typing import Any

from services.platform_config import data_dir, upload_dir


def check_mongodb() -> dict[str, Any]:
    try:
        from src.services.mongodb_service import MongoDBService

        svc = MongoDBService()
        if svc.connect():
            svc.disconnect()
            return {"status": "up", "detail": "connected"}
        return {"status": "down", "detail": "connection failed"}
    except Exception as exc:
        return {"status": "down", "detail": str(exc)[:200]}


def check_storage() -> dict[str, Any]:
    try:
        d = data_dir()
        u = upload_dir()
        test = d / ".health_probe"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        stores = {
            "connectors": (d / "connectors.json").exists(),
            "audit_log": (d / "audit_events.jsonl").exists(),
            "mcp_log": (d / "mcp_invocations.jsonl").exists(),
            "transfer_plans": (d / "transfer_plans.json").exists(),
            "schedules": (d / "schedules.json").exists(),
            "workspace": (d / "workspace.json").exists(),
            "integrations": (d / "integrations.json").exists(),
            "upload_registry": (d / "upload_registry.json").exists(),
        }
        return {
            "status": "up",
            "data_dir": str(d),
            "upload_dir": str(u),
            "writable": True,
            "stores": stores,
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
