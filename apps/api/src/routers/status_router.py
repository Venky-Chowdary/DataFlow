"""Public service status — the data behind the marketing status page.

Reuses ``services.health_service.aggregate_health`` (the readiness owner) and
publishes only a component → state summary: no error strings, hostnames,
driver lists or versions, so an unauthenticated caller learns nothing an
attacker could use. Results are cached briefly so a burst of status-page
visitors cannot turn into a burst of database probes.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter(tags=["status"])

CACHE_SECONDS = 15.0

COMPONENTS: tuple[tuple[str, str, str], ...] = (
    ("api", "API", "Transfer, connector, and job endpoints"),
    ("mongodb", "Control plane", "Jobs, schedules, audit, and workspace state"),
    ("storage", "File storage", "Uploads, quarantine extracts, and proof packs"),
    ("connectors", "Connector runtime", "Database and SaaS drivers used by transfers"),
)

_OPERATIONAL = {"up", "ready", "healthy", "ok"}
_DEGRADED = {"provisioning", "degraded", "starting", "unknown"}

_lock = threading.Lock()
_cache: dict[str, Any] = {"at": 0.0, "payload": None}


def component_state(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in _OPERATIONAL:
        return "operational"
    if value in _DEGRADED:
        return "degraded"
    return "outage"


def build_status() -> dict[str, Any]:
    try:
        from services.health_service import aggregate_health

        services = dict(aggregate_health().get("services") or {})
    except Exception:
        logger.exception("Status probe failed")
        services = {"api": "up"}
    components = [
        {"id": cid, "name": name, "description": desc, "status": component_state(services.get(cid))}
        for cid, name, desc in COMPONENTS
    ]
    states = {c["status"] for c in components}
    overall = "outage" if "outage" in states else "degraded" if "degraded" in states else "operational"
    return {
        "status": overall,
        "components": components,
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cache_seconds": int(CACHE_SECONDS),
    }


@router.get("/status")
async def public_status() -> dict[str, Any]:
    now = time.monotonic()
    with _lock:
        cached = _cache["payload"]
        if cached is not None and now - _cache["at"] < CACHE_SECONDS:
            return cached
    payload = build_status()
    with _lock:
        _cache.update(at=now, payload=payload)
    return payload
