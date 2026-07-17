"""Append-only workspace audit log — real events, redacted secrets.

When MongoDB is connected, events are written to an ``audit_events`` collection
so they are shared across Railway replicas. Otherwise events fall back to a
local JSONL file.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir

STORE_PATH = data_dir() / "audit_events.jsonl"
MAX_EVENTS = int(__import__("os").getenv("DATAFLOW_AUDIT_MAX_EVENTS", "5000"))

_SENSITIVE_KEYS = frozenset({
    "password", "secret", "token", "api_key", "connection_string",
    "authorization", "credential", "private_key",
})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if k.lower() in _SENSITIVE_KEYS:
                out[k] = "[REDACTED]"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(value, list):
        return [_redact(v) for v in value[:50]]
    if isinstance(value, str) and len(value) > 512:
        return value[:512] + "…"
    return value


def _mongo_collection():
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if mongo and getattr(mongo, "client", None) and type(mongo).__name__ != "MemoryMongoDBService":
            return mongo.get_database().get("audit_events")
    except Exception:
        pass
    return None


def append_audit_event(
    *,
    action: str,
    resource: str,
    actor: str = "system",
    level: str = "info",
    correlation_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record a redacted audit event to MongoDB (preferred) or a local JSONL file."""
    event = {
        "_id": str(uuid.uuid4()),
        "id": str(uuid.uuid4()),
        "time": _now(),
        "actor": actor,
        "action": action,
        "resource": resource,
        "level": level,
        "correlation_id": correlation_id,
        "details": _redact(details or {}),
    }

    coll = _mongo_collection()
    if coll is not None:
        try:
            coll.insert_one(event)
            return event
        except Exception:
            pass

    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Remove MongoDB-specific _id before writing to file
    file_event = {k: v for k, v in event.items() if k != "_id"}
    with STORE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(file_event, ensure_ascii=False) + "\n")
    _trim_if_needed()
    return event


def list_audit_events(
    *,
    limit: int = 100,
    level: str | None = None,
    actor: str | None = None,
) -> list[dict[str, Any]]:
    """Return the most recent audit events, newest first."""
    coll = _mongo_collection()
    if coll is not None:
        try:
            query: dict[str, Any] = {}
            if level and level != "all":
                query["level"] = level
            if actor:
                query["actor"] = actor
            cursor = coll.find(query).sort("time", -1).limit(limit)
            return [{k: v for k, v in doc.items() if k != "_id"} for doc in cursor]
        except Exception:
            pass

    if not STORE_PATH.exists():
        return []
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    events: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if level and level != "all" and ev.get("level") != level:
            continue
        if actor and ev.get("actor") != actor:
            continue
        events.append(ev)
        if len(events) >= limit:
            break
    return events


def _trim_if_needed() -> None:
    if not STORE_PATH.exists():
        return
    lines = STORE_PATH.read_text(encoding="utf-8").splitlines()
    if len(lines) <= MAX_EVENTS:
        return
    trimmed = lines[-MAX_EVENTS:]
    STORE_PATH.write_text("\n".join(trimmed) + "\n", encoding="utf-8")


def actor_from_request(request: Any) -> str:
    """Extract the actor email from a FastAPI request, if available."""
    if request is None:
        return "anonymous"
    user = getattr(request.state, "user", None)
    if user and isinstance(user, dict):
        return str(user.get("email") or user.get("sub") or "anonymous")
    return "anonymous"
