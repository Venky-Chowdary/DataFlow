"""Append-only workspace audit log — real events, redacted secrets.

When MongoDB is connected, events are written to an ``audit_events`` collection
so they are shared across Railway replicas. Otherwise events fall back to a
local JSONL file.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "audit_events.jsonl"
MAX_EVENTS = int(__import__("os").getenv("DATAFLOW_AUDIT_MAX_EVENTS", "5000"))
_APPEND_LOCK = threading.Lock()

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
            # ``Database.get`` is not a valid PyMongo API; ``get_collection`` is.
            return mongo.get_database().get_collection("audit_events")
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return None


def _hmac_event_hash(event: dict[str, Any], secret: bytes) -> str:
    import hashlib
    import hmac as hmac_mod

    body_for_hash = {k: v for k, v in event.items() if k not in ("_id", "event_hash")}
    canon = json.dumps(body_for_hash, sort_keys=True, separators=(",", ":"), default=json_default)
    return hmac_mod.new(secret, canon.encode("utf-8"), hashlib.sha256).hexdigest()


def _platform_hmac_secret() -> bytes:
    try:
        from services.auth_service import _token_secret

        raw = _token_secret()
        return raw if isinstance(raw, bytes) else str(raw or "").encode("utf-8")
    except Exception:
        from services.brand_env import getenv_brand

        return (getenv_brand("AUTH_SECRET", "") or "dev-only-not-for-production").encode("utf-8")


def _latest_hash_from_file() -> str | None:
    if not STORE_PATH.exists():
        return None
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        h = ev.get("event_hash")
        if h:
            return str(h)
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
    """Record a redacted audit event to MongoDB (preferred) or a local JSONL file.

    Events are hash-chained with HMAC-SHA256 (keyed by the platform auth secret):
    ``prev_hash`` + ``event_hash`` over a canonical payload. Plain SHA-256 chains
    can be reforged by anyone with store write access; HMAC requires the secret.

    Process-local ``_APPEND_LOCK`` serializes writers to reduce chain forks. Cross-
    process/replica races still need a single authoritative store (Mongo preferred).
    """
    with _APPEND_LOCK:
        secret = _platform_hmac_secret()
        prev = latest_event_hash()
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
            "prev_hash": prev,
            "hash_alg": "HMAC-SHA256",
        }
        event["event_hash"] = _hmac_event_hash(event, secret)

        coll = _mongo_collection()
        if coll is not None:
            try:
                coll.insert_one(event)
                return event
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "Mongo audit insert failed; falling back to file with file-local prev: %s",
                    exc,
                    exc_info=exc,
                )
                # Rebuild chain tip from the file store so we do not link to a Mongo
                # tip that never landed in JSONL.
                file_prev = _latest_hash_from_file()
                event["prev_hash"] = file_prev
                event["event_hash"] = _hmac_event_hash(event, secret)

        STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_event = {k: v for k, v in event.items() if k != "_id"}
        with STORE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(file_event, ensure_ascii=False, default=json_default) + "\n")
        _trim_if_needed()
        return event


def latest_event_hash() -> str | None:
    """Return the most recent ``event_hash`` for hash chaining, if any."""
    events = list_audit_events(limit=1)
    if not events:
        return None
    h = events[0].get("event_hash")
    return str(h) if h else None


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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
