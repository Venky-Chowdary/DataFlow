"""SSO state store — move the transient OAuth state parameter off the router.

When MongoDB is available the state tokens are persisted to a collection so
multi-instance API deployments can validate callbacks.  Otherwise they fall back
to a JSON file in ``data_dir``.
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

try:
    from src.services.mongodb_service import get_mongodb_service
except ImportError:
    from services.mongodb_service import get_mongodb_service

STATE_PATH = data_dir() / "sso_state.json"
STATE_TTL_MINUTES = 10


def _mongo_backend():
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _is_expired(timestamp: str) -> bool:
    try:
        created = datetime.fromisoformat(timestamp)
        return datetime.now(timezone.utc) - created > timedelta(minutes=STATE_TTL_MINUTES)
    except Exception:
        return True


def _load_file() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return raw
    except Exception:
        return {}


def _save_file(data: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(STATE_PATH)


def _cleanup(states: dict[str, Any]) -> dict[str, Any]:
    return {state: info for state, info in states.items() if not _is_expired(info.get("created_at", ""))}


def set_state(state: str, sso_type: str, extra: dict[str, Any] | None = None) -> str:
    """Store an SSO state token and return it."""
    svc = _mongo_backend()
    payload = {
        "sso_type": sso_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload["extra"] = extra
    if svc:
        db = svc.get_database()
        db["sso_states"].replace_one(
            {"_id": state},
            {"_id": state, **payload},
            upsert=True,
        )
        return state
    states = _cleanup(_load_file())
    states[state] = payload
    _save_file(states)
    return state


def get_state(state: str, sso_type: str) -> dict[str, Any] | None:
    """Validate a state token, return its metadata, and consume it."""
    if not state:
        return None
    svc = _mongo_backend()
    if svc:
        db = svc.get_database()
        doc = db["sso_states"].find_one_and_delete({"_id": state})
        if not doc:
            return None
        if doc.get("sso_type") != sso_type or _is_expired(doc.get("created_at", "")):
            return None
        return doc
    states = _cleanup(_load_file())
    info = states.pop(state, None)
    _save_file(states)
    if not info:
        return None
    if info.get("sso_type") != sso_type or _is_expired(info.get("created_at", "")):
        return None
    return info


def get_and_pop(state: str, sso_type: str) -> bool:
    """Validate a state token, require the expected SSO type, and consume it."""
    return get_state(state, sso_type) is not None


def generate_state(sso_type: str, extra: dict[str, Any] | None = None) -> str:
    return set_state(secrets.token_urlsafe(16), sso_type, extra=extra)
