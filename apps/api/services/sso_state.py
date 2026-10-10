"""Mongo is the multi-host backend.

The flocked file backend is safe across workers on one host only and is not shared
across hosts.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo.errors import DuplicateKeyError, PyMongoError

from services.metadata_backend import json_doc_transaction, mongo_database
from services.platform_config import data_dir

logger = logging.getLogger(__name__)

STATE_PATH = data_dir() / "sso_state.json"
STATE_TTL_MINUTES = 10
_REPLAY_INDEX_LOCK = threading.Lock()
_REPLAY_INDEX_READY = False


class SsoStoreUnavailable(RuntimeError):
    """The configured MongoDB SSO state or replay store could not be used."""


def _is_expired(timestamp: str) -> bool:
    try:
        created = datetime.fromisoformat(timestamp)
    except (TypeError, ValueError):
        return True
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - created > timedelta(minutes=STATE_TTL_MINUTES)


def _state_doc_is_live(info: Any) -> bool:
    return isinstance(info, dict) and not _is_expired(info.get("created_at", ""))


def _cleanup(states: dict[str, Any]) -> dict[str, Any]:
    return {state: info for state, info in states.items() if _state_doc_is_live(info)}


def _mongo_error(operation: str, sso_type: str) -> SsoStoreUnavailable:
    logger.error(
        "SSO state store unavailable (sso_type=%s reason=%s)",
        sso_type,
        operation,
    )
    return SsoStoreUnavailable(f"SSO state store unavailable ({operation})")


def set_state(state: str, sso_type: str, extra: dict[str, Any] | None = None) -> str:
    """Store an SSO state token and return it."""
    payload = {
        "sso_type": sso_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload["extra"] = extra
    try:
        db = mongo_database()
        if db is not None:
            db["sso_states"].replace_one(
                {"_id": state},
                {"_id": state, **payload},
                upsert=True,
            )
            return state
    except PyMongoError as exc:
        raise _mongo_error("mongo_write_failed", sso_type) from exc

    with json_doc_transaction(STATE_PATH, {}) as states:
        live_states = _cleanup(states)
        states.clear()
        states.update(live_states)
        states[state] = payload
    return state


def get_state(state: str, sso_type: str) -> dict[str, Any] | None:
    """Validate an SSO state token, return its metadata, and consume it."""
    if not state:
        return None
    try:
        db = mongo_database()
        if db is not None:
            doc = db["sso_states"].find_one_and_delete({"_id": state})
            if not doc:
                return None
            if doc.get("sso_type") != sso_type or _is_expired(doc.get("created_at", "")):
                return None
            return doc
    except PyMongoError as exc:
        raise _mongo_error("mongo_read_failed", sso_type) from exc

    with json_doc_transaction(STATE_PATH, {}) as states:
        live_states = _cleanup(states)
        states.clear()
        states.update(live_states)
        info = states.pop(state, None)
    if not info:
        return None
    if info.get("sso_type") != sso_type or _is_expired(info.get("created_at", "")):
        return None
    return info


def get_and_pop(state: str, sso_type: str) -> bool:
    """Validate the expected SSO type and consume the state token."""
    return get_state(state, sso_type) is not None


def generate_state(sso_type: str, extra: dict[str, Any] | None = None) -> str:
    """Generate and store a high-entropy state token."""
    return set_state(secrets.token_urlsafe(16), sso_type, extra=extra)


def _ensure_replay_ttl_index(db: Any) -> None:
    global _REPLAY_INDEX_READY
    if _REPLAY_INDEX_READY:
        return
    with _REPLAY_INDEX_LOCK:
        if _REPLAY_INDEX_READY:
            return
        db["sso_replay"].create_index(
            "expires_at",
            expireAfterSeconds=0,
            name="sso_replay_expires_at_ttl",
        )
        _REPLAY_INDEX_READY = True


def _replay_key(namespace: str, token_id: str) -> str:
    digest = hashlib.sha256(token_id.encode("utf-8")).hexdigest()
    return f"{namespace}:{digest}"


def _expiry_from_entry(entry: Any) -> datetime | None:
    if not isinstance(entry, dict):
        return None
    raw = entry.get("expires_at")
    if isinstance(raw, datetime):
        expiry = raw
    elif isinstance(raw, str):
        try:
            expiry = datetime.fromisoformat(raw)
        except ValueError:
            return None
    else:
        return None
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return expiry


def claim_once(namespace: str, token_id: str, expires_at: datetime) -> bool:
    """Atomically claim a namespaced identifier until its expiration time."""
    if not token_id:
        raise ValueError("token_id must not be empty")
    key = _replay_key(namespace, token_id)
    try:
        db = mongo_database()
        if db is not None:
            collection = db["sso_replay"]
            try:
                _ensure_replay_ttl_index(db)
                collection.insert_one({"_id": key, "expires_at": expires_at})
            except DuplicateKeyError:
                return False
            return True
    except PyMongoError as exc:
        raise _mongo_error("mongo_replay_failed", namespace) from exc

    path = data_dir() / "sso_replay.json"
    now = datetime.now(timezone.utc)
    with json_doc_transaction(path, {}) as replay:
        expired: list[str] = []
        for existing_key, entry in replay.items():
            expiry = _expiry_from_entry(entry)
            if expiry is None or expiry <= now:
                expired.append(existing_key)
        for existing_key in expired:
            replay.pop(existing_key, None)
        if key in replay:
            return False
        replay[key] = {"expires_at": expires_at.isoformat()}
    return True
