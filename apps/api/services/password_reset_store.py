"""Password-reset link tokens (single-use, 30-minute TTL).

Only ``HMAC-SHA256(pepper, token)`` is stored — the raw token never touches
storage or logs. Issuing a new token invalidates every earlier unused one
for the same account. ``consume`` marks the token used atomically (a
conditional update / locked file write), so a double-click can never reset
twice.

Mongo when configured; ``password_reset_tokens.json`` under data_dir()
otherwise.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir, getenv_brand

COLLECTION = "password_reset_tokens"
_FILE = "password_reset_tokens.json"
_LOCK = threading.Lock()

TOKEN_TTL = timedelta(minutes=30)
TOKEN_BYTES = 32  # secrets.token_urlsafe(32) -> 256 bits of entropy

_PREPARED: set[str] = set()


def _pepper() -> str:
    """Canonical OTP pepper — fails closed in production (otp_service owns it)."""
    from services import otp_service

    return otp_service._pepper()


def _hash(token: str) -> str:
    return hmac.new(_pepper().encode(), token.encode(), hashlib.sha256).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _parse_ts(value: Any) -> datetime | None:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    if parsed is None:
        return None
    # BSON datetimes read back naive (UTC by convention) and legacy rows may
    # carry offset-less ISO stamps — comparisons against _now() must never
    # raise "can't compare offset-naive and offset-aware datetimes".
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _database():
    db = mongo_database()
    if db is None:
        return None
    if db.name not in _PREPARED:
        db[COLLECTION].create_index(
            [("token_hash", 1)], unique=True, name="token_hash_unique")
        db[COLLECTION].create_index(
            [("email", 1), ("used_at", 1)])
        db[COLLECTION].create_index(
            [("expires_at", 1)], expireAfterSeconds=0, name="reset_token_ttl")
        _PREPARED.add(db.name)
    return db


def _path():
    return data_dir() / _FILE


def _load() -> dict[str, Any]:
    doc = load_json_doc(_path(), {"tokens": {}})
    return doc if isinstance(doc, dict) else {"tokens": {}}


def _active_doc(doc: dict[str, Any]) -> str:
    """'ok' | 'expired' | 'used' | 'invalid' for a stored token doc."""
    if doc.get("used_at"):
        return "used"
    expires = _parse_ts(doc.get("expires_at"))
    if expires is None or expires <= _now():
        return "expired"
    return "ok"


def _to_doc(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in row.items() if k != "_id"}


# ── public API ────────────────────────────────────────────────────────────


def issue(*, email: str, ip: str = "", ua: str = "") -> tuple[str, dict[str, Any]]:
    """Mint a new token for ``email``; every earlier unused token is voided.

    Returns ``(raw_token, stored_doc)``. The raw token leaves this process
    only inside the email — it is never logged or stored.
    """
    token = secrets.token_urlsafe(TOKEN_BYTES)
    now = _now()
    doc = {
        "id": f"prt-{uuid.uuid4().hex[:24]}",
        "token_hash": _hash(token),
        "email": email,
        "created_at": now,
        "expires_at": now + TOKEN_TTL,
        "used_at": None,
        "request_ip": ip,
        "request_ua": ua,
    }
    db = _database()
    if db is not None:
        db[COLLECTION].update_many(
            {"email": email, "used_at": None}, {"$set": {"used_at": now,
                                                        "superseded": True}})
        db[COLLECTION].insert_one(doc)
        return token, _to_doc(doc)
    with _LOCK:
        store = _load()
        tokens = store.setdefault("tokens", {})
        for row in tokens.values():
            if row.get("email") == email and not row.get("used_at"):
                row["used_at"] = now.isoformat()
                row["superseded"] = True
        tokens[doc["id"]] = _to_doc(doc)
        with json_doc_transaction(_path(), {"tokens": {}}) as data:
            data.clear()
            data.update(store)
    return token, doc


def _find(token_hash: str) -> dict[str, Any] | None:
    db = _database()
    if db is not None:
        row = db[COLLECTION].find_one({"token_hash": token_hash})
        return _to_doc(row) if row else None
    with _LOCK:
        store = _load()
    for row in store.get("tokens", {}).values():
        if hmac.compare_digest(row.get("token_hash", ""), token_hash):
            return row
    return None


def status_by_id(token_id: str) -> str:
    """'ok' | 'expired' | 'used' | 'superseded' | 'invalid' for a stored
    token id — the outbox drain uses this to skip rows whose link is dead
    before they burn a send."""
    db = _database()
    if db is not None:
        row = db[COLLECTION].find_one({"id": token_id})
        doc = _to_doc(row) if row else None
    else:
        with _LOCK:
            store = _load()
        doc = store.get("tokens", {}).get(token_id)
    if doc is None:
        return "invalid"
    if doc.get("superseded"):
        return "superseded"
    return _active_doc(doc)


def inspect(token: str) -> str:
    """Validate a token WITHOUT consuming it — 'ok' | 'expired' | 'used' | 'invalid'."""
    if not token:
        return "invalid"
    doc = _find(_hash(token))
    if doc is None:
        return "invalid"
    return _active_doc(doc)


def consume(token: str) -> tuple[str, dict[str, Any] | None]:
    """Atomically mark the token used.

    Returns ``(status, doc)`` where status is 'ok' | 'expired' | 'used' |
    'invalid'. Only one caller can ever receive 'ok' for a given token —
    the conditional update makes the second consume see ``used_at`` set.
    """
    if not token:
        return "invalid", None
    now = _now()
    token_hash = _hash(token)
    db = _database()
    if db is not None:
        row = db[COLLECTION].find_one_and_update(
            {"token_hash": token_hash, "used_at": None,
             "expires_at": {"$gt": now}},
            {"$set": {"used_at": now}},
            return_document=True)
        if row is not None:
            return "ok", _to_doc(row)
        doc = _find(token_hash)
        return (_active_doc(doc) if doc else "invalid"), doc
    with _LOCK:
        store = _load()
        for row in store.get("tokens", {}).values():
            if hmac.compare_digest(row.get("token_hash", ""), token_hash):
                status = _active_doc(row)
                if status != "ok":
                    return status, row
                row["used_at"] = now.isoformat()
                with json_doc_transaction(_path(), {"tokens": {}}) as data:
                    data.clear()
                    data.update(store)
                return "ok", row
        return "invalid", None


def purge_expired() -> int:
    """Delete fully-expired tokens. Returns the count removed."""
    now = _now()
    db = _database()
    if db is not None:
        return db[COLLECTION].delete_many({"expires_at": {"$lt": now}}).deleted_count
    with _LOCK:
        store = _load()
        tokens = store.get("tokens", {})
        dead = [k for k, r in tokens.items()
                if (_parse_ts(r.get("expires_at")) or now) < now]
        for k in dead:
            del tokens[k]
        if dead:
            with json_doc_transaction(_path(), {"tokens": {}}) as data:
                data.clear()
                data.update(store)
        return len(dead)
