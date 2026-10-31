"""Server-side auth session store (Phase D3 — jti + revocation).

Tokens carry a ``jti``; Verify requires the session row to exist and not be
revoked. Logout and password-rotation helpers delete or mark sessions so stolen
tokens die server-side (SOC 2 access-control expectation).

Durability (QA round 11, S7): rows live in MongoDB (collection
``auth_sessions``) when a real client is connected, and are mirrored to the
local JSON file, which is the store on its own when Mongo is absent. A
redeploy that loses the file therefore keeps every session, its revocation
and its idle clock. Reads merge both: a session is revoked when either store
says so, and its last activity is the latest either store saw.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_STORE_NAME = "auth_sessions.json"
COLLECTION = "auth_sessions"
_INDEXED: set[str] = set()


def _path():
    return data_dir() / _STORE_NAME


# ── file store ────────────────────────────────────────────────────────────


def _load() -> dict[str, Any]:
    path = _path()
    if not path.exists():
        return {"sessions": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"sessions": {}}
        sessions = raw.get("sessions")
        if not isinstance(sessions, dict):
            return {"sessions": {}}
        return {"sessions": sessions}
    except Exception as exc:
        logger.warning("auth_sessions load failed: %s", exc)
        return {"sessions": {}}


def _save(data: dict[str, Any]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(path)


def _file_put(jti: str, fields: dict[str, Any]) -> None:
    with _LOCK:
        data = _load()
        row = data["sessions"].get(jti)
        row = dict(row) if isinstance(row, dict) else {}
        row.update(fields)
        data["sessions"][jti] = row
        _save(data)


# ── Mongo store ───────────────────────────────────────────────────────────


def _collection():
    try:
        from services.metadata_backend import mongo_database

        db = mongo_database()
    except Exception:  # noqa: BLE001 — durability degrade must not 500 auth
        return None
    if db is None:
        return None
    coll = db[COLLECTION]
    if db.name not in _INDEXED:
        try:
            coll.create_index([("email", 1)], name="email")
            # Housekeeping: Mongo drops a row a week after it expires.
            coll.create_index([("expires_dt", 1)], name="expires_ttl", expireAfterSeconds=7 * 86400)
        except Exception:  # noqa: BLE001
            logger.warning("auth_sessions index ensure failed", exc_info=True)
        _INDEXED.add(db.name)
    return coll


def _mongo(fn, default=None):
    coll = _collection()
    if coll is None:
        return default
    try:
        return fn(coll)
    except Exception as exc:  # noqa: BLE001 — the file store still answers
        logger.warning("auth_sessions mongo call failed (%s)", type(exc).__name__)
        return default


def _session_row(jti: str) -> dict[str, Any] | None:
    """The merged row: revoked if either store says so, latest activity wins."""
    if not jti:
        return None
    doc = _mongo(lambda c: c.find_one({"_id": jti}, {"_id": False, "expires_dt": False}))
    with _LOCK:
        local = _load()["sessions"].get(jti)
    rows = [r for r in (doc, local) if isinstance(r, dict) and r]
    if not rows:
        return None
    merged: dict[str, Any] = {}
    for row in reversed(rows):  # Mongo's fields win over the file's
        merged.update(row)
    revoked = [int(r["revoked_at"]) for r in rows if r.get("revoked_at") is not None]
    merged["revoked_at"] = min(revoked) if revoked else None
    seen = max(int(r.get("last_seen_at") or r.get("created_at") or 0) for r in rows)
    merged["last_seen_at"] = seen or None
    # Factors only accumulate within a session: a step-up recorded in either store counts.
    amr = {str(m) for r in rows for m in (r.get("amr") or [])}
    if amr:
        merged["amr"] = sorted(amr)
    return merged


# ── API ───────────────────────────────────────────────────────────────────


def create_session(email: str, *, expires_at: int, amr: list[str] | None = None) -> str:
    """Persist a new session and return its jti.

    ``amr`` records how the session was authenticated (``pwd``, ``otp``,
    ``recovery``, ``sso``) for the workspace MFA policy (services.mfa_enforcement).
    """
    jti = uuid.uuid4().hex
    row: dict[str, Any] = {
        "email": email.strip().lower(),
        "created_at": int(time.time()),
        "expires_at": int(expires_at),
        "revoked_at": None,
    }
    if amr:
        row["amr"] = sorted({str(m) for m in amr})
    expires_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
    _mongo(lambda c: c.insert_one({"_id": jti, **row, "expires_dt": expires_dt}))
    _file_put(jti, row)
    return jti


def session_amr(jti: str) -> list[str]:
    """Authentication methods recorded on a session ([] for rows that predate them)."""
    row = _session_row(jti)
    if not row:
        return []
    return [str(m) for m in row.get("amr") or []]


def add_session_amr(jti: str, *methods: str) -> bool:
    """Record a second factor passed inside an existing session (step-up)."""
    row = _session_row(jti)
    if not row or row.get("revoked_at") is not None:
        return False
    amr = sorted({*(str(m) for m in row.get("amr") or []), *(str(m) for m in methods)})
    _mongo(lambda c: c.update_one({"_id": jti}, {"$set": {"amr": amr}}))
    _file_put(jti, {**row, "amr": amr})
    return True


def session_active(jti: str, email: str) -> bool:
    row = _session_row(jti)
    if not row:
        return False
    if row.get("revoked_at") is not None:
        return False
    if int(row.get("expires_at") or 0) < int(time.time()):
        return False
    return str(row.get("email") or "").strip().lower() == email.strip().lower()


def revoke_session(jti: str) -> bool:
    """Revoke one session by jti. Returns True when a row was updated."""
    row = _session_row(jti)
    if not row:
        return False
    if row.get("revoked_at") is not None:
        return True
    now = int(time.time())
    _mongo(lambda c: c.update_one({"_id": jti}, {"$set": {"revoked_at": now}}))
    _file_put(jti, {**row, "revoked_at": now})
    return True


def revoke_all_for_email(email: str) -> int:
    """Revoke every active session for ``email`` (password rotate / force logout)."""
    normalized = email.strip().lower()
    now = int(time.time())
    ids = _mongo(
        lambda c: [d["_id"] for d in c.find({"email": normalized, "revoked_at": None}, {"_id": 1})],
        default=[],
    ) or []
    if ids:
        _mongo(lambda c: c.update_many({"_id": {"$in": ids}}, {"$set": {"revoked_at": now}}))
    revoked = set(ids)
    with _LOCK:
        data = _load()
        changed = False
        for jti, row in list(data["sessions"].items()):
            if not isinstance(row, dict) or row.get("revoked_at") is not None:
                continue
            if str(row.get("email") or "").strip().lower() != normalized:
                continue
            row["revoked_at"] = now
            data["sessions"][jti] = row
            revoked.add(jti)
            changed = True
        if changed:
            _save(data)
    return len(revoked)


def purge_expired(*, now: int | None = None) -> int:
    """Drop expired/revoked rows older than 7 days (Mongo also has a TTL index)."""
    ts = int(now if now is not None else time.time())
    cutoff = ts - 7 * 86400
    stale = {"$or": [{"expires_at": {"$lt": cutoff}}, {"revoked_at": {"$ne": None, "$lt": cutoff}}]}
    removed = int(_mongo(lambda c: c.delete_many(stale).deleted_count, default=0) or 0)
    with _LOCK:
        data = _load()
        keep: dict[str, Any] = {}
        dropped = 0
        for jti, row in data["sessions"].items():
            if not isinstance(row, dict):
                dropped += 1
                continue
            expires = int(row.get("expires_at") or 0)
            revoked = row.get("revoked_at")
            if expires < cutoff or (revoked is not None and int(revoked) < cutoff):
                dropped += 1
                continue
            keep[jti] = row
        if dropped:
            data["sessions"] = keep
            _save(data)
    return max(removed, dropped)


def session_last_seen(jti: str) -> int | None:
    """When this session last made a request (``created_at`` before any touch)."""
    row = _session_row(jti)
    if not row:
        return None
    return int(row.get("last_seen_at") or 0) or None


def touch_session(jti: str, *, min_interval: int = 60, now: int | None = None) -> None:
    """Record activity; writes at most once per ``min_interval`` seconds.

    A session with no activity on record is recorded as seen now, so its idle
    clock starts instead of never running.
    """
    if not jti:
        return
    current = int(now if now is not None else time.time())
    row = _session_row(jti)
    last = int((row or {}).get("last_seen_at") or 0)
    if last and current - last < min_interval:
        return
    _mongo(lambda c: c.update_one({"_id": jti}, {"$set": {"last_seen_at": current}}, upsert=True))
    _file_put(jti, {"last_seen_at": current})
