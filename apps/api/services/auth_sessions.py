"""Server-side auth session store (Phase D3 — jti + revocation).

Tokens carry a ``jti``; Verify requires the session row to exist and not be
revoked. Logout and password-rotation helpers delete or mark sessions so stolen
tokens die server-side (SOC 2 access-control expectation).
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_STORE_NAME = "auth_sessions.json"


def _path():
    return data_dir() / _STORE_NAME


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


def create_session(email: str, *, expires_at: int) -> str:
    """Persist a new session and return its jti."""
    jti = uuid.uuid4().hex
    now = int(time.time())
    with _LOCK:
        data = _load()
        data["sessions"][jti] = {
            "email": email.strip().lower(),
            "created_at": now,
            "expires_at": int(expires_at),
            "revoked_at": None,
        }
        _save(data)
    return jti


def session_active(jti: str, email: str) -> bool:
    if not jti:
        return False
    now = int(time.time())
    with _LOCK:
        data = _load()
        row = data["sessions"].get(jti)
        if not isinstance(row, dict):
            return False
        if row.get("revoked_at") is not None:
            return False
        if int(row.get("expires_at") or 0) < now:
            return False
        if str(row.get("email") or "").strip().lower() != email.strip().lower():
            return False
        return True


def revoke_session(jti: str) -> bool:
    """Revoke one session by jti. Returns True when a row was updated."""
    if not jti:
        return False
    now = int(time.time())
    with _LOCK:
        data = _load()
        row = data["sessions"].get(jti)
        if not isinstance(row, dict):
            return False
        if row.get("revoked_at") is not None:
            return True
        row["revoked_at"] = now
        data["sessions"][jti] = row
        _save(data)
    return True


def revoke_all_for_email(email: str) -> int:
    """Revoke every active session for ``email`` (password rotate / force logout)."""
    normalized = email.strip().lower()
    now = int(time.time())
    count = 0
    with _LOCK:
        data = _load()
        for jti, row in list(data["sessions"].items()):
            if not isinstance(row, dict):
                continue
            if str(row.get("email") or "").strip().lower() != normalized:
                continue
            if row.get("revoked_at") is not None:
                continue
            row["revoked_at"] = now
            data["sessions"][jti] = row
            count += 1
        if count:
            _save(data)
    return count


def purge_expired(*, now: int | None = None) -> int:
    """Drop expired/revoked rows older than 7 days — housekeeping for local JSON store."""
    ts = int(now if now is not None else time.time())
    cutoff = ts - 7 * 86400
    removed = 0
    with _LOCK:
        data = _load()
        keep: dict[str, Any] = {}
        for jti, row in data["sessions"].items():
            if not isinstance(row, dict):
                removed += 1
                continue
            expires = int(row.get("expires_at") or 0)
            revoked = row.get("revoked_at")
            if expires < cutoff or (revoked is not None and int(revoked) < cutoff):
                removed += 1
                continue
            keep[jti] = row
        if removed:
            data["sessions"] = keep
            _save(data)
    return removed
