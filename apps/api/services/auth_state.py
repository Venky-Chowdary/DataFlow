"""Shared auth state (P0-1): sessions and rate-limit counters.

Two instances of the API must share lockouts and session revocation — the
JSON file + in-process dicts can't. With ``DATAWRAP_REDIS_URL`` set (and
only then) this module backs both concerns with Redis; without it a
per-process store keeps dev/test working, and Mongo (when configured) is
the durable record sessions are mirrored into.

Session model per the handover: jti, user_id (normalized email), device_id,
ip, ua, created_at, last_seen_at, idle_expires_at, absolute_expires_at,
revoked_at. Rate limiting uses fixed-window counters (key → count, TTL =
window), which is what a /24-or-email abuse cap needs.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any, Protocol

from services.brand_env import getenv_brand
from services.metadata_backend import mongo_database

logger = logging.getLogger(__name__)

_SESSION_PREFIX = "dw:auth:sess:"
_USER_SESSIONS_PREFIX = "dw:auth:user:"
_RL_PREFIX = "dw:auth:rl:"
_MONGO_COLLECTION = "auth_sessions_shared"


class StateBackend(Protocol):
    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str, ttl_sec: int) -> None: ...
    def delete(self, key: str) -> None: ...
    def sadd(self, key: str, member: str) -> None: ...
    def smembers(self, key: str) -> list[str]: ...
    def srem(self, key: str, member: str) -> None: ...
    def incr(self, key: str, ttl_sec: int) -> int: ...


class _MemoryBackend:
    """Single-process fallback — dev/test only."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[str, float | None]] = {}
        self._sets: dict[str, set[str]] = {}
        self._counters: dict[str, tuple[int, float]] = {}
        self._lock = threading.Lock()

    def _prune(self, key: str) -> None:
        item = self._data.get(key)
        if item and item[1] is not None and item[1] <= time.time():
            self._data.pop(key, None)

    def get(self, key: str) -> str | None:
        with self._lock:
            self._prune(key)
            item = self._data.get(key)
            return item[0] if item else None

    def set(self, key: str, value: str, ttl_sec: int) -> None:
        with self._lock:
            self._data[key] = (value, time.time() + ttl_sec if ttl_sec > 0 else None)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def sadd(self, key: str, member: str) -> None:
        with self._lock:
            self._sets.setdefault(key, set()).add(member)

    def smembers(self, key: str) -> list[str]:
        with self._lock:
            return sorted(self._sets.get(key, set()))

    def srem(self, key: str, member: str) -> None:
        with self._lock:
            self._sets.get(key, set()).discard(member)

    def incr(self, key: str, ttl_sec: int) -> int:
        with self._lock:
            count, reset_at = self._counters.get(key, (0, time.time() + ttl_sec))
            if reset_at <= time.time():
                count, reset_at = 0, time.time() + ttl_sec
            self._counters[key] = (count + 1, reset_at)
            return count + 1


class _RedisBackend:
    def __init__(self, client: Any) -> None:
        self._c = client

    def get(self, key: str) -> str | None:
        v = self._c.get(key)
        return v.decode() if isinstance(v, bytes) else v

    def set(self, key: str, value: str, ttl_sec: int) -> None:
        self._c.set(key, value, ex=ttl_sec if ttl_sec > 0 else None)

    def delete(self, key: str) -> None:
        self._c.delete(key)

    def sadd(self, key: str, member: str) -> None:
        self._c.sadd(key, member)

    def smembers(self, key: str) -> list[str]:
        return sorted(
            m.decode() if isinstance(m, bytes) else m for m in self._c.smembers(key)
        )

    def srem(self, key: str, member: str) -> None:
        self._c.srem(key, member)

    def incr(self, key: str, ttl_sec: int) -> int:
        pipe = self._c.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl_sec, nx=True)
        count, _ = pipe.execute()
        return int(count)


_REDIS_CLIENT: Any = None
_REDIS_LOCK = threading.Lock()


def _redis_client():
    global _REDIS_CLIENT
    url = (getenv_brand("REDIS_URL") or "").strip()
    if not url:
        return None
    with _REDIS_LOCK:
        if _REDIS_CLIENT is None:
            import redis

            _REDIS_CLIENT = redis.from_url(
                url, socket_timeout=2, socket_connect_timeout=2, decode_responses=False
            )
        return _REDIS_CLIENT


def backend_kind() -> str:
    return "redis" if (getenv_brand("REDIS_URL") or "").strip() else "memory"


class SharedAuthState:
    """One facade over the session + rate-limit backend."""

    def __init__(self, backend: StateBackend) -> None:
        self._b = backend

    # ── sessions ─────────────────────────────────────────────────────────

    def create_session(
        self,
        *,
        user_id: str,
        device_id: str = "",
        ip: str = "",
        ua: str = "",
        idle_seconds: int = 12 * 3600,
        absolute_seconds: int = 24 * 3600,
        jti: str | None = None,
        csrf_token: str = "",
    ) -> dict[str, Any]:
        now = int(time.time())
        jti = jti or uuid.uuid4().hex
        doc = {
            "jti": jti,
            "user_id": user_id,
            "csrf": csrf_token,
            "device_id": device_id,
            "ip": ip,
            "ua": ua[:512],
            "created_at": now,
            "last_seen_at": now,
            "idle_expires_at": now + idle_seconds,
            "absolute_expires_at": now + absolute_seconds,
            "revoked_at": None,
        }
        self._b.set(f"{_SESSION_PREFIX}{jti}", json.dumps(doc), absolute_seconds + 3600)
        self._b.sadd(f"{_USER_SESSIONS_PREFIX}{user_id}", jti)
        self._mongo_mirror(doc)
        return doc

    def get_session(self, jti: str) -> dict[str, Any] | None:
        raw = self._b.get(f"{_SESSION_PREFIX}{jti}")
        if not raw:
            return None
        try:
            doc = json.loads(raw)
        except (ValueError, TypeError):
            return None
        now = int(time.time())
        if doc.get("revoked_at") or now >= int(doc.get("absolute_expires_at") or 0) \
                or now >= int(doc.get("idle_expires_at") or 0):
            return doc if doc.get("revoked_at") else None
        return doc

    def session_active(self, jti: str) -> bool:
        doc = self.get_session(jti)
        return doc is not None and not doc.get("revoked_at")

    def touch_session(self, jti: str, *, idle_seconds: int = 12 * 3600) -> None:
        doc = self.get_session(jti)
        if not doc:
            return
        now = int(time.time())
        doc["last_seen_at"] = now
        doc["idle_expires_at"] = min(now + idle_seconds, int(doc["absolute_expires_at"]))
        ttl = int(doc["absolute_expires_at"]) - now + 3600
        self._b.set(f"{_SESSION_PREFIX}{jti}", json.dumps(doc), max(1, ttl))

    def add_amr(self, jti: str, *methods: str) -> bool:
        """Record a second factor passed inside this cookie session (MFA step-up)."""
        doc = self.get_session(jti)
        if not doc or doc.get("revoked_at"):
            return False
        doc["amr"] = sorted({*(str(m) for m in doc.get("amr") or []), *methods})
        ttl = int(doc["absolute_expires_at"]) - int(time.time()) + 3600
        self._b.set(f"{_SESSION_PREFIX}{jti}", json.dumps(doc), max(1, ttl))
        self._mongo_mirror(doc)
        return True

    def revoke_session(self, jti: str) -> None:
        raw = self._b.get(f"{_SESSION_PREFIX}{jti}")
        if not raw:
            return
        try:
            doc = json.loads(raw)
        except (ValueError, TypeError):
            return
        doc["revoked_at"] = int(time.time())
        remaining = max(1, int(doc["absolute_expires_at"]) - int(time.time()) + 3600)
        self._b.set(f"{_SESSION_PREFIX}{jti}", json.dumps(doc), remaining)
        self._mongo_mirror(doc)

    def rotate_session(self, jti: str) -> dict[str, Any] | None:
        """Privilege-change rotation (P3-2): mint a fresh jti carrying the
        same identity/device/CSRF binding, revoke the old one. Callers on
        login, verify and password-change paths so a stolen pre-elevation
        session id dies immediately."""
        old = self.get_session(jti)
        if not old:
            return None
        remaining_idle = max(0, int(old["idle_expires_at"]) - int(time.time()))
        remaining_abs = max(0, int(old["absolute_expires_at"]) - int(time.time()))
        new = self.create_session(
            user_id=old["user_id"],
            device_id=old.get("device_id", ""),
            ip=old.get("ip", ""),
            ua=old.get("ua", ""),
            idle_seconds=remaining_idle or 1,
            absolute_seconds=remaining_abs or 1,
            csrf_token=old.get("csrf", ""),
        )
        self.revoke_session(jti)
        return new

    def revoke_all_for_user(self, user_id: str) -> int:
        jtis = self._b.smembers(f"{_USER_SESSIONS_PREFIX}{user_id}")
        for jti in jtis:
            self.revoke_session(jti)
        return len(jtis)

    def list_sessions(self, user_id: str) -> list[dict[str, Any]]:
        out = []
        for jti in self._b.smembers(f"{_USER_SESSIONS_PREFIX}{user_id}"):
            doc = self.get_session(jti)
            if doc and not doc.get("revoked_at"):
                out.append(doc)
        return sorted(out, key=lambda d: int(d.get("last_seen_at") or 0), reverse=True)

    def _mongo_mirror(self, doc: dict[str, Any]) -> None:
        """Mongo is the durable record; the hot path stays in the backend."""
        try:
            db = mongo_database()
            if db is not None:
                db[_MONGO_COLLECTION].replace_one({"_id": doc["jti"]}, {"_id": doc["jti"], **doc}, upsert=True)
        except Exception:  # noqa: BLE001 — durability degrade must not 500 auth
            logger.warning("auth session mongo mirror failed")

    # ── rate limits (fixed-window counters) ───────────────────────────────

    def rate_limit_hit(self, key: str, *, limit: int, window_sec: int) -> tuple[bool, int]:
        """Consume one unit. Returns (allowed, retry_after_sec)."""
        count = self._b.incr(f"{_RL_PREFIX}{key}", window_sec)
        if count <= limit:
            return True, 0
        return False, window_sec


_STATE: SharedAuthState | None = None
_STATE_LOCK = threading.Lock()


def get_auth_state() -> SharedAuthState:
    """Redis when DATAWRAP_REDIS_URL is configured; in-memory otherwise."""
    global _STATE
    with _STATE_LOCK:
        if _STATE is not None:
            return _STATE
        client = _redis_client()
        if client is not None:
            try:
                client.ping()
                _STATE = SharedAuthState(_RedisBackend(client))
                return _STATE
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "auth state Redis unreachable (%s) — falling back to per-process",
                    type(exc).__name__,
                )
        _STATE = SharedAuthState(_MemoryBackend())
        return _STATE


def reset_auth_state() -> None:
    """Test hook."""
    global _STATE, _REDIS_CLIENT
    with _STATE_LOCK:
        _STATE = None
        _REDIS_CLIENT = None
