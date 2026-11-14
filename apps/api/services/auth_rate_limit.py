"""Login rate limit + exponential lockout (audit ITEM 4, SEC-7, REL-1).

Reuses ``TokenBucketStore`` from ``mcp_rate_limit`` (do not invent a second
bucket) for per-address and per-account throttles.

Failed sign-ins lock with exponential backoff per **(account, address)** pair
and, at a higher threshold, per address. The account as a whole is never
hard-locked: failures from anywhere only slow attempts down, and not from an
address the account has signed in from (SEC-7: nine anonymous failures from
nine addresses used to lock the administrator out everywhere).

The lockout table is bounded and every entry has a TTL; with Redis configured
(``DATAFLOW_AUTH_LOCKOUT_REDIS_URL`` or ``REDIS_URL``) it is shared by every
replica and survives a restart (REL-1).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from services.brand_env import getenv_brand
from services.mcp_rate_limit import TokenBucketStore

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = (getenv_brand(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = (getenv_brand(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class _LockoutState:
    failures: int = 0
    # Wall clock (seconds since the epoch): comparable across replicas.
    locked_until: float = 0.0
    # How many lockout cycles have fired; drives exponential backoff.
    lockout_streak: int = 0

    def to_json(self) -> str:
        return json.dumps([self.failures, self.locked_until, self.lockout_streak])

    @classmethod
    def from_json(cls, raw: Any) -> "_LockoutState":
        try:
            failures, locked_until, streak = json.loads(raw)
            return cls(int(failures), float(locked_until), int(streak))
        except Exception:  # noqa: BLE001 - a corrupt entry counts as none
            return cls()


class _MemoryLockouts:
    """Per-process lockout table, bounded (LRU) and with per-entry expiry (REL-1)."""

    def __init__(self) -> None:
        self._data: OrderedDict[str, tuple[_LockoutState, float]] = OrderedDict()

    def get(self, key: str, *, now: float) -> _LockoutState | None:
        item = self._data.get(key)
        if item is None:
            return None
        if item[1] <= now:
            self._data.pop(key, None)
            return None
        self._data.move_to_end(key)
        return item[0]

    def put(self, key: str, state: _LockoutState, *, ttl: float, now: float) -> None:
        self._data[key] = (state, now + max(1.0, ttl))
        self._data.move_to_end(key)
        cap = max(100, _env_int("DATAFLOW_AUTH_LOCKOUT_MAX_KEYS", 10000))
        while len(self._data) > cap:
            self._data.popitem(last=False)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()

    def values(self) -> list[_LockoutState]:
        return [state for state, _exp in self._data.values()]

    def __len__(self) -> int:
        return len(self._data)


class _RedisLockouts:
    """Lockout table shared by every replica: one key per principal with a TTL."""

    def __init__(self, client: Any, prefix: str) -> None:
        self._c = client
        self._prefix = prefix

    def get(self, key: str, *, now: float) -> _LockoutState | None:
        raw = self._c.get(self._prefix + key)
        return None if raw is None else _LockoutState.from_json(raw)

    def put(self, key: str, state: _LockoutState, *, ttl: float, now: float) -> None:
        self._c.set(self._prefix + key, state.to_json(), ex=max(1, int(ttl + 0.999)))

    def delete(self, key: str) -> None:
        self._c.delete(self._prefix + key)

    def clear(self) -> None:
        for key in self._c.scan_iter(match=self._prefix + "*", count=500):
            self._c.delete(key)

    def values(self) -> list[_LockoutState]:
        return []

    def __len__(self) -> int:
        return sum(1 for _ in self._c.scan_iter(match=self._prefix + "*", count=500))


_RATE_STORE = TokenBucketStore()
_LOCK = threading.RLock()
#: The in-process table (also the fallback when Redis is down).
_LOCKOUTS = _MemoryLockouts()
_BACKEND: dict[str, Any] = {"store": None, "url": None}


def _lockout_backend_url() -> str:
    """Redis for lockouts: ``DATAFLOW_AUTH_LOCKOUT_REDIS_URL``, else ``REDIS_URL``.

    ``DATAFLOW_AUTH_LOCKOUT_BACKEND=memory`` keeps them per process (tests).
    """
    if (getenv_brand("AUTH_LOCKOUT_BACKEND", "") or "").strip().lower() == "memory":
        return ""
    return (getenv_brand("AUTH_LOCKOUT_REDIS_URL", "") or getenv_brand("REDIS_URL", "") or "").strip()


def _store() -> Any:
    url = _lockout_backend_url()
    if not url:
        return _LOCKOUTS
    if _BACKEND["store"] is not None and _BACKEND["url"] == url:
        return _BACKEND["store"]
    try:
        import redis

        client = redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
        client.ping()
        prefix = (getenv_brand("AUTH_LOCKOUT_PREFIX", "") or "dw:authlock:").strip()
        _BACKEND.update(store=_RedisLockouts(client, prefix), url=url)
        return _BACKEND["store"]
    except Exception as exc:  # noqa: BLE001 - per-process limits beat no limits
        logger.warning("Login lockout store: Redis unreachable (%s); using this process only",
                       type(exc).__name__)
        return _LOCKOUTS


def _get(key: str, now: float) -> _LockoutState | None:
    try:
        return _store().get(key, now=now)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Login lockout read failed (%s)", type(exc).__name__)
        return _LOCKOUTS.get(key, now=now)


def _put(key: str, state: _LockoutState, now: float) -> None:
    window = max(60.0, _env_float("DATAFLOW_AUTH_FAILURE_WINDOW_SEC", 900.0))
    # Keep the entry while it is locked, and long enough after for the
    # backoff streak to matter; a quiet account forgets it.
    ttl = max(state.locked_until - now, 0.0) + max(window, _lock_settings()[2])
    try:
        _store().put(key, state, ttl=ttl, now=now)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Login lockout write failed (%s)", type(exc).__name__)
        _LOCKOUTS.put(key, state, ttl=ttl, now=now)


def _delete(key: str) -> None:
    try:
        _store().delete(key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Login lockout delete failed (%s)", type(exc).__name__)
    _LOCKOUTS.delete(key)


def auth_rate_limit_enabled() -> bool:
    raw = getenv_brand("DATAFLOW_AUTH_RATE_LIMIT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def public_rate_limit_enabled() -> bool:
    """Unauthenticated catalog / SSO discovery / MCP handshake.

    Off unless ``DATAFLOW_PUBLIC_RATE_LIMIT=1``. Production must set that on
    the API service — it is not implied by ``ENV=production``, so a shared
    test IP and a health fleet are not throttled by a code deploy alone.
    ``/health`` is excluded even when the limiter is on.
    """
    raw = getenv_brand("DATAFLOW_PUBLIC_RATE_LIMIT", "").strip().lower()
    return raw in {"1", "true", "on", "yes"}


def check_public_rate_limit(ip: str, path: str = "") -> dict[str, float | bool | str]:
    """Per-IP bucket for unauthenticated routes. Health probes are not counted."""
    if not public_rate_limit_enabled():
        return {"allowed": True, "disabled": True}
    route = (path or "").split("?", 1)[0]
    if route in {"/", "/health"} or route.startswith("/api/v1/health") or route.startswith("/health"):
        return {"allowed": True, "health": True}
    capacity = max(1.0, _env_float("DATAFLOW_PUBLIC_RATE_BURST", 120.0))
    refill = max(0.05, _env_float("DATAFLOW_PUBLIC_RATE_QPS", 2.0))
    max_keys = _env_int("DATAFLOW_PUBLIC_RATE_MAX_KEYS", 5000)
    ip_k = _norm_ip(ip)
    result = _RATE_STORE.consume(
        f"public:{ip_k}",
        capacity=capacity,
        refill_per_sec=refill,
        max_keys=max_keys,
    )
    if not result.get("allowed"):
        return {
            "allowed": False,
            "locked": False,
            "retry_after_sec": result.get("retry_after_sec", 60),
            "principal": f"public:{ip_k}",
        }
    return {"allowed": True, "principal": f"public:{ip_k}"}


def reset_auth_rate_limits() -> None:
    """Test helper — clear rate buckets and lockout state."""
    _RATE_STORE.clear()
    with _LOCK:
        _LOCKOUTS.clear()
        store = _store()
        if store is not _LOCKOUTS:
            try:
                store.clear()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Login lockout clear failed (%s)", type(exc).__name__)


def _norm_ip(ip: str) -> str:
    return (ip or "unknown").strip().lower() or "unknown"


def _norm_email(email: str) -> str:
    return (email or "").strip().lower() or "anonymous"


def _lockout_key(kind: str, value: str) -> str:
    return f"{kind}:{value}"


def _pair_key(ip_k: str, email_k: str) -> str:
    return _lockout_key("pair", f"{email_k}|{ip_k}")


def _known_key(ip_k: str, email_k: str) -> str:
    return _lockout_key("known", f"{email_k}|{ip_k}")


def _lock_settings() -> tuple[int, float, float]:
    lockout_after = max(1, _env_int("DATAFLOW_AUTH_LOCKOUT_FAILURES", 8))
    base_sec = max(1.0, _env_float("DATAFLOW_AUTH_LOCKOUT_SEC", 300.0))
    max_sec = max(base_sec, _env_float("DATAFLOW_AUTH_LOCKOUT_MAX_SEC", 3600.0))
    return lockout_after, base_sec, max_sec


def _locked(key: str, state: _LockoutState | None, now: float) -> dict[str, float | bool | str] | None:
    if state is not None and state.locked_until > now:
        return {
            "allowed": False,
            "locked": True,
            "retry_after_sec": max(0.1, round(state.locked_until - now, 2)),
            "principal": key,
        }
    return None


def _maybe_engage_lockout(key: str, threshold: int, *, now: float) -> dict[str, float | bool | str] | None:
    """If the failure threshold is reached, engage exponential lockout and deny."""
    _after, base_sec, max_sec = _lock_settings()
    with _LOCK:
        state = _get(key, now)
        if state is None:
            return None
        denied = _locked(key, state, now)
        if denied is not None:
            return denied
        if state.failures < threshold:
            return None
        state.lockout_streak = max(1, state.lockout_streak + 1)
        # Exponential: base * 2^(streak-1), capped.
        lock_sec = min(max_sec, base_sec * (2 ** (state.lockout_streak - 1)))
        state.locked_until = now + lock_sec
        state.failures = 0
        _put(key, state, now)
        return {
            "allowed": False,
            "locked": True,
            "retry_after_sec": float(lock_sec),
            "principal": key,
            "newly_locked": True,
        }


def _account_slowdown(email_k: str, now: float) -> dict[str, float | bool | str] | None:
    """The account-wide soft throttle (SEC-7): a delay, never a hard lock.

    After ``DATAFLOW_AUTH_ACCOUNT_SLOW_FAILURES`` failures from anywhere, an
    attempt from an address that has not signed in to this account before
    waits for a delay that doubles per further failure, up to
    ``DATAFLOW_AUTH_ACCOUNT_SLOW_MAX_SEC``. Known addresses are not slowed.
    """
    key = _lockout_key("email", email_k)
    state = _get(key, now)
    if state is None or state.locked_until <= now:
        return None
    return {
        "allowed": False,
        "locked": False,
        "soft": True,
        "retry_after_sec": max(0.1, round(state.locked_until - now, 2)),
        "principal": key,
    }


def check_login_rate_limit(*, ip: str, email: str) -> dict[str, float | bool | str]:
    """Deny when this account+address pair, or this address, is locked or throttled.

    SEC-7: the hard lockout is keyed per (account, address), so failures from
    anonymous addresses never lock the account itself; the account as a whole
    is only slowed down (``soft``), and not for an address it signed in from.
    """
    if not auth_rate_limit_enabled():
        return {"allowed": True, "disabled": True}

    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    pair_key = _pair_key(ip_k, email_k)
    ip_key = _lockout_key("ip", ip_k)
    now = time.time()
    lockout_after = _lock_settings()[0]
    ip_after = max(lockout_after, _env_int("DATAFLOW_AUTH_IP_LOCKOUT_FAILURES", 30))

    for key in (pair_key, ip_key):
        denied = _locked(key, _get(key, now), now)
        if denied is not None:
            return denied

    # Engage every over-threshold principal in one pass so sibling failure
    # counters cannot immediately re-lock after the first dimension unlocks.
    engaged: dict[str, float | bool | str] | None = None
    for key, threshold in ((pair_key, lockout_after), (ip_key, ip_after)):
        denied = _maybe_engage_lockout(key, threshold, now=now)
        if denied is not None and engaged is None:
            engaged = denied
    if engaged is not None:
        return engaged

    known = _get(_known_key(ip_k, email_k), now) is not None
    if not known:
        slowed = _account_slowdown(email_k, now)
        if slowed is not None:
            return slowed

    capacity = max(1.0, _env_float("DATAFLOW_AUTH_RATE_BURST", 10.0))
    refill = max(0.05, _env_float("DATAFLOW_AUTH_RATE_QPS", 0.2))
    max_keys = _env_int("DATAFLOW_AUTH_RATE_MAX_KEYS", 5000)

    # Independent buckets — either may refuse.
    ip_result = _RATE_STORE.consume(
        f"ip:{ip_k}",
        capacity=capacity,
        refill_per_sec=refill,
        max_keys=max_keys,
    )
    if not ip_result.get("allowed"):
        return {
            "allowed": False,
            "locked": False,
            "retry_after_sec": ip_result.get("retry_after_sec", 60),
            "principal": f"ip:{ip_k}",
        }

    if not known:
        # The account bucket slows a spray from many addresses; an address
        # this account signed in from is never throttled by strangers.
        email_result = _RATE_STORE.consume(
            f"email:{email_k}",
            capacity=capacity,
            refill_per_sec=refill,
            max_keys=max_keys,
        )
        if not email_result.get("allowed"):
            return {
                "allowed": False,
                "locked": False,
                "soft": True,
                "retry_after_sec": email_result.get("retry_after_sec", 60),
                "principal": f"email:{email_k}",
            }

    return {"allowed": True, "principal": f"ip:{ip_k}|email:{email_k}"}


def record_login_failure(*, ip: str, email: str) -> dict[str, bool]:
    """Count a failed sign-in. ``account_slowed`` is True when this failure
    started the account-wide slowdown (the owner is told once)."""
    if not auth_rate_limit_enabled():
        return {"account_slowed": False}
    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    now = time.time()
    slow_after = max(1, _env_int("DATAFLOW_AUTH_ACCOUNT_SLOW_FAILURES", 2 * _lock_settings()[0]))
    slow_max = max(1.0, _env_float("DATAFLOW_AUTH_ACCOUNT_SLOW_MAX_SEC", 60.0))
    started = False
    with _LOCK:
        for key in (_pair_key(ip_k, email_k), _lockout_key("ip", ip_k)):
            state = _get(key, now) or _LockoutState()
            state.failures += 1
            _put(key, state, now)
        account_key = _lockout_key("email", email_k)
        account = _get(account_key, now) or _LockoutState()
        account.failures += 1
        over = account.failures - slow_after
        if over >= 0:
            started = over == 0
            account.lockout_streak = over + 1
            account.locked_until = now + min(slow_max, float(2 ** min(over, 16)))
        _put(account_key, account, now)
    return {"account_slowed": started}


def record_login_success(*, ip: str, email: str) -> None:
    if not auth_rate_limit_enabled():
        return
    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    now = time.time()
    with _LOCK:
        for key in (_pair_key(ip_k, email_k), _lockout_key("ip", ip_k), _lockout_key("email", email_k)):
            _delete(key)
        # Remember the address: strangers' failures never slow it down.
        known_days = max(1.0, _env_float("DATAFLOW_AUTH_KNOWN_ADDRESS_DAYS", 30.0))
        try:
            _store().put(_known_key(ip_k, email_k), _LockoutState(), ttl=known_days * 86400, now=now)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Login known-address write failed (%s)", type(exc).__name__)


__all__ = [
    "auth_rate_limit_enabled",
    "check_login_rate_limit",
    "check_public_rate_limit",
    "public_rate_limit_enabled",
    "record_login_failure",
    "record_login_success",
    "reset_auth_rate_limits",
]
