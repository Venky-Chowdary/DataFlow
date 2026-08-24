"""Login rate limit + exponential lockout (audit ITEM 4).

Reuses ``TokenBucketStore`` from ``mcp_rate_limit`` (do not invent a second
bucket). Applies **independent** per-IP and per-account throttles — either
exhausted bucket denies the attempt. Failed logins escalate lockout with
exponential backoff on both dimensions.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

from services.mcp_rate_limit import TokenBucketStore


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class _LockoutState:
    failures: int = 0
    locked_until: float = 0.0
    # How many lockout cycles have fired; drives exponential backoff.
    lockout_streak: int = 0


_RATE_STORE = TokenBucketStore()
_LOCK = threading.Lock()
_LOCKOUTS: dict[str, _LockoutState] = {}


def auth_rate_limit_enabled() -> bool:
    raw = os.getenv("DATAFLOW_AUTH_RATE_LIMIT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def reset_auth_rate_limits() -> None:
    """Test helper — clear rate buckets and lockout state."""
    _RATE_STORE.clear()
    with _LOCK:
        _LOCKOUTS.clear()


def _norm_ip(ip: str) -> str:
    return (ip or "unknown").strip().lower() or "unknown"


def _norm_email(email: str) -> str:
    return (email or "").strip().lower() or "anonymous"


def _lockout_key(kind: str, value: str) -> str:
    return f"{kind}:{value}"


def _check_lockout(key: str, *, now: float) -> dict[str, float | bool | str] | None:
    with _LOCK:
        state = _LOCKOUTS.get(key)
        if state is None:
            return None
        if state.locked_until > now:
            return {
                "allowed": False,
                "locked": True,
                "retry_after_sec": max(0.1, round(state.locked_until - now, 2)),
                "principal": key,
            }
    return None


def _maybe_engage_lockout(key: str, *, now: float) -> dict[str, float | bool | str] | None:
    """If failure threshold reached, engage exponential lockout and deny."""
    lockout_after = max(1, _env_int("DATAFLOW_AUTH_LOCKOUT_FAILURES", 8))
    base_sec = max(1.0, _env_float("DATAFLOW_AUTH_LOCKOUT_SEC", 300.0))
    max_sec = max(base_sec, _env_float("DATAFLOW_AUTH_LOCKOUT_MAX_SEC", 3600.0))

    with _LOCK:
        state = _LOCKOUTS.get(key)
        if state is None:
            return None
        if state.locked_until > now:
            return {
                "allowed": False,
                "locked": True,
                "retry_after_sec": max(0.1, round(state.locked_until - now, 2)),
                "principal": key,
            }
        if state.failures < lockout_after:
            return None
        state.lockout_streak = max(1, state.lockout_streak + 1)
        # Exponential: base * 2^(streak-1), capped.
        lock_sec = min(max_sec, base_sec * (2 ** (state.lockout_streak - 1)))
        state.locked_until = now + lock_sec
        state.failures = 0
        return {
            "allowed": False,
            "locked": True,
            "retry_after_sec": float(lock_sec),
            "principal": key,
        }


def check_login_rate_limit(*, ip: str, email: str) -> dict[str, float | bool | str]:
    """Deny when per-IP or per-account throttle/lockout is exhausted."""
    if not auth_rate_limit_enabled():
        return {"allowed": True, "disabled": True}

    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    ip_lock_key = _lockout_key("ip", ip_k)
    email_lock_key = _lockout_key("email", email_k)
    now = time.monotonic()

    for key in (ip_lock_key, email_lock_key):
        denied = _check_lockout(key, now=now)
        if denied is not None:
            return denied

    # Engage every over-threshold principal in one pass so sibling failure
    # counters cannot immediately re-lock after the first dimension unlocks.
    engaged: dict[str, float | bool | str] | None = None
    for key in (ip_lock_key, email_lock_key):
        denied = _maybe_engage_lockout(key, now=now)
        if denied is not None and engaged is None:
            engaged = denied
    if engaged is not None:
        return engaged

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
            "retry_after_sec": email_result.get("retry_after_sec", 60),
            "principal": f"email:{email_k}",
        }

    return {"allowed": True, "principal": f"ip:{ip_k}|email:{email_k}"}


def record_login_failure(*, ip: str, email: str) -> None:
    if not auth_rate_limit_enabled():
        return
    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    with _LOCK:
        for key in (_lockout_key("ip", ip_k), _lockout_key("email", email_k)):
            state = _LOCKOUTS.get(key)
            if state is None:
                state = _LockoutState()
                _LOCKOUTS[key] = state
            state.failures += 1


def record_login_success(*, ip: str, email: str) -> None:
    if not auth_rate_limit_enabled():
        return
    ip_k = _norm_ip(ip)
    email_k = _norm_email(email)
    with _LOCK:
        for key in (_lockout_key("ip", ip_k), _lockout_key("email", email_k)):
            state = _LOCKOUTS.get(key)
            if state is not None:
                state.failures = 0
                state.locked_until = 0.0
                state.lockout_streak = 0


__all__ = [
    "auth_rate_limit_enabled",
    "check_login_rate_limit",
    "record_login_failure",
    "record_login_success",
    "reset_auth_rate_limits",
]
