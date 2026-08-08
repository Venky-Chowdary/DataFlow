"""Login rate limit + progressive lockout (audit §6.2).

Per-IP and per-email token buckets with failed-attempt lockout. Fail-closed on
excess attempts (HTTP 429). Disable only via ``DATAFLOW_AUTH_RATE_LIMIT=0``
(never the default in production).
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field


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
class _Bucket:
    tokens: float
    updated_at: float = field(default_factory=time.monotonic)
    failures: int = 0
    locked_until: float = 0.0


_LOCK = threading.Lock()
_BUCKETS: dict[str, _Bucket] = {}


def auth_rate_limit_enabled() -> bool:
    raw = os.getenv("DATAFLOW_AUTH_RATE_LIMIT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def reset_auth_rate_limits() -> None:
    """Test helper — clear all buckets."""
    with _LOCK:
        _BUCKETS.clear()


def _client_key(ip: str, email: str) -> str:
    ip_k = (ip or "unknown").strip().lower() or "unknown"
    email_k = (email or "").strip().lower() or "anonymous"
    return f"{ip_k}|{email_k}"


def check_login_rate_limit(*, ip: str, email: str) -> dict[str, float | bool | str]:
    """Consume one attempt token. Returns allowed / retry_after_sec / locked."""
    if not auth_rate_limit_enabled():
        return {"allowed": True, "disabled": True}

    key = _client_key(ip, email)
    capacity = max(1.0, _env_float("DATAFLOW_AUTH_RATE_BURST", 10.0))
    refill_per_sec = max(0.05, _env_float("DATAFLOW_AUTH_RATE_QPS", 0.2))
    lockout_after = max(3, _env_int("DATAFLOW_AUTH_LOCKOUT_FAILURES", 8))
    lockout_sec = max(30.0, _env_float("DATAFLOW_AUTH_LOCKOUT_SEC", 300.0))
    now = time.monotonic()

    with _LOCK:
        bucket = _BUCKETS.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=capacity, updated_at=now)
            _BUCKETS[key] = bucket

        if bucket.locked_until > now:
            return {
                "allowed": False,
                "locked": True,
                "retry_after_sec": max(0.1, round(bucket.locked_until - now, 2)),
                "principal": key,
            }
        # Progressive lockout after consecutive auth failures.
        if bucket.failures >= lockout_after:
            bucket.locked_until = now + lockout_sec
            bucket.failures = 0
            return {
                "allowed": False,
                "locked": True,
                "retry_after_sec": lockout_sec,
                "principal": key,
            }

        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_sec)
        bucket.updated_at = now
        if bucket.tokens < 1.0:
            need = 1.0 - bucket.tokens
            retry = need / refill_per_sec
            return {
                "allowed": False,
                "locked": False,
                "retry_after_sec": max(0.1, round(retry, 2)),
                "principal": key,
            }
        bucket.tokens -= 1.0
        return {"allowed": True, "principal": key}


def record_login_failure(*, ip: str, email: str) -> None:
    if not auth_rate_limit_enabled():
        return
    key = _client_key(ip, email)
    with _LOCK:
        bucket = _BUCKETS.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=0.0)
            _BUCKETS[key] = bucket
        bucket.failures += 1


def record_login_success(*, ip: str, email: str) -> None:
    if not auth_rate_limit_enabled():
        return
    key = _client_key(ip, email)
    with _LOCK:
        bucket = _BUCKETS.get(key)
        if bucket is not None:
            bucket.failures = 0
            bucket.locked_until = 0.0
