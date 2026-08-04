"""Per-principal MCP tool-call rate limiter (token bucket).

Fail-open when disabled. When enabled, excess calls return retry_after_sec so
the router can emit HTTP 429. Treat MCP as a production API surface forever.
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


_LOCK = threading.Lock()
_BUCKETS: dict[str, _Bucket] = {}


def rate_limit_enabled() -> bool:
    """Default on at a generous ceiling; set DATAFLOW_MCP_RATE_LIMIT=0 to disable."""
    raw = os.getenv("DATAFLOW_MCP_RATE_LIMIT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def check_mcp_rate_limit(principal: str) -> dict[str, float | bool | str]:
    """Consume one token for ``principal``.

    Returns ``{"allowed": True, ...}`` or ``{"allowed": False, "retry_after_sec": N}``.
    """
    if not rate_limit_enabled():
        return {"allowed": True, "disabled": True}

    key = (principal or "anonymous").strip().lower() or "anonymous"
    capacity = max(1.0, _env_float("DATAFLOW_MCP_RATE_BURST", 30.0))
    refill_per_sec = max(0.1, _env_float("DATAFLOW_MCP_RATE_QPS", 5.0))
    now = time.monotonic()

    with _LOCK:
        bucket = _BUCKETS.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=capacity, updated_at=now)
            _BUCKETS[key] = bucket
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_sec)
        bucket.updated_at = now
        if bucket.tokens < 1.0:
            need = 1.0 - bucket.tokens
            retry = need / refill_per_sec
            return {
                "allowed": False,
                "retry_after_sec": max(0.1, round(retry, 2)),
                "principal": key,
            }
        bucket.tokens -= 1.0
        # Bound map growth for long-lived processes.
        if len(_BUCKETS) > _env_int("DATAFLOW_MCP_RATE_MAX_KEYS", 5000):
            oldest = sorted(_BUCKETS.items(), key=lambda kv: kv[1].updated_at)[:500]
            for drop_key, _ in oldest:
                _BUCKETS.pop(drop_key, None)
        return {"allowed": True, "principal": key, "tokens_remaining": round(bucket.tokens, 2)}


def reset_mcp_rate_limits() -> None:
    """Test helper — clear in-memory buckets."""
    with _LOCK:
        _BUCKETS.clear()
