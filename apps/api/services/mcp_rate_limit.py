"""Per-principal MCP tool-call rate limiter (token bucket).

Fail-open when disabled. When enabled, excess calls return retry_after_sec so
the router can emit HTTP 429. Treat MCP as a production API surface forever.

``TokenBucketStore`` is the shared primitive reused by login throttling
(audit ITEM 4) — do not re-implement a second bucket.
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


class TokenBucketStore:
    """Thread-safe in-process token buckets keyed by principal string."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[str, _Bucket] = {}

    def clear(self) -> None:
        with self._lock:
            self._buckets.clear()

    def consume(
        self,
        key: str,
        *,
        capacity: float,
        refill_per_sec: float,
        max_keys: int = 5000,
    ) -> dict[str, float | bool | str]:
        """Consume one token for ``key``.

        Returns ``{"allowed": True, ...}`` or
        ``{"allowed": False, "retry_after_sec": N}``.
        """
        principal = (key or "anonymous").strip().lower() or "anonymous"
        capacity = max(1.0, float(capacity))
        refill_per_sec = max(0.05, float(refill_per_sec))
        now = time.monotonic()

        with self._lock:
            bucket = self._buckets.get(principal)
            if bucket is None:
                bucket = _Bucket(tokens=capacity, updated_at=now)
                self._buckets[principal] = bucket
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_sec)
            bucket.updated_at = now
            if bucket.tokens < 1.0:
                need = 1.0 - bucket.tokens
                retry = need / refill_per_sec
                return {
                    "allowed": False,
                    "retry_after_sec": max(0.1, round(retry, 2)),
                    "principal": principal,
                }
            bucket.tokens -= 1.0
            if len(self._buckets) > max(100, int(max_keys)):
                oldest = sorted(self._buckets.items(), key=lambda kv: kv[1].updated_at)[:500]
                for drop_key, _ in oldest:
                    self._buckets.pop(drop_key, None)
            return {
                "allowed": True,
                "principal": principal,
                "tokens_remaining": round(bucket.tokens, 2),
            }


# MCP tool-call store (process-local).
_MCP_STORE = TokenBucketStore()


def rate_limit_enabled() -> bool:
    """Default on at a generous ceiling; set DATAFLOW_MCP_RATE_LIMIT=0 to disable."""
    raw = os.getenv("DATAFLOW_MCP_RATE_LIMIT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def check_mcp_rate_limit(principal: str) -> dict[str, float | bool | str]:
    """Consume one token for ``principal``."""
    if not rate_limit_enabled():
        return {"allowed": True, "disabled": True}

    return _MCP_STORE.consume(
        principal,
        capacity=_env_float("DATAFLOW_MCP_RATE_BURST", 30.0),
        refill_per_sec=_env_float("DATAFLOW_MCP_RATE_QPS", 5.0),
        max_keys=_env_int("DATAFLOW_MCP_RATE_MAX_KEYS", 5000),
    )


def reset_mcp_rate_limits() -> None:
    """Test helper — clear in-memory buckets."""
    _MCP_STORE.clear()


__all__ = [
    "TokenBucketStore",
    "check_mcp_rate_limit",
    "rate_limit_enabled",
    "reset_mcp_rate_limits",
]
