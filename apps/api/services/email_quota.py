"""Resend free-tier quota guard (handover addendum).

Shared day/month send counters so every API instance enforces the same
provider cap:

- ``DATAWRAP_REDIS_URL`` set  → Redis counters (shared across instances).
  Redis errors fail *closed*: the send is refused with
  ``email_quota_exceeded`` rather than silently overrunning the provider
  quota — the caller's OTP challenge stays resendable.
- unset                       → per-process counters (dev/test only).

Limits: ``DATAWRAP_EMAIL_DAILY_LIMIT`` (100), ``DATAWRAP_EMAIL_MONTHLY_LIMIT``
(3000). One ops event per send carries the counts; crossing 80% of either
window emits a single ``email.quota.alert`` per window.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

from services.brand_env import getenv_brand
from services.email_sender import EmailQuotaExceeded
from services.ops_events import emit_event

logger = logging.getLogger(__name__)

_KEY_PREFIX = "dw:emailq"
_ALERT_FRACTION = 0.8
_KEY_GRACE_SEC = 2 * 24 * 3600


def _daily_limit() -> int:
    try:
        return max(1, int(getenv_brand("EMAIL_DAILY_LIMIT", "100") or "100"))
    except ValueError:
        return 100


def _monthly_limit() -> int:
    try:
        return max(1, int(getenv_brand("EMAIL_MONTHLY_LIMIT", "3000") or "3000"))
    except ValueError:
        return 3000


def _windows(now: datetime | None = None) -> tuple[tuple[str, datetime], tuple[str, datetime]]:
    """Return ((day_key, day_reset_at), (month_key, month_reset_at)) in UTC."""
    now = now or datetime.now(UTC)
    day_reset = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        month_reset = now.replace(year=now.year + 1, month=1, day=1,
                                  hour=0, minute=0, second=0, microsecond=0)
    else:
        month_reset = now.replace(month=now.month + 1, day=1,
                                  hour=0, minute=0, second=0, microsecond=0)
    return (
        (f"{_KEY_PREFIX}:day:{now:%Y%m%d}", day_reset),
        (f"{_KEY_PREFIX}:month:{now:%Y%m}", month_reset),
    )


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

_REDIS_CLIENT: Any = None
_REDIS_FAILED = False
_REDIS_LOCK = threading.Lock()

_MEM_COUNTERS: dict[str, int] = {}
_MEM_ALERTED: set[str] = set()
_MEM_LOCK = threading.Lock()


def _redis_url() -> str:
    return (getenv_brand("REDIS_URL") or "").strip()


def _get_redis():
    """Lazy client; raises on connect failure (caller fails closed)."""
    global _REDIS_CLIENT, _REDIS_FAILED
    with _REDIS_LOCK:
        if _REDIS_CLIENT is not None:
            return _REDIS_CLIENT
        import redis

        client = redis.from_url(_redis_url(), socket_timeout=2, socket_connect_timeout=2)
        client.ping()
        _REDIS_CLIENT = client
        _REDIS_FAILED = False
        return client


def _reset_backends() -> None:
    """Test hook."""
    global _REDIS_CLIENT, _REDIS_FAILED
    with _REDIS_LOCK:
        _REDIS_CLIENT = None
        _REDIS_FAILED = False
    with _MEM_LOCK:
        _MEM_COUNTERS.clear()
        _MEM_ALERTED.clear()


# ---------------------------------------------------------------------------
# Counter ops
# ---------------------------------------------------------------------------

def _incr_redis(key: str, reset_at: datetime) -> int:
    client = _get_redis()
    pipe = client.pipeline()
    pipe.incr(key)
    pipe.expire(key, int(reset_at.timestamp() - datetime.now(UTC).timestamp()) + _KEY_GRACE_SEC)
    count, _ = pipe.execute()
    return int(count)


def _incr_memory(key: str) -> int:
    with _MEM_LOCK:
        _MEM_COUNTERS[key] = _MEM_COUNTERS.get(key, 0) + 1
        return _MEM_COUNTERS[key]


def _alert_once(kind: str, window_key: str, used: int, limit: int) -> None:
    """Emit the 80% alert once per window."""
    alert_key = f"{_KEY_PREFIX}:alert:{kind}:{window_key.rsplit(':', 1)[-1]}"
    fired = False
    if _redis_url():
        try:
            client = _get_redis()
            fired = bool(client.set(alert_key, "1", nx=True, ex=31 * 24 * 3600))
        except Exception:  # noqa: BLE001 — alerting must never block sends
            fired = False
    else:
        with _MEM_LOCK:
            if alert_key not in _MEM_ALERTED:
                _MEM_ALERTED.add(alert_key)
                fired = True
    if fired:
        emit_event(
            logger,
            "email.quota.alert",
            outcome="threshold",
            level=logging.WARNING,
            kind=kind,
            used=used,
            limit=limit,
            pct=round(100.0 * used / limit, 1),
        )


def consume_send_slot() -> dict[str, int]:
    """Count one send against the day/month windows; raise when over quota.

    Returns the used counts (for events/tests). Raises EmailQuotaExceeded
    with a retry_after when a window limit is reached, and also when the
    configured Redis backend is unreachable (fail closed).
    """
    (day_key, day_reset), (month_key, month_reset) = _windows()
    daily_limit = _daily_limit()
    monthly_limit = _monthly_limit()
    now = datetime.now(UTC)

    if _redis_url():
        try:
            day_used = _incr_redis(day_key, day_reset)
            month_used = _incr_redis(month_key, month_reset)
        except Exception as exc:  # noqa: BLE001 — fail closed, sender retries later
            _REDIS_FAILED = True
            logger.warning("email quota backend unavailable: %s", type(exc).__name__)
            raise EmailQuotaExceeded(
                "email quota backend unavailable; send refused", retry_after_sec=60
            ) from exc
    else:
        day_used = _incr_memory(day_key)
        month_used = _incr_memory(month_key)

    emit_event(
        logger,
        "email.send",
        outcome="counted",
        day_used=day_used,
        day_limit=daily_limit,
        month_used=month_used,
        month_limit=monthly_limit,
        backend="redis" if _redis_url() else "memory",
    )

    if day_used >= _ALERT_FRACTION * daily_limit:
        _alert_once("day", day_key, day_used, daily_limit)
    if month_used >= _ALERT_FRACTION * monthly_limit:
        _alert_once("month", month_key, month_used, monthly_limit)

    if day_used > daily_limit:
        raise EmailQuotaExceeded(
            "daily email quota reached",
            retry_after_sec=max(1, int((day_reset - now).total_seconds())),
        )
    if month_used > monthly_limit:
        raise EmailQuotaExceeded(
            "monthly email quota reached",
            retry_after_sec=max(1, int((month_reset - now).total_seconds())),
        )
    return {"day_used": day_used, "month_used": month_used}


def quota_status() -> dict[str, Any]:
    """Read-only view for metrics/admin surfaces."""
    (day_key, day_reset), (month_key, month_reset) = _windows()
    day_used = month_used = 0
    backend = "memory"
    if _redis_url():
        backend = "redis"
        try:
            client = _get_redis()
            day_used = int(client.get(day_key) or 0)
            month_used = int(client.get(month_key) or 0)
        except Exception:  # noqa: BLE001
            backend = "redis-unreachable"
    else:
        with _MEM_LOCK:
            day_used = _MEM_COUNTERS.get(day_key, 0)
            month_used = _MEM_COUNTERS.get(month_key, 0)
    return {
        "backend": backend,
        "day_used": day_used,
        "day_limit": _daily_limit(),
        "day_resets_at": day_reset.isoformat(),
        "month_used": month_used,
        "month_limit": _monthly_limit(),
        "month_resets_at": month_reset.isoformat(),
    }
