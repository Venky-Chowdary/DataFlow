"""Resend free-tier quota guard (handover addendum).

Shared day/month send counters so every API instance enforces the same
provider cap:

- ``DATAWRAP_REDIS_URL`` set  → Redis counters (shared across instances).
  Redis errors fail *closed*: the send is refused with
  ``email_quota_exceeded`` rather than silently overrunning the provider
  quota — the caller's OTP challenge stays resendable.
- MongoDB configured          → ``email_quota`` collection counters — the
  same durable store as users/challenges, shared across replicas and
  surviving redeploys. Mongo errors fail closed, same as Redis.
- otherwise                   → ``email_quota.json`` under ``data_dir()`` —
  durable per instance (survives restart when the volume is mounted),
  like every other file-backed store on this tier.

Limits: ``DATAWRAP_EMAIL_DAILY_LIMIT`` (100), ``DATAWRAP_EMAIL_MONTHLY_LIMIT``
(3000). One ops event per send carries the counts; crossing 80% of either
window emits a single ``email.quota.alert`` per window.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.email_sender import EmailQuotaExceeded
from services.metadata_backend import (
    json_doc_transaction, load_json_doc, mongo_database)
from services.ops_events import emit_event
from services.platform_config import data_dir

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
_BACKEND_FAILED = False  # set when ANY durable backend (redis/mongo/file) errors
_REDIS_LOCK = threading.Lock()

_COLLECTION = "email_quota"
_MONGO_INDEXES_READY = False


def _file_path() -> Path:
    return data_dir() / "email_quota.json"


def _db():
    """The same durable store as users/challenges, when configured. The
    TTL index on resets_at is created once per process; a setup failure
    raises — falling back to the file tier would split quota state
    across replicas."""
    global _MONGO_INDEXES_READY
    db = mongo_database()
    if db is None:
        return None
    if not _MONGO_INDEXES_READY:
        try:
            db[_COLLECTION].create_index("resets_at", expireAfterSeconds=0)
        except Exception:
            logger.error("email_quota: Mongo index setup failed; refusing "
                         "the file fallback (quota state would split "
                         "across replicas)", exc_info=True)
            raise
        _MONGO_INDEXES_READY = True
    return db


def _resets_live(value) -> bool:
    """resets_at is a BSON datetime on new docs (TTL index self-deletes
    expired counters) and an ISO string on docs written before the fix —
    honor both."""
    now = datetime.now(UTC)
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)) > now
    return str(value or "") > now.isoformat()


def _backend() -> str:
    if _redis_url():
        return "redis"
    if _db() is not None:
        return "mongo"
    return "file"


def _redis_url() -> str:
    return (getenv_brand("REDIS_URL") or "").strip()


def _get_redis():
    """Lazy client; raises on connect failure (caller fails closed)."""
    global _REDIS_CLIENT, _BACKEND_FAILED
    with _REDIS_LOCK:
        if _REDIS_CLIENT is not None:
            return _REDIS_CLIENT
        import redis

        client = redis.from_url(_redis_url(), socket_timeout=2, socket_connect_timeout=2)
        client.ping()
        _REDIS_CLIENT = client
        _BACKEND_FAILED = False
        return client


def _reset_backends() -> None:
    """Test hook — clears counters on every backend (incl. the file)."""
    global _REDIS_CLIENT, _BACKEND_FAILED
    with _REDIS_LOCK:
        _REDIS_CLIENT = None
        _BACKEND_FAILED = False
    try:
        _file_path().unlink(missing_ok=True)
    except OSError:
        pass


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


def _incr_mongo(key: str, reset_at: datetime) -> int:
    """Atomic $inc upsert — safe across replicas."""
    from pymongo import ReturnDocument

    db = _db()
    assert db is not None
    doc = db[_COLLECTION].find_one_and_update(
        {"_id": key},
        {"$inc": {"count": 1},
         "$setOnInsert": {"resets_at": reset_at}},
        upsert=True, return_document=ReturnDocument.AFTER)
    return int(doc["count"])


def _incr_file(key: str, reset_at: datetime) -> int:
    """Durable per-instance counter — survives restart on a mounted volume;
    two instances sharing the backend see each other's counts."""
    with json_doc_transaction(_file_path(), {"counters": {}}) as doc:
        counters = doc.setdefault("counters", {})
        now = datetime.now(UTC).isoformat()
        for k in [k for k, v in counters.items()
                  if str(v.get("expires", "")) < now]:
            del counters[k]
        entry = counters.setdefault(
            key, {"count": 0, "expires": reset_at.isoformat()})
        entry["count"] += 1
        return int(entry["count"])


def _get_file(key: str) -> int:
    doc = load_json_doc(_file_path(), {"counters": {}})
    entry = doc.get("counters", {}).get(key) or {}
    if str(entry.get("expires", "")) < datetime.now(UTC).isoformat():
        return 0
    return int(entry.get("count", 0))


def _alert_once(kind: str, window_key: str, used: int, limit: int) -> None:
    """Emit the 80% alert once per window."""
    alert_key = f"{_KEY_PREFIX}:alert:{kind}:{window_key.rsplit(':', 1)[-1]}"
    fired = False
    backend = _backend()
    if backend == "redis":
        try:
            client = _get_redis()
            fired = bool(client.set(alert_key, "1", nx=True, ex=31 * 24 * 3600))
        except Exception:  # noqa: BLE001 — alerting must never block sends
            fired = False
    elif backend == "mongo":
        try:
            from pymongo.errors import DuplicateKeyError

            _db()[_COLLECTION].insert_one(
                {"_id": alert_key,
                 "resets_at": datetime.now(UTC) + timedelta(days=31)})
            fired = True
        except DuplicateKeyError:
            fired = False
        except Exception:  # noqa: BLE001 — alerting must never block sends
            fired = False
    else:
        with json_doc_transaction(_file_path(),
                                  {"counters": {}, "alerts": {}}) as doc:
            alerts = doc.setdefault("alerts", {})
            if alert_key not in alerts:
                alerts[alert_key] = datetime.now(UTC).isoformat()
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

    backend = _backend()
    try:
        if backend == "redis":
            day_used = _incr_redis(day_key, day_reset)
            month_used = _incr_redis(month_key, month_reset)
        elif backend == "mongo":
            day_used = _incr_mongo(day_key, day_reset)
            month_used = _incr_mongo(month_key, month_reset)
        else:
            day_used = _incr_file(day_key, day_reset)
            month_used = _incr_file(month_key, month_reset)
    except EmailQuotaExceeded:
        raise
    except Exception as exc:  # noqa: BLE001 — fail closed, sender retries later
        _BACKEND_FAILED = True
        logger.warning("email quota backend unavailable: %s", type(exc).__name__)
        raise EmailQuotaExceeded(
            "email quota backend unavailable; send refused", retry_after_sec=60
        ) from exc

    emit_event(
        logger,
        "email.send",
        outcome="counted",
        day_used=day_used,
        day_limit=daily_limit,
        month_used=month_used,
        month_limit=monthly_limit,
        backend=backend,
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
    backend = _backend()
    try:
        if backend == "redis":
            client = _get_redis()
            day_used = int(client.get(day_key) or 0)
            month_used = int(client.get(month_key) or 0)
        elif backend == "mongo":
            for key, which in ((day_key, "day"), (month_key, "month")):
                doc = _db()[_COLLECTION].find_one({"_id": key}) or {}
                if _resets_live(doc.get("resets_at")):
                    if which == "day":
                        day_used = int(doc.get("count", 0))
                    else:
                        month_used = int(doc.get("count", 0))
        else:
            day_used = _get_file(day_key)
            month_used = _get_file(month_key)
    except Exception:  # noqa: BLE001
        backend = f"{backend}-unreachable"
    return {
        "backend": backend,
        "day_used": day_used,
        "day_limit": _daily_limit(),
        "day_resets_at": day_reset.isoformat(),
        "month_used": month_used,
        "month_limit": _monthly_limit(),
        "month_resets_at": month_reset.isoformat(),
    }
