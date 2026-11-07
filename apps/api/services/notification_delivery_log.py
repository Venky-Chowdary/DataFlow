"""Append-only log of every notification delivery attempt (QA SC9).

``log_job_notifications`` wrote the per-channel results onto the job document
— so a run refused before a job existed (``job_id == ""``), a test-send, and
every deployment on the memory/file job store left no trace, and nobody could
answer "did the failure alert for last night's run reach the receiver?".

Every ``send_to_channel`` call now appends one record here: Mongo
``notification_deliveries`` when Mongo is the control plane, a JSONL file
otherwise (the same split as the quarantine DLQ). Records never carry the
channel URL, headers, signing secret or payload; the error text is redacted.
Mongo rows expire through a TTL index after the retention window, and the
JSONL file is rotated at a size cap, so the log cannot grow without bound.
"""

from __future__ import annotations

import contextvars
import json
import logging
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

COLLECTION = "notification_deliveries"
LOG_PATH = data_dir() / "notification_deliveries.jsonl"
_JSONL_MAX_BYTES = 20 * 1024 * 1024
_DEFAULT_RETENTION_DAYS = 30
_MAX_LIMIT = 500
_ERROR_CHARS = 300

_URL_RE = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"'`]+", re.IGNORECASE)
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_INDEX_LOCK = threading.Lock()
_INDEXED: set[str] = set()
_FILE_LOCK = threading.Lock()
#: What the deliveries sent inside a ``delivery_context`` block are about.
_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "notification_delivery_context", default=None
)


@contextmanager
def delivery_context(**fields: Any) -> Iterator[None]:
    """Tag every delivery sent in this block (``schedule_id``, ``job_id``, ``event``).

    A context, not a parameter: the broadcast helpers keep their signatures,
    and a caller several frames up (the scheduler) can name the schedule.
    """
    merged = {**(_CONTEXT.get() or {}), **{k: v for k, v in fields.items() if v not in (None, "")}}
    token = _CONTEXT.set(merged)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_CONTEXT.get() or {})


def retention_days() -> int:
    raw = getenv_brand("NOTIFICATION_DELIVERY_RETENTION_DAYS", str(_DEFAULT_RETENTION_DAYS))
    try:
        days = int(str(raw).strip())
    except ValueError:
        return _DEFAULT_RETENTION_DAYS
    return days if days > 0 else _DEFAULT_RETENTION_DAYS


def redact_error(text: Any) -> str:
    """Error text safe to keep: no URL (webhook paths carry secrets), no address."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = _EMAIL_RE.sub("<redacted-email>", _URL_RE.sub("<url>", raw))
    return cleaned[:_ERROR_CHARS]


def _collection() -> Any | None:
    from services.control_plane_store import mongo_collection

    coll = mongo_collection(COLLECTION)
    if coll is None:
        return None
    key = str(getattr(coll, "full_name", "") or id(coll))
    if key not in _INDEXED:
        with _INDEX_LOCK:
            if key not in _INDEXED:
                try:
                    coll.create_index("expires_at", expireAfterSeconds=0, name="delivery_ttl")
                    coll.create_index(
                        [("channel_id", 1), ("attempted_at", -1)], name="delivery_by_channel"
                    )
                    coll.create_index(
                        [("schedule_id", 1), ("attempted_at", -1)], name="delivery_by_schedule"
                    )
                except Exception:  # noqa: BLE001 - an index is an optimisation
                    logger.debug("notification delivery indexes not created", exc_info=True)
                _INDEXED.add(key)
    return coll


def _event_of(payload: dict[str, Any], context: dict[str, Any]) -> str:
    explicit = str(context.get("event") or "").strip()
    if explicit:
        return explicit[:64]
    status = str((payload or {}).get("status") or "").strip()
    return f"job.{status}"[:64] if status else "message"


def build_record(
    *,
    channel: Any,
    payload: dict[str, Any] | None,
    result: dict[str, Any] | None,
    duration_ms: float,
    context: dict[str, Any] | None = None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    ctx = {**current_context(), **(context or {})}
    body = payload if isinstance(payload, dict) else {}
    res = result if isinstance(result, dict) else {}
    now = datetime.now(timezone.utc)
    status = res.get("status")
    if error is not None:
        message = f"{type(error).__name__}: {error}"
    else:
        message = res.get("error") or ""
    return {
        "id": str(uuid.uuid4()),
        "channel_id": str(getattr(channel, "id", "") or ""),
        "kind": str(getattr(channel, "kind", "") or ""),
        "workspace_id": str(getattr(channel, "workspace_id", "") or ""),
        "event": _event_of(body, ctx),
        "schedule_id": str(ctx.get("schedule_id") or ""),
        "job_id": str(ctx.get("job_id") or body.get("job_id") or ""),
        "attempted_at": now.isoformat(),
        "http_status": int(status) if isinstance(status, int) else None,
        "ok": bool(res.get("ok")) and error is None,
        "error": redact_error(message),
        "duration_ms": round(float(duration_ms), 1),
        "signed": bool(res.get("signed")),
        "signature_id": str(res.get("signature_timestamp") or ""),
        "expires_at": (now + timedelta(days=retention_days())).isoformat(),
    }


def _append_jsonl(record: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FILE_LOCK:
        try:
            if path.exists() and path.stat().st_size >= _JSONL_MAX_BYTES:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                path.rename(path.with_name(f"{path.stem}.{stamp}{path.suffix}"))
        except OSError:
            logger.debug("notification delivery log rotation skipped", exc_info=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=json_default) + "\n")


def record_delivery(record: dict[str, Any]) -> None:
    """Append one attempt. Never raises: logging must not fail a delivery."""
    try:
        coll = _collection()
        if coll is not None:
            doc = dict(record)
            doc["_id"] = record["id"]
            # A BSON date, so the TTL index can expire it.
            doc["expires_at"] = datetime.fromisoformat(record["expires_at"])
            coll.insert_one(doc)
            return
    except Exception:  # noqa: BLE001
        logger.warning("notification delivery: Mongo append failed, using the file log", exc_info=True)
    try:
        _append_jsonl(record, LOG_PATH)
    except Exception:  # noqa: BLE001
        logger.exception("notification delivery could not be recorded")


# ── One alert per failure ──────────────────────────────────────────────────
CLAIMS_COLLECTION = "notification_alert_claims"
_CLAIM_DAYS = 7
_LOCAL_CLAIMS: dict[str, float] = {}
_LOCAL_CLAIMS_CAP = 20000
_CLAIM_LOCK = threading.Lock()


def claim_alert(key: str, channel_id: str) -> bool:
    """First claimant of (failure, channel) wins; every later sender stands down.

    Keyed on the job (or, before any job exists, the schedule run) plus the
    event bucket, so the engine's job alert, the exception-level alert and the
    scheduler's run alert can no longer each page the same channel for one
    failure. Mongo makes the claim cross-process; without it the claim is
    per-process. A claim store error answers True: an extra alert beats a
    lost one.
    """
    claim_id = f"{key}|{channel_id}"
    coll = None
    try:
        from services.control_plane_store import mongo_collection

        coll = mongo_collection(CLAIMS_COLLECTION)
    except Exception:  # noqa: BLE001
        coll = None
    if coll is not None:
        try:
            from pymongo.errors import DuplicateKeyError

            if str(getattr(coll, "full_name", "")) + "#claims" not in _INDEXED:
                try:
                    coll.create_index("expires_at", expireAfterSeconds=0, name="claim_ttl")
                except Exception:  # noqa: BLE001
                    pass
                _INDEXED.add(str(getattr(coll, "full_name", "")) + "#claims")
            now = datetime.now(timezone.utc)
            try:
                coll.insert_one({"_id": claim_id, "at": now,
                                 "expires_at": now + timedelta(days=_CLAIM_DAYS)})
                return True
            except DuplicateKeyError:
                return False
        except Exception:  # noqa: BLE001
            logger.warning("alert claim store unavailable; sending", exc_info=True)
            return True
    import time as _time

    with _CLAIM_LOCK:
        if claim_id in _LOCAL_CLAIMS:
            return False
        if len(_LOCAL_CLAIMS) >= _LOCAL_CLAIMS_CAP:
            for old in sorted(_LOCAL_CLAIMS, key=_LOCAL_CLAIMS.get)[: _LOCAL_CLAIMS_CAP // 10]:
                _LOCAL_CLAIMS.pop(old, None)
        _LOCAL_CLAIMS[claim_id] = _time.time()
    return True


def _public(row: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in row.items() if k != "_id"}
    expires = out.get("expires_at")
    if isinstance(expires, datetime):
        out["expires_at"] = expires.isoformat()
    return out


def list_deliveries(
    *,
    workspace_id: str,
    channel_id: str = "",
    schedule_id: str = "",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Newest first. Always scoped to one workspace ("" = unscoped channels)."""
    cap = max(1, min(int(limit or 50), _MAX_LIMIT))
    query: dict[str, Any] = {"workspace_id": workspace_id or ""}
    if channel_id:
        query["channel_id"] = channel_id
    if schedule_id:
        query["schedule_id"] = schedule_id
    coll = _collection()
    if coll is not None:
        try:
            return [_public(dict(d)) for d in coll.find(query).sort("attempted_at", -1).limit(cap)]
        except Exception:  # noqa: BLE001
            logger.warning("notification delivery: Mongo read failed, using the file log", exc_info=True)
    path = LOG_PATH
    if not path.exists():
        return []
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in reversed(lines):
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict) or str(row.get("expires_at") or "") < now:
            continue
        if any(str(row.get(k) or "") != v for k, v in query.items()):
            continue
        rows.append(row)
        if len(rows) >= cap:
            break
    return rows
