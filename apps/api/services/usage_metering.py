"""Usage metering — row/byte accounting for future billing.

Records per-job usage into Mongo (or a local JSON file in single-instance mode).
Does not charge cards; it only accumulates trustworthy counters once job
accounting is lease-fenced.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import json_default

_logger = logging.getLogger(__name__)
STORE_PATH = data_dir() / "usage_metering.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mongo():
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if getattr(mongo, "client", None):
            return mongo.get_database()["usage_events"]
    except Exception:
        return None
    return None


def record_transfer_usage(
    *,
    job_id: str,
    workspace_id: str = "",
    rows_written: int = 0,
    bytes_processed: int = 0,
    source_type: str = "",
    dest_type: str = "",
    metadata: dict[str, Any] | None = None,
) -> str:
    """Append one usage event. Returns event id."""
    event_id = str(uuid.uuid4())
    event = {
        "id": event_id,
        "job_id": job_id,
        "workspace_id": workspace_id or "",
        "rows_written": int(rows_written or 0),
        "bytes_processed": int(bytes_processed or 0),
        "source_type": source_type,
        "dest_type": dest_type,
        "metadata": metadata or {},
        "created_at": _now(),
    }
    coll = _mongo()
    if coll is not None:
        try:
            coll.insert_one({**event, "_id": event_id})
            return event_id
        except Exception:
            _logger.exception("Mongo usage insert failed; falling back to file")

    data = {"events": []}
    if STORE_PATH.exists():
        try:
            data = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        except Exception:
            data = {"events": []}
    events = list(data.get("events") or [])
    events.append(event)
    data["events"] = events[-5000:]
    write_json_atomic(STORE_PATH, data, indent=2, default=json_default)
    return event_id


def _load_events(workspace_id: str = "") -> list[dict[str, Any]]:
    coll = _mongo()
    events: list[dict[str, Any]] = []
    if coll is not None:
        try:
            filt = {"workspace_id": workspace_id} if workspace_id else {}
            events = list(coll.find(filt))
            for e in events:
                e.pop("_id", None)
        except Exception:
            events = []
    else:
        if STORE_PATH.exists():
            try:
                events = list(json.loads(STORE_PATH.read_text(encoding="utf-8")).get("events") or [])
            except Exception:
                events = []
        if workspace_id:
            events = [e for e in events if e.get("workspace_id") == workspace_id]
    return events


def _event_day(created_at: Any) -> str | None:
    if created_at is None:
        return None
    if hasattr(created_at, "isoformat"):
        created_at = created_at.isoformat()
    text = str(created_at)
    if not text:
        return None
    # ISO date prefix YYYY-MM-DD
    return text[:10] if len(text) >= 10 else None


def summarize_usage(workspace_id: str = "", *, days: int | None = None) -> dict[str, Any]:
    """Return aggregate rows/bytes for a workspace (or global).

    When ``days`` is set, also returns a per-day breakdown covering that window
    (UTC calendar days, inclusive of today).
    """
    events = _load_events(workspace_id)
    window_start: datetime | None = None
    if days is not None:
        days_n = max(1, min(int(days), 366))
        window_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=days_n - 1)
        filtered: list[dict[str, Any]] = []
        for e in events:
            day = _event_day(e.get("created_at"))
            if not day:
                continue
            try:
                event_dt = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if event_dt >= window_start:
                filtered.append(e)
        events = filtered
    else:
        days_n = None

    totals = {
        "workspace_id": workspace_id,
        "event_count": len(events),
        "rows_written": sum(int(e.get("rows_written") or 0) for e in events),
        "bytes_processed": sum(int(e.get("bytes_processed") or 0) for e in events),
    }

    if days_n is None or window_start is None:
        return totals

    by_day_map: dict[str, dict[str, int]] = defaultdict(
        lambda: {"rows_written": 0, "bytes_processed": 0, "event_count": 0}
    )
    for e in events:
        day = _event_day(e.get("created_at"))
        if not day:
            continue
        bucket = by_day_map[day]
        bucket["rows_written"] += int(e.get("rows_written") or 0)
        bucket["bytes_processed"] += int(e.get("bytes_processed") or 0)
        bucket["event_count"] += 1

    # Emit every calendar day in the window so charts have contiguous series.
    daily: list[dict[str, Any]] = []
    cursor = window_start
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= today:
        key = cursor.date().isoformat()
        stats = by_day_map.get(key) or {"rows_written": 0, "bytes_processed": 0, "event_count": 0}
        daily.append({"date": key, **stats})
        cursor += timedelta(days=1)

    return {
        **totals,
        "days": days_n,
        "daily": daily,
        "totals": {
            "rows_written": totals["rows_written"],
            "bytes_processed": totals["bytes_processed"],
            "event_count": totals["event_count"],
        },
    }
