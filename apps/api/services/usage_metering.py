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
from typing import Any, Callable

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter

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


def _load_events(
    workspace_id: str = "", workspace_filter: WorkspaceFilter | None = None
) -> list[dict[str, Any]]:
    """Events for ``workspace_filter``, else exactly ``workspace_id`` ("" = all)."""
    if workspace_filter is None:
        workspace_filter = (
            WorkspaceFilter.only(workspace_id) if workspace_id else WorkspaceFilter.everything()
        )
    coll = _mongo()
    events: list[dict[str, Any]] = []
    if coll is not None:
        try:
            filt = workspace_filter.mongo_query()
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
        events = [e for e in events if workspace_filter.matches(e.get("workspace_id"))]
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


def summarize_usage(
    workspace_id: str = "",
    *,
    days: int | None = None,
    workspace_filter: WorkspaceFilter | None = None,
) -> dict[str, Any]:
    """Return aggregate rows/bytes for a workspace (or global).

    ``workspace_filter`` (the caller's tenant scope) decides which events count;
    ``workspace_id`` is then only the label. When ``days`` is set, also returns
    a per-day breakdown covering that window (UTC calendar days, inclusive of
    today).
    """
    events = _load_events(workspace_id, workspace_filter)
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


def assign_unscoped_workspaces(
    workspace_for: Callable[[dict[str, Any]], str], *, apply: bool
) -> dict[str, int]:
    """Stamp legacy unscoped usage events with ``workspace_for(event)`` (D154 backfill).

    Scoped events are never touched; an event that cannot be placed is skipped.
    """
    unscoped = WorkspaceFilter.only("")
    counts = {"assigned": 0, "skipped": 0}
    coll = _mongo()
    if coll is not None:
        for doc in list(coll.find(unscoped.mongo_query())):
            ws = (workspace_for(doc) or "").strip()
            counts["assigned" if ws else "skipped"] += 1
            if ws and apply:
                coll.update_one({"_id": doc["_id"]}, {"$set": {"workspace_id": ws}})
        return counts
    if not STORE_PATH.exists():
        return counts
    data = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    for event in data.get("events") or []:
        if not unscoped.matches(event.get("workspace_id")):
            continue
        ws = (workspace_for(event) or "").strip()
        counts["assigned" if ws else "skipped"] += 1
        if ws:
            event["workspace_id"] = ws
    if apply and counts["assigned"]:
        write_json_atomic(STORE_PATH, data, indent=2, default=json_default)
    return counts
