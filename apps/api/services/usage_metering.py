"""Usage metering — row/byte accounting for future billing.

Records per-job usage into Mongo (or a local JSON file in single-instance mode).
Does not charge cards; it only accumulates trustworthy counters once job
accounting is lease-fenced.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
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


def summarize_usage(workspace_id: str = "") -> dict[str, Any]:
    """Return aggregate rows/bytes for a workspace (or global)."""
    coll = _mongo()
    events: list[dict[str, Any]] = []
    if coll is not None:
        try:
            filt = {"workspace_id": workspace_id} if workspace_id else {}
            events = list(coll.find(filt))
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
    return {
        "workspace_id": workspace_id,
        "event_count": len(events),
        "rows_written": sum(int(e.get("rows_written") or 0) for e in events),
        "bytes_processed": sum(int(e.get("bytes_processed") or 0) for e in events),
    }
