"""Durable quarantine dead-letter queue — Mongo primary, JSONL fallback.

Jobs already persist ``rejected_details`` on the job document. This module
adds a workspace-scoped, replay-auditable DLQ so remediations survive job GC
and multi-instance deploys share the same remediation trail.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

DLQ_PATH = data_dir() / "quarantine_dlq.jsonl"
_MONGO_COLL = "quarantine_dlq"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dlq_coll():
    from services.control_plane_store import mongo_collection

    return mongo_collection(_MONGO_COLL)


def append_dlq_event(
    *,
    job_id: str,
    action: str,
    rows: int = 0,
    child_job_id: str = "",
    workspace_id: str = "",
    details: dict[str, Any] | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Append a DLQ event. Prefer Mongo; fall back to JSONL. Never silently drops."""
    event = {
        "id": str(uuid.uuid4()),
        "ts": _now(),
        "job_id": job_id,
        "child_job_id": child_job_id or None,
        "action": action,
        "rows": int(rows or 0),
        "workspace_id": workspace_id or "",
        "details": details or {},
    }
    coll = _dlq_coll()
    if coll is not None and path is None:
        try:
            doc = dict(event)
            doc["_id"] = event["id"]
            coll.insert_one(doc)
            return event
        except Exception as exc:
            logger.warning("DLQ Mongo append failed, falling back to JSONL: %s", exc)

    target: Path = path or DLQ_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, default=json_default) + "\n"
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            with target.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
            return event
        except OSError as exc:
            last_exc = exc
            logger.warning("DLQ append failed (attempt %s): %s", attempt + 1, exc)
            time.sleep(0.05 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def persist_rejected_rows(
    *,
    job_id: str,
    rejected_details: list[dict[str, Any]] | None,
    workspace_id: str = "",
    source: str = "transfer",
) -> dict[str, Any] | None:
    """Persist rejected/quarantined rows to the DLQ. Returns event or None if empty."""
    rows = list(rejected_details or [])
    if not rows:
        return None
    return append_dlq_event(
        job_id=job_id,
        action="quarantine",
        rows=len(rows),
        workspace_id=workspace_id,
        details={"source": source, "rejected_details": rows[:500]},
    )


def list_dlq_events(*, job_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    coll = _dlq_coll()
    if coll is not None:
        try:
            query: dict[str, Any] = {}
            if job_id:
                query["job_id"] = job_id
            docs = list(coll.find(query).sort("ts", -1).limit(max(1, int(limit))))
            out: list[dict[str, Any]] = []
            for d in docs:
                row = dict(d)
                row.pop("_id", None)
                out.append(row)
            return out
        except Exception:
            logger.debug("DLQ Mongo list failed", exc_info=True)

    path: Path = DLQ_PATH
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        if job_id and ev.get("job_id") != job_id:
            continue
        events.append(ev)
        if len(events) >= limit:
            break
    return events
