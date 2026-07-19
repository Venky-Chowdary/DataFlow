"""Durable quarantine dead-letter queue (append-only JSONL).

Jobs already persist ``rejected_details`` on the job document. This module
adds a workspace-scoped, replay-auditable DLQ so remediations survive job GC
and multi-instance deploys share the same remediation trail.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

DLQ_PATH = data_dir() / "quarantine_dlq.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_dlq_event(
    *,
    job_id: str,
    action: str,
    rows: int = 0,
    child_job_id: str = "",
    workspace_id: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
    path: Path = DLQ_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=json_default) + "\n")
    return event


def list_dlq_events(*, job_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
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
        if len(events) >= max(1, min(limit, 500)):
            break
    return events
