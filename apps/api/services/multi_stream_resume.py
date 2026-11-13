"""Per-table progress for a resumed multi-table load (backlog C5).

The job checkpoint is one position, so a sequential multi-table job resumed
after a cancel or a lost worker re-read every table from the start except the
one the checkpoint named: tables that had finished were read again (an append
re-wrote them; an overwrite dropped and reloaded them).

Each table that finishes is now recorded on the job document
(``stream_progress``: table → rows, sync mode, finish time). A run of the same
job skips a recorded table and reads only the unfinished ones; the table the
job checkpoint names resumes from its own offset as before. The record is
cleared when the job's tables are undone (C4), because the rows it vouches for
are gone.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

PROGRESS_FIELD = "stream_progress"


def _store() -> Any:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _key(name: str) -> str:
    return str(name or "").strip().lower()


def completed_streams(job_id: str) -> dict[str, dict[str, Any]]:
    """Tables an earlier pass of this job finished, keyed by folded name."""
    if not job_id:
        return {}
    try:
        job = _store().get_job(job_id) or {}
    except Exception:  # noqa: BLE001 — unknown progress: every table is read
        logger.warning("stream progress unreadable for job %s", job_id, exc_info=True)
        return {}
    raw = job.get(PROGRESS_FIELD)
    if not isinstance(raw, dict):
        return {}
    return {
        _key(name): dict(entry)
        for name, entry in raw.items()
        if isinstance(entry, dict) and entry.get("status") == "completed"
    }


def _write(job_id: str, progress: dict[str, Any]) -> None:
    store = _store()
    if not store.update_job_fields(job_id, {PROGRESS_FIELD: progress}, touch_updated_at=False):
        raise RuntimeError(f"stream progress for job {job_id} was not recorded")


def record_stream_completed(
    job_id: str, stream_name: str, *, rows: int, sync_mode: str, table: str = ""
) -> None:
    """Record that ``stream_name`` finished in this pass (never raises).

    A record that cannot be written only costs a re-read on resume (the
    at-least-once behaviour before C5), so it is logged, not raised.
    """
    if not job_id or not stream_name:
        return
    try:
        job = _store().get_job(job_id) or {}
        progress = dict(job.get(PROGRESS_FIELD) or {})
        progress[_key(stream_name)] = {
            "status": "completed",
            "stream": stream_name,
            "table": table or stream_name,
            "rows": int(rows or 0),
            "sync_mode": str(sync_mode or ""),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        _write(job_id, progress)
    except Exception:  # noqa: BLE001 — see docstring
        logger.warning("stream progress not recorded for %s (job %s)", stream_name, job_id, exc_info=True)


def forget_streams(job_id: str, tables: list[str]) -> None:
    """Drop the records of tables whose rows were undone (never raises).

    Tables an earlier pass finished and this pass did not touch keep their
    record: the undo leaves them alone (D134), so a later resume still skips them.
    """
    if not job_id or not tables:
        return
    gone = {_key(t) for t in tables}
    try:
        job = _store().get_job(job_id) or {}
        progress = {
            name: entry
            for name, entry in dict(job.get(PROGRESS_FIELD) or {}).items()
            if _key(name) not in gone and _key((entry or {}).get("table") or "") not in gone
        }
        _write(job_id, progress)
    except Exception:  # noqa: BLE001 — a stale record is cleared on the next undo
        logger.warning("stream progress not cleared for job %s", job_id, exc_info=True)


def forget_all_progress(job_id: str) -> None:
    """A fresh (not resumed) pass starts with no record (never raises)."""
    if not job_id:
        return
    try:
        job = _store().get_job(job_id) or {}
        if job.get(PROGRESS_FIELD):
            _write(job_id, {})
    except Exception:  # noqa: BLE001 — a stale record is only read on a resume
        logger.warning("stream progress not reset for job %s", job_id, exc_info=True)


def skip_reason(entry: dict[str, Any]) -> str:
    rows = int(entry.get("rows") or 0)
    return (
        f"finished in an earlier pass of this job ({rows:,} row(s)); not read again "
        "on resume"
    )
