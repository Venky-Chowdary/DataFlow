"""Read transfer jobs from Mongo when it answers, else the engine job_store.

Pilot and Transfer Studio must see the same history. When Mongo is down the
engine already writes to the memory/file job_store; a status question that
only talks to Mongo then fails with "MongoDB unavailable" even though the
run the operator just started is sitting in the local store.
"""

from __future__ import annotations

from typing import Any

from services.jobs import job_store


def summarize_listed_job(doc: dict[str, Any]) -> dict[str, Any]:
    """Operator-facing row shared by ``list_jobs`` and the briefing."""
    rejected = doc.get("rejected_rows")
    if rejected in (None, 0, "0"):
        details = doc.get("rejected_details") or []
        rejected = len(details) if details else 0
    error = str(doc.get("error") or doc.get("message") or "").strip()
    return {
        "id": str(doc.get("_id") or doc.get("id") or doc.get("job_id") or ""),
        "source": (
            doc.get("source_name")
            or doc.get("source_type")
            or doc.get("source")
            or ""
        ),
        "destination": (
            doc.get("destination_collection")
            or doc.get("destination_type")
            or doc.get("destination")
            or ""
        ),
        "status": doc.get("status"),
        "records": doc.get("records_processed") or doc.get("rows_processed") or 0,
        "rejected_rows": int(rejected or 0),
        "error": error[:240] or None,
        "created_at": str(doc.get("created_at") or ""),
        "source_table": (
            doc.get("source_table")
            or doc.get("table_name")
            or doc.get("source_name")
        ),
        "dest_table": doc.get("destination_collection") or doc.get("dest_table"),
    }


def _record_as_job_doc(record: Any) -> dict[str, Any]:
    """Map an engine ``JobRecord`` onto the mongo job document Pilot already reads."""
    if record is None:
        return {}
    if hasattr(record, "__dataclass_fields__"):
        rec = {
            "job_id": getattr(record, "job_id", ""),
            "status": getattr(record, "status", ""),
            "source": getattr(record, "source", ""),
            "destination": getattr(record, "destination", ""),
            "rows_processed": getattr(record, "rows_processed", 0),
            "rejected_details": list(getattr(record, "rejected_details", []) or []),
            "message": getattr(record, "message", "") or "",
            "created_at": getattr(record, "created_at", ""),
            "operation": getattr(record, "operation", ""),
            "table_name": getattr(record, "table_name", ""),
        }
    elif isinstance(record, dict):
        rec = dict(record)
    else:
        return {}
    return {
        "_id": rec.get("job_id") or rec.get("_id") or rec.get("id"),
        "id": rec.get("job_id") or rec.get("id"),
        "status": rec.get("status"),
        "source_name": rec.get("source") or rec.get("source_name"),
        "source_type": rec.get("source_type") or rec.get("driver") or "",
        "destination_collection": rec.get("destination") or rec.get("destination_collection"),
        "destination_type": rec.get("destination_type") or "",
        "records_processed": rec.get("rows_processed") or rec.get("records_processed") or 0,
        "rejected_rows": rec.get("rejected_rows")
        or len(rec.get("rejected_details") or []),
        "rejected_details": rec.get("rejected_details") or [],
        "error": rec.get("message") or rec.get("error") or "",
        "created_at": rec.get("created_at") or "",
        "operation": rec.get("operation") or "",
        "table_name": rec.get("table_name") or "",
    }


def _from_engine_store(limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw = list(job_store.list_recent(limit=limit) or [])
    summaries = [summarize_listed_job(_record_as_job_doc(row)) for row in raw]
    by_status: dict[str, int] = {}
    try:
        stats = job_store.stats() or {}
        total = int(stats.get("total_jobs") or len(summaries))
    except Exception:
        total = len(summaries)
        stats = {}
    for job in summaries:
        key = str(job.get("status") or "unknown")
        by_status[key] = by_status.get(key, 0) + 1
    if total and not by_status:
        # stats() counts without returning the breakdown; keep what we showed.
        by_status = {"unknown": total}
    return summaries, {"total": total, "by_status": by_status}


def list_transfer_jobs(
    limit: int = 10,
    workspace_id: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    """Recent jobs plus a whole-history count.

    Mongo is preferred when it answers. A connection failure falls through to
    the engine job_store — the same store a transfer writes when Mongo is down.
    An empty successful Mongo read is left empty so a process-global leftover
    in the file store cannot invent jobs the operator's workspace does not have.
    """
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        raw = mongo.list_jobs(limit=limit, workspace_id=workspace_id)
        counts = mongo.count_jobs(workspace_id=workspace_id)
        summaries = [summarize_listed_job(j) for j in (raw or []) if isinstance(j, dict)]
        return summaries, counts or {"total": 0, "by_status": {}}, "mongo"
    except Exception:
        summaries, counts = _from_engine_store(limit)
        return summaries, counts, "job_store"


def read_transfer_job(job_id: str) -> dict[str, Any] | None:
    """One job from Mongo, or the engine store when Mongo misses or is down."""
    needle = (job_id or "").strip()
    if not needle:
        return None
    try:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(needle)
        if job:
            return job
    except Exception:
        job = None
    try:
        record = job_store.get(needle)
    except Exception:
        record = None
    if record is None:
        return None
    return _record_as_job_doc(record)
