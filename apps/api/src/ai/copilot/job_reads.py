"""Read transfer jobs from Mongo when it answers, else the engine job_store.

Pilot and Transfer Studio must see the same history. When Mongo is down the
engine already writes to the memory/file job_store; a status question that
only talks to Mongo then fails with "MongoDB unavailable" even though the
run the operator just started is sitting in the local store.
"""

from __future__ import annotations

from typing import Any

from services.job_list_scope import job_list_workspace_scope
from services.jobs import job_store


def summarize_listed_job(doc: dict[str, Any]) -> dict[str, Any]:
    """Operator-facing row shared by ``list_jobs`` and the briefing."""
    rejected = doc.get("rejected_rows")
    if rejected in (None, 0, "0"):
        details = doc.get("rejected_details") or []
        rejected = len(details) if details else 0
    # `error` must mean an error: progress text ("Row fidelity verified…",
    # "Validating mapping and schema…") landed here through the message
    # fallback and false-alarmed every client that polls error!=null (QA J01).
    # Only a *failed* job may read its failure text out of `message`.
    status = str(doc.get("status") or "").lower()
    error = str(doc.get("error") or "").strip()
    message = str(doc.get("message") or "").strip()
    if not error and message and status in {"failed", "error", "cancelled"}:
        error = message
    from services.job_status import job_stall_seconds, job_timestamp_iso

    stall = job_stall_seconds(doc)
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
        "message": message[:240] or None,
        "stalled": stall > 0,
        "stall_seconds": int(stall),
        "created_at": job_timestamp_iso(doc.get("created_at")),
        "completed_at": job_timestamp_iso(doc.get("completed_at")),
        "source_table": (
            doc.get("source_table")
            or doc.get("table_name")
            or doc.get("source_name")
        ),
        "dest_table": doc.get("destination_collection") or doc.get("dest_table"),
    }


def job_governance(job: dict[str, Any]) -> dict[str, Any]:
    """Who approved a job and on what grounds, read back for audit (QA MX3-19).

    Confirmation is stamped by ``/copilot/confirm``; acknowledgements and
    signed risk contracts travel on the persisted transfer request. A contract
    whose signature no longer verifies is listed with ``verified=False``.
    """
    from services.migration_risk_contract import mapping_risk_contract

    req = job.get("transfer_request") or {}
    if not isinstance(req, dict):
        req = {}
    contracts = []
    for mapping in req.get("mappings") or []:
        if not isinstance(mapping, dict):
            continue
        raw = mapping.get("risk_contract") or mapping.get("riskContract")
        if not isinstance(raw, dict):
            continue
        contracts.append({
            "column": raw.get("column") or mapping.get("target") or "",
            "execution_policy": raw.get("execution_policy") or "",
            "quarantine_policy": raw.get("quarantine_policy") or "",
            "approved_by": raw.get("approved_by") or "",
            "approved_at": raw.get("approved_at") or "",
            "reason": raw.get("reason") or "",
            "verified": mapping_risk_contract(mapping) is not None,
        })
    confirmation = job.get("confirmation")
    return {
        "approval": dict(confirmation) if isinstance(confirmation, dict) else None,
        "acknowledgments": {
            "compliance": bool(req.get("compliance_acknowledged")),
            "schema_drift": bool(req.get("schema_drift_acknowledged")),
            "fk_risk": bool(req.get("fk_risk_acknowledged")),
            "actor": req.get("acknowledgment_actor") or "",
            "reason": req.get("acknowledgment_reason") or "",
        },
        "risk_contracts": contracts,
        "contract_id": req.get("contract_id") or "",
        "require_signed_contract": bool(req.get("require_signed_contract")),
        "triggered_by": job.get("triggered_by") or req.get("triggered_by") or "",
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
    # A failed engine job stores its refusal in `message` — same J01 rule as
    # summarize_listed_job: only a failure-class status may read it as error.
    _status = str(rec.get("status") or "").lower()
    _error = str(rec.get("error") or "").strip()
    if not _error and _status in {"failed", "error", "cancelled"}:
        _error = str(rec.get("message") or "").strip()
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
        # Same J01 rule: `message` is progress text, not an error field.
        "error": _error,
        "message": rec.get("message") or "",
        "created_at": rec.get("created_at") or "",
        "operation": rec.get("operation") or "",
        "table_name": rec.get("table_name") or "",
    }


def _mongo_already_unreachable(mongo: Any) -> bool:
    """True when this process already failed to open Mongo.

    ``list_jobs`` otherwise re-runs the 5s server-selection ping on every
    last-transfer question, which is how "status of my last transfer" sat
    on a spinner and then said the store was empty.
    """
    return (
        type(mongo).__name__ == "MongoDBService"
        and getattr(mongo, "client", None) is None
    )


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
        if _mongo_already_unreachable(mongo):
            summaries, counts = _from_engine_store(limit)
            return summaries, counts, "job_store"
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

        mongo = get_mongodb_service()
        if not _mongo_already_unreachable(mongo):
            job = mongo.get_job(needle)
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
