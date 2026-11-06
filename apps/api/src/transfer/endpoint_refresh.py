"""Dial the connectors' *current* hosts when a job runs, not the hosts stored at job start.

A job document keeps the ``transfer_request`` exactly as it was resolved when
the job was submitted: host, port, database and credentials included. Resume,
retry and the orphan reclaim rebuild the request from that record. The REST
routes re-resolve the saved connectors in memory (D172), but in claim-queue
mode the worker only receives the job id and rebuilds the request from the
stored record again — so a resumed job dialled the host the connector had at
job start, and would read or write the wrong database if that host still
answered (D289).

This is the one place the refresh happens at execution time, for every path
that ends in :func:`src.transfer.background._run_transfer` — first run, local
resume, worker resume, reclaim. It never carries credentials into logs or
audit records: only host, port and database are compared and reported.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

AUDIT_ACTION = "job.endpoint_refreshed"
JOB_FIELD = "endpoint_refresh"


def _identity(endpoint: Any) -> dict[str, Any]:
    """Connection identity of an endpoint (never its credentials)."""
    if endpoint is None:
        return {}
    get = endpoint.get if isinstance(endpoint, dict) else lambda k, d=None: getattr(endpoint, k, d)
    dsn = str(get("connection_string") or "")
    if dsn:
        from services.append_continuity import _dsn_identity

        dsn = _dsn_identity(dsn)
    return {
        "host": str(get("host") or ""),
        "port": int(get("port") or 0),
        "database": str(get("database") or ""),
        "dsn": dsn,
    }


def _label(ident: dict[str, Any]) -> str:
    if ident.get("dsn"):
        return str(ident["dsn"])
    host = ident.get("host") or "?"
    port = ident.get("port") or ""
    db = ident.get("database") or ""
    return f"{host}{':' + str(port) if port else ''}{'/' + db if db else ''}"


def refresh_endpoints_for_run(job_id: str, request: Any, mongo: Any) -> list[dict[str, Any]]:
    """Re-resolve both saved connectors and record every endpoint that moved.

    Raises ``ValueError`` (from ``refresh_saved_connectors``) when a saved
    connector no longer exists; the caller fails the job with that text.
    Returns the list of changes, each naming the role, the connector id and
    the stored -> current identity, after writing them to the audit log and
    onto the job document.
    """
    from .adapters import refresh_saved_connectors

    stored: dict[str, Any] = {}
    try:
        job = mongo.get_job(job_id) if hasattr(mongo, "get_job") else None
        payload = (job or {}).get("transfer_request") if isinstance(job, dict) else None
        if isinstance(payload, dict):
            stored = payload
    except Exception as exc:  # noqa: BLE001 — the stored record is evidence, not a dependency
        logger.debug("job %s: stored transfer_request unreadable: %s", job_id, exc)

    refresh_saved_connectors(request)

    changes: list[dict[str, Any]] = []
    for role in ("source", "destination"):
        current = getattr(request, role, None)
        if current is None or not getattr(current, "connector_id", None):
            continue
        before = _identity(stored.get(role)) if stored.get(role) else _identity(current)
        after = _identity(current)
        if before and before != after:
            changes.append(
                {
                    "role": role,
                    "connector_id": str(current.connector_id),
                    "from": _label(before),
                    "to": _label(after),
                }
            )
    if not changes:
        return []

    note = "; ".join(f"{c['role']} {c['from']} -> {c['to']}" for c in changes)
    logger.warning(
        "job %s: saved connector endpoint changed since the job was recorded — "
        "dialling the connector's current host (%s)",
        job_id,
        note,
    )
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action=AUDIT_ACTION,
            resource=f"job:{job_id}",
            actor="system",
            level="warn",
            outcome="success",
            workspace_id=getattr(request, "workspace_id", None) or None,
            details={"job_id": job_id, "changes": changes},
        )
    except Exception as exc:  # noqa: BLE001 — the run must not fail on an audit sink outage
        logger.warning("job %s: endpoint refresh audit not written: %s", job_id, exc)
    try:
        if hasattr(mongo, "update_job_fields"):
            mongo.update_job_fields(
                job_id,
                {
                    JOB_FIELD: {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "changes": changes,
                        "note": (
                            "The saved connector moved after this job was recorded; "
                            f"this run dialled its current endpoint ({note})."
                        ),
                    }
                },
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("job %s: endpoint refresh note not stored: %s", job_id, exc)
    return changes
