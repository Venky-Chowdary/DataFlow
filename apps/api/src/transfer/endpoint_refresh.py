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
    _audit_refresh(job_id, request, {"job_id": job_id, "changes": changes}, level="warn")
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


def _audit_refresh(job_id: str, request: Any, details: dict[str, Any], *, level: str) -> None:
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action=AUDIT_ACTION,
            resource=f"job:{job_id}",
            actor="system",
            level=level,
            outcome="success",
            workspace_id=getattr(request, "workspace_id", None) or None,
            details=details,
        )
    except Exception as exc:  # noqa: BLE001 — the run must not fail on an audit sink outage
        logger.warning("job %s: endpoint refresh audit not written: %s", job_id, exc)


class ReplayConnectorMissing(ValueError):
    """The destination's saved connector is gone: a replay has nowhere honest to write."""


class ReplayEndpointChanged(ValueError):
    """The saved connector now points at another server or database than the job wrote to."""


def refresh_replay_destination(
    job_id: str, stored: dict[str, Any], request: Any, *, acknowledged: bool = False
) -> dict[str, Any]:
    """Point a quarantine replay at the destination connector as it is saved *now* (D413).

    The replay rebuilds its request from the parent job's ``transfer_request``,
    whose host and credentials are those the connection had when the job ran.
    Retry, rerun and resume already dial the saved connector (D172/D289);
    replay wrote to the frozen endpoint, so a host move or credential rotation
    made quarantined rows unrecoverable, or sent them to the old server.

    Only the destination is refreshed: a replay writes stored rows and never
    reads the source. Every resolution is audited (``job.endpoint_refreshed``,
    R11) with the stored -> current identity, never credentials. Returns the
    endpoint the replay dials and where it came from, for the job record and
    for any connectivity error (AC-4).
    """
    from .adapters import refresh_saved_endpoint

    dest = getattr(request, "destination", None)
    snapshot = _identity((stored or {}).get("destination") or dest)
    if dest is None or not getattr(dest, "connector_id", None):
        return {"origin": "job_snapshot", "endpoint": _label(snapshot), "changes": []}
    connector_id = str(dest.connector_id)
    from services.connector_store import get_connector

    workspace = getattr(request, "workspace_id", None) or None
    if get_connector(connector_id, workspace_id=workspace) is None:
        raise ReplayConnectorMissing(
            f"Replay refused: the destination connection {connector_id} that job {job_id} "
            f"wrote to (table {getattr(dest, 'table', '') or '?'}) no longer exists in this "
            "workspace. Nothing was written and the quarantined rows stay open. Re-create "
            "the connection, or replay into a connection that exists."
        )
    refreshed = refresh_saved_endpoint(dest, workspace_id=workspace)
    current = _identity(refreshed)
    recorded = bool(snapshot.get("host") or snapshot.get("dsn"))
    if recorded and snapshot != current and not acknowledged:
        # A credential rotation keeps host and database: it passes. A move to
        # another server or database may be a different system (AC-3).
        raise ReplayEndpointChanged(
            f"Replay refused: the destination connection {connector_id} now points at "
            f"{_label(current)}, but job {job_id} wrote to {_label(snapshot)}. Nothing was "
            "written and the quarantined rows stay open. If the rows belong on the new "
            "endpoint, replay again with acknowledge_endpoint_change=true."
        )
    request.destination = refreshed
    changes = (
        [{"role": "destination", "connector_id": connector_id,
          "from": _label(snapshot), "to": _label(current)}]
        if recorded and snapshot != current
        else []
    )
    _audit_refresh(
        job_id,
        request,
        {"job_id": job_id, "operation": "quarantine_replay", "connector_id": connector_id,
         "endpoint": _label(current), "changes": changes, "acknowledged": bool(acknowledged)},
        level="warn" if changes else "info",
    )
    return {
        "origin": "saved_connector",
        "connector_id": connector_id,
        "endpoint": _label(current),
        "snapshot_endpoint": _label(snapshot),
        "changes": changes,
    }


def replay_endpoint_note(resolution: dict[str, Any] | None) -> str:
    """Which endpoint a replay dialled and where it came from (AC-4)."""
    if not resolution:
        return ""
    if resolution.get("origin") == "saved_connector":
        moved = (
            f", refreshed from the job snapshot {resolution.get('snapshot_endpoint')}"
            if resolution.get("changes")
            else ""
        )
        return (
            f"The replay dialled {resolution.get('endpoint')} from the saved connection "
            f"{resolution.get('connector_id')}{moved}."
        )
    return (
        f"The replay dialled {resolution.get('endpoint')} from the job snapshot "
        "(this destination has no saved connection)."
    )
