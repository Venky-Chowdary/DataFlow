"""Ops surface — freshness SLOs, quarantine DLQ, and CDC lease operator actions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ops", tags=["Ops"])


@router.get("/freshness")
async def get_freshness(
    request: Request,
    warn_seconds: float = Query(60.0, ge=1.0, le=86400.0),
    critical_seconds: float | None = Query(None, ge=1.0, le=86400.0),
    heartbeat_stale_seconds: float = Query(300.0, ge=30.0, le=86400.0),
):
    """Pipeline CDC lag / heartbeat summary for Overview and Pipelines.

    ``stale_count`` / ``alerts`` / ``worst_lag_seconds`` are one workspace's:
    the ``X-Workspace-Id`` workspace, else the caller's own. Only a platform
    admin without a header gets the process-wide view (D154).
    """
    from services.ops_metrics import freshness_summary
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request)
    return freshness_summary(
        max_lag_warn_seconds=warn_seconds,
        max_lag_critical_seconds=critical_seconds,
        heartbeat_stale_seconds=heartbeat_stale_seconds,
        workspace_id=scope.workspace_id,
        workspace_filter=scope.single_workspace_filter(),
    )


@router.get("/metrics/json")
async def get_metrics_json(request: Request):
    """JSON mirror of in-process Prometheus metrics (UI-friendly).

    Process counters and gauges are platform-wide, so only a platform admin
    sees them; anyone else gets the per-pipeline series of their own
    workspace (D154).
    """
    from services.ops_metrics import snapshot
    from services.workspace_access import tenant_scope

    return snapshot(workspace_filter=tenant_scope(request).single_workspace_filter())


@router.get("/dlq")
async def get_dlq(
    request: Request,
    job_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """Quarantine dead-letter queue events (newest first).

    ``count`` / ``total`` are one workspace's queue — the ``X-Workspace-Id``
    workspace, else the caller's own; only a platform admin without a header
    gets the platform queue (D154). ``returned`` is the page.
    """
    from services.quarantine_dlq import count_dlq_events, list_dlq_events
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request)
    workspace_filter = scope.single_workspace_filter()
    workspace_id = scope.workspace_id
    events = list_dlq_events(job_id=job_id, workspace_filter=workspace_filter, limit=limit)
    total = count_dlq_events(job_id=job_id, workspace_filter=workspace_filter)
    by_action: dict[str, int] = {}
    for ev in events:
        action = str(ev.get("action") or "unknown")
        by_action[action] = by_action.get(action, 0) + 1
    return {
        "events": events,
        "count": total,
        "total": total,
        "returned": len(events),
        "limit": limit,
        "workspace_id": workspace_id or None,
        "by_action": by_action,
        "open_rows": sum(int(ev.get("rows") or 0) for ev in events if "fail" in str(ev.get("action") or "")),
    }


@router.get("/cdc-leases")
async def get_cdc_lease(cursor_key: str = Query(..., min_length=1, max_length=512)):
    """Operator snapshot of a CDC resource lease (fencing generation, staleness)."""
    from services.cdc_lease import lease_backend_name, lease_view, parse_holder_job_id

    view = lease_view(cursor_key)
    if view is None:
        return {
            "found": False,
            "cursor_key": cursor_key,
            "backend": lease_backend_name(),
        }
    holder = str(view.get("holder_id") or "")
    meta = view.get("meta") if isinstance(view.get("meta"), dict) else {}
    return {
        "found": True,
        "cursor_key": cursor_key,
        "lease": view,
        "holder_job_id": parse_holder_job_id(holder) or meta.get("job_id"),
        "backend": view.get("backend") or lease_backend_name(),
    }


@router.get("/cdc-leases/list")
async def list_cdc_leases(
    resource: str = Query("", max_length=512),
    job_id: str = Query("", max_length=128),
    stale_only: bool = Query(False),
) -> dict[str, Any]:
    """Held CDC leases, newest heartbeat first.

    A refused run names the resource it collided on; force-release needs the
    cursor key. This is how an operator gets from one to the other.
    """
    from services.cdc_lease import lease_backend_name, list_lease_views

    leases = list_lease_views(resource=resource, job_id=job_id, stale_only=stale_only)
    return {
        "leases": leases,
        "count": len(leases),
        "stale_count": sum(1 for lease in leases if lease.get("stale")),
        "backend": lease_backend_name(),
    }


class ForceReleaseBody(BaseModel):
    cursor_key: str = Field(..., min_length=1, max_length=512)
    expected_generation: int | None = Field(None, ge=1)
    reason: str = Field("", max_length=300)


@router.post("/cdc-leases/force-release")
async def post_force_release_cdc_lease(body: ForceReleaseBody, request: Request) -> dict[str, Any]:
    """Break a live CDC lease so another job can acquire (fencing-aware).

    Honesty: does not stop the prior holder process — that worker will fail on
    the next renew. Prefer cancelling the holder job when it is still running.
    """
    from services.cdc_lease import force_release_lease

    actor = ""
    try:
        actor = str(getattr(request.state, "actor", "") or request.headers.get("x-actor") or "")
    except Exception:
        actor = ""
    result = force_release_lease(
        body.cursor_key,
        expected_generation=body.expected_generation,
        reason=body.reason,
        actor=actor,
    )
    if result.get("reason") == "missing_cursor_key":
        raise HTTPException(status_code=400, detail="cursor_key is required")
    return result


@router.get("/cdc-cursors")
async def get_cdc_cursor(cursor_key: str = Query(..., min_length=1, max_length=512)):
    """Read the persisted CDC/sync watermark for operator recovery."""
    from services.sync_cursor import get_watermark

    wm = get_watermark(cursor_key)
    return {
        "cursor_key": cursor_key,
        "found": wm is not None,
        "watermark": wm,
    }


@router.get("/cdc-cursors/keys")
async def list_cdc_cursor_keys() -> dict[str, Any]:
    """List persisted cursor keys so a reset can name one instead of guessing."""
    from services.sync_cursor import list_cursor_keys

    keys = sorted(list_cursor_keys())
    return {"count": len(keys), "cursor_keys": keys}


class ClearCursorBody(BaseModel):
    cursor_key: str = Field(..., min_length=1, max_length=512)
    reason: str = Field("", max_length=300)


@router.post("/cdc-cursors/clear")
async def post_clear_cdc_cursor(body: ClearCursorBody, request: Request) -> dict[str, Any]:
    """Clear a CDC watermark so the next run re-snapshots (gap recovery).

    Honesty: clearing does not rewind the destination — at-least-once upsert
    may re-apply rows from the new snapshot handoff. Prefer when_needed/initial.
    """
    from services.sync_cursor import clear_watermark

    actor = ""
    try:
        actor = str(getattr(request.state, "actor", "") or request.headers.get("x-actor") or "")
    except Exception:
        actor = ""
    result = clear_watermark(body.cursor_key)
    result["actor"] = actor or None
    result["reason_note"] = (body.reason or "").strip() or None
    if result.get("reason") == "missing_cursor_key":
        raise HTTPException(status_code=400, detail="cursor_key is required")
    return result


def _capture_connector(request: Request, connector_id: str, workspace_id: str, *, write: bool):
    """The saved PostgreSQL source connector the caller may act on, or 404/422."""
    from services.connector_store import get_connector
    from services.workspace_access import resolve_read_workspace, resolve_write_workspace

    ws = (
        resolve_write_workspace(request, workspace_id)
        if write
        else resolve_read_workspace(request, workspace_id)
    )
    connector = get_connector(connector_id, ws or None)
    if connector is None:
        raise HTTPException(status_code=404, detail="Connector not found")
    if (connector.type or "").lower() not in {"postgresql", "postgres"}:
        raise HTTPException(
            status_code=422,
            detail=(
                "Only PostgreSQL CDC keeps a server-side replication slot; "
                f"connector {connector_id} is {connector.type}."
            ),
        )
    return connector, ws


@router.get("/cdc-captures")
async def get_cdc_captures(
    request: Request,
    connector_id: str = Query(..., min_length=1, max_length=128),
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Replication slots and publications on a PostgreSQL source, with owner and state.

    C11: names the Datawrap-created slots no live CDC schedule uses, or whose
    source table is gone, how much WAL each retains, and a warning per slot.
    External slots are listed for visibility and never cleaned up.
    ``auto_drop`` states the deployment setting ``cdc_orphan_auto_drop``
    (off by default; PATCH /api/v1/workspace/settings, platform admin,
    audited): only when it is on does the scheduler drop a source-table-gone
    slot by itself after the grace period. Off, it warns and alerts.
    """
    import asyncio

    from services.cdc_capture_inventory import inventory_pg_captures

    connector, ws = _capture_connector(request, connector_id, workspace_id, write=False)
    try:
        return await asyncio.to_thread(
            inventory_pg_captures, connector.to_dict(), connector_id=connector_id, workspace_id=ws
        )
    except Exception as exc:  # noqa: BLE001 - name the source failure, never a 500
        raise HTTPException(
            status_code=502, detail=f"Could not read the source's replication slots: {exc}"
        ) from exc


class CdcCaptureCleanupBody(BaseModel):
    connector_id: str = Field(..., min_length=1, max_length=128)
    slot_names: list[str] = Field(default_factory=list, max_length=100)
    publication_names: list[str] = Field(default_factory=list, max_length=100)
    include_unregistered: bool = False
    dry_run: bool = True
    reason: str = Field("", max_length=300)


@router.post("/cdc-captures/cleanup")
async def post_cdc_captures_cleanup(
    body: CdcCaptureCleanupBody,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Drop orphaned Datawrap replication slots / publications on a PostgreSQL source.

    ``dry_run`` defaults to true: the response names what would be dropped.
    Only slots Datawrap created, with no reader attached, that no live CDC
    schedule uses or whose source table is gone, are dropped. Audited.
    """
    import asyncio

    from services.audit_coverage import record_security_event
    from services.cdc_capture_inventory import cleanup_pg_captures

    connector, ws = _capture_connector(request, body.connector_id, workspace_id, write=True)
    try:
        result = await asyncio.to_thread(
            cleanup_pg_captures,
            connector.to_dict(),
            connector_id=body.connector_id,
            workspace_id=ws,
            slot_names=body.slot_names,
            publication_names=body.publication_names,
            include_unregistered=body.include_unregistered,
            dry_run=body.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 - name the source failure, never a 500
        raise HTTPException(
            status_code=502, detail=f"Could not clean up the source's replication slots: {exc}"
        ) from exc
    if not body.dry_run:
        record_security_event(
            "cdc.capture.cleanup",
            outcome="succeeded" if result["dropped"] else "skipped",
            request=request,
            resource=f"connector:{body.connector_id}",
            details={
                "connector_id": body.connector_id,
                "dropped": result["dropped"],
                "results": result["results"],
                "reason": (body.reason or "").strip() or None,
            },
        )
    return result


class SourceHaProbeBody(BaseModel):
    type: str = Field(..., min_length=1, max_length=64)
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    schema_name: str = Field("", alias="schema")
    connection_string: str = ""
    ssl: bool = False
    multi_subnet_failover: bool = False
    application_intent: str = Field("", max_length=32)

    model_config = {"populate_by_name": True}


@router.post("/source-ha/probe")
async def post_source_ha_probe(body: SourceHaProbeBody) -> dict[str, Any]:
    """Probe SQL Server Always On / Oracle Data Guard role on a live connection.

    Honesty: returns STANDALONE / PRIMARY on single-node hosts. Does not prove
    dual-node failover; use for operator visibility before CDC.
    """
    from services.source_ha_probe import probe_source_ha_safe

    cfg = {
        "type": body.type,
        "host": body.host,
        "port": body.port,
        "database": body.database,
        "username": body.username,
        "password": body.password,
        "schema": body.schema_name,
        "connection_string": body.connection_string,
        "ssl": body.ssl,
        "multi_subnet_failover": body.multi_subnet_failover,
        "application_intent": body.application_intent,
    }
    probe = probe_source_ha_safe(cfg)
    return {"ok": True, "source_ha": probe.to_dict(), **probe.job_fields()}


class CdcRetentionProbeBody(BaseModel):
    type: str = Field(..., min_length=1, max_length=64)
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    schema_name: str = Field("", alias="schema")
    connection_string: str = ""
    ssl: bool = False
    table: str = ""
    cursor_key: str = Field("", max_length=512)
    watermark: str | None = None
    multi_subnet_failover: bool = False

    model_config = {"populate_by_name": True}


@router.post("/cdc-retention/probe")
async def post_cdc_retention_probe(body: CdcRetentionProbeBody) -> dict[str, Any]:
    """Probe watermark vs live CDC retention (SQL Server min_lsn / Oracle oldest SCN).

    Honesty: ``gap`` means reset watermark + re-snapshot (or ``when_needed``).
    PostgreSQL ``wal_status=lost`` / dropped slot is the same class — recreating
    the slot at current WAL skips the lost window. Continuous CDC across the
    retention window is not claimed.
    """
    from services.cdc_retention_probe import probe_cdc_retention

    cfg = {
        "type": body.type,
        "host": body.host,
        "port": body.port,
        "database": body.database,
        "username": body.username,
        "password": body.password,
        "schema": body.schema_name,
        "connection_string": body.connection_string,
        "ssl": body.ssl,
        "multi_subnet_failover": body.multi_subnet_failover,
    }
    probe = probe_cdc_retention(
        cfg,
        table=body.table,
        schema=body.schema_name,
        cursor_key=body.cursor_key,
        watermark=body.watermark,
    )
    return {"ok": True, "retention": probe.to_dict(), **probe.job_fields()}


_MAIL_HEALTH_WINDOW_HOURS = 24


@router.get("/mail")
async def get_mail_outbox_health(
    request: Request,
    limit: int = Query(20, ge=1, le=200),
) -> dict[str, Any]:
    """The platform mail outbox as an operator sees it (D130).

    Signup codes and reset links are queued here and sent by the API process;
    a provider refusal (Cloudflare edge 403 "error code: 1010", a bad key,
    an unverified domain) used to leave only a row marked ``failed`` and a
    bare ``mail.send_failed`` audit event. This answers "is mail leaving,
    and if not, why": the configured provider and User-Agent, the status
    histogram, and the recent failures with their cause. Bodies are never
    returned and recipients are masked. Platform-wide, so platform
    administrators only.
    """
    from services import mail_outbox
    from services.email_sender import email_provider_name
    from services.notification_service import USER_AGENT
    from services.workspace_access import require_platform_admin, tenant_scope

    require_platform_admin(
        request,
        tenant_scope(request),
        "ops.mail.read",
        "The mail outbox is platform-wide: platform administrators only.",
    )
    counts = mail_outbox.counts()
    failures = mail_outbox.recent_failures(limit=limit)
    # "Healthy" is about now, not history: one refusal last month must not
    # read as a broken mailer forever. The histogram keeps the history.
    since = (datetime.now(timezone.utc) - timedelta(hours=_MAIL_HEALTH_WINDOW_HOURS)).isoformat()
    recent = [
        f for f in mail_outbox.recent_failures(limit=200)
        if str(f.get("finished_at") or f.get("next_attempt_at") or f.get("created_at") or "") >= since
    ]
    return {
        "provider": email_provider_name(),
        "user_agent": USER_AGENT,
        "counts": counts,
        "recent_failures": failures,
        "health_window_hours": _MAIL_HEALTH_WINDOW_HOURS,
        "failures_in_window": len(recent),
        "healthy": not recent,
    }
