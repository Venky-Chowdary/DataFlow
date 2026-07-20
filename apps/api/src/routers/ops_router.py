"""Ops surface — freshness SLOs, quarantine DLQ, and CDC lease operator actions."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ops", tags=["Ops"])


@router.get("/freshness")
async def get_freshness(
    warn_seconds: float = Query(60.0, ge=1.0, le=86400.0),
    critical_seconds: float | None = Query(None, ge=1.0, le=86400.0),
    heartbeat_stale_seconds: float = Query(300.0, ge=30.0, le=86400.0),
):
    """Pipeline CDC lag / heartbeat summary for Overview and Pipelines."""
    from services.ops_metrics import freshness_summary

    return freshness_summary(
        max_lag_warn_seconds=warn_seconds,
        max_lag_critical_seconds=critical_seconds,
        heartbeat_stale_seconds=heartbeat_stale_seconds,
    )


@router.get("/metrics/json")
async def get_metrics_json():
    """JSON mirror of in-process Prometheus metrics (UI-friendly)."""
    from services.ops_metrics import snapshot

    return snapshot()


@router.get("/dlq")
async def get_dlq(
    job_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """Quarantine dead-letter queue events (newest first)."""
    from services.quarantine_dlq import list_dlq_events

    events = list_dlq_events(job_id=job_id, limit=limit)
    by_action: dict[str, int] = {}
    for ev in events:
        action = str(ev.get("action") or "unknown")
        by_action[action] = by_action.get(action, 0) + 1
    return {
        "events": events,
        "count": len(events),
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

    Honesty: ``gap`` means reset watermark + re-snapshot. Continuous CDC across
    the retention window is not claimed.
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
