"""Ops surface — freshness SLOs and quarantine DLQ for the product UI."""

from __future__ import annotations

from fastapi import APIRouter, Query

router = APIRouter(prefix="/ops", tags=["Ops"])


@router.get("/freshness")
async def get_freshness(
    warn_seconds: float = Query(60.0, ge=1.0, le=86400.0),
):
    """Pipeline CDC lag / heartbeat summary for Overview and Pipelines."""
    from services.ops_metrics import freshness_summary

    return freshness_summary(max_lag_warn_seconds=warn_seconds)


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
