"""Audit log API — real workspace events."""

from __future__ import annotations

from fastapi import APIRouter, Query

router = APIRouter(prefix="/audit", tags=["Audit"])


@router.get("/events")
async def list_events(
    limit: int = Query(50, ge=1, le=500),
    level: str | None = Query(None, description="info | success | warn | error | all"),
):
    from services.audit_log import list_audit_events

    events = list_audit_events(limit=limit, level=level)
    return {"events": events, "count": len(events)}
