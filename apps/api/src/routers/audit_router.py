"""Audit log API — real workspace events + tip anchors."""

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


@router.get("/tip")
async def get_audit_tip():
    """Return the HMAC chain tip plus the latest external anchor receipt."""
    from services.audit_anchor import latest_anchor, list_anchors
    from services.audit_log import latest_event_hash

    tip = latest_event_hash()
    anchor = latest_anchor()
    return {
        "tip_hash": tip,
        "anchor": anchor,
        "anchors_recent": list_anchors(limit=5),
        "matched": bool(tip and anchor and anchor.get("tip_hash") == tip),
        "honesty": (
            "Tip is HMAC-SHA256 chain head. Anchor is a diligence seal "
            "(stub by default — not auditor WORM / TSA until provider configured)."
        ),
    }


@router.post("/tip/anchor")
async def force_anchor_tip():
    """Manually seal the current tip (ops / compliance export)."""
    from services.audit_anchor import anchor_tip, latest_anchor
    from services.audit_log import latest_event_hash

    tip = latest_event_hash()
    if not tip:
        return {"ok": False, "error": "No audit events yet", "anchor": None}
    receipt = anchor_tip(tip)
    return {"ok": bool(receipt), "tip_hash": tip, "anchor": receipt or latest_anchor()}
