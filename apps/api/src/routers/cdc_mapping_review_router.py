"""CDC mapping-review HTTP API — list / acknowledge open DDL drift signals."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from services.audit_log import actor_from_request, append_audit_event
from services.cdc_mapping_review import acknowledge_review, list_reviews

router = APIRouter(prefix="/cdc/mapping-reviews", tags=["CDC Mapping Review"])


@router.get("")
def list_cdc_mapping_reviews(
    status: str = Query("open", description="open | acknowledged | all"),
    source_key: str | None = Query(None, description="Required filter — source fingerprint"),
    limit: int = Query(50, ge=1, le=200),
):
    if not (source_key or "").strip():
        raise HTTPException(
            status_code=400,
            detail="source_key query parameter is required (no cluster-wide listing)",
        )
    reviews = list_reviews(source_key=source_key.strip(), status=status, limit=limit)
    return {
        "reviews": reviews,
        "count": len(reviews),
        "honesty": (
            "Open reviews mean CDC saw schema drift. Acknowledge after Map review — "
            "schedules with matching sources skip until acknowledged. Still at-least-once."
        ),
    }


@router.post("/{review_id}/acknowledge")
def acknowledge_cdc_mapping_review(review_id: str, request: Request):
    updated = acknowledge_review(review_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Open mapping review not found")
    try:
        append_audit_event(
            action="cdc.mapping_review.acknowledge",
            resource=f"cdc_review:{review_id}",
            actor=actor_from_request(request),
            level="info",
            details={
                "source_key": updated.get("source_key"),
                "table": updated.get("table"),
            },
        )
    except Exception:
        pass
    return updated
