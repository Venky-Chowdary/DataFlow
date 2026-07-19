"""Usage metering API — rows/bytes accounting summary."""

from __future__ import annotations

from fastapi import APIRouter, Query

router = APIRouter(prefix="/usage", tags=["Usage"])


@router.get("/summary")
async def usage_summary(
    days: int = Query(30, ge=1, le=366, description="UTC calendar days to include"),
    workspace_id: str = Query("", description="Optional workspace filter"),
):
    """Return rows/bytes per day plus window totals."""
    from services.usage_metering import summarize_usage

    return summarize_usage(workspace_id=workspace_id or "", days=days)
