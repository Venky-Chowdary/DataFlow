"""Usage metering API — rows/bytes accounting summary."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request

from services.workspace_access import resolve_read_workspace

router = APIRouter(prefix="/usage", tags=["Usage"])


@router.get("/summary")
async def usage_summary(
    request: Request,
    days: int = Query(30, ge=1, le=366, description="UTC calendar days to include"),
    workspace_id: str = Query("", description="Optional workspace filter"),
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Return rows/bytes per day plus window totals."""
    from services.usage_metering import summarize_usage

    header_ws = resolve_read_workspace(request, x_workspace_id)
    q_ws = (workspace_id or "").strip()
    if q_ws and header_ws and q_ws != header_ws:
        raise HTTPException(status_code=403, detail="workspace_id does not match X-Workspace-Id")
    effective = header_ws or q_ws
    if effective and not header_ws:
        resolve_read_workspace(request, effective)
    return summarize_usage(workspace_id=effective, days=days)
