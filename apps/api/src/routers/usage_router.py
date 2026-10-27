"""Usage metering API — rows/bytes accounting summary."""

from __future__ import annotations

from fastapi import APIRouter, Header, Query, Request

from services.workspace_access import tenant_scope

router = APIRouter(prefix="/usage", tags=["Usage"])


@router.get("/summary")
async def usage_summary(
    request: Request,
    days: int = Query(30, ge=1, le=366, description="UTC calendar days to include"),
    workspace_id: str = Query("", description="Optional workspace filter"),
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Return rows/bytes per day plus window totals.

    One workspace's usage: the header (or query) workspace, else the caller's
    own. Platform-wide totals are for platform admins only (D154).
    """
    from services.usage_metering import summarize_usage

    scope = tenant_scope(request, x_workspace_id).narrowed(workspace_id)
    return summarize_usage(
        workspace_id=scope.workspace_id,
        days=days,
        workspace_filter=scope.single_workspace_filter(),
    )
