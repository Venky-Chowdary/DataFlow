"""Shared workspace gating for control-plane routers (schedules, contracts, usage)."""

from __future__ import annotations

from fastapi import Header, HTTPException, Request

from services.team_store import can_read_workspace, can_write_workspace, require_workspace_isolation


def actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def resolve_read_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    workspace_id = (x_workspace_id or "").strip()
    if require_workspace_isolation() and not workspace_id:
        raise HTTPException(status_code=400, detail="X-Workspace-Id required")
    if workspace_id and not can_read_workspace(workspace_id, actor_email(request)):
        raise HTTPException(status_code=403, detail="Access to workspace denied")
    return workspace_id


def resolve_write_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    workspace_id = (x_workspace_id or "").strip()
    if require_workspace_isolation() and not workspace_id:
        raise HTTPException(status_code=400, detail="X-Workspace-Id required")
    if workspace_id and not can_write_workspace(workspace_id, actor_email(request)):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")
    return workspace_id


def assert_resource_workspace(request: Request, resource_workspace_id: str | None) -> None:
    """Deny cross-workspace resource access when isolation is on or resource is scoped."""
    rid = (resource_workspace_id or "").strip()
    if not rid:
        if require_workspace_isolation():
            raise HTTPException(status_code=404, detail="Not found")
        return
    if not can_read_workspace(rid, actor_email(request)):
        raise HTTPException(status_code=404, detail="Not found")
    # The request's declared scope is the tenant boundary, not just the actor's
    # membership: a user who belongs to two workspaces must not see one
    # workspace's resource while operating in the other. List endpoints already
    # filter by scope, so an id-addressed read has to agree with them.
    scope = (request.headers.get("X-Workspace-Id") or "").strip()
    if scope and scope != rid:
        raise HTTPException(status_code=404, detail="Not found")
