"""Shared workspace gating for control-plane routers (schedules, contracts, usage)."""

from __future__ import annotations

from fastapi import Header, HTTPException, Request

from services.team_store import can_read_workspace, can_write_workspace, require_workspace_isolation


def actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def requested_workspace_id(request: Request) -> str:
    """Return the requested header scope without authorizing it."""
    try:
        return str(request.headers.get("X-Workspace-Id") or "").strip()
    except Exception:
        return ""


def authorized_workspace_id(request: Request) -> str:
    """Return the requested workspace only when the actor may read it."""
    cache_key = "_authorized_workspace_id_cache"
    try:
        state = request.state
        cached = getattr(state, cache_key, None)
        if cached is not None:
            return str(cached)

        workspace_id = requested_workspace_id(request)
        if not workspace_id:
            state.workspace_header_rejected = False
            setattr(state, cache_key, "")
            return ""

        if can_read_workspace(workspace_id, actor_email(request)):
            result = workspace_id
            state.workspace_header_rejected = False
        else:
            result = ""
            state.workspace_header_rejected = True
        setattr(state, cache_key, result)
        return result
    except Exception:
        try:
            request.state.workspace_header_rejected = True
            setattr(request.state, cache_key, "")
        except Exception:
            pass
        return ""


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


def workspace_resource_is_visible(
    resource_workspace_id: str | None,
    workspace_id: str,
) -> bool:
    """Apply the same legacy-resource visibility rule across scoped reads."""
    scope = (workspace_id or "").strip()
    if not scope:
        return True
    resource_scope = (resource_workspace_id or "").strip()
    if not resource_scope:
        return not require_workspace_isolation()
    return resource_scope == scope


def assert_resource_workspace(
    request: Request,
    resource_workspace_id: str | None,
    *,
    write: bool = False,
) -> None:
    """Deny cross-workspace resource access when isolation is on or resource is scoped.

    ``write=True`` is required on every route that mutates the resource: the
    caller's RBAC permission comes from their effective role, which is
    independent of the resource's workspace, so read membership in that
    workspace must not be enough to delete or change what lives there.
    """
    rid = (resource_workspace_id or "").strip()
    scope = (request.headers.get("X-Workspace-Id") or "").strip()
    if not rid:
        if require_workspace_isolation():
            raise HTTPException(status_code=404, detail="Not found")
        return
    actor = actor_email(request)
    if not can_read_workspace(rid, actor):
        raise HTTPException(status_code=404, detail="Not found")
    # The request's declared scope is the tenant boundary, not just the actor's
    # membership: a user who belongs to two workspaces must not see one
    # workspace's resource while operating in the other. List endpoints already
    # filter by scope, so an id-addressed read has to agree with them.
    if scope and not workspace_resource_is_visible(rid, scope):
        raise HTTPException(status_code=404, detail="Not found")
    if write and not can_write_workspace(rid, actor):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")
