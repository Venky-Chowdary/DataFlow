"""Shared workspace gating for control-plane routers (schedules, contracts, usage).

The tenant-scope resolver lives in ``services.tenant_scope``; this module is the
router-facing surface over it.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, Request

from services.team_store import can_read_workspace, require_workspace_isolation
from services.tenant_scope import (
    TenantScope,
    WorkspaceFilter,
    resolve_tenant_scope,
)

__all__ = [
    "TenantScope",
    "WorkspaceFilter",
    "actor_email",
    "assert_connector_accessible",
    "assert_resource_workspace",
    "authorized_workspace_id",
    "requested_workspace_id",
    "resolve_read_workspace",
    "resolve_tenant_scope",
    "resolve_write_workspace",
    "tenant_scope",
    "workspace_resource_is_visible",
]


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

        user = getattr(state, "user", None)
        if isinstance(user, dict) and str(user.get("auth_kind") or "") == "api_key":
            # An API key reads its own workspace only, never its creator's
            # other memberships (tenant scope, services.effective_principal).
            from services.tenant_scope import _base_scope

            allowed = workspace_id in _base_scope(request).member_workspace_ids
        else:
            allowed = can_read_workspace(workspace_id, actor_email(request))
        if allowed:
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


def tenant_scope(
    request: Request,
    x_workspace_id: str | None = None,
) -> TenantScope:
    """The request's tenant scope (D154); ``x_workspace_id`` defaults to the header."""
    return resolve_tenant_scope(request, x_workspace_id)


def resolve_read_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    """The one workspace a read acts in ("" = every workspace, admins only).

    A restricted actor without a header reads their own workspace rather than
    the old platform-wide default; with isolation enforced and no membership
    at all there is nothing to read (403).
    """
    scope = resolve_tenant_scope(request, x_workspace_id)
    if scope.enforced and not scope.unrestricted and not scope.workspace_id:
        raise HTTPException(status_code=403, detail="No workspace membership")
    return scope.workspace_id


def resolve_write_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    return resolve_tenant_scope(request, x_workspace_id).target_workspace_id(write=True)


def workspace_resource_is_visible(
    resource_workspace_id: str | None,
    workspace_id: str,
) -> bool:
    """Visibility inside one already-resolved workspace scope ("" = all).

    For request-bound reads use ``tenant_scope(request).allows`` — it also
    knows who is asking.
    """
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
    """404 an id-addressed resource the request's tenant scope does not cover.

    Same rule as the lists: foreign workspaces, a workspace other than the
    declared header scope, and (isolation enforced) unscoped legacy rows for
    anyone but a platform admin. ``write=True`` is required on every route
    that mutates the resource: the caller's RBAC permission comes from their
    role, which is independent of the resource's workspace, so read
    membership there must not be enough to change what lives in it (403).
    """
    scope = resolve_tenant_scope(request)
    if not scope.allows(resource_workspace_id):
        raise HTTPException(status_code=404, detail="Not found")
    if write and not scope.can_write(resource_workspace_id):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")


def assert_connector_accessible(
    request: Request, connector_id: str | None, *, write: bool = False
) -> None:
    """Authorize a saved-connector id against the connector's own workspace.

    404 when the tenant scope does not cover it; with ``write`` also 403 when
    the actor holds no admin/editor role there. A ``connector_id`` expands into
    stored host and credentials and, as a destination, is written to, so seeing
    it (a viewer of that workspace) is not enough to load data into it. An id
    that does not exist is left to the caller's own not-found handling.
    """
    from services.connector_store import connector_owner_workspace

    cid = str(connector_id or "").strip()
    if not cid:
        return
    exists, owner = connector_owner_workspace(cid)
    if not exists:
        return
    scope = resolve_tenant_scope(request)
    if not scope.allows(owner):
        raise HTTPException(status_code=404, detail="Connector not found")
    if write and not scope.can_write(owner):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")
