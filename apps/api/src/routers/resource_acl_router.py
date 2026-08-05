"""Resource ACL admin API — instance grants beyond coarse RBAC roles."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.resource_acl import (
    ACL_ROLES,
    RESOURCE_TYPES,
    list_grants,
    revoke_grant,
    upsert_grant,
)
from services.team_store import can_write_workspace

router = APIRouter(prefix="/resource-acls", tags=["Resource ACLs"])

ResourceType = Literal["connector", "job", "contract", "transform", "schedule"]
AclRole = Literal["viewer", "editor", "owner"]


def _actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def _require_admin_or_workspace_write(request: Request, workspace_id: str) -> str:
    ws = (workspace_id or "").strip()
    user = getattr(request.state, "user", None) or {}
    role = str(user.get("role") or "").lower()
    if role == "admin":
        return ws
    if ws and not can_write_workspace(ws, _actor_email(request)):
        raise HTTPException(status_code=403, detail="Workspace write access required to manage ACLs")
    if not ws:
        raise HTTPException(status_code=400, detail="X-Workspace-Id required")
    return ws


class GrantBody(BaseModel):
    resource_type: ResourceType
    resource_id: str = Field(..., min_length=1)
    principal: str = Field(..., min_length=1)
    role: AclRole = "viewer"


@router.get("")
def list_resource_acls(
    request: Request,
    resource_type: str | None = None,
    resource_id: str | None = None,
    principal: str | None = None,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """List ACL grants for the workspace (tenant)."""
    ws = _require_admin_or_workspace_write(request, workspace_id)
    if resource_type and resource_type not in RESOURCE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported resource_type: {resource_type}")
    grants = list_grants(
        tenant_id=ws,
        resource_type=resource_type,
        resource_id=resource_id,
        principal=principal,
    )
    return {
        "grants": [g.to_dict() for g in grants],
        "count": len(grants),
        "roles": list(ACL_ROLES),
        "resource_types": sorted(RESOURCE_TYPES),
        "honesty": (
            "When any grants exist on a resource, non-grantees are denied even if coarse "
            "RBAC would allow. No grants → fall through to workspace RBAC."
        ),
    }


@router.post("")
def upsert_resource_acl(
    body: GrantBody,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    ws = _require_admin_or_workspace_write(request, workspace_id)
    try:
        grant = upsert_grant(
            tenant_id=ws,
            resource_type=body.resource_type,
            resource_id=body.resource_id.strip(),
            principal=body.principal,
            role=body.role,
            created_by=_actor_email(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return grant.to_dict()


@router.delete("/{grant_id}")
def delete_resource_acl(
    grant_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    ws = _require_admin_or_workspace_write(request, workspace_id)
    if not revoke_grant(grant_id, tenant_id=ws):
        raise HTTPException(status_code=404, detail="Grant not found")
    return {"ok": True, "id": grant_id}
