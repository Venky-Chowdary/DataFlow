"""Accounts, workspaces and memberships — what a client admin manages in Settings.

Two separate authorities meet here. A *platform* role (``admin`` / ``member``) says
who may create accounts and workspaces for the deployment; a *workspace* role
(``admin`` / ``editor`` / ``viewer``) says what someone may do inside one tenant.
Every mutation writes an audit event with the actor, because "who added this user"
is the first question asked after an incident.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from services.audit_log import append_audit_event
from services.team_store import (
    ROLES,
    LastAdminProtected,
    MemberNotFound,
    PermissionDenied,
    TeamStoreError,
    WorkspaceNotFound,
    add_workspace_member,
    can_read_workspace,
    create_workspace,
    delete_workspace,
    get_workspace,
    get_workspace_role,
    list_memberships_for_user,
    list_workspace_members,
    list_workspaces,
    list_workspaces_for_user,
    remove_workspace_member,
    rename_workspace,
)
from services.user_store import (
    UserStoreError,
    create_user,
    delete_user,
    get_user,
    list_users,
    normalize_email,
    reset_password,
    set_password,
    update_user,
)
from src.services.auth_service import auth_required, lookup_user

router = APIRouter(prefix="/team", tags=["Team"])


class UserCreateBody(BaseModel):
    email: str = Field(..., max_length=254)
    name: str = Field(default="", max_length=128)
    platform_role: str = Field(default="member", pattern="^(admin|member)$")
    password: str | None = Field(default=None, min_length=12, max_length=256)
    workspace_id: str = Field(default="", max_length=128)
    workspace_role: str = Field(default="viewer", pattern="^(admin|editor|viewer)$")


class UserUpdateBody(BaseModel):
    name: str | None = Field(default=None, max_length=128)
    platform_role: str | None = Field(default=None, pattern="^(admin|member)$")
    status: str | None = Field(default=None, pattern="^(active|disabled)$")


class PasswordBody(BaseModel):
    password: str = Field(..., min_length=12, max_length=256)


class WorkspaceBody(BaseModel):
    name: str = Field(..., max_length=128)


class MemberRoleBody(BaseModel):
    role: str = Field(..., pattern="^(admin|editor|viewer)$")


class MemberBody(BaseModel):
    email: str = Field(..., max_length=254)
    role: str = Field(default="viewer", pattern="^(admin|editor|viewer)$")
    name: str = Field(default="", max_length=128)
    create_account: bool = Field(
        default=False,
        description="Create a login for this email when none exists, returning a one-time password.",
    )


def _actor(request: Request) -> str:
    return normalize_email(getattr(request.state, "user_email", None) or "") or "anonymous"


def _is_platform_admin(email: str) -> bool:
    """Whether this signed-in identity administers the deployment.

    ``lookup_user`` covers both the environment-provisioned bootstrap admin and
    stored accounts, so a deployment always has someone who can create the first
    workspace. A deployment running with authentication switched off has no
    identities to authorize against, so the single local operator is the admin —
    otherwise a developer box could never create its first workspace, while a
    client deployment (auth required) still enforces the role.
    """
    if not auth_required():
        return True
    user = lookup_user(email)
    return bool(user) and str(user.get("role", "")).strip().lower() == "admin"


def _require_platform_admin(request: Request) -> str:
    actor = _actor(request)
    if not _is_platform_admin(actor):
        raise HTTPException(status_code=403, detail="Platform administrator role required")
    return actor


def _audit(action: str, *, actor: str, resource: str, level: str, details: dict[str, Any]) -> None:
    append_audit_event(action=action, resource=resource, actor=actor, level=level, details=details)


_TEAM_ERROR_STATUS: dict[type[TeamStoreError], int] = {
    WorkspaceNotFound: 404,
    MemberNotFound: 404,
    PermissionDenied: 403,
    # A state conflict, not malformed input: the request is well-formed and would
    # be accepted once another admin exists.
    LastAdminProtected: 409,
}


def _http_error(error: TeamStoreError) -> HTTPException:
    """Answer with the refusal the store gave, not a blanket 403."""
    status = _TEAM_ERROR_STATUS.get(type(error), 400)
    return HTTPException(status_code=status, detail=str(error))


# ─────────────────────────────────────────────────────────────────────────────
# User accounts
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/users")
async def get_users(request: Request):
    """Accounts with their workspace memberships, so roles are readable in one view."""
    _require_platform_admin(request)
    users = list_users()
    for user in users:
        user["workspaces"] = list_memberships_for_user(str(user.get("email") or ""))
    return {"users": users, "workspace_roles": list(ROLES)}


@router.post("/users")
async def post_user(body: UserCreateBody, request: Request):
    """Create a login. Returns a one-time password when the admin did not set one."""
    actor = _require_platform_admin(request)
    try:
        user, issued = create_user(
            email=body.email,
            name=body.name,
            role=body.platform_role,
            password=body.password,
            created_by=actor,
        )
    except UserStoreError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    membership = None
    if body.workspace_id:
        if not get_workspace(body.workspace_id):
            raise HTTPException(status_code=404, detail="Workspace not found")
        try:
            membership = add_workspace_member(
                workspace_id=body.workspace_id,
                email=user["email"],
                role=body.workspace_role,
                added_by=actor,
                actor_is_platform_admin=True,
            ).to_dict()
        except TeamStoreError as e:
            raise _http_error(e) from e
    _audit(
        "team.user.create",
        actor=actor,
        resource="/team/users",
        level="success",
        details={
            "email": user["email"],
            "platform_role": user["role"],
            "workspace_id": body.workspace_id,
            "workspace_role": body.workspace_role if body.workspace_id else "",
            "password_issued": issued is not None,
        },
    )
    return {"user": user, "membership": membership, "temporary_password": issued}


@router.patch("/users/{email}")
async def patch_user(email: str, body: UserUpdateBody, request: Request):
    actor = _require_platform_admin(request)
    target = normalize_email(email)
    if target == actor and body.status == "disabled":
        raise HTTPException(status_code=400, detail="You cannot disable your own account")
    if target == actor and body.platform_role == "member":
        raise HTTPException(status_code=400, detail="You cannot drop your own administrator role")
    try:
        user = update_user(
            email=target,
            name=body.name,
            role=body.platform_role,
            status=body.status,
        )
    except UserStoreError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    _audit(
        "team.user.update",
        actor=actor,
        resource=f"/team/users/{target}",
        level="info",
        details={"email": target, "platform_role": user["role"], "status": user["status"]},
    )
    return {"user": user}


@router.post("/users/{email}/password")
async def post_user_password(email: str, body: PasswordBody, request: Request):
    actor = _require_platform_admin(request)
    try:
        user = set_password(email=email, password=body.password)
    except UserStoreError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    _audit(
        "team.user.password_set",
        actor=actor,
        resource=f"/team/users/{normalize_email(email)}",
        level="warn",
        details={"email": user["email"]},
    )
    return {"user": user}


@router.post("/users/{email}/reset-password")
async def post_user_reset_password(email: str, request: Request):
    """Issue a one-time password; the account must change it at next sign-in."""
    actor = _require_platform_admin(request)
    try:
        user, issued = reset_password(email=email)
    except UserStoreError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    _audit(
        "team.user.password_reset",
        actor=actor,
        resource=f"/team/users/{user['email']}",
        level="warn",
        details={"email": user["email"]},
    )
    return {"user": user, "temporary_password": issued}


@router.delete("/users/{email}")
async def delete_user_route(email: str, request: Request):
    actor = _require_platform_admin(request)
    target = normalize_email(email)
    if target == actor:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")
    if not get_user(target):
        raise HTTPException(status_code=404, detail="Account not found")
    delete_user(email=target)
    _audit(
        "team.user.delete",
        actor=actor,
        resource=f"/team/users/{target}",
        level="warn",
        details={"email": target},
    )
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Workspaces
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/workspaces")
async def get_workspaces(request: Request):
    """Workspaces the caller may act in — every one for a platform administrator."""
    actor = _actor(request)
    platform_admin = _is_platform_admin(actor)
    workspaces = list_workspaces() if platform_admin else list_workspaces_for_user(actor)
    return {
        "workspaces": [
            {**ws.to_dict(), "member_count": len(list_workspace_members(ws.id))} for ws in workspaces
        ],
        "platform_admin": platform_admin,
        "actor": actor,
    }


@router.post("/workspaces")
async def post_workspace(body: WorkspaceBody, request: Request):
    actor = _require_platform_admin(request)
    ws = create_workspace(name=body.name, created_by=actor)
    _audit(
        "team.workspace.create",
        actor=actor,
        resource=f"/team/workspaces/{ws.id}",
        level="success",
        details={"workspace_id": ws.id, "name": ws.name},
    )
    return {"workspace": ws.to_dict()}


@router.patch("/workspaces/{workspace_id}")
async def patch_workspace(workspace_id: str, body: WorkspaceBody, request: Request):
    actor = _require_platform_admin(request)
    ws = rename_workspace(workspace_id=workspace_id, name=body.name)
    if not ws:
        raise HTTPException(status_code=404, detail="Workspace not found")
    _audit(
        "team.workspace.rename",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}",
        level="info",
        details={"workspace_id": workspace_id, "name": ws.name},
    )
    return {"workspace": ws.to_dict()}


@router.delete("/workspaces/{workspace_id}")
async def delete_workspace_route(workspace_id: str, request: Request):
    actor = _require_platform_admin(request)
    members = list_workspace_members(workspace_id)
    if not delete_workspace(workspace_id=workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    _audit(
        "team.workspace.delete",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}",
        level="warn",
        details={"workspace_id": workspace_id, "members_removed": len(members)},
    )
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Members
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/workspaces/{workspace_id}/members")
async def get_members(workspace_id: str, request: Request):
    actor = _actor(request)
    platform_admin = _is_platform_admin(actor)
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    if not platform_admin and not can_read_workspace(workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    members = list_workspace_members(workspace_id)
    accounts = {str(u.get("email")): u for u in list_users()}
    for member in members:
        account = accounts.get(member["email"])
        if account is not None:
            member["has_account"] = True
            member["account_status"] = account.get("status")
            member["name"] = account.get("name")
            continue
        # Environment-provisioned identities (bootstrap admin, DATAFLOW_AUTH_USERS)
        # can sign in without a stored account — reporting "no account" for them
        # would tell an operator to re-invite someone who is already logged in.
        env_user = lookup_user(member["email"])
        member["has_account"] = env_user is not None
        member["account_status"] = "provisioned" if env_user else "no_account"
        member["name"] = str(env_user.get("name", "")) if env_user else ""
    return {"members": members, "roles": list(ROLES)}


@router.post("/workspaces/{workspace_id}/members")
async def post_member(workspace_id: str, body: MemberBody, request: Request):
    """Add a member, optionally creating their login in the same step.

    An email with no account cannot sign in, so the response says so explicitly
    rather than leaving an admin to discover it at the login screen.
    """
    actor = _actor(request)
    platform_admin = _is_platform_admin(actor)
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    email = normalize_email(body.email)
    account = get_user(email)
    issued: str | None = None
    if account is None and body.create_account:
        try:
            account, issued = create_user(
                email=email,
                name=body.name,
                role="member",
                created_by=actor,
            )
        except UserStoreError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
    try:
        membership = add_workspace_member(
            workspace_id=workspace_id,
            email=email,
            role=body.role,
            added_by=actor,
            actor_is_platform_admin=platform_admin,
        )
    except TeamStoreError as e:
        raise _http_error(e) from e
    _audit(
        "team.member.add",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}/members",
        level="success",
        details={
            "workspace_id": workspace_id,
            "email": email,
            "role": membership.role,
            "account_created": issued is not None,
        },
    )
    return {
        "membership": membership.to_dict(),
        "has_account": account is not None,
        "temporary_password": issued,
    }


@router.patch("/workspaces/{workspace_id}/members/{email}")
async def patch_member_role(
    workspace_id: str, email: str, body: MemberRoleBody, request: Request
):
    """Change an existing member's role without re-inviting them."""
    actor = _actor(request)
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    target = normalize_email(email)
    previous = get_workspace_role(workspace_id=workspace_id, email=target)
    if not previous:
        raise HTTPException(
            status_code=404, detail=f"{target} is not a member of this workspace"
        )
    try:
        membership = add_workspace_member(
            workspace_id=workspace_id,
            email=target,
            role=body.role,
            added_by=actor,
            actor_is_platform_admin=_is_platform_admin(actor),
        )
    except TeamStoreError as e:
        raise _http_error(e) from e
    _audit(
        "team.member.role_change",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}/members/{target}",
        level="warn",
        details={
            "workspace_id": workspace_id,
            "email": target,
            "from_role": previous,
            "to_role": membership.role,
        },
    )
    return {"membership": membership.to_dict()}


@router.delete("/workspaces/{workspace_id}/members/{email}")
async def delete_member(workspace_id: str, email: str, request: Request):
    actor = _actor(request)
    try:
        remove_workspace_member(
            workspace_id=workspace_id,
            email=email,
            removed_by=actor,
            actor_is_platform_admin=_is_platform_admin(actor),
        )
    except TeamStoreError as e:
        raise _http_error(e) from e
    _audit(
        "team.member.remove",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}/members/{normalize_email(email)}",
        level="warn",
        details={"workspace_id": workspace_id, "email": normalize_email(email)},
    )
    return {"ok": True}
