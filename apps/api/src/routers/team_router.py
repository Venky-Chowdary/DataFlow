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
    MemberAlreadyExists,
    MemberNotFound,
    PermissionDenied,
    TeamStoreBusy,
    TeamStoreError,
    WorkspaceNotFound,
    add_workspace_member,
    break_glass_set_admin,
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
    validate_email,
)
from src.services.auth_service import lookup_user

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


def _is_platform_admin(request: Request) -> bool:
    """Whether the *request principal* administers the deployment.

    Decided on the identity the auth middleware attached — for an API key, the
    key itself (``tenant_scope.actor_is_unrestricted``: an ``admin`` key minted
    by a platform admin). Looking the creator's email up instead made every
    key a platform admin minted, even a viewer key, a platform admin here.
    """
    from services.tenant_scope import actor_is_unrestricted

    user = getattr(request.state, "user", None)
    if user is None:
        email = getattr(request.state, "user_email", None)
        user = lookup_user(email) if email else None
    return actor_is_unrestricted(user)


def _require_platform_admin(request: Request) -> str:
    actor = _actor(request)
    if not _is_platform_admin(request):
        raise HTTPException(status_code=403, detail="Platform administrator role required")
    return actor


def _principal(request: Request, workspace_id: str):
    from services.effective_principal import request_principal

    return request_principal(request, workspace_id)


def _audit_refusal(
    error: TeamStoreError,
    *,
    principal: Any,
    operation: str,
    workspace_id: str,
    target: str,
    attempted_role: str | None,
    from_role: str = "",
) -> None:
    """Every refused membership change names who tried what on whom."""
    if isinstance(error, (WorkspaceNotFound, MemberAlreadyExists, TeamStoreBusy)):
        return
    _audit(
        "team.member.denied",
        actor=principal.email,
        resource=f"/team/workspaces/{workspace_id}/members/{target}",
        level="warn",
        details={
            "workspace_id": workspace_id,
            "operation": operation,
            "target": target,
            "attempted_role": attempted_role,
            "from_role": from_role or None,
            "reason": getattr(error, "reason", "refused"),
            "detail": str(error),
            "principal": principal.describe(),
        },
    )


def _audit(action: str, *, actor: str, resource: str, level: str, details: dict[str, Any]) -> None:
    append_audit_event(action=action, resource=resource, actor=actor, level=level, details=details)


_TEAM_ERROR_STATUS: dict[type[TeamStoreError], int] = {
    WorkspaceNotFound: 404,
    MemberNotFound: 404,
    PermissionDenied: 403,
    TeamStoreBusy: 503,
    # A state conflict, not malformed input: the request is well-formed and would
    # be accepted once another admin exists.
    LastAdminProtected: 409,
    # Same class of answer: the membership this add would create is already there.
    MemberAlreadyExists: 409,
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
            "password_issued_present": issued is not None,
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
    platform_admin = _is_platform_admin(request)
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
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    if not _principal(request, workspace_id).member_role:
        raise HTTPException(status_code=403, detail="Workspace access denied")
    members = list_workspace_members(workspace_id)
    accounts = {str(u.get("email")): u for u in list_users()}
    for member in members:
        account = accounts.get(member["email"])
        invited_name = str(member.get("name") or "")
        if account is not None:
            member["has_account"] = True
            member["account_status"] = account.get("status")
            # The member's own account name wins; the invite name fills a blank
            # or the placeholder create_user derives from the email local part.
            own = str(account.get("name") or "").strip()
            placeholder = own == member["email"].split("@")[0]
            member["name"] = (invited_name or own) if (not own or placeholder) else own
            continue
        # Environment-provisioned identities (bootstrap admin, DATAFLOW_AUTH_USERS)
        # can sign in without a stored account — reporting "no account" for them
        # would tell an operator to re-invite someone who is already logged in.
        env_user = lookup_user(member["email"])
        member["has_account"] = env_user is not None
        member["account_status"] = "provisioned" if env_user else "no_account"
        env_name = str(env_user.get("name", "")).strip() if env_user else ""
        member["name"] = env_name or invited_name
    return {"members": members, "roles": list(ROLES)}


@router.post("/workspaces/{workspace_id}/members")
async def post_member(workspace_id: str, body: MemberBody, request: Request):
    """Add a member, optionally creating their login in the same step.

    An email with no account cannot sign in, so the response says so explicitly
    rather than leaving an admin to discover it at the login screen.
    """
    actor = _actor(request)
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    try:
        email = validate_email(body.email)
    except UserStoreError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    principal = _principal(request, workspace_id)
    # Membership first, so authority and the duplicate refusal are both decided
    # before a login is minted: creating an account for someone who is already a
    # member left a fresh one-time password behind a 409 nobody could act on.
    try:
        membership = add_workspace_member(
            workspace_id=workspace_id,
            email=email,
            role=body.role,
            added_by=actor,
            refuse_existing=True,
            name=body.name,
            principal=principal,
        )
    except TeamStoreError as e:
        _audit_refusal(
            e,
            principal=principal,
            operation="add",
            workspace_id=workspace_id,
            target=email,
            attempted_role=body.role,
        )
        raise _http_error(e) from e
    account = get_user(email)
    issued: str | None = None
    if account is None and body.create_account:
        try:
            account, issued = create_user(
                email=email,
                name=membership.name,
                role="member",
                created_by=actor,
            )
        except UserStoreError as e:
            # The membership stands and is reported as "no login yet" rather than
            # being rolled back into a silent no-op.
            raise HTTPException(status_code=400, detail=str(e)) from e
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
    principal = _principal(request, workspace_id)
    try:
        membership = add_workspace_member(
            workspace_id=workspace_id,
            email=target,
            role=body.role,
            added_by=actor,
            principal=principal,
        )
    except TeamStoreError as e:
        _audit_refusal(
            e,
            principal=principal,
            operation="role_change",
            workspace_id=workspace_id,
            target=target,
            attempted_role=body.role,
            from_role=previous,
        )
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
    principal = _principal(request, workspace_id)
    previous = get_workspace_role(workspace_id=workspace_id, email=email)
    try:
        remove_workspace_member(
            workspace_id=workspace_id,
            email=email,
            removed_by=actor,
            principal=principal,
        )
    except TeamStoreError as e:
        _audit_refusal(
            e,
            principal=principal,
            operation="remove",
            workspace_id=workspace_id,
            target=normalize_email(email),
            attempted_role=None,
            from_role=previous,
        )
        raise _http_error(e) from e
    _audit(
        "team.member.remove",
        actor=actor,
        resource=f"/team/workspaces/{workspace_id}/members/{normalize_email(email)}",
        level="warn",
        details={"workspace_id": workspace_id, "email": normalize_email(email)},
    )
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Break-glass recovery
# ─────────────────────────────────────────────────────────────────────────────


class BreakGlassRestoreBody(BaseModel):
    email: str = Field(..., max_length=254)
    reason: str = Field(..., min_length=1, max_length=500)


@router.post("/workspaces/{workspace_id}/break-glass/restore-admin")
async def break_glass_restore_admin(
    workspace_id: str, body: BreakGlassRestoreBody, request: Request
):
    """Restore a workspace admin when nobody who can sign in is admin any more.

    The ordinary role routes cannot do this once the last real admin is gone:
    they need an admin of that workspace. Before this route the only fix was a
    database edit by support. It is deliberately narrow:

    - a platform administrator, signed in as themselves — never an API key,
      even one a platform admin minted (automation does not break glass);
    - a written reason, recorded verbatim;
    - the person restored must be able to sign in, or the lockout continues;
    - every attempt, granted or refused, is an audit event at ``warn``.
    """
    principal = _principal(request, workspace_id)
    target = normalize_email(body.email)
    reason = " ".join(body.reason.split())

    def refuse(status: int, detail: str, code: str) -> HTTPException:
        _audit(
            "team.break_glass.denied",
            actor=principal.email,
            resource=f"/team/workspaces/{workspace_id}/break-glass/restore-admin",
            level="warn",
            details={
                "workspace_id": workspace_id,
                "target": target,
                "attempted_role": "admin",
                "reason_code": code,
                "reason": reason,
                "principal": principal.describe(),
            },
        )
        return HTTPException(status_code=status, detail=detail)

    if not reason:
        raise HTTPException(status_code=422, detail="A reason is required for break-glass")
    if principal.kind != "session" or not _is_platform_admin(request):
        raise refuse(
            403,
            "Break-glass requires a platform administrator signed in as themselves",
            "not_platform_admin_session",
        )
    if not get_workspace(workspace_id):
        raise HTTPException(status_code=404, detail="Workspace not found")
    try:
        target = validate_email(target)
    except UserStoreError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    from services.sign_in_capability import can_sign_in

    if not can_sign_in(target):
        raise refuse(
            400,
            f"{target} cannot sign in, so restoring them would not end the lockout — "
            "create or re-enable their account first",
            "target_cannot_sign_in",
        )
    try:
        previous, membership = break_glass_set_admin(
            workspace_id=workspace_id, email=target, actor=principal.email
        )
    except TeamStoreError as e:
        raise _http_error(e) from e
    _audit(
        "team.break_glass.restore_admin",
        actor=principal.email,
        resource=f"/team/workspaces/{workspace_id}/members/{target}",
        level="warn",
        details={
            "workspace_id": workspace_id,
            "target": target,
            "from_role": previous or None,
            "to_role": membership.role,
            "reason": reason,
            "principal": principal.describe(),
        },
    )
    return {"membership": membership.to_dict(), "from_role": previous or None}
