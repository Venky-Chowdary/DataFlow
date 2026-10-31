"""IAM routes for scoped service accounts and API-key rotation."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from services import audit_log, integrations_store, scim_service, team_store
from services.effective_role import (
    membership_role_to_gate_role,
    resolve_effective_role,
    workspace_id_from_request_headers,
)
from services.rbac import (
    PrivilegeEscalation,
    assert_grant_within,
    principal_permissions,
    role_permissions,
)
from services.workspace_access import actor_email, tenant_scope
from src.services import auth_service as _auth_service

router = APIRouter(prefix="/iam", tags=["IAM"])


class ServiceAccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    role: str = Field(min_length=1, max_length=32)
    scopes: list[str] = Field(max_length=64)
    expires_in: str = Field(
        default=integrations_store.DEFAULT_API_KEY_LIFETIME,
        min_length=1,
        max_length=16,
    )


class ApiKeyRotation(BaseModel):
    # 0 = the old key is revoked at once. A grace is opt-in, short, listed
    # (``grace_until``) and revocable.
    overlap_seconds: int = Field(
        default=0, ge=0, le=integrations_store.MAX_ROTATION_GRACE_SECONDS
    )


class RotatedKeyRepair(BaseModel):
    dry_run: bool = True


class ScimTokenCreate(BaseModel):
    name: str = Field(default="SCIM provisioning token", min_length=1, max_length=64)
    expires_in: str = Field(
        default=integrations_store.DEFAULT_API_KEY_LIFETIME,
        min_length=1,
        max_length=16,
    )


class ScimGroupMapping(BaseModel):
    workspace_id: str = Field(min_length=1, max_length=128)
    role: str = Field(min_length=1, max_length=32)


class ScimGroupMappingsUpdate(BaseModel):
    mappings: dict[str, ScimGroupMapping]


def _audit_key_details(key: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": key.get("id"),
        "prefix": key.get("prefix"),
        "kind": key.get("kind") or "api_key",
        "role": key.get("role"),
        "scopes": key.get("scopes"),
        "expires_at": key.get("expires_at"),
        "rotated_from": key.get("rotated_from"),
        "rotated_to": key.get("rotated_to"),
    }


def _caller_permissions(request: Request) -> set[str]:
    user = getattr(request.state, "user", None)
    role = getattr(request.state, "effective_role", None)
    if not role:
        role = resolve_effective_role(
            user,
            workspace_id_from_request_headers(request.headers),
        )
    return principal_permissions(user, role)


def enforce_grant_bounds(
    request: Request,
    *,
    route: str,
    target_role: str,
    target_scopes: list[str] | None,
) -> None:
    if not _auth_service.auth_required():
        return

    # What the minted credential will carry: the target role's full table
    # (a key's own role is its platform role), narrowed by its scopes.
    granted = set(role_permissions(target_role))
    if target_scopes is not None:
        granted &= {scope for scope in target_scopes if isinstance(scope, str)}
    try:
        assert_grant_within(_caller_permissions(request), granted)
    except PrivilegeEscalation as exc:
        actor = actor_email(request)
        audit_log.append_audit_event(
            action="iam.privilege_escalation.denied",
            resource=route,
            actor=actor,
            level="warn",
            details={
                "route": route,
                "caller": actor,
                "target_role": target_role,
                "target_scopes": target_scopes,
                "missing": list(exc.missing),
            },
        )
        raise HTTPException(
            status_code=403,
            detail=(
                "Cannot grant permissions you do not hold: "
                + ", ".join(exc.missing)
            ),
        ) from exc


@router.post("/service-accounts")
def create_service_account(body: ServiceAccountCreate, request: Request):
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name must not be blank")
    enforce_grant_bounds(
        request,
        route="POST /api/v1/iam/service-accounts",
        target_role=body.role,
        target_scopes=body.scopes,
    )
    actor = actor_email(request)
    try:
        key = integrations_store.create_api_key(
            body.name,
            actor,
            role=body.role,
            expires_in=body.expires_in,
            scopes=body.scopes,
            kind="service_account",
            workspace_id=tenant_scope(request).workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit_log.append_audit_event(
        action="iam.service_account.create",
        resource=f"service_account:{key['id']}",
        actor=actor,
        level="info",
        details=_audit_key_details(key),
    )
    return key


@router.get("/service-accounts")
def list_service_accounts(request: Request):
    # The key store is deployment-wide: list only what this caller's workspace
    # owns (every key for a platform admin), like GET /workspace/api-keys.
    return [
        key
        for key in integrations_store.list_api_keys()
        if key.get("kind") in {"service_account", "scim"} and _key_visible(request, key)
    ]


def _key_visible(request: Request, key: dict[str, Any]) -> bool:
    """The same visibility as the workspace key list: own workspace, or platform admin."""
    from src.routers.workspace_router import _own_workspace_key

    return _own_workspace_key(request, key)


@router.post("/api-keys/{key_id}/rotate")
def rotate_api_key(key_id: str, body: ApiKeyRotation, request: Request):
    # Read revoked keys too, so re-rotating a key that was already rotated
    # (and therefore revoked) answers "already rotated", not "not found".
    existing = next(
        (
            integrations_store._public_api_key(item)
            for item in integrations_store.load_api_key_records()
            if item.get("id") == key_id
        ),
        None,
    )
    # A key in another workspace is not there for this caller: rotating it
    # minted a credential the caller owned for someone else's workspace.
    if existing is None or not _key_visible(request, existing):
        raise HTTPException(status_code=404, detail="API key not found")
    if existing is not None:
        enforce_grant_bounds(
            request,
            route=f"POST /api/v1/iam/api-keys/{key_id}/rotate",
            target_role=existing.get("role") or "viewer",
            target_scopes=existing.get("scopes"),
        )
    actor = actor_email(request)
    try:
        key = integrations_store.rotate_api_key(
            key_id,
            actor=actor,
            overlap_seconds=body.overlap_seconds,
        )
    except integrations_store.ApiKeyNotFound as exc:
        raise HTTPException(status_code=404, detail="API key not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    details = _audit_key_details(key)
    details["rotated_to"] = key["id"]
    audit_log.append_audit_event(
        action="iam.api_key.rotate",
        resource=f"api_key:{key_id}",
        actor=actor,
        level="info",
        details=details,
    )
    return key


@router.post("/api-keys/repair-rotated")
def repair_rotated_api_keys(request: Request, body: RotatedKeyRepair | None = None):
    """Platform-admin repair for keys the pre-fix rotate endpoint orphaned.

    Dry run by default: reports each orphan and the workspace/owner it would
    get back. ``{"dry_run": false}`` applies it, after which the keys are
    listed in their workspace and revocable with DELETE.
    """
    from services.tenant_scope import actor_is_unrestricted

    user = getattr(request.state, "user", None)
    if not actor_is_unrestricted(user):
        raise HTTPException(status_code=403, detail="Platform administrator role required")
    dry_run = True if body is None else body.dry_run
    repairs = integrations_store.repair_rotated_api_keys(dry_run=dry_run)
    audit_log.append_audit_event(
        action="iam.api_key.repair_rotated",
        resource="api_keys",
        actor=actor_email(request),
        level="info" if dry_run else "warn",
        details={"dry_run": dry_run, "repairs": repairs},
    )
    return {"dry_run": dry_run, "repairs": repairs}


@router.delete("/service-accounts/{key_id}")
def revoke_service_account(key_id: str, request: Request):
    actor = actor_email(request)
    key = next(
        (
            item
            for item in integrations_store.list_api_keys()
            if item.get("id") == key_id
            and item.get("kind") in {"service_account", "scim"}
        ),
        None,
    )
    if key is None or not _key_visible(request, key):
        raise HTTPException(status_code=404, detail="Service account not found")
    try:
        if not integrations_store.revoke_api_key(key_id):
            raise integrations_store.ApiKeyNotFound(key_id)
    except integrations_store.ApiKeyNotFound as exc:
        raise HTTPException(status_code=404, detail="Service account not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit_log.append_audit_event(
        action="iam.service_account.revoke",
        resource=f"service_account:{key_id}",
        actor=actor,
        level="info",
        details=_audit_key_details(key),
    )
    return {"id": key_id, "revoked": True}


@router.post("/scim-token")
def create_scim_token(request: Request, body: ScimTokenCreate | None = None):
    enforce_grant_bounds(
        request,
        route="POST /api/v1/iam/scim-token",
        target_role="admin",
        target_scopes=["scim.provision"],
    )
    actor = actor_email(request)
    try:
        key = integrations_store.create_api_key(
            (body.name if body else "SCIM provisioning token"),
            actor,
            role="admin",
            expires_in=(body.expires_in if body else integrations_store.DEFAULT_API_KEY_LIFETIME),
            scopes=["scim.provision"],
            kind="scim",
            workspace_id=tenant_scope(request).workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit_log.append_audit_event(
        action="iam.scim.token.create",
        resource=f"scim_token:{key['id']}",
        actor=actor,
        level="info",
        details=_audit_key_details(key),
    )
    return key


@router.get("/scim/group-mappings")
def get_scim_group_mappings():
    try:
        return {"mappings": scim_service.get_group_mappings()}
    except scim_service.ScimError as exc:
        raise HTTPException(status_code=503, detail="SCIM storage is unavailable") from exc


@router.put("/scim/group-mappings")
def put_scim_group_mappings(body: ScimGroupMappingsUpdate, request: Request):
    mappings: dict[str, dict[str, str]] = {}
    for display_name, mapping in body.mappings.items():
        key = display_name.strip().lower()
        if not key:
            raise HTTPException(status_code=400, detail="Group displayName is required")
        if mapping.role not in team_store.ROLES:
            raise HTTPException(status_code=400, detail="role is not a supported workspace role")
        if team_store.get_workspace(mapping.workspace_id) is None:
            raise HTTPException(status_code=400, detail="workspace_id does not exist")
        enforce_grant_bounds(
            request,
            route="PUT /api/v1/iam/scim/group-mappings",
            target_role=membership_role_to_gate_role(mapping.role),
            target_scopes=None,
        )
        mappings[key] = {
            "workspace_id": mapping.workspace_id,
            "role": mapping.role,
        }
    actor = actor_email(request)
    try:
        scim_service.set_group_mappings(mappings)
    except scim_service.ScimError as exc:
        raise HTTPException(status_code=500, detail="SCIM storage is unavailable") from exc
    audit_log.append_audit_event(
        action="iam.scim.group_mappings.update",
        resource="scim_group_mappings",
        actor=actor,
        level="info",
        details={"mappings": mappings},
    )
    return {"mappings": mappings}
