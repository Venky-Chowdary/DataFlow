"""IAM routes for scoped service accounts and API-key rotation."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from services import audit_log, integrations_store, scim_service, team_store
from services.workspace_access import actor_email

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
    overlap_seconds: int = Field(default=86400, ge=0, le=604800)


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


@router.post("/service-accounts")
def create_service_account(body: ServiceAccountCreate, request: Request):
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name must not be blank")
    actor = actor_email(request)
    try:
        key = integrations_store.create_api_key(
            body.name,
            actor,
            role=body.role,
            expires_in=body.expires_in,
            scopes=body.scopes,
            kind="service_account",
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
def list_service_accounts():
    return [
        key
        for key in integrations_store.list_api_keys()
        if key.get("kind") in {"service_account", "scim"}
    ]


@router.post("/api-keys/{key_id}/rotate")
def rotate_api_key(key_id: str, body: ApiKeyRotation, request: Request):
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
    if key is None:
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
    actor = actor_email(request)
    try:
        key = integrations_store.create_api_key(
            (body.name if body else "SCIM provisioning token"),
            actor,
            role="admin",
            expires_in=(body.expires_in if body else integrations_store.DEFAULT_API_KEY_LIFETIME),
            scopes=["scim.provision"],
            kind="scim",
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
