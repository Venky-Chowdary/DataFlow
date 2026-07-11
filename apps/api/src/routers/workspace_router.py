"""Workspace settings API — persisted org profile, SSO, AI keys, API keys."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/workspace", tags=["Workspace"])


class WorkspaceSettingsBody(BaseModel):
    org_name: str | None = Field(default=None, max_length=128)
    timezone: str | None = Field(default=None, max_length=64)
    retention_days: int | None = Field(default=None, ge=1, le=3650)


class SsoConfigBody(BaseModel):
    enabled: bool | None = None
    entity_id: str | None = None
    sso_url: str | None = None
    x509_cert: str | None = None
    email_attribute: str | None = None
    issuer: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    redirect_uri: str | None = None
    scopes: str | None = None
    tenant_id: str | None = None


class AiProviderBody(BaseModel):
    enabled: bool | None = None
    api_key: str | None = None
    model: str | None = None
    base_url: str | None = None


class ApiKeyCreateBody(BaseModel):
    name: str = Field(default="API key", max_length=64)


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


@router.get("/settings")
async def get_settings():
    from services.workspace_store import get_workspace_settings

    return get_workspace_settings()


@router.patch("/settings")
async def patch_settings(body: WorkspaceSettingsBody, request: Request):
    from services.audit_log import append_audit_event
    from services.workspace_store import update_workspace_settings

    actor = _actor(request)
    updated = update_workspace_settings(
        org_name=body.org_name,
        timezone=body.timezone,
        retention_days=body.retention_days,
        actor=actor,
    )
    append_audit_event(
        action="workspace.settings.update",
        resource="/workspace/settings",
        actor=actor,
        level="info",
        details={
            "org_name": updated["org_name"],
            "timezone": updated["timezone"],
            "retention_days": updated["retention_days"],
        },
    )
    return updated


@router.get("/sso")
async def get_sso():
    from services.integrations_store import get_sso_configs

    configs = get_sso_configs()
    return {"providers": configs}


@router.patch("/sso/{sso_type}")
async def patch_sso(sso_type: str, body: SsoConfigBody, request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import update_sso_config, validate_sso_config

    try:
        updated = update_sso_config(sso_type, body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    check = validate_sso_config(sso_type)
    append_audit_event(
        action="workspace.sso.update",
        resource=f"/workspace/sso/{sso_type}",
        actor=_actor(request),
        level="info",
        details={"type": sso_type, "enabled": updated.get("enabled"), "ready": check["ready"]},
    )
    return {"config": updated, "validation": check}


@router.post("/sso/{sso_type}/test")
async def test_sso(sso_type: str):
    from services.integrations_store import validate_sso_config

    return validate_sso_config(sso_type)


@router.get("/ai-providers")
async def get_ai_providers():
    from services.integrations_store import get_ai_provider_configs

    return {"providers": get_ai_provider_configs()}


@router.patch("/ai-providers/{provider}")
async def patch_ai_provider(provider: str, body: AiProviderBody, request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import update_ai_provider

    try:
        updated = update_ai_provider(provider, body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    append_audit_event(
        action="workspace.ai_provider.update",
        resource=f"/workspace/ai-providers/{provider}",
        actor=_actor(request),
        level="info",
        details={"provider": provider, "enabled": updated.get("enabled"), "model": updated.get("model")},
    )
    return updated


@router.get("/api-keys")
async def get_api_keys():
    from services.integrations_store import list_api_keys

    return {"keys": list_api_keys()}


@router.post("/api-keys")
async def post_api_key(body: ApiKeyCreateBody, request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import create_api_key

    actor = _actor(request)
    created = create_api_key(body.name, actor)
    append_audit_event(
        action="workspace.api_key.create",
        resource="/workspace/api-keys",
        actor=actor,
        level="success",
        details={"id": created["id"], "name": created["name"], "prefix": created["prefix"]},
    )
    return created


@router.delete("/api-keys/{key_id}")
async def delete_api_key(key_id: str, request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import revoke_api_key

    if not revoke_api_key(key_id):
        raise HTTPException(status_code=404, detail="API key not found")
    append_audit_event(
        action="workspace.api_key.revoke",
        resource=f"/workspace/api-keys/{key_id}",
        actor=_actor(request),
        level="warn",
        details={"id": key_id},
    )
    return {"ok": True}
