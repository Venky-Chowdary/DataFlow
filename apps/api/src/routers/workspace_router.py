"""Workspace settings API — persisted org profile, SSO, AI keys, API keys."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from services.team_store import (
    add_workspace_member,
    can_admin_workspace,
    can_read_workspace,
    can_write_workspace,
    create_workspace,
    get_workspace,
    list_workspace_members,
    list_workspaces_for_user,
    remove_workspace_member,
)

router = APIRouter(prefix="/workspace", tags=["Workspace"])


class WorkspaceCreateBody(BaseModel):
    name: str = Field(default="", max_length=128)


class MemberAddBody(BaseModel):
    email: str = Field(..., max_length=128)
    role: str = Field(default="viewer", pattern="^(owner|editor|viewer)$")


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


# ═══════════════════════════════════════════════════════════════════════════════
# Workspace / team management
# ═══════════════════════════════════════════════════════════════════════════════


@router.post("/workspaces")
async def post_workspace(body: WorkspaceCreateBody, request: Request):
    ws = create_workspace(name=body.name, created_by=_actor(request))
    return ws.to_dict()


@router.get("/workspaces")
async def get_workspaces(request: Request):
    return {"workspaces": [ws.to_dict() for ws in list_workspaces_for_user(_actor(request))]}


@router.get("/workspaces/{workspace_id}")
async def get_workspace_by_id(workspace_id: str, request: Request):
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return ws.to_dict()


@router.get("/workspaces/{workspace_id}/members")
async def get_workspace_members_route(workspace_id: str, request: Request):
    if not can_admin_workspace(workspace_id, _actor(request)) and not any(
        m["workspace_id"] == workspace_id
        for m in list_workspace_members(workspace_id)
        if m["email"] == _actor(request)
    ):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    return {"members": list_workspace_members(workspace_id)}


@router.post("/workspaces/{workspace_id}/members")
async def post_workspace_member(workspace_id: str, body: MemberAddBody, request: Request):
    membership = add_workspace_member(
        workspace_id=workspace_id,
        email=body.email,
        role=body.role,
        added_by=_actor(request),
    )
    if not membership:
        raise HTTPException(status_code=403, detail="Unable to add member")
    return membership.to_dict()


@router.delete("/workspaces/{workspace_id}/members/{email}")
async def delete_workspace_member(workspace_id: str, email: str, request: Request):
    if not remove_workspace_member(
        workspace_id=workspace_id,
        email=email,
        removed_by=_actor(request),
    ):
        raise HTTPException(status_code=403, detail="Unable to remove member")
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════════
# Notification channels (Slack / Teams / Email / ServiceNow / Webhook)
# ═══════════════════════════════════════════════════════════════════════════════


class NotificationChannelCreate(BaseModel):
    workspace_id: str = Field(default="", max_length=128)
    kind: str = Field(..., pattern="^(slack|teams|email|servicenow|webhook)$")
    label: str = Field(default="", max_length=128)
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class NotificationChannelUpdate(BaseModel):
    label: str | None = Field(default=None, max_length=128)
    enabled: bool | None = None
    config: dict[str, Any] | None = None


@router.get("/notifications")
async def get_notifications(workspace_id: str | None = None, request: Request = None):
    actor = _actor(request) if request else "anonymous"
    if workspace_id and not can_read_workspace(workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    from services.notification_store import list_channels

    channels = list_channels(workspace_id=workspace_id or "")
    return {"channels": [c.to_dict() for c in channels]}


@router.post("/notifications")
async def post_notification(body: NotificationChannelCreate, request: Request):
    actor = _actor(request)
    ws_id = body.workspace_id or ""
    if ws_id and not can_write_workspace(ws_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    from services.notification_store import create_channel

    try:
        channel = create_channel(
            workspace_id=ws_id,
            kind=body.kind,
            label=body.label,
            enabled=body.enabled,
            config=body.config,
            created_by=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return channel.to_dict()


@router.patch("/notifications/{channel_id}")
async def patch_notification(channel_id: str, body: NotificationChannelUpdate, request: Request):
    actor = _actor(request)
    from services.notification_store import get_channel, update_channel

    channel = get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.workspace_id and not can_write_workspace(channel.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    updates = body.model_dump(exclude_none=True)
    updated = update_channel(channel_id, updates=updates)
    if not updated:
        raise HTTPException(status_code=500, detail="Update failed")
    return updated.to_dict()


@router.delete("/notifications/{channel_id}")
async def delete_notification(channel_id: str, request: Request):
    actor = _actor(request)
    from services.notification_store import delete_channel, get_channel

    channel = get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.workspace_id and not can_write_workspace(channel.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    if not delete_channel(channel_id):
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"ok": True}


@router.post("/notifications/{channel_id}/test")
async def test_notification(channel_id: str, request: Request):
    actor = _actor(request)
    from services.notification_service import build_job_payload, send_to_channel
    from services.notification_store import get_channel_decrypted

    channel = get_channel_decrypted(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.workspace_id and not can_write_workspace(channel.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    payload = build_job_payload(
        job_id="test",
        status="completed",
        source="test_source",
        destination="test_destination",
        records_transferred=42,
        rejected_rows=0,
        error="",
        retry_url="",
    )
    result = send_to_channel(channel, payload)
    return {"success": result.get("ok"), "detail": result}
