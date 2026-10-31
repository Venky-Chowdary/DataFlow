"""Workspace settings API — persisted org profile, SSO, AI keys, API keys."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from services.brand import PRODUCT_SLUG

from services.byok_key_manager import (
    create_key,
    get_key,
    list_keys,
    public_key_dict,
    rotate_key,
)
from services.admin_audit import record_admin_change
from services.effective_principal import request_may_write_workspace
from services.team_store import get_workspace
from services.workspace_access import assert_resource_workspace, tenant_scope
from services.tenant_store import (
    Tenant,
    create_tenant,
    delete_tenant,
    get_tenant,
    get_tenant_for_workspace,
    list_tenants,
    update_tenant,
)

router = APIRouter(prefix="/workspace", tags=["Workspace"])


class WorkspaceSettingsBody(BaseModel):
    org_name: str | None = Field(default=None, max_length=128)
    timezone: str | None = Field(default=None, max_length=64)
    retention_days: int | None = Field(default=None, ge=1, le=3650)
    studio_schema_browser: bool | None = None
    studio_canvas: bool | None = None


class TenantCreateBody(BaseModel):
    workspace_id: str = Field(default="", max_length=128)
    name: str = Field(default="", max_length=128)
    custom_domain: str = Field(default="", max_length=253)
    data_region: str = Field(default="", max_length=32)
    byok_key_id: str = Field(default="", max_length=128)
    security_contact_email: str = Field(default="", max_length=128)
    mfa_required: bool = Field(default=False)
    session_timeout_hours: int = Field(default=8, ge=1, le=24)
    enforce_session_timeout: bool = Field(default=False)
    ip_allowlist: list[str] = Field(default_factory=list)
    egress_allowlist: list[str] = Field(default_factory=list)


class TenantUpdateBody(BaseModel):
    name: str | None = Field(default=None, max_length=128)
    custom_domain: str | None = Field(default=None, max_length=253)
    data_region: str | None = Field(default=None, max_length=32)
    byok_key_id: str | None = Field(default=None, max_length=128)
    security_contact_email: str | None = Field(default=None, max_length=128)
    mfa_required: bool | None = Field(default=None)
    session_timeout_hours: int | None = Field(default=None, ge=1, le=24)
    # Turns session_timeout_hours from recorded policy into an enforced idle timeout.
    enforce_session_timeout: bool | None = Field(default=None)
    ip_allowlist: list[str] | None = Field(default=None)
    egress_allowlist: list[str] | None = Field(default=None)


class BYOKKeyCreateBody(BaseModel):
    label: str = Field(default="", max_length=128)
    provider: str = Field(default="local", pattern="^(local|wrapped|aws_kms|azure_keyvault|gcp_kms)$")
    key_material: str = Field(default="", max_length=4096)


class BenchmarkRequest(BaseModel):
    rows: int = Field(default=100_000, ge=1_000, le=2_000_000)
    format: str = Field(default="json", pattern="^(json|csv|md)$")


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
    # Domains whose sign-ins route to this IdP (``POST /auth/sso/route``).
    email_domains: list[str] | None = Field(default=None, max_length=100)
    # ``client_secret`` blank keeps the stored secret; this removes it.
    clear_client_secret: bool | None = None


class AiProviderBody(BaseModel):
    enabled: bool | None = None
    api_key: str | None = None
    model: str | None = None
    base_url: str | None = None


class PilotEngineBody(BaseModel):
    engine: str = Field(description="auto | local | hybrid | cloud")


class ApiKeyCreateBody(BaseModel):
    name: str = Field(default="API key", max_length=64)
    role: str = Field(
        default="editor",
        description="viewer, operator, editor, or admin. Editor can create connectors, transfers, and schedules.",
    )
    expires_in: str = Field(
        default="90d",
        description="7d, 30d, 60d, 90d, 365d, or never. Logging out does not revoke the key.",
    )
    scopes: list[str] | None = Field(
        default=None,
        max_length=64,
        description=(
            "Optional permission names (e.g. connector.read) narrowing the key below its "
            "role, enforced like service-account scopes. Omit for the full role."
        ),
    )


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def _can_admin_workspace(request: Request, workspace_id: str) -> bool:
    """Whether this caller may administer the workspace the call names.

    Resolved through the same two authorities the API gate uses — the platform
    role and the membership row — so a platform administrator administers every
    workspace and a workspace admin need not be a platform admin. Reading the
    membership row alone refused the platform admin who created the workspace
    and never joined it.
    """
    from src.services import auth_service

    if not auth_service.auth_required():
        return True
    from services.effective_role import resolve_effective_role

    user = getattr(request.state, "user", None)
    if not isinstance(user, dict):
        user = {"email": _actor(request), "role": "viewer"}
    return resolve_effective_role(user, workspace_id) == "admin"


def _is_platform_admin(request: Request) -> bool:
    """Platform admin (or the single operator of an auth-disabled deployment):
    the tenant scope's "unrestricted" actor, so an admin-role API key counts
    only when an admin minted it."""
    from services.tenant_scope import actor_is_unrestricted

    return actor_is_unrestricted(getattr(request.state, "user", None))


def _require_tenant_admin(request: Request, workspace_id: str) -> None:
    """Fail closed: a workspace-bound tenant needs that workspace's admin, an
    unbound one (legacy ``workspace_id: ""``) needs a platform admin."""
    if workspace_id:
        if not _can_admin_workspace(request, workspace_id):
            raise HTTPException(status_code=403, detail="Workspace admin required")
    elif not _is_platform_admin(request):
        raise HTTPException(
            status_code=403,
            detail="Platform admin required for a tenant not bound to a workspace",
        )


def _named_workspace_id(request: Request) -> str:
    return (request.headers.get("x-workspace-id") or "").strip()


@router.get("/settings")
async def get_settings():
    from services.workspace_store import get_workspace_settings

    return {
        **get_workspace_settings(),
        # What retention_days does, stated by the API itself (QA enterprise #4).
        "retention_enforcement": {
            "applies_to": "finished jobs (completed, failed, cancelled) in the requesting workspace",
            "mode": "admin_purge",
            "scheduled": False,
            "purge_route": "POST /api/v1/workspace/jobs/retention/purge (dry run first)",
        },
    }


@router.patch("/settings")
async def patch_settings(body: WorkspaceSettingsBody, request: Request):
    from services.workspace_store import update_workspace_settings

    from services.workspace_store import InvalidTimezone

    actor = _actor(request)
    try:
        updated = update_workspace_settings(
            org_name=body.org_name,
            timezone=body.timezone,
            retention_days=body.retention_days,
            studio_schema_browser=body.studio_schema_browser,
            studio_canvas=body.studio_canvas,
            actor=actor,
        )
    except InvalidTimezone as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    record_admin_change(
        request,
        "workspace.settings.update",
        target="workspace_settings",
        details={
            "org_name": updated["org_name"],
            "timezone": updated["timezone"],
            "retention_days": updated["retention_days"],
            "studio_schema_browser": updated["studio_schema_browser"],
            "studio_canvas": updated["studio_canvas"],
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
    from services.integrations_store import update_sso_config, validate_sso_config

    from services.integrations_store import SsoConfigRefused

    try:
        updated = update_sso_config(sso_type, body.model_dump(exclude_none=True))
    except SsoConfigRefused as exc:
        raise HTTPException(
            status_code=400,
            detail={"message": str(exc), "missing_fields": exc.missing},
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    check = validate_sso_config(sso_type)
    record_admin_change(
        request,
        "workspace.sso.update",
        target=sso_type,
        details={
            "type": sso_type,
            "enabled": updated.get("enabled"),
            "ready": check["ready"],
            "changed_fields": sorted(body.model_dump(exclude_none=True).keys()),
        },
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
def patch_ai_provider(provider: str, body: AiProviderBody, request: Request):
    # Sync on purpose: the live key check does blocking network I/O, so FastAPI
    # must run this in a worker thread instead of on the event loop.
    from services.integrations_store import update_ai_provider

    payload = body.model_dump(exclude_none=True)
    raw_key = str(payload.get("api_key") or "").strip()
    if (
        provider in {"openai", "anthropic"}
        and raw_key
        and not raw_key.startswith("•")
        and not raw_key.startswith("[")
    ):
        from ..ai.llm.provider import clear_auth_failures, verify_cloud_api_key

        clear_auth_failures()
        ok, err = verify_cloud_api_key(provider, raw_key)
        if not ok:
            raise HTTPException(status_code=400, detail=err or "API key rejected")

    try:
        updated = update_ai_provider(provider, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    record_admin_change(
        request,
        "workspace.ai_provider.update",
        target=provider,
        details={"provider": provider, "enabled": updated.get("enabled"), "model": updated.get("model")},
    )
    return updated


@router.delete("/ai-providers/{provider}/key")
def delete_ai_provider_key_route(provider: str, request: Request):
    """Forget the saved key; on auto Pilot returns to the local engine."""
    from services.integrations_store import delete_ai_provider_key

    from ..ai.llm.provider import clear_auth_failures

    try:
        updated = delete_ai_provider_key(provider)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    clear_auth_failures()

    record_admin_change(
        request,
        "workspace.ai_provider.key_removed",
        target=provider,
        details={"provider": provider},
    )
    return updated


@router.post("/ai-providers/{provider}/test")
def test_ai_provider(provider: str):
    """Live-check the stored key for a provider without asking for it again.

    Sync on purpose: the check does blocking network I/O.
    """
    from services.integrations_store import resolve_provider_api_key

    from ..ai.llm.provider import clear_auth_failures, get_model_capabilities, verify_cloud_api_key

    if provider == "ollama":
        return _test_ollama(get_model_capabilities)
    if provider not in {"openai", "anthropic"}:
        raise HTTPException(status_code=400, detail=f"{provider} has no cloud key to test")

    clear_auth_failures()
    key = resolve_provider_api_key(provider)
    if not key:
        return {
            "ok": False,
            "provider": provider,
            "error": "No API key is saved for this provider.",
            "capabilities": get_model_capabilities(),
        }
    ok, err = verify_cloud_api_key(provider, key)
    return {
        "ok": ok,
        "provider": provider,
        "error": "" if ok else (err or "API key rejected"),
        "capabilities": get_model_capabilities(),
    }


def _test_ollama(get_model_capabilities) -> dict[str, Any]:
    """Probe the Ollama server now and check the configured model is pulled."""
    from services.integrations_store import (
        ai_provider_enabled,
        ollama_status,
        resolve_provider_model,
    )

    status = ollama_status(fresh=True)
    model = resolve_provider_model("ollama", "llama3.2")
    error = ""
    if not ai_provider_enabled("ollama"):
        error = "Ollama is disabled in Settings."
    elif not status["reachable"]:
        error = status["detail"] or f"Ollama is not reachable at {status['base_url']}."
    elif model and not any(
        name == model or name.split(":", 1)[0] == model.split(":", 1)[0] for name in status["models"]
    ):
        error = f"Ollama is reachable but model {model!r} is not pulled (run: ollama pull {model})."
    return {
        "ok": not error,
        "provider": "ollama",
        "error": error,
        "base_url": status["base_url"],
        "models": status["models"],
        "capabilities": get_model_capabilities(),
    }


@router.get("/pilot-engine")
async def get_pilot_engine():
    from services.integrations_store import get_pilot_engine_preference

    from ..ai.llm.provider import pilot_engine_decision

    decision = pilot_engine_decision()
    return {"preference": get_pilot_engine_preference(), **decision}


@router.patch("/pilot-engine")
async def patch_pilot_engine(body: PilotEngineBody, request: Request):
    from services.integrations_store import set_pilot_engine_preference

    from ..ai.llm.provider import pilot_engine_decision

    try:
        saved = set_pilot_engine_preference(body.engine)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    decision = pilot_engine_decision()
    record_admin_change(
        request,
        "workspace.pilot_engine.update",
        target="pilot_engine",
        details={"preference": saved, "engine": decision["engine"], "source": decision["source"]},
    )
    return {"preference": saved, **decision}


def _own_workspace_key(request: Request, key: dict[str, Any]) -> bool:
    """A workspace key is visible to its own workspace; a platform admin sees all.

    Keys minted before keys carried a workspace (``workspace_id`` ""), and
    keys minted through ``/iam``, belong to no workspace and are platform-admin
    only. This holds whether or not DATAFLOW_WORKSPACE_ADMIN_IAM is on.
    """
    scope = tenant_scope(request)
    if scope.unrestricted:
        return True
    return bool(key.get("workspace_id")) and key["workspace_id"] == scope.workspace_id


def _with_access_label(key: dict[str, Any]) -> dict[str, Any]:
    """Say what the key can reach (D219) — the label is computed, not stored.

    An ``admin`` key a platform admin minted with no workspace is unrestricted:
    it reads and acts in every workspace. A key minted in a workspace belongs
    to that workspace, whoever minted it. ``actor_is_unrestricted`` is the one rule the
    request gate applies to the key, so the label cannot drift from it.
    """
    from services.integrations_store import resolve_stored_api_key_role
    from services.tenant_scope import actor_is_unrestricted

    principal = {
        "email": key.get("created_by") or "",
        "role": resolve_stored_api_key_role(key.get("role")),
        "auth_kind": "api_key",
        "workspace_id": key.get("workspace_id") or "",
    }
    if actor_is_unrestricted(principal):
        access = "platform"
        label = (
            "Platform-wide: minted by a platform admin with the admin role and no "
            "workspace, this key reads and acts in every workspace."
        )
    elif key.get("workspace_id"):
        access = "workspace"
        label = "This workspace only."
    else:
        access = "workspace"
        label = "Only the workspaces its creator belongs to; not platform-wide."
    return {**key, "access": access, "access_label": label}


@router.get("/api-keys")
async def get_api_keys(request: Request):
    from services.integrations_store import list_api_keys

    return {
        "keys": [_with_access_label(k) for k in list_api_keys() if _own_workspace_key(request, k)]
    }


@router.post("/api-keys")
async def post_api_key(body: ApiKeyCreateBody, request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import create_api_key

    actor = _actor(request)
    from src.routers.iam_router import enforce_grant_bounds

    enforce_grant_bounds(
        request,
        route="POST /api/v1/workspace/api-keys",
        target_role=body.role,
        target_scopes=body.scopes,
    )
    try:
        created = create_api_key(
            body.name,
            actor,
            role=body.role,
            expires_in=body.expires_in,
            scopes=body.scopes,
            workspace_id=tenant_scope(request).workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    append_audit_event(
        action="workspace.api_key.create",
        resource="/workspace/api-keys",
        actor=actor,
        level="success",
        details={
            "id": created["id"],
            "workspace_id": created["workspace_id"],
            "name": created["name"],
            "prefix": created["prefix"],
            "role": created["role"],
            "scopes": created.get("scopes"),
            "lifetime": created.get("lifetime"),
            "expires_at": created.get("expires_at"),
        },
    )
    return _with_access_label(created)


@router.delete("/api-keys/{key_id}")
async def delete_api_key(key_id: str, request: Request):
    from services.integrations_store import list_api_keys, revoke_api_key

    if not any(
        k["id"] == key_id and _own_workspace_key(request, k) for k in list_api_keys()
    ) or not revoke_api_key(key_id):
        raise HTTPException(status_code=404, detail="API key not found")
    record_admin_change(
        request,
        "workspace.api_key.revoke",
        target=key_id,
        details={"id": key_id},
    )
    return {"ok": True}


class JobRetentionPurgeBody(BaseModel):
    dry_run: bool = True
    confirm_token: str = Field(default="", max_length=128)


@router.post("/jobs/retention/purge")
def purge_job_retention(body: JobRetentionPurgeBody, request: Request):
    """Delete this workspace's finished jobs older than ``retention_days``.

    Dry run first (default): returns the count and a ``confirm_token``; apply
    with ``dry_run: false`` and that token. Never touches unfinished jobs or
    another workspace. Nothing runs on a schedule. See services.job_retention.
    """
    from services.job_retention import RetentionTokenInvalid, purge_expired_jobs
    from services.workspace_store import get_workspace_settings

    scope = tenant_scope(request)
    workspace_id = str(scope.workspace_id or "")
    if not workspace_id:
        raise HTTPException(status_code=400, detail="Name the workspace (X-Workspace-Id) to purge its jobs")
    if not _can_admin_workspace(request, workspace_id):
        raise HTTPException(status_code=403, detail="Workspace admin required")
    retention_days = int(get_workspace_settings()["retention_days"])
    try:
        result = purge_expired_jobs(
            workspace_id=workspace_id,
            retention_days=retention_days,
            dry_run=body.dry_run,
            confirm_token=body.confirm_token,
        )
    except RetentionTokenInvalid as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    record_admin_change(
        request,
        "workspace.jobs.retention.purge",
        target=workspace_id,
        details={k: v for k, v in result.items() if k not in {"confirm_token", "sample_job_ids"}},
    )
    return result


@router.get("/pii-hash-key")
async def get_pii_hash_key_status(request: Request):
    """Versions and dates of this workspace's hash_pii key — never key material."""
    from services.pii_hash_keys import key_status

    return key_status(tenant_scope(request).workspace_id)


@router.post("/pii-hash-key/rotate")
async def rotate_pii_hash_key(request: Request):
    """Start a new hash_pii key version for this workspace (changes future digests)."""
    from services.audit_log import append_audit_event
    from services.pii_hash_keys import PiiHashKeyUnavailable, rotate_workspace_key

    workspace_id = tenant_scope(request).workspace_id
    try:
        status = rotate_workspace_key(workspace_id, actor=_actor(request))
    except PiiHashKeyUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    append_audit_event(
        action="workspace.pii_hash_key.rotate",
        resource="/workspace/pii-hash-key",
        actor=_actor(request),
        level="warn",
        details={"workspace_id": workspace_id, "active_version": status["active_version"]},
    )
    return status


# Workspace creation, membership and accounts live in ``src.routers.team_router``.
# Two routers owning the same collection meant the UI could add a member through
# one authorization rule and read it back through another.


@router.get("/workspaces/{workspace_id}")
async def get_workspace_by_id(workspace_id: str, request: Request):
    if not _can_admin_workspace(request, workspace_id):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return ws.to_dict()


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


def _assert_channel_visible(request: Request, channel_workspace_id: str) -> None:
    """A channel outside the caller's tenant scope is a 404, like a missing id (D222).

    The list route's rule: a workspace channel belongs to that workspace, and
    an unscoped (deployment-wide) channel to an unrestricted actor while
    isolation is enforced. Only a caller who can see the channel gets the 403
    for lacking write access.
    """
    if not (channel_workspace_id or "").strip():
        # Deployment-wide channel: the platform admin's, whichever workspace
        # they are looking at (the list route shows these to them only).
        if not tenant_scope(request).unrestricted:
            raise HTTPException(status_code=404, detail="Channel not found")
        return
    assert_resource_workspace(request, channel_workspace_id, not_found_detail="Channel not found")


def _channel_scope_writable(request: Request, workspace_id: str) -> bool:
    """Write access to channels of ``workspace_id``; unscoped ones are deployment-wide."""
    if workspace_id:
        return request_may_write_workspace(request, workspace_id)
    from src.services import auth_service

    return not auth_service.auth_required() or _is_platform_admin(request)


@router.get("/notifications")
async def get_notifications(workspace_id: str | None = None, request: Request = None):
    # The request's tenant scope decides which workspace this lists (D154): the
    # named one (403 unless the caller is in it), else the header / own workspace.
    # Unscoped channels are deployment-wide, so only an unrestricted actor reads them.
    scope = tenant_scope(request)
    if workspace_id and workspace_id.strip():
        scope = scope.narrowed(workspace_id)
    workspace_id = scope.workspace_id
    if not workspace_id and not scope.unrestricted:
        raise HTTPException(
            status_code=403,
            detail="Name a workspace to list its notification channels",
        )
    from services.notification_store import list_channels

    channels = list_channels(workspace_id=workspace_id)
    return {"channels": [c.to_dict() for c in channels]}


@router.post("/notifications")
async def post_notification(body: NotificationChannelCreate, request: Request):
    actor = _actor(request)
    # Stamp the request's workspace when the body names none: the list reads
    # the caller's workspace, so a channel stored with workspace_id '' was
    # never listed (QA enterprise #45).
    ws_id = (body.workspace_id or "").strip() or tenant_scope(request).workspace_id
    if not _channel_scope_writable(request, ws_id):
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
    out = channel.to_dict()
    secret = getattr(channel, "created_signing_secret", "")
    if secret:
        out.update(_signing_disclosure(secret))
    return out


def _signing_disclosure(secret: str) -> dict[str, Any]:
    from services.webhook_signing import SIGNATURE_HEADER, TIMESTAMP_HEADER

    return {
        "signing_secret": secret,
        "signing": {
            "shown_once": True,
            "algorithm": "HMAC-SHA256",
            "signature_header": SIGNATURE_HEADER,
            "timestamp_header": TIMESTAMP_HEADER,
            "signed_message": "<timestamp>.<raw request body>",
            "header_format": "t=<timestamp>,v1=<hex digest>",
            "replay_window_seconds": 300,
            "docs": "docs/WEBHOOK_SIGNING.md",
        },
    }


@router.post("/notifications/{channel_id}/signing-secret/rotate")
async def rotate_notification_signing_secret(channel_id: str, request: Request):
    """Issue a new signing secret for a webhook channel (shown once).

    The old secret stops signing immediately; update the receiver first or
    accept both during the switch on the receiving side.
    """
    from services.notification_store import get_channel, rotate_signing_secret

    channel = get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    if channel.workspace_id and not request_may_write_workspace(request, channel.workspace_id):
        raise HTTPException(status_code=403, detail="Workspace access denied")
    try:
        rotated = rotate_signing_secret(channel_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if rotated is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    updated, secret = rotated
    return {**updated.to_dict(), **_signing_disclosure(secret)}


@router.patch("/notifications/{channel_id}")
async def patch_notification(channel_id: str, body: NotificationChannelUpdate, request: Request):
    actor = _actor(request)
    from services.notification_store import get_channel, update_channel

    channel = get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    _assert_channel_visible(request, channel.workspace_id)
    if not _channel_scope_writable(request, channel.workspace_id):
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
    _assert_channel_visible(request, channel.workspace_id)
    if not _channel_scope_writable(request, channel.workspace_id):
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
    _assert_channel_visible(request, channel.workspace_id)
    if not _channel_scope_writable(request, channel.workspace_id):
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


# ═══════════════════════════════════════════════════════════════════════════════
# Tenant / enterprise SaaS settings
# ═══════════════════════════════════════════════════════════════════════════════


def _resolve_request_tenant(request: Request, workspace_id: str | None = None):
    tenant_id = getattr(request.state, "tenant_id", None)
    if tenant_id:
        return get_tenant(tenant_id)
    ws = workspace_id or request.headers.get("x-workspace-id", "") or getattr(request.state, "tenant_workspace_id", "")
    return get_tenant_for_workspace(ws) if ws else None


def _authorized_request_tenant(request: Request):
    """The request's tenant, only when the caller administers its workspace.

    The tenant comes from the custom domain or the ``X-Workspace-Id`` header,
    both caller-controlled, so the workspace it belongs to is checked here
    rather than trusted. A tenant bound to no workspace is platform-admin only.
    """
    tenant = _resolve_request_tenant(request)
    if tenant is not None:
        _require_tenant_admin(request, tenant.workspace_id)
    else:
        named = _named_workspace_id(request)
        if named and not _can_admin_workspace(request, named):
            raise HTTPException(status_code=403, detail="Workspace admin required")
    return tenant


@router.get("/tenant")
async def get_current_tenant(request: Request):
    tenant = _authorized_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured for this workspace or domain")
    return tenant.to_dict()


@router.get("/tenants")
async def get_all_tenants(request: Request):
    """List tenants the caller is allowed to see.

    Platform admins see every tenant (or the header workspace when named).
    Workspace admins see only tenants they administer — never another
    customer's domain, MFA flag, or IP allowlist.
    """
    from src.services import auth_service

    named = (request.headers.get("x-workspace-id") or "").strip()
    tenants = list_tenants()
    if auth_service.auth_required() and not _is_platform_admin(request):
        tenants = [
            t
            for t in tenants
            if t.workspace_id and _can_admin_workspace(request, t.workspace_id)
        ]
    if named:
        tenants = [t for t in tenants if t.workspace_id == named]
    return {"tenants": [t.to_dict() for t in tenants]}


@router.post("/tenant")
async def post_tenant(body: TenantCreateBody, request: Request):
    from services.audit_log import append_audit_event

    actor = _actor(request)
    # A tenant is read back by the workspace the request is scoped to, so it must
    # be created against one. An unnamed workspace produced a tenant no GET could
    # ever resolve — saved, invisible, and never authorized against a workspace.
    named = (request.headers.get("x-workspace-id") or "").strip()
    workspace_id = (body.workspace_id or "").strip() or named
    if not workspace_id:
        raise HTTPException(
            status_code=400,
            detail="workspace_id is required — a tenant belongs to a workspace",
        )
    # Authority over the named workspace is asked first: borrowing the header's
    # scope to write into someone else's workspace is the stronger refusal.
    if not _can_admin_workspace(request, workspace_id):
        raise HTTPException(status_code=403, detail="Workspace admin required to create a tenant")
    if body.workspace_id and named and body.workspace_id.strip() != named:
        raise HTTPException(
            status_code=403,
            detail="workspace_id does not match X-Workspace-Id",
        )
    try:
        tenant = create_tenant(
            workspace_id=workspace_id,
            name=body.name,
            custom_domain=body.custom_domain,
            data_region=body.data_region,
            byok_key_id=body.byok_key_id,
            security_contact_email=body.security_contact_email,
            mfa_required=body.mfa_required,
            session_timeout_hours=body.session_timeout_hours,
            enforce_session_timeout=body.enforce_session_timeout,
            ip_allowlist=body.ip_allowlist,
            egress_allowlist=body.egress_allowlist,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    append_audit_event(
        action="workspace.tenant.create",
        resource=f"/workspace/tenant/{tenant.id}",
        actor=actor,
        level="info",
        details={
            "tenant_id": tenant.id,
            "workspace_id": tenant.workspace_id,
            "custom_domain": tenant.custom_domain,
            "data_region": tenant.data_region,
        },
    )
    return tenant.to_dict()


@router.patch("/tenant/{tenant_id}")
async def patch_tenant(tenant_id: str, body: TenantUpdateBody, request: Request):
    from services.audit_log import append_audit_event

    tenant = get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    actor = _actor(request)
    _require_tenant_admin(request, tenant.workspace_id)
    try:
        updated = update_tenant(tenant_id, **body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not updated:
        raise HTTPException(status_code=404, detail="Tenant not found")
    append_audit_event(
        action="workspace.tenant.update",
        resource=f"/workspace/tenant/{tenant_id}",
        actor=actor,
        level="info",
        details={
            "tenant_id": tenant_id,
            "fields": sorted(body.model_dump(exclude_none=True).keys()),
        },
    )
    return updated.to_dict()


@router.delete("/tenant/{tenant_id}")
async def delete_tenant_route(tenant_id: str, request: Request):
    from services.audit_log import append_audit_event

    tenant = get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    actor = _actor(request)
    _require_tenant_admin(request, tenant.workspace_id)
    if not delete_tenant(tenant_id):
        raise HTTPException(status_code=404, detail="Tenant not found")
    append_audit_event(
        action="workspace.tenant.delete",
        resource=f"/workspace/tenant/{tenant_id}",
        actor=actor,
        level="warn",
        details={"tenant_id": tenant_id, "workspace_id": tenant.workspace_id},
    )
    return {"ok": True}


@router.get("/tenant/byok-keys")
async def get_tenant_byok_keys(request: Request):
    tenant = _authorized_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    return {"keys": [public_key_dict(k) for k in list_keys(tenant.id)]}


@router.post("/tenant/byok-keys")
async def post_tenant_byok_key(request: Request, body: BYOKKeyCreateBody):
    tenant = _authorized_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    try:
        key = create_key(
            tenant_id=tenant.id,
            label=body.label,
            provider=body.provider,
            key_material=body.key_material or None,
        )
        # Link the new active key to the tenant if none is configured.
        if not tenant.byok_key_id:
            update_tenant(tenant.id, byok_key_id=key.id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    from services.audit_log import append_audit_event

    append_audit_event(
        action="workspace.byok.create",
        resource=f"/workspace/tenant/byok-keys/{key.id}",
        actor=_actor(request),
        level="success",
        workspace_id=tenant.workspace_id,
        tenant_id=tenant.id,
        details={"key_id": key.id, "provider": key.provider, "label": key.label},
    )
    return public_key_dict(key)


@router.post("/tenant/byok-keys/{key_id}/rotate")
async def rotate_tenant_byok_key(key_id: str, request: Request):
    tenant = _authorized_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    key = get_key(key_id)
    if not key or key.tenant_id != tenant.id:
        raise HTTPException(status_code=404, detail="Key not found")
    new_key = rotate_key(tenant.id, label=f"Rotated from {key.id[:8]}")
    update_tenant(tenant.id, byok_key_id=new_key.id)
    from services.audit_log import append_audit_event

    append_audit_event(
        action="workspace.byok.rotate",
        resource=f"/workspace/tenant/byok-keys/{new_key.id}",
        actor=_actor(request),
        level="warn",
        workspace_id=tenant.workspace_id,
        tenant_id=tenant.id,
        details={"previous_key_id": key.id, "new_key_id": new_key.id},
    )
    return public_key_dict(new_key)


# ═══════════════════════════════════════════════════════════════════════════════
# Security posture / compliance metadata
# ═══════════════════════════════════════════════════════════════════════════════


def _security_posture(tenant: Tenant | None = None) -> dict[str, Any]:
    from services.byok_key_manager import key_status_summary
    from services.cdc_effectively_once import (
        DELIVERY_DEFAULT,
        EXACTLY_ONCE_CLAIMED,
        honesty_dict,
    )
    from services.platform_config import is_production, validate_production_config
    from services.recovery_honesty import honesty_dict as recovery_honesty_dict
    from services.secret_vault import secrets_encryption_ready

    from src.middleware.tenant_middleware import _allowlist_enforced

    region = tenant.data_region if tenant else "us-east-1"
    ip_flag = bool(_allowlist_enforced())
    ip_enforced = bool(
        tenant and tenant.ip_allowlist and (tenant.custom_domain or "").strip() and ip_flag
    )
    session_enforced = bool(tenant and getattr(tenant, "enforce_session_timeout", False))
    byok = key_status_summary(tenant.id) if tenant else {"configured": False}
    encryption_ready = bool(secrets_encryption_ready())
    prod = is_production()
    prod_errors = validate_production_config() if prod else []
    audit_logging = True  # append-only JSONL audit path is always mounted
    cdc_honesty = honesty_dict()
    recovery_honesty = recovery_honesty_dict()
    return {
        "tenant_id": tenant.id if tenant else None,
        "workspace_id": tenant.workspace_id if tenant else None,
        "custom_domain": tenant.custom_domain if tenant else None,
        "custom_domain_cors": {
            "enabled": True,
            "origin": (
                f"https://{(tenant.custom_domain or '').strip()}"
                if tenant and (tenant.custom_domain or "").strip()
                else None
            ),
            "hint": (
                "Set tenant.custom_domain (e.g. data.customer.com) and point DNS to the "
                "Datawrap web host. API CORS accepts the tenant origin live via "
                "TenantAwareCORSMiddleware. Also set DATAWRAP_WEB_DOMAIN / CORS_EXTRA_ORIGINS "
                "for the primary SPA origin."
            ),
        },
        "data_region": region,
        "environment": "production" if prod else "development",
        "encryption_at_rest": encryption_ready,
        "secrets_encryption_ready": encryption_ready,
        "production_config_ok": not prod_errors,
        "production_config_errors": prod_errors,
        "byok": byok,
        "audit_logging": audit_logging,
        "pii_detection": False,  # detector hooks exist; not attested as always-on
        "ip_allowlist_enabled": bool(tenant and tenant.ip_allowlist),
        "egress_allowlist_enabled": bool(tenant and tenant.egress_allowlist),
        # Enforced only when Host resolves this tenant (custom domain) AND the
        # operator turned DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE on; otherwise a
        # request it would block is audited (tenant.ip_allowlist.would_block)
        # and allowed. The shared app host never evaluates the CIDR list.
        "ip_allowlist_enforced": ip_enforced,
        "mfa_required": tenant.mfa_required if tenant else False,
        # Login MFA is not wired; the flag is policy memory only.
        "mfa_enforced": False,
        "session_timeout_hours": tenant.session_timeout_hours if tenant else 8,
        # Idle timeout enforced on sign-in sessions when the tenant turns
        # enforce_session_timeout on (services.session_policy). API keys and
        # the absolute token TTL (DATAFLOW_TOKEN_TTL_SEC) are separate.
        "session_timeout_enforced": session_enforced,
        "enforcement": {
            "ip_allowlist": {
                "enforced": ip_enforced,
                "scope": "requests whose Host is the tenant custom domain",
                "requires": ["tenant.ip_allowlist", "tenant.custom_domain",
                             "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE=1"],
                "missing": [m for m, ok in (
                    ("tenant.ip_allowlist", bool(tenant and tenant.ip_allowlist)),
                    ("tenant.custom_domain", bool(tenant and (tenant.custom_domain or "").strip())),
                    ("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE=1", ip_flag),
                ) if not ok],
                "not_covered": "the shared app host and API keys used against it",
            },
            "session_timeout": {
                "enforced": session_enforced,
                "kind": "idle",
                "hours": tenant.session_timeout_hours if tenant else None,
                "scope": "sign-in sessions of members of this workspace (strictest workspace wins)",
                "not_covered": "API keys; absolute token lifetime is DATAFLOW_TOKEN_TTL_SEC",
            },
            "mfa": {"enforced": False, "status": "design_only", "design": "docs/MFA_ENFORCEMENT_DESIGN.md"},
        },
        # True when this tenant has an active BYOK key. New connector secrets
        # are wrapped with that key; enc:v1 leftovers stay readable until re-saved.
        "byok_encrypts_secrets": bool(tenant and byok.get("active_count")),
        "tls_version": "1.3",
        # Surfaced CDC posture — explicit EO/ALO/AMO; only ALO is claimed.
        "cdc_delivery": DELIVERY_DEFAULT,
        "cdc_exactly_once_claimed": EXACTLY_ONCE_CLAIMED,
        "catalog_tiles_are_not_transfer_live": True,
        "customer_tenant_warehouse_sku_claimed": False,
        "cdc_at_most_once_claimed": False,
        "cdc_honesty": cdc_honesty,
        # Recovery Integrity — refuse invent of one-click undo / restore product.
        "transfer_undo_claimed": bool(recovery_honesty.get("transfer_undo_claimed")),
        "recovery_honesty": recovery_honesty,
        "deployment": {
            "models": ["saas_multi_tenant", "customer_vpc_self_host", "air_gapped_compose"],
            "saas_multi_tenant": "Workspace isolation + optional DATAFLOW_REQUIRE_WORKSPACE hard gate",
            "customer_vpc_self_host": "Docker Compose / container deploy in customer VPC — same Transfer Studio engine",
            "air_gapped": "Supported via offline image load; customer supplies Postgres/object store; no Datawrap SaaS egress required",
            "private_link": "not_first_class",
            "data_plane": (
                "Transfer bytes move source→Datawrap worker→destination over connector TLS "
                "(sslmode/rediss/https). Control plane API should sit behind HTTPS. "
                "Datawrap does not claim zero-copy bypass of the worker for DB→DB migrations."
            ),
            "data_loss_controls": [
                "preflight_gates_g1_g9",
                "quarantine_not_silent_drop",
                "post_load_reconciliation_checksum",
                "sql_null_vs_empty_preserved",
                "checkpoint_fail_closed",
                f"cdc_delivery_{DELIVERY_DEFAULT}_pk_lsn_upsert",
                "no_product_transfer_undo",
            ],
        },
        "private_networking": {
            "vpc_peering": False,
            "private_link": False,
            "status": "not_first_class",
            "note": "Connect via customer network controls / VPN; product VPC peering is not shipped",
        },
        "compliance": [
            {
                "framework": "SOC 2 Type II",
                "status": "controls_scaffolded",
                "evidence": "Audit logs + RBAC + secrets encryption scaffolding; formal attestation not claimed",
            },
            {
                "framework": "GDPR",
                "status": "partial",
                "evidence": "Workspace isolation and audit trails available; DPA/residency attestation is customer-owned",
            },
            {
                "framework": "HIPAA",
                "status": "not_attested",
                "evidence": "BYOK and encryption controls are prerequisites only — no HIPAA BAA attestation in-product",
            },
        ],
        "attestations": [
            {"name": "Penetration testing", "last_completed": None, "next_due": None, "status": "customer_owned"},
            {"name": "Vulnerability scanning", "status": "recommended_external"},
        ],
    }


def _posture_scope_workspace(request: Request, tenant: Tenant | None) -> str:
    """The workspace a posture/report is about: the tenant's, else the named
    one, else the caller's own workspace (single membership / bound key)."""
    if tenant and tenant.workspace_id:
        return tenant.workspace_id
    named = (request.headers.get("x-workspace-id") or "").strip() or str(
        getattr(request.state, "tenant_workspace_id", "") or ""
    )
    if named:
        return named
    try:
        return str(tenant_scope(request).workspace_id or "")
    except HTTPException:
        return ""


@router.get("/security/posture")
async def get_security_posture(request: Request):
    """Posture for the workspace this request names.

    The body is not cacheable: an earlier unscoped read (allowlist off) must
    not be reused after an admin saves a CIDR for the workspace being viewed.
    ``scope_workspace_id`` is that workspace even when it has no tenant yet, so
    a client can refuse to paint another scope's answer as Disabled.
    """
    from fastapi.encoders import jsonable_encoder

    tenant = _authorized_request_tenant(request)
    body = _security_posture(tenant)
    body["scope_workspace_id"] = _posture_scope_workspace(request, tenant) or None
    return JSONResponse(
        content=jsonable_encoder(body),
        headers={"Cache-Control": "no-store"},
    )


@router.get("/security/report")
async def get_security_report(request: Request):
    """Return a downloadable, human-readable compliance report for the tenant."""
    tenant = _authorized_request_tenant(request)
    posture = _security_posture(tenant)
    # The report is about the workspace the request is scoped to, tenant or not.
    # "none — request carried no workspace scope" was printed whenever the
    # workspace had no tenant profile, even though the request named it.
    scope_ws = _posture_scope_workspace(request, tenant)
    if scope_ws and not posture.get("workspace_id"):
        posture["workspace_id"] = scope_ws
    workspace_name = ""
    if scope_ws:
        ws_row = get_workspace(scope_ws)
        workspace_name = getattr(ws_row, "name", "") if ws_row else ""

    lines = [
        "# Datawrap Security & Compliance Report",
        f"Generated: {datetime.now(timezone.utc).isoformat()}Z",
        f"Environment: {posture['environment']}",
        # "default" read as a configured tenant named default; a report handed to
        # an auditor has to say when no tenant profile backs these controls.
        f"Tenant ID: {posture['tenant_id'] or 'none — no tenant profile configured for this workspace'}",
        f"Workspace ID: {posture['workspace_id'] or 'none — request carried no workspace scope'}",
        *( [f"Workspace: {workspace_name}"] if workspace_name else [] ),
        "Scope: this report covers only the workspace above.",
        f"Custom domain: {posture['custom_domain'] or 'not configured'}",
        f"Primary data region: {posture['data_region']}",
        "",
        "## Controls",
        f"- Encryption at rest: {'enabled' if posture['encryption_at_rest'] else 'disabled'}",
        f"- TLS minimum version: {posture['tls_version']}",
        f"- Audit logging: {'enabled' if posture['audit_logging'] else 'disabled'}",
        f"- PII detection: {'enabled' if posture['pii_detection'] else 'disabled'}",
        f"- IP allowlisting: {'enforced on custom domain' if posture.get('ip_allowlist_enforced') else ('stored, not enforced (missing: ' + ', '.join(posture['enforcement']['ip_allowlist']['missing']) + ')' if posture['ip_allowlist_enabled'] else 'disabled')}",
        f"- Connector egress allowlist: {'on' if posture.get('egress_allowlist_enabled') else 'off (private ranges refused; metadata and link-local cannot be listed)'}",
        f"- MFA required for admins: recorded={('yes' if posture['mfa_required'] else 'no')}; enforced=no (login MFA is not wired; design: docs/MFA_ENFORCEMENT_DESIGN.md)",
        f"- Session timeout: {posture['session_timeout_hours']}h idle; enforced={'yes (sign-in sessions; API keys excluded)' if posture['session_timeout_enforced'] else 'no (recorded only; turn on enforce_session_timeout)'}",
        "",
        "## Deployment & data path",
        f"- Models: {', '.join((posture.get('deployment') or {}).get('models') or [])}",
        f"- Data plane: {(posture.get('deployment') or {}).get('data_plane', '')}",
        f"- Loss controls: {', '.join((posture.get('deployment') or {}).get('data_loss_controls') or [])}",
        f"- Private Link: {(posture.get('private_networking') or {}).get('status', 'unknown')}",
        "",
        "## Key management",
        f"- BYOK configured: {'yes' if posture['byok']['configured'] else 'no'}",
        f"- BYOK wraps new connector secrets: {'yes' if posture.get('byok_encrypts_secrets') else 'no — platform Fernet until an active key exists'}",
    ]
    if posture["byok"]["configured"]:
        lines.append(f"- Active keys: {posture['byok']['active_count']}")
        lines.append(f"- Providers: {', '.join(posture['byok']['providers'])}")
        lines.append(f"- Rotation required: {'yes' if posture['byok']['rotated'] else 'no'}")
    lines.extend(["", "## Compliance roadmap"])
    for c in posture["compliance"]:
        lines.append(f"- {c['framework']}: {c['status']} — {c['evidence']}")
    lines.extend(["", "## Attestations"])
    for a in posture["attestations"]:
        status = a.get("status") or ("complete" if a.get("last_completed") else "pending")
        lines.append(f"- {a['name']}: {status}")

    report = "\n".join(lines)
    return PlainTextResponse(
        report,
        media_type="text/markdown",
        headers={"Content-Disposition": f'attachment; filename="{PRODUCT_SLUG}-compliance-report.md"'},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Benchmarks — reproducible scale proof for procurement
# ═══════════════════════════════════════════════════════════════════════════════


def _baseline_competitors() -> list[dict[str, Any]]:
    from services.benchmark_runs import baseline_competitors

    return baseline_competitors()


def _markdown_benchmark_report(report: dict[str, Any]) -> str:
    lines = [
        "# Datawrap Benchmark Report",
        f"Generated: {report['timestamp']}Z",
        f"Workload: {report['rows']:,} rows from CSV → SQLite (local API host)",
        "",
        "> This report measures synthetic CSV → SQLite only. It is **not** a MongoDB→Snowflake",
        "> or warehouse throughput claim. Use the live job theater for per-route rows/sec.",
        "",
        "## Results",
        f"- Success: {report['success']}",
        f"- Elapsed: {report['elapsed_seconds']:.3f}s",
        f"- Throughput: {report['records_per_second']:,.1f} rows/sec (CSV→SQLite)",
        f"- Peak memory: {report['peak_memory_mb']} MB ({report.get('peak_memory_basis') or 'process'})",
        f"- Row count verified: {report['destination_summary'].get('verified', False)}",
        "",
        "## Competitor baselines",
        "| Product | Typical rows/sec | Datawrap vs this | Resume | Notes |",
        "|---------|------------------|------------------|--------|-------|",
    ]
    from services.benchmark_runs import comparison

    for c in report["competitors"]:
        label = (c.get("comparison") or comparison(report.get("records_per_second") or 0, c["typical_rps"]))["label"]
        lines.append(f"| {c['product']} | {c['typical_rps']:,} | {label} | {'Yes' if c['resume_from_checkpoint'] else 'No'} | {c['notes']} |")
    lines.extend(["", "This report was produced by the Datawrap benchmark harness and can be reproduced locally without cloud credentials."])
    return "\n".join(lines)


@router.get("/proofs/ledger")
async def get_proof_ledger():
    """Customer-visible migration proof ledger (SKU + fidelity + integrity comparison)."""
    from services.proof_ledger import build_proof_ledger

    return JSONResponse(build_proof_ledger())


@router.post("/proofs/fidelity")
async def run_fidelity_proof():
    """Run the rich-type CSV→SQLite fidelity proof and persist an artifact."""
    from services.proof_ledger import run_fidelity_proof as _run

    try:
        result = await asyncio.to_thread(_run)
    except Exception as exc:
        return JSONResponse(
            {"success": False, "error": str(exc), "tier": "fidelity", "route": "csv→sqlite"},
            status_code=500,
        )
    status = 200 if result.get("success") else 422
    return JSONResponse(result, status_code=status)


@router.post("/proofs/desktop-lab")
async def run_desktop_lab_proof():
    """Exercise the desktop-lab catalog slots as source and destination.

    80 is catalog slots, not unique engines. Hosted twins share a driver.
    """
    from services.desktop_lab import DESKTOP_LAB_MIN_DUPLEX, run_desktop_lab

    try:
        result = await asyncio.to_thread(run_desktop_lab, persist=True)
    except Exception as exc:
        return JSONResponse(
            {"success": False, "error": str(exc), "tier": "desktop_lab"},
            status_code=500,
        )
    duplex = int(result.get("catalog_slots_duplex_passed") or 0)
    ops = int(result.get("catalog_slots_operations_passed") or 0)
    slots = int(result.get("catalog_slots") or 0)
    result["success"] = (
        duplex >= DESKTOP_LAB_MIN_DUPLEX
        and ops == slots
        and duplex == slots
        and int(result.get("failed") or 0) == 0
        and int(result.get("skipped") or 0) == 0
    )
    # A completed measurement is 200 even when slots skipped/failed. 422 hid the
    # pass/fail/skip report behind a generic Proofs toast.
    return JSONResponse(result, status_code=200)


@router.get("/proofs/desktop-lab")
async def get_desktop_lab_proof():
    """Last persisted desktop-lab duplex report, if any."""
    from services.desktop_lab import last_desktop_lab_report

    report = last_desktop_lab_report()
    if report is None:
        return JSONResponse({"available": False, "catalog_slots": 0})
    return JSONResponse({"available": True, **report})


@router.post("/proofs/desktop-lab-cross")
async def run_desktop_lab_cross_proof():
    """Unique-engine source × dest cartesian on live desktop backends.

    Not 80×80 catalog aliases. Hosted twins stay on the duplex lab.
    """
    from services.desktop_lab_cross import run_live_engine_cross_matrix

    try:
        result = await asyncio.to_thread(run_live_engine_cross_matrix, persist=True)
    except Exception as exc:
        return JSONResponse(
            {"success": False, "error": str(exc), "tier": "desktop_lab_cross"},
            status_code=500,
        )
    passed = int(result.get("passed") or 0)
    failed = int(result.get("failed") or 0)
    result["success"] = passed > 0 and failed == 0
    # Completed cartesian is 200 with success=false when pairs failed. Skip
    # reasons stay in the body so Proofs can show them instead of a generic error.
    return JSONResponse(result, status_code=200)


@router.get("/proofs/desktop-lab-cross")
async def get_desktop_lab_cross_proof():
    """Last persisted unique-engine cartesian, if any."""
    from services.desktop_lab_cross import last_cross_report

    report = last_cross_report()
    if report is None:
        return JSONResponse({"available": False, "pairs": 0})
    return JSONResponse({"available": True, **report})


@router.post("/benchmark")
async def run_workspace_benchmark(body: BenchmarkRequest):
    """Run the local benchmark to completion and return the report.

    Kept for API clients. It runs in a child process like the background run
    (so this API process's environment is untouched and memory is measured),
    but a large run outlives proxy timeouts: the page uses ``/benchmark/runs``.
    """
    from services.benchmark_runs import BenchmarkBusy, run_blocking

    try:
        run = await asyncio.to_thread(run_blocking, body.rows)
    except BenchmarkBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    report = run.get("report") or {
        "target": "sqlite", "rows": body.rows, "success": False,
        "error": run.get("error") or "Benchmark failed",
    }
    if not run.get("report"):
        return JSONResponse(report, status_code=500)
    if body.format == "md":
        return PlainTextResponse(
            _markdown_benchmark_report(report),
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="{PRODUCT_SLUG}-benchmark-report.md"'},
        )
    return JSONResponse(report)


@router.post("/benchmark/runs")
async def start_benchmark_run(body: BenchmarkRequest, request: Request):
    """Start the Proofs → Scale benchmark in the background; poll ``GET .../runs/{id}``.

    The run belongs to the request's workspace; only that workspace (or a
    platform admin) can poll it.
    """
    from services.benchmark_runs import BenchmarkBusy, start_run

    scope = tenant_scope(request)
    try:
        run, started = start_run(
            body.rows, workspace_id=scope.workspace_id, started_by=_actor(request)
        )
    except BenchmarkBusy as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse({"run": run, "started": started}, status_code=202 if started else 200)


@router.get("/benchmark/runs/{run_id}")
async def get_benchmark_run(run_id: str, request: Request, format: str = "json"):
    from services.benchmark_runs import get_run

    scope = tenant_scope(request)
    run = get_run(run_id, workspace_id=scope.workspace_id, unrestricted=scope.unrestricted)
    if run is None:
        raise HTTPException(status_code=404, detail="Benchmark run not found")
    if format == "md":
        if not run.get("report"):
            raise HTTPException(status_code=409, detail="The benchmark run has not finished")
        return PlainTextResponse(
            _markdown_benchmark_report(run["report"]),
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="{PRODUCT_SLUG}-benchmark-report.md"'},
        )
    return JSONResponse({"run": run})
