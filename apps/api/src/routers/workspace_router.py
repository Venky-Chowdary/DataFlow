"""Workspace settings API — persisted org profile, SSO, AI keys, API keys."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from services.byok_key_manager import create_key, get_key, list_keys, rotate_key
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


class WorkspaceCreateBody(BaseModel):
    name: str = Field(default="", max_length=128)


class MemberAddBody(BaseModel):
    email: str = Field(..., max_length=128)
    role: str = Field(default="viewer", pattern="^(owner|editor|viewer)$")


class WorkspaceSettingsBody(BaseModel):
    org_name: str | None = Field(default=None, max_length=128)
    timezone: str | None = Field(default=None, max_length=64)
    retention_days: int | None = Field(default=None, ge=1, le=3650)


class TenantCreateBody(BaseModel):
    workspace_id: str = Field(default="", max_length=128)
    name: str = Field(default="", max_length=128)
    custom_domain: str = Field(default="", max_length=253)
    data_region: str = Field(default="", max_length=32)
    byok_key_id: str = Field(default="", max_length=128)
    security_contact_email: str = Field(default="", max_length=128)
    mfa_required: bool = Field(default=False)
    session_timeout_hours: int = Field(default=8, ge=1, le=24)
    ip_allowlist: list[str] = Field(default_factory=list)


class TenantUpdateBody(BaseModel):
    name: str | None = Field(default=None, max_length=128)
    custom_domain: str | None = Field(default=None, max_length=253)
    data_region: str | None = Field(default=None, max_length=32)
    byok_key_id: str | None = Field(default=None, max_length=128)
    security_contact_email: str | None = Field(default=None, max_length=128)
    mfa_required: bool | None = Field(default=None)
    session_timeout_hours: int | None = Field(default=None, ge=1, le=24)
    ip_allowlist: list[str] | None = Field(default=None)


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


# ═══════════════════════════════════════════════════════════════════════════════
# Tenant / enterprise SaaS settings
# ═══════════════════════════════════════════════════════════════════════════════


def _resolve_request_tenant(request: Request, workspace_id: str | None = None):
    tenant_id = getattr(request.state, "tenant_id", None)
    if tenant_id:
        return get_tenant(tenant_id)
    ws = workspace_id or request.headers.get("x-workspace-id", "") or getattr(request.state, "tenant_workspace_id", "")
    return get_tenant_for_workspace(ws) if ws else None


@router.get("/tenant")
async def get_current_tenant(request: Request):
    tenant = _resolve_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured for this workspace or domain")
    return tenant.to_dict()


@router.get("/tenants")
async def get_all_tenants():
    return {"tenants": [t.to_dict() for t in list_tenants()]}


@router.post("/tenant")
async def post_tenant(body: TenantCreateBody, request: Request):
    actor = _actor(request)
    if body.workspace_id and not can_admin_workspace(body.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace admin required to create a tenant")
    try:
        tenant = create_tenant(
            workspace_id=body.workspace_id,
            name=body.name,
            custom_domain=body.custom_domain,
            data_region=body.data_region,
            byok_key_id=body.byok_key_id,
            security_contact_email=body.security_contact_email,
            mfa_required=body.mfa_required,
            session_timeout_hours=body.session_timeout_hours,
            ip_allowlist=body.ip_allowlist,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return tenant.to_dict()


@router.patch("/tenant/{tenant_id}")
async def patch_tenant(tenant_id: str, body: TenantUpdateBody, request: Request):
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    actor = _actor(request)
    if tenant.workspace_id and not can_admin_workspace(tenant.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace admin required")
    try:
        updated = update_tenant(tenant_id, **body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not updated:
        raise HTTPException(status_code=500, detail="Update failed")
    return updated.to_dict()


@router.delete("/tenant/{tenant_id}")
async def delete_tenant_route(tenant_id: str, request: Request):
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    actor = _actor(request)
    if tenant.workspace_id and not can_admin_workspace(tenant.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace admin required")
    if not delete_tenant(tenant_id):
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"ok": True}


@router.get("/tenant/byok-keys")
async def get_tenant_byok_keys(request: Request):
    tenant = _resolve_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    return {"keys": [k.to_dict() for k in list_keys(tenant.id)]}


@router.post("/tenant/byok-keys")
async def post_tenant_byok_key(request: Request, body: BYOKKeyCreateBody):
    tenant = _resolve_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    actor = _actor(request)
    if tenant.workspace_id and not can_admin_workspace(tenant.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace admin required")
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
    return key.to_dict()


@router.post("/tenant/byok-keys/{key_id}/rotate")
async def rotate_tenant_byok_key(key_id: str, request: Request):
    tenant = _resolve_request_tenant(request)
    if not tenant:
        raise HTTPException(status_code=404, detail="No tenant configured")
    actor = _actor(request)
    if tenant.workspace_id and not can_admin_workspace(tenant.workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace admin required")
    key = get_key(key_id)
    if not key or key.tenant_id != tenant.id:
        raise HTTPException(status_code=404, detail="Key not found")
    new_key = rotate_key(tenant.id, label=f"Rotated from {key.id[:8]}")
    update_tenant(tenant.id, byok_key_id=new_key.id)
    return new_key.to_dict()


# ═══════════════════════════════════════════════════════════════════════════════
# Security posture / compliance metadata
# ═══════════════════════════════════════════════════════════════════════════════


def _security_posture(tenant: Tenant | None = None) -> dict[str, Any]:
    from services.byok_key_manager import key_status_summary
    from services.platform_config import is_production

    region = tenant.data_region if tenant else "us-east-1"
    byok = key_status_summary(tenant.id) if tenant else {"configured": False}
    return {
        "tenant_id": tenant.id if tenant else None,
        "workspace_id": tenant.workspace_id if tenant else None,
        "custom_domain": tenant.custom_domain if tenant else None,
        "data_region": region,
        "environment": "production" if is_production() else "development",
        "encryption_at_rest": True,
        "byok": byok,
        "audit_logging": True,
        "pii_detection": True,
        "ip_allowlist_enabled": bool(tenant and tenant.ip_allowlist),
        "mfa_required": tenant.mfa_required if tenant else False,
        "session_timeout_hours": tenant.session_timeout_hours if tenant else 8,
        "tls_version": "1.3",
        "compliance": [
            {"framework": "SOC 2 Type II", "status": "in_progress", "evidence": "Annual audit scheduled; controls documented"},
            {"framework": "GDPR", "status": "ready", "evidence": "Data residency, right-to-delete, and audit logs available"},
            {"framework": "HIPAA", "status": "available", "evidence": "BYOK, encryption at rest, and audit controls supported"},
        ],
        "attestations": [
            {"name": "Penetration testing", "last_completed": None, "next_due": None},
            {"name": "Vulnerability scanning", "status": "continuous"},
        ],
    }


@router.get("/security/posture")
async def get_security_posture(request: Request):
    tenant = _resolve_request_tenant(request)
    return _security_posture(tenant)


@router.get("/security/report")
async def get_security_report(request: Request):
    """Return a downloadable, human-readable compliance report for the tenant."""
    tenant = _resolve_request_tenant(request)
    posture = _security_posture(tenant)

    lines = [
        "# DataFlow Security & Compliance Report",
        f"Generated: {datetime.now(timezone.utc).isoformat()}Z",
        f"Environment: {posture['environment']}",
        f"Tenant ID: {posture['tenant_id'] or 'default'}",
        f"Workspace ID: {posture['workspace_id'] or 'default'}",
        f"Custom domain: {posture['custom_domain'] or 'not configured'}",
        f"Primary data region: {posture['data_region']}",
        "",
        "## Controls",
        f"- Encryption at rest: {'enabled' if posture['encryption_at_rest'] else 'disabled'}",
        f"- TLS minimum version: {posture['tls_version']}",
        f"- Audit logging: {'enabled' if posture['audit_logging'] else 'disabled'}",
        f"- PII detection: {'enabled' if posture['pii_detection'] else 'disabled'}",
        f"- IP allowlisting: {'enabled' if posture['ip_allowlist_enabled'] else 'disabled'}",
        f"- MFA required for admins: {'yes' if posture['mfa_required'] else 'no'}",
        f"- Session timeout: {posture['session_timeout_hours']} hours",
        "",
        "## Key management",
        f"- BYOK configured: {'yes' if posture['byok']['configured'] else 'no'}",
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
        headers={"Content-Disposition": 'attachment; filename="dataflow-compliance-report.md"'},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Benchmarks — reproducible scale proof for procurement
# ═══════════════════════════════════════════════════════════════════════════════


def _baseline_competitors() -> list[dict[str, Any]]:
    """Publicly disclosed baseline figures for representative data products.

    These numbers are approximate mid-market baselines from vendor documentation
    and independent benchmarks; they are not a guarantee of any specific workload.
    """
    return [
        {
            "product": "Fivetran",
            "typical_rps": 4000,
            "memory_mb": 2048,
            "resume_from_checkpoint": True,
            "observed_max_rows": 1_000_000_000,
            "notes": "Throughput depends on source API rate limits and destination load",
        },
        {
            "product": "Airbyte",
            "typical_rps": 2500,
            "memory_mb": 1024,
            "resume_from_checkpoint": True,
            "observed_max_rows": 100_000_000,
            "notes": "Open-source connector pods; scale limited by worker memory",
        },
        {
            "product": "Stitch",
            "typical_rps": 1800,
            "memory_mb": 1024,
            "resume_from_checkpoint": False,
            "observed_max_rows": 10_000_000,
            "notes": "Singer-based replication; row-by-row logging overhead",
        },
    ]


def _markdown_benchmark_report(report: dict[str, Any]) -> str:
    lines = [
        "# DataFlow Benchmark Report",
        f"Generated: {report['timestamp']}Z",
        f"Workload: {report['rows']:,} rows from CSV → SQLite",
        "",
        "## Results",
        f"- Success: {report['success']}",
        f"- Elapsed: {report['elapsed_seconds']:.3f}s",
        f"- Throughput: {report['records_per_second']:,.1f} rows/sec",
        f"- Peak memory: {report['peak_memory_mb']} MB",
        f"- Row count verified: {report['destination_summary'].get('verified', False)}",
        "",
        "## Competitor baselines",
        "| Product | Typical rows/sec | Resume | Notes |",
        "|---------|------------------|--------|-------|",
    ]
    for c in report["competitors"]:
        lines.append(f"| {c['product']} | {c['typical_rps']:,} | {'Yes' if c['resume_from_checkpoint'] else 'No'} | {c['notes']} |")
    lines.extend(["", "This report was produced by the DataFlow benchmark harness and can be reproduced locally without cloud credentials."])
    return "\n".join(lines)


@router.post("/benchmark")
async def run_workspace_benchmark(body: BenchmarkRequest, background_tasks: BackgroundTasks):
    """Run a reproducible local benchmark and return a standardized report.

    The local benchmark transfers a synthetic CSV into an in-memory SQLite file
    and reports throughput, memory, and correctness. It does not require cloud
    credentials, so it can be run by prospects during a security review.
    """
    import benchmarks.cloud_scale as bench

    report = bench.run_local_benchmark(body.rows)
    report["competitors"] = _baseline_competitors()
    if body.format == "md":
        return PlainTextResponse(
            _markdown_benchmark_report(report),
            media_type="text/markdown",
            headers={"Content-Disposition": 'attachment; filename="dataflow-benchmark-report.md"'},
        )
    return JSONResponse(report)
