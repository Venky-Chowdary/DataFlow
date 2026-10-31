"""Enforce workspace IP allowlists after authentication (QA round 11, S7).

Runs inside ``AuthMiddleware`` (it needs the authenticated actor) and before
RBAC and the routers, so a refused request learns nothing about the
workspace. Public paths (sign-in, health, discovery, webhooks) are exempt,
the same list the custom-domain resolver uses. The policy itself lives in
services.ip_allowlist_policy; every block, break-glass use and kill-switch
admission is audited.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from services import ip_allowlist_policy as policy
from services.audit_coverage import record_security_event
from src.middleware.tenant_middleware import _is_public_path


def _auth_kind(request: Request) -> str:
    user = getattr(request.state, "user", None)
    if isinstance(user, dict) and str(user.get("auth_kind") or "") == "api_key":
        return "api_key"
    if getattr(request.state, "cookie_session_jti", None):
        return "cookie_session"
    return "session"


def _details(request: Request, refusal: policy.Refusal) -> dict:
    return {
        "workspace_id": refusal.workspace_id,
        "tenant_id": refusal.tenant_id,
        "client_ip": refusal.client_ip,
        "path": request.url.path,
        "method": request.method,
        "auth_kind": _auth_kind(request),
    }


class WorkspaceIpAllowlistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS" or _is_public_path(request.url.path):
            return await call_next(request)
        if not getattr(request.state, "user_email", None):
            return await call_next(request)
        from src.services.auth_service import auth_required

        if not auth_required():
            return await call_next(request)
        refusal = policy.evaluate(request)
        if refusal is None:
            return await call_next(request)

        details = _details(request, refusal)
        admit = ""
        if not policy.enforcement_on():
            admit, reason_code = "switched_off_by_platform", policy.BLOCK_REASON
        elif not policy.client_ip_trusted(refusal.client_ip):
            # Hop count unset and a private/proxy address: never block on it.
            admit, reason_code = "proxy_hop_count_unset", policy.UNTRUSTED_REASON
        if admit:
            record_security_event(
                "tenant.ip_allowlist.would_block", outcome="skipped", request=request,
                resource=request.url.path, reason_code=reason_code, level="warn",
                details={**details, "enforcement": admit}, workspace_id=refusal.workspace_id,
            )
            return await call_next(request)

        reason = policy.break_glass_reason(request) if refusal.unrestricted else ""
        if reason:
            record_security_event(
                "workspace.ip_allowlist.break_glass", outcome="succeeded", request=request,
                resource=request.url.path, reason_code="platform_admin_break_glass", level="warn",
                details={**details, "reason": reason}, workspace_id=refusal.workspace_id,
            )
            return await call_next(request)

        record_security_event(
            "workspace.ip_allowlist.blocked", outcome="failed", request=request,
            resource=request.url.path, reason_code=policy.BLOCK_REASON, status=403, level="warn",
            details=details, workspace_id=refusal.workspace_id,
        )
        body = {
            "detail": (
                "This workspace only accepts requests from its IP allowlist, and your "
                f"address ({refusal.client_ip or 'unknown'}) is not on it. Ask a workspace "
                "administrator to add it."
            ),
            "reason": policy.BLOCK_REASON,
            "workspace_id": refusal.workspace_id,
            "ip": refusal.client_ip,
        }
        if refusal.unrestricted:
            body["break_glass_header"] = policy.BREAK_GLASS_HEADER
            body["break_glass_hint"] = (
                "Platform administrators can send this header with a reason of at least "
                f"{policy.BREAK_GLASS_MIN_REASON} characters; every use is audited."
            )
        return JSONResponse(status_code=403, content=body)
