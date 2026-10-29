"""Tenant/custom-domain resolution and IP allowlist middleware.

Reads the ``Host`` header and, when it matches a configured tenant custom
domain, attaches ``tenant_id`` and the tenant's workspace/data region to the
request state.  Also enforces the tenant IP allowlist before the request reaches
authentication or business logic.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.audit_coverage import record_security_event
from services.brand_env import getenv_brand_bool
from services.platform_config import docs_enabled
from services.tenant_store import (
    is_request_ip_allowed,
    resolve_tenant_from_host,
)

# The bare root is public only as an exact match: as a prefix it would cover
# every path and switch the tenant IP allowlist off.
_PUBLIC_EXACT_PATHS = frozenset({"/"})

# Matched on a path-segment boundary: "/health" covers "/health" and
# "/health/ready", never "/healthz" or "/health-admin".
_PUBLIC_PREFIXES = (
    "/health",
    "/api/v1/health",
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/auth/bootstrap",
    "/auth/login",
    "/auth/logout",
    "/auth/bootstrap",
    "/auth/sso/providers",
    "/api/v1/auth/sso/providers",
    # Unauthenticated-by-design surfaces that the old "/" catch-all kept
    # reachable (signup/verify/reset, the Resend webhook which is called from
    # the provider's servers and can never be on a tenant allowlist, SSO
    # hand-offs, the contact-sales form).  Mirrors auth_middleware's list;
    # tests/test_tenant_middleware_allowlist.py guards against drift.
    "/api/v1/auth/signup",
    "/api/v1/auth/verify-email",
    "/api/v1/auth/resend",
    "/api/v1/auth/resend-webhook",
    "/api/v1/auth/form-token",
    "/api/v1/auth/password/forgot",
    "/api/v1/auth/password/reset",
    "/api/v1/auth/sso",
    "/api/v1/public/form-token",
    "/api/v1/public/email-events",
    "/api/v1/contact-sales",
    "/api/v1/catalog",
    "/auth/signup",
    "/auth/verify-email",
    "/auth/resend",
    "/auth/resend-webhook",
    "/auth/form-token",
    "/auth/password/forgot",
    "/auth/password/reset",
    "/auth/sso",
    "/public/form-token",
    "/public/email-events",
    "/catalog",
    "/.well-known/oauth-protected-resource",
)

if docs_enabled():
    _PUBLIC_PREFIXES = _PUBLIC_PREFIXES + ("/docs", "/redoc", "/openapi.json")


def _matches_segment_prefix(path: str, prefix: str) -> bool:
    prefix = prefix.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


def _is_public_mcp_path(path: str) -> bool:
    if path in ("/api/v1/mcp", "/api/v1/mcp/"):
        return True
    if _matches_segment_prefix(path, "/api/v1/mcp/manifest") or _matches_segment_prefix(
        path, "/api/v1/mcp/status"
    ):
        return True
    return path.rstrip("/") == "/api/v1/mcp/tools"


def _is_public_path(path: str) -> bool:
    if path in _PUBLIC_EXACT_PATHS:
        return True
    if any(_matches_segment_prefix(path, p) for p in _PUBLIC_PREFIXES if p.strip("/")):
        return True
    return _is_public_mcp_path(path)


def _allowlist_enforced() -> bool:
    """``DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE`` (default off).

    Before the segment-boundary fix a bare ``"/"`` prefix made every path
    public, so the per-tenant allowlist was never enforced in production.
    Turning it on is an operator decision; until then a request that the
    allowlist would block is audited and allowed.
    """
    return getenv_brand_bool("TENANT_IP_ALLOWLIST_ENFORCE", False)


def _client_ip(request: Request) -> str:
    """Phase D1 — trust-boundary aware (never left-most XFF by default)."""
    from services.client_ip import client_ip_from_request

    return client_ip_from_request(request)


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        host = (request.headers.get("host") or "").split(":")[0].strip().lower()
        tenant = None
        if host:
            tenant = resolve_tenant_from_host(host)

        if tenant:
            request.state.tenant_id = tenant.id
            request.state.tenant_workspace_id = tenant.workspace_id
            request.state.data_region = tenant.data_region or "us-east-1"

            if tenant.ip_allowlist and not _is_public_path(request.url.path):
                client_ip = _client_ip(request)
                if not is_request_ip_allowed(client_ip, tenant):
                    if not _allowlist_enforced():
                        record_security_event(
                            "tenant.ip_allowlist.would_block",
                            outcome="skipped",
                            request=request,
                            resource=request.url.path,
                            reason_code="ip_not_in_tenant_allowlist",
                            level="warn",
                            details={
                                "tenant_id": tenant.id,
                                "workspace_id": tenant.workspace_id,
                                "path": request.url.path,
                                "client_ip": client_ip,
                                "enforce_flag": "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE",
                            },
                        )
                        return await call_next(request)
                    return JSONResponse(
                        status_code=403,
                        content={
                            "detail": "Access denied by tenant IP allowlist",
                            "ip": client_ip,
                        },
                    )

        return await call_next(request)
