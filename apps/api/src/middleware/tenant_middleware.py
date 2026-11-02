"""Tenant/custom-domain resolution, and the public paths no allowlist covers.

Reads the ``Host`` header and, when it matches a configured tenant custom
domain, attaches ``tenant_id`` and the tenant's workspace/data region to the
request state. The workspace IP allowlist is enforced after authentication
(src/middleware/workspace_ip_allowlist_middleware.py); the public-path list
below is what it exempts.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from services.platform_config import docs_enabled
from services.tenant_store import resolve_tenant_from_host

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
    """Kill switch for workspace IP allowlists (services.ip_allowlist_policy).

    On by default once a workspace saves a list; the platform can switch it
    off deployment-wide, which turns refusals into audited admissions.
    """
    from services.ip_allowlist_policy import enforcement_on

    return enforcement_on()


class TenantMiddleware(BaseHTTPMiddleware):
    """Attach the custom-domain tenant to the request.

    The IP allowlist is no longer decided here: before auth there is no
    actor, and the shared app host never resolved a tenant, so the list was
    never applied there (QA round 11, S7). It is enforced after auth by
    ``WorkspaceIpAllowlistMiddleware`` for every host, which also reads the
    workspace this middleware resolves from the Host header.
    """

    async def dispatch(self, request: Request, call_next):
        host = (request.headers.get("host") or "").split(":")[0].strip().lower()
        tenant = resolve_tenant_from_host(host) if host else None
        if tenant:
            request.state.tenant_id = tenant.id
            request.state.tenant_workspace_id = tenant.workspace_id
            request.state.data_region = tenant.data_region or "us-east-1"
        return await call_next(request)
