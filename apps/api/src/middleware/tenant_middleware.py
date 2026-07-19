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

from services.platform_config import docs_enabled
from services.tenant_store import (
    is_request_ip_allowed,
    resolve_tenant_from_host,
)

_PUBLIC_PREFIXES = (
    "/health",
    "/",
    "/api/v1/auth/login",
    "/api/v1/auth/bootstrap",
    "/auth/login",
    "/auth/bootstrap",
    "/auth/sso/providers",
    "/api/v1/auth/sso/providers",
    "/api/v1/mcp",
)

if docs_enabled():
    _PUBLIC_PREFIXES = _PUBLIC_PREFIXES + ("/docs", "/redoc", "/openapi.json")


def _is_public_path(path: str) -> bool:
    return any(path.startswith(p) for p in _PUBLIC_PREFIXES)


def _client_ip(request: Request) -> str:
    for header in ("x-forwarded-for", "x-real-ip"):
        value = request.headers.get(header, "")
        if value:
            return value.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return ""


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
                    return JSONResponse(
                        status_code=403,
                        content={
                            "detail": "Access denied by tenant IP allowlist",
                            "ip": client_ip,
                        },
                    )

        return await call_next(request)
