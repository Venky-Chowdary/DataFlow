"""Tenant-aware CORS policy for customer vanity / custom domains.

Starlette's ``CORSMiddleware`` loads ``allow_origins`` once at process start.
Enterprise tenants configure ``custom_domain`` at runtime via workspace APIs,
so we subclass and re-check Origin against live tenant hosts on every request.

D337: a saved domain is only a claim. A tenant origin is trusted only when

* its DNS ownership challenge passed (services.custom_domain_verification);
  an unverified domain is answered like any stranger origin: no
  ``Access-Control-Allow-Origin`` and no ``Access-Control-Allow-Credentials``;
* it is ``https://<domain>`` exactly (no other scheme or port);
* the request belongs to that tenant's workspace: a non-preflight response
  carries the CORS headers only when ``X-Workspace-Id`` names the tenant's
  workspace, so one tenant's site can never read another workspace's data
  through a signed-in browser. A preflight carries no credentials and no
  data, so a verified origin may pass it.

``DATAWRAP_CORS_INCLUDE_TENANT_DOMAINS=0`` turns tenant origins off entirely.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from starlette.datastructures import MutableHeaders
from starlette.middleware.cors import CORSMiddleware

from services.platform_config import cors_origins


def origin_host(origin: str) -> str:
    try:
        parsed = urlparse(origin)
        host = (parsed.hostname or "").strip().lower()
        return host
    except Exception:
        return ""


def _tenant_origins_enabled() -> bool:
    from services.brand_env import getenv_brand

    return getenv_brand("CORS_INCLUDE_TENANT_DOMAINS", "1").lower() not in ("0", "false", "no", "off")


def verified_tenant_for_origin(origin: str) -> Any:
    """The tenant whose DNS-verified custom domain is exactly this origin, or None."""
    if not origin or not _tenant_origins_enabled():
        return None
    try:
        parsed = urlparse(origin)
        if parsed.scheme != "https" or parsed.port is not None or parsed.path not in ("", "/"):
            return None
    except ValueError:
        return None
    host = origin_host(origin)
    if not host:
        return None
    try:
        from services.tenant_store import get_tenant_by_domain

        return get_tenant_by_domain(host, verified_only=True)
    except Exception:
        return None


def tenant_custom_origin_allowed(origin: str) -> bool:
    """True when Origin is a tenant custom domain whose DNS ownership is proven."""
    return verified_tenant_for_origin(origin) is not None


def tenant_origin_allowed_for_request(origin: str, request_headers: Any) -> bool:
    """A verified tenant origin, on a request scoped to that tenant's workspace."""
    tenant = verified_tenant_for_origin(origin)
    if tenant is None:
        return False
    from services.effective_role import workspace_id_from_request_headers

    return bool(tenant.workspace_id) and workspace_id_from_request_headers(request_headers) == tenant.workspace_id


def is_allowed_browser_origin(origin: str) -> bool:
    if not origin:
        return False
    if origin in cors_origins():
        return True
    return tenant_custom_origin_allowed(origin)


class TenantAwareCORSMiddleware(CORSMiddleware):
    """CORSMiddleware that accepts DNS-verified tenant custom domains as Origins."""

    def is_allowed_origin(self, origin: str) -> bool:
        if super().is_allowed_origin(origin):
            return True
        return tenant_custom_origin_allowed(origin)

    def preflight_response(self, request_headers):  # type: ignore[override]
        response = super().preflight_response(request_headers)
        if not self.is_allowed_origin(request_headers["origin"]):
            # A refused preflight must not carry the credentials grant either.
            if "access-control-allow-credentials" in response.headers:
                del response.headers["access-control-allow-credentials"]
        return response

    async def send(self, message, send, request_headers) -> None:  # type: ignore[override]
        origin = request_headers.get("origin")
        if (
            message["type"] == "http.response.start"
            and origin is not None
            and not CORSMiddleware.is_allowed_origin(self, origin)
            and not tenant_origin_allowed_for_request(origin, request_headers)
        ):
            # A stranger origin (or a tenant origin outside its workspace):
            # no allow-origin and no allow-credentials, just Vary.
            message.setdefault("headers", [])
            headers = MutableHeaders(scope=message)
            headers["Vary"] = ", ".join([*headers.getlist("Vary"), "Origin"])
            await send(message)
            return
        await super().send(message, send, request_headers)
