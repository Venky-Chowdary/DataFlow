"""Tenant-aware CORS policy for customer vanity / custom domains.

Starlette's ``CORSMiddleware`` loads ``allow_origins`` once at process start.
Enterprise tenants configure ``custom_domain`` at runtime via workspace APIs,
so we subclass and re-check Origin against live tenant hosts on every request.
"""

from __future__ import annotations

from urllib.parse import urlparse

from starlette.middleware.cors import CORSMiddleware

from services.platform_config import cors_origins


def origin_host(origin: str) -> str:
    try:
        parsed = urlparse(origin)
        host = (parsed.hostname or "").strip().lower()
        return host
    except Exception:
        return ""


def tenant_custom_origin_allowed(origin: str) -> bool:
    """True when Origin matches a configured tenant custom_domain."""
    host = origin_host(origin)
    if not host:
        return False
    try:
        from services.tenant_store import get_tenant_by_domain

        return get_tenant_by_domain(host) is not None
    except Exception:
        return False


def is_allowed_browser_origin(origin: str) -> bool:
    if not origin:
        return False
    if origin in cors_origins():
        return True
    return tenant_custom_origin_allowed(origin)


class TenantAwareCORSMiddleware(CORSMiddleware):
    """CORSMiddleware that accepts live tenant custom domains as Origins."""

    def is_allowed_origin(self, origin: str) -> bool:
        if super().is_allowed_origin(origin):
            return True
        return tenant_custom_origin_allowed(origin)
