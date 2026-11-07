"""The public origin (scheme + host) that links handed to users must use.

QA round 11: the MCP manifest named ``http://dataflow-api-production-722b.up.railway.app``
because it echoed ``request.base_url``. Behind the web nginx the Host header is
the Railway backend (nginx rewrites it so the edge routes the request) and the
scheme is plain http (TLS ends at the edge), so the request itself cannot say
what the browser used. SAML (``auth_router._saml_base_url``) had the same
class of bug the other way round: it trusted any ``X-Forwarded-Host``.

Resolution order:

1. ``X-Forwarded-Host`` (else ``Host``) when it is on the allow-list: the
   hosts of the configured origins below, ``DATAWRAP_PUBLIC_ORIGIN_ALLOWED_HOSTS``
   (comma list), and saved tenant custom domains. A tenant on its own domain
   gets links on that domain.
2. ``DATAWRAP_PUBLIC_API_ORIGIN``, then the web origin (``DATAWRAP_WEB_URL`` /
   ``DATAWRAP_WEB_DOMAIN``): the browser reaches the API same-origin through
   the web nginx at ``/api``.
3. Outside production only: the request's own base URL (local development).

In production the scheme is always https. A forwarded host that is not on the
allow-list is ignored, never echoed.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlsplit

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)


def _origin(value: str) -> str:
    v = (value or "").strip().rstrip("/")
    if not v:
        return ""
    if "://" not in v:
        v = f"https://{v}"
    parts = urlsplit(v)
    if not parts.hostname:
        return ""
    return f"{parts.scheme}://{parts.netloc}"


def configured_origin() -> str:
    """The operator-configured public origin, or ``""``."""
    explicit = _origin(getenv_brand("PUBLIC_API_ORIGIN", "") or "")
    if explicit:
        return explicit
    # Not platform_config.web_url(): on Railway it falls back to this service's
    # own RAILWAY_PUBLIC_DOMAIN, which on the API service is the backend host.
    return _origin(getenv_brand("WEB_URL", "") or "") or _origin(getenv_brand("WEB_DOMAIN", "") or "")


def allowed_hosts() -> set[str]:
    hosts: set[str] = set()
    for origin in (
        _origin(getenv_brand("PUBLIC_API_ORIGIN", "") or ""),
        _origin(getenv_brand("WEB_URL", "") or ""),
        _origin(getenv_brand("WEB_DOMAIN", "") or ""),
    ):
        if origin:
            hosts.add(urlsplit(origin).netloc.lower())
    extra = getenv_brand("PUBLIC_ORIGIN_ALLOWED_HOSTS", "") or ""
    hosts.update(h.strip().lower() for h in extra.split(",") if h.strip())
    try:
        from services.tenant_store import list_tenants

        hosts.update(t.normalized_domain for t in list_tenants() if t.normalized_domain and t.workspace_id)
    except Exception:  # noqa: BLE001 — the allow-list only ever narrows
        pass
    return hosts


def _production() -> bool:
    from services.platform_config import is_production

    return is_production()


def public_origin(request: Any = None) -> str:
    """``https://host[:port]`` for links shown to users (no trailing slash)."""
    prod = _production()
    if request is not None:
        headers = request.headers
        forwarded = (headers.get("x-forwarded-host") or "").split(",")[0].strip().lower()
        own = (headers.get("host") or "").strip().lower()
        allowed = allowed_hosts()
        for host in (forwarded, own):
            if host and host in allowed:
                proto = (headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
                if prod:
                    scheme = "https"
                else:
                    scheme = proto if proto in ("http", "https") else request.url.scheme
                return f"{scheme}://{host}"
    origin = configured_origin()
    if not origin and request is not None and not prod:
        origin = str(request.base_url).rstrip("/")
    if not origin and request is not None:
        # Production with no public origin configured (a deploy error that
        # validate_production_config also reports): the request host, never http.
        logger.warning("public origin not configured; set the public API origin or web domain")
        origin = f"https://{request.url.netloc}"
    if prod and origin.startswith("http://"):
        origin = "https://" + origin[len("http://"):]
    return origin


__all__ = ["allowed_hosts", "configured_origin", "public_origin"]
