"""Trust-boundary-aware client IP resolution (Phase D1 / audit §6.3).

The model is Werkzeug ``ProxyFix(x_for=N)``: every proxy in front of the app
APPENDS the address of the peer it received the request from to
``X-Forwarded-For`` (nginx ``$proxy_add_x_forwarded_for``, the Railway edge,
AWS ALB and Cloudflare all do this). A proxy never appends its own address.
So with N trusted proxies the right-most N entries were written by them, and
entry ``hops[-N]`` is the address the outermost trusted proxy saw connect to
it: the real client. Everything left of it was supplied by the client and is
never trusted.

Railway (browser -> edge -> nginx web -> edge -> API) is N=3::

    X-Forwarded-For: <anything the client sent>, <client>, <edge-1 peer>, <nginx egress>
                                                 ^ hops[-3]

The count comes from ``DATAWRAP_TRUSTED_PROXY_COUNT`` (the ``DATAFLOW_``
alias and a bare ``TRUSTED_PROXY_COUNT`` are accepted). Default 0: forwarded
headers are ignored and the socket peer is used, which cannot be spoofed.
When the header has fewer hops than trusted proxies the request did not come
through the whole chain (or the count is wrong), so the socket peer is used
rather than a client-supplied value. See docs/DEPLOYMENT.md.
"""

from __future__ import annotations

import os
from typing import Any


def trusted_proxy_count() -> int:
    try:
        from services.brand_env import getenv_brand

        raw = getenv_brand("TRUSTED_PROXY_COUNT", "") or ""
    except Exception:  # noqa: BLE001 - brand helper unavailable in a bare import
        raw = ""
    if not str(raw).strip():
        raw = os.environ.get("TRUSTED_PROXY_COUNT", "0")
    try:
        return max(0, int(str(raw or "0").strip() or "0"))
    except ValueError:
        return 0


def _strip_port(value: str) -> str:
    """``203.0.113.5:443`` -> ``203.0.113.5``; ``[2001:db8::1]:443`` -> ``2001:db8::1``."""
    v = value.strip()
    if v.startswith("["):
        end = v.find("]")
        return v[1:end] if end > 0 else v
    if v.count(":") == 1:
        return v.split(":", 1)[0]
    return v


def resolve_client_ip(
    *,
    x_forwarded_for: str = "",
    x_real_ip: str = "",
    remote_addr: str = "",
    trusted_proxies: int | None = None,
) -> str:
    """Return the client IP under the configured trust boundary."""
    n = trusted_proxy_count() if trusted_proxies is None else max(0, int(trusted_proxies))
    remote = (remote_addr or "").strip()
    if n <= 0:
        # Fail closed against a spoofed allowlist bypass: socket peer only.
        return remote

    hops = [h.strip() for h in (x_forwarded_for or "").split(",") if h.strip()]
    if hops:
        if len(hops) < n:
            # Shorter than the trusted chain: never take a client-supplied hop.
            return remote
        return _strip_port(hops[-n])

    real = (x_real_ip or "").strip()
    # X-Real-IP is forgeable unless the edge overwrites it; only accepted when
    # at least one trusted proxy is in front and it sent no X-Forwarded-For.
    return _strip_port(real) if real else remote


def client_ip_from_request(request: Any, *, trusted_proxies: int | None = None) -> str:
    """Starlette/FastAPI Request -> client IP."""
    headers = getattr(request, "headers", {}) or {}
    client = getattr(request, "client", None)
    remote = ""
    if client is not None:
        remote = str(getattr(client, "host", "") or "")
    return resolve_client_ip(
        x_forwarded_for=str(headers.get("x-forwarded-for") or ""),
        x_real_ip=str(headers.get("x-real-ip") or ""),
        remote_addr=remote,
        trusted_proxies=trusted_proxies,
    )


__all__ = [
    "client_ip_from_request",
    "resolve_client_ip",
    "trusted_proxy_count",
]
