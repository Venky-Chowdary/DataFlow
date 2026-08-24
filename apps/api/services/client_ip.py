"""Trust-boundary-aware client IP resolution (Phase D1 / audit §6.3).

Never trust the left-most ``X-Forwarded-For`` hop by default — that value is
client-controlled. When ``TRUSTED_PROXY_COUNT`` (or brand alias) is N>0, strip
N hops from the right (our proxies) and take the right-most remaining hop.
When N=0, ignore forwarded headers and use the socket peer only.
"""

from __future__ import annotations

from typing import Any


def trusted_proxy_count() -> int:
    try:
        from services.brand_env import getenv_brand

        raw = getenv_brand("TRUSTED_PROXY_COUNT", "0")
    except Exception:
        import os

        raw = os.environ.get("DATAFLOW_TRUSTED_PROXY_COUNT") or os.environ.get(
            "TRUSTED_PROXY_COUNT", "0"
        )
    try:
        return max(0, int(str(raw or "0").strip() or "0"))
    except ValueError:
        return 0


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
        # Fail closed against spoofed allowlist bypass.
        return remote

    hops = [h.strip() for h in (x_forwarded_for or "").split(",") if h.strip()]
    if hops:
        if len(hops) <= n:
            # Fewer hops than trusted proxies → cannot identify client; socket only.
            return remote
        untrusted = hops[:-n]
        return untrusted[-1] if untrusted else remote

    real = (x_real_ip or "").strip()
    # X-Real-IP is also forgeable unless the edge proxy overwrites it; only
    # accept when we have at least one trusted proxy in front.
    return real or remote


def client_ip_from_request(request: Any, *, trusted_proxies: int | None = None) -> str:
    """Starlette/FastAPI Request → client IP."""
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
