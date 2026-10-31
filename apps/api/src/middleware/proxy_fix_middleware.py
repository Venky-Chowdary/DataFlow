"""Client address and scheme behind the configured proxy chain (ProxyFix).

Outermost ASGI layer. With ``DATAWRAP_TRUSTED_PROXY_COUNT`` = N > 0 it
rewrites the request's ``client`` to ``X-Forwarded-For`` ``hops[-N]``
(services.client_ip, Werkzeug ProxyFix semantics) and its ``scheme`` to the
``X-Forwarded-Proto`` the nearest proxy sent, so ``request.client``,
``request.url`` and every audit/allowlist/rate-limit decision see the browser,
not the Railway edge. N = 0 (default) changes nothing.

uvicorn runs with ``--no-proxy-headers`` (scripts/start-api.sh): its own
middleware with ``--forwarded-allow-ips '*'`` — the only setting that works
behind Railway's edge, whose address is not fixed — takes the LEFT-most
``X-Forwarded-For`` entry (uvicorn 0.54 ``_TrustedHosts.get_trusted_client_address``),
which the client controls. One place decides, from the hop count.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send

from services.client_ip import resolve_client_ip, trusted_proxy_count

#: The socket peer before rewriting, for diagnostics.
SOCKET_PEER_KEY = "dw_socket_peer"


def _header(scope: Scope, name: bytes) -> str:
    values = [v.decode("latin-1") for k, v in scope.get("headers") or [] if k == name]
    return ", ".join(values)


class ProxyFixMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        n = trusted_proxy_count()
        if n > 0:
            client = scope.get("client")
            peer = str(client[0]) if client else ""
            scope[SOCKET_PEER_KEY] = peer
            ip = resolve_client_ip(
                x_forwarded_for=_header(scope, b"x-forwarded-for"),
                x_real_ip=_header(scope, b"x-real-ip"),
                remote_addr=peer,
                trusted_proxies=n,
            )
            if ip and ip != peer:
                scope["client"] = (ip, 0)
            proto = _header(scope, b"x-forwarded-proto").split(",")[-1].strip().lower()
            if proto in ("http", "https"):
                if scope["type"] == "websocket":
                    scope["scheme"] = "wss" if proto == "https" else "ws"
                else:
                    scope["scheme"] = proto
        await self.app(scope, receive, send)
