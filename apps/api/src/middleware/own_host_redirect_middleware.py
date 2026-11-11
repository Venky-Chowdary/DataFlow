"""Redirects never name the API service's own host (D339).

Starlette's ``redirect_slashes`` (``GET /api/v1/schedules`` -> 307
``/api/v1/schedules/``) builds an absolute ``Location`` from the request's own
``Host`` and scheme. Behind the web nginx / Railway edge that is the internal
API host over plain http (TLS ends at the edge), so a client following it left
the public origin and could re-send its bearer there.

This layer rewrites a redirect ``Location`` that points at the request's own
host to the same path and query, relative (RFC 9110 allows a relative
``Location``). The client resolves it against the URL it actually used —
``https://<public origin>/api/v1/schedules/`` — so it stays on that origin and
scheme without the API having to know it. Redirects to any other host (an IdP,
the web origin) are untouched. Both trailing-slash directions keep working;
only the host disappears from the header.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_REDIRECTS = frozenset({301, 302, 303, 307, 308})


def _own_host(scope: Scope) -> str:
    for key, value in scope.get("headers") or []:
        if key == b"host":
            return value.decode("latin-1").strip().lower()
    server = scope.get("server")
    if server:
        host, port = server[0], server[1]
        return f"{host}:{port}".lower() if port not in (None, 80, 443) else str(host).lower()
    return ""


def relative_own_host_location(location: str, own_host: str) -> str | None:
    """``location`` as path + query when it is absolute on ``own_host``; else ``None``."""
    if not location or not own_host or "://" not in location:
        return None
    parts = urlsplit(location)
    if parts.scheme not in ("http", "https") or parts.netloc.lower() != own_host.lower():
        return None
    relative = parts.path or "/"
    if parts.query:
        relative += f"?{parts.query}"
    if parts.fragment:
        relative += f"#{parts.fragment}"
    return relative


class OwnHostRedirectMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        own = _own_host(scope)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start" and message.get("status") in _REDIRECTS:
                headers = list(message.get("headers") or [])
                for i, (key, value) in enumerate(headers):
                    if key.lower() == b"location":
                        rel = relative_own_host_location(value.decode("latin-1"), own)
                        if rel is not None:
                            headers[i] = (key, rel.encode("latin-1"))
                            message = {**message, "headers": headers}
                        break
            await send(message)

        await self.app(scope, receive, send_wrapper)
