"""Bind the authenticated caller for saved-connector resolution.

Runs inside ``AuthMiddleware`` (identity is already on ``scope["state"]``) and
binds ``services.connector_access`` for the rest of the request, so every
connector lookup the request reaches — routers, transfer engine, preflight,
Pilot tools — is limited to workspaces the caller may use. Pure ASGI so the
binding is set in the same context the endpoint (and its threadpool) runs in.
"""

from __future__ import annotations

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from services.connector_access import bind_connector_caller, caller_from_request


class ConnectorAccessMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        caller = caller_from_request(Request(scope))
        with bind_connector_caller(caller):
            await self.app(scope, receive, send)
