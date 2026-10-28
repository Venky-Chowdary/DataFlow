"""Bind the request's tenant scope for store-level reads (D154).

Routers authorize through ``services.workspace_access.tenant_scope``. Pilot and
MCP tools, and anything else that reads a tenant-owned store without a request
in hand, ask the store directly; those stores consult the scope bound here
(``services.tenant_scope.current_workspace_filter``), so they cannot return
another workspace's rows even when a caller forgets to pass a filter. Code that
runs without a request (schedulers, workers, scripts) has nothing bound and
keeps system scope.

Runs inside ``AuthMiddleware`` (it needs the authenticated actor).
"""

from __future__ import annotations

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from services.tenant_scope import bind_tenant_scope, scope_for_binding


class TenantScopeMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        with bind_tenant_scope(scope_for_binding(Request(scope))):
            await self.app(scope, receive, send)
