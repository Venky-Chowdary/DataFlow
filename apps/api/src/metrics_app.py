"""``/metrics`` for processes that serve no API: workers and the scheduler (audit M-7).

In the worker topology the API pods run no transfers, so their in-process
counters stay at zero while the workers had no HTTP endpoint at all. A worker
or scheduler started with ``DATAFLOW_METRICS_PORT`` serves two routes on that
port:

``/health``   liveness for the orchestrator's probes; public, no data.
``/metrics``  the same exposition the API serves, behind the same chain the
              API uses: tenant resolution, authentication, the workspace IP
              allowlist and RBAC (``GET /metrics`` needs ``workspace.manage``).
              A scraper sends an API key as a bearer token, exactly as it
              does against the API. It is never served unauthenticated. The
              deployment-wide series (HTTP traffic, job gauges) need a
              platform administrator's key.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from starlette.concurrency import run_in_threadpool

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

_server: Any = None
_thread: threading.Thread | None = None


def _platform_admin(request: Any) -> bool:
    """Deployment-wide series are every tenant's: platform administrators only (ops review M9).

    ``GET /metrics`` itself needs ``workspace.manage`` in some workspace. A
    platform-admin API key (minted by a platform admin outside any
    workspace) is what a deployment's scraper should use to collect them.
    """
    from services.workspace_access import tenant_scope

    return bool(tenant_scope(request).unrestricted)


def _caller_filter(request: Any) -> Any:
    """The workspaces whose pipelines a scoped scrape may show (none when unknown; never unscoped rows)."""
    from services.workspace_access import tenant_scope

    strict = getattr(tenant_scope(request), "strict_filter", None)
    return strict() if callable(strict) else None


async def metrics_response(request: Any) -> PlainTextResponse:
    """The exposition body. Built off the event loop: it reads the job store."""
    from services.ops_metrics import prometheus_text

    deployment_wide = await run_in_threadpool(_platform_admin, request)
    scope = None if deployment_wide else await run_in_threadpool(_caller_filter, request)
    return PlainTextResponse(
        content=await run_in_threadpool(prometheus_text, deployment_wide=deployment_wide, workspace_filter=scope),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


def build_metrics_app(*, role: str, health: Any = None) -> FastAPI:
    """A two-route app with the API's own security middleware, in the API's order."""
    from services.rbac import RBACMiddleware
    from src.middleware.auth_middleware import AuthMiddleware
    from src.middleware.tenant_middleware import TenantMiddleware
    from src.middleware.tenant_scope_middleware import TenantScopeMiddleware
    from src.middleware.workspace_ip_allowlist_middleware import WorkspaceIpAllowlistMiddleware

    app = FastAPI(title=f"Datawrap {role} metrics", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TenantScopeMiddleware)
    app.add_middleware(RBACMiddleware)
    app.add_middleware(WorkspaceIpAllowlistMiddleware)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(TenantMiddleware)

    @app.get("/health")
    async def health_route() -> dict[str, Any]:
        extra = health() if callable(health) else {}
        return {"status": "ok", "role": role, **(extra or {})}

    @app.get("/metrics")
    async def metrics_route(request: Request) -> PlainTextResponse:
        return await metrics_response(request)

    return app


def metrics_port() -> int | None:
    """``DATAFLOW_METRICS_PORT`` as a port number, None when unset or invalid."""
    raw = (getenv_brand("METRICS_PORT", "") or "").strip()
    if not raw:
        return None
    try:
        port = int(raw)
    except ValueError:
        logger.error("METRICS_PORT=%r is not a port number; metrics endpoint not started", raw)
        return None
    return port if 0 < port < 65536 else None


def start_metrics_server(*, role: str, health: Any = None, port: int | None = None) -> bool:
    """Serve ``/health`` and ``/metrics`` on a daemon thread. False when no port is configured."""
    global _server, _thread
    chosen = port if port is not None else metrics_port()
    if chosen is None:
        return False
    if _thread is not None and _thread.is_alive():
        return True
    import uvicorn

    config = uvicorn.Config(
        build_metrics_app(role=role, health=health),
        host=getenv_brand("METRICS_HOST", "0.0.0.0") or "0.0.0.0",  # nosec B104 - pod-internal port; /metrics is authenticated
        port=chosen,
        log_level="warning",
        access_log=False,
        proxy_headers=False,
    )
    server = uvicorn.Server(config)
    # The worker owns SIGTERM (drain); uvicorn must not replace its handlers.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    _server = server
    _thread = threading.Thread(target=server.run, name="df-metrics-http", daemon=True)
    _thread.start()
    logger.info("%s metrics endpoint listening on port %s (/health public, /metrics authenticated)", role, chosen)
    return True


def stop_metrics_server(timeout: float = 5.0) -> None:
    global _server, _thread
    if _server is not None:
        _server.should_exit = True
    if _thread is not None:
        _thread.join(timeout)
    _server = None
    _thread = None
