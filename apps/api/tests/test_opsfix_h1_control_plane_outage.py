"""Ops review High 1: a control-plane outage must not stall the event loop.

The tenant settings live in the shared control-plane database on a
multi-replica deployment. When that database stops answering, an ``async``
middleware that reads it synchronously holds the only event loop for the
whole server-selection timeout, one request after another, and the liveness
probe (1 s) then restarts every API pod. These tests run the real pymongo
client against a closed port (a database that went away after the client
connected), with the client settings ``MongoDBService.connect`` uses.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi import FastAPI

from services import control_plane_store, mongodb_service, shared_doc_store, tenant_store

DOWN_URI = "mongodb://127.0.0.1:1/"


def _reset_outage() -> None:
    reset = getattr(shared_doc_store, "reset_outage_state", None)
    if reset is not None:
        reset()


@pytest.fixture
def mongo_down(monkeypatch):
    """The shared backend, on a client whose server is gone (production timeouts)."""
    from pymongo import MongoClient

    svc = mongodb_service.MongoDBService(DOWN_URI)
    svc.client = MongoClient(
        DOWN_URI, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000, connectTimeoutMS=5000, connect=False
    )
    svc.db_name = "opsfix_h1_unreachable"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    _reset_outage()
    yield svc
    _reset_outage()
    svc.client.close()


def _ticker(lags: list[float]):
    async def tick() -> None:
        while True:
            started = time.perf_counter()
            await asyncio.sleep(0.02)
            lags.append(time.perf_counter() - started - 0.02)

    return tick


def _tenant_app() -> FastAPI:
    from src.middleware.tenant_middleware import TenantMiddleware

    app = FastAPI()
    app.add_middleware(TenantMiddleware)

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/health/ready")
    async def ready():
        return {"ok": True}

    @app.get("/api/v1/thing")
    async def thing():
        return {"ok": True}

    return app


def test_outage_does_not_stall_the_loop_and_probes_answer_fast(mongo_down):
    """p1: three concurrent requests during an outage; the loop keeps turning."""

    async def scenario():
        lags: list[float] = []
        ticker = asyncio.create_task(_ticker(lags)())
        transport = httpx.ASGITransport(app=_tenant_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://app.example") as client:
            api = asyncio.create_task(client.get("/api/v1/thing"))
            await asyncio.sleep(0.05)  # the tenant lookup is now waiting on the database
            started = time.perf_counter()
            health = await client.get("/health")
            ready = await client.get("/health/ready")
            probe_seconds = time.perf_counter() - started
            refused = await api
            started = time.perf_counter()
            again = await client.get("/api/v1/thing")
            again_seconds = time.perf_counter() - started
        ticker.cancel()
        return health, ready, probe_seconds, refused, again, again_seconds, max(lags or [0.0])

    health, ready, probe_seconds, refused, again, again_seconds, stall = asyncio.run(scenario())
    assert health.status_code == 200 and ready.status_code == 200
    assert probe_seconds < 0.5, f"/health took {probe_seconds:.2f}s during a control-plane outage"
    assert stall < 0.3, f"the event loop stalled {stall:.2f}s on a control-plane read"
    # Fail closed: the request that needs its tenant is refused, never served without one.
    assert refused.status_code == 503 and refused.headers.get("retry-after")
    assert again.status_code == 503
    assert again_seconds < 0.3, (
        f"a second request during the outage took {again_seconds:.2f}s: a failed load must be "
        "remembered briefly instead of costing every request a full timeout"
    )


def test_control_plane_read_is_bounded_near_one_second(mongo_down):
    started = time.perf_counter()
    with pytest.raises(control_plane_store.ControlPlaneUnavailable):
        tenant_store.resolve_tenant_from_host("acme.example.com")
    elapsed = time.perf_counter() - started
    assert elapsed < 2.0, f"a control-plane read waited {elapsed:.2f}s (client timeout is 5 s)"


def test_failed_load_is_remembered_only_for_a_bounded_time(mongo_down, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_OUTAGE_SECONDS", "0.3")
    calls = {"n": 0}
    real = shared_doc_store._collection

    def counting(name):
        calls["n"] += 1
        return real(name)

    monkeypatch.setattr(shared_doc_store, "_collection", counting)
    with pytest.raises(control_plane_store.ControlPlaneUnavailable):
        tenant_store.list_tenants()
    with pytest.raises(control_plane_store.ControlPlaneUnavailable, match="unavailable"):
        tenant_store.list_tenants()
    window = shared_doc_store.outage_retry_at() - time.monotonic()
    assert 0 < window <= 0.3
    time.sleep(0.35)
    before = calls["n"]
    started = time.perf_counter()
    with pytest.raises(control_plane_store.ControlPlaneUnavailable):
        tenant_store.list_tenants()
    assert calls["n"] > before
    assert time.perf_counter() - started > 0.3, "after the window the database is tried again"


def test_outage_window_has_an_upper_bound(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_OUTAGE_SECONDS", "3600")
    assert shared_doc_store.outage_seconds() <= 10.0
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_TIMEOUT_SECONDS", "3600")
    assert shared_doc_store.operation_timeout_seconds() <= 5.0


def test_metrics_app_probe_never_reads_the_store(monkeypatch):
    from fastapi.testclient import TestClient

    from src import metrics_app
    from src.middleware import tenant_middleware

    reads: list[str] = []
    monkeypatch.setattr(tenant_middleware, "resolve_tenant_from_host", lambda host: reads.append(host))
    with TestClient(metrics_app.build_metrics_app(role="worker")) as client:
        assert client.get("/health").status_code == 200
    assert reads == [], "the probe path read the tenant store"


# ---------------------------------------------------------------------------
# The other async callers of the control-plane stores
# ---------------------------------------------------------------------------

SLOW = 0.6


async def _health_while(app: FastAPI, method: str, path: str, **kwargs) -> tuple[float, float]:
    """/health latency and the worst loop stall while ``method path`` is in flight."""
    lags: list[float] = []
    ticker = asyncio.create_task(_ticker(lags)())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Timed from before the slow request starts: on a blocked loop even the
        # short sleep cannot return until the blocking call ends.
        started = time.perf_counter()
        slow = asyncio.create_task(client.request(method, path, **kwargs))
        await asyncio.sleep(0.05)
        response = await client.get("/health")
        elapsed = time.perf_counter() - started - 0.05
        assert response.status_code == 200
        await slow
    ticker.cancel()
    return elapsed, max(lags or [0.0])


def _with_health(app: FastAPI) -> FastAPI:
    @app.get("/health")
    async def health():
        return {"ok": True}

    return app


def test_ip_allowlist_evaluation_runs_off_the_loop(monkeypatch):
    from starlette.middleware.base import BaseHTTPMiddleware

    from services import ip_allowlist_policy
    from src.middleware.workspace_ip_allowlist_middleware import WorkspaceIpAllowlistMiddleware
    from src.services import auth_service

    def slow_evaluate(_request):
        time.sleep(SLOW)
        return None

    monkeypatch.setattr(ip_allowlist_policy, "evaluate", slow_evaluate)
    monkeypatch.setattr(auth_service, "auth_required", lambda: True)

    class _SignedIn(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.user_email = "a@example.com"
            return await call_next(request)

    app = _with_health(FastAPI())
    app.add_middleware(WorkspaceIpAllowlistMiddleware)
    app.add_middleware(_SignedIn)

    @app.get("/api/v1/thing")
    async def thing():
        return {"ok": True}

    elapsed, stall = asyncio.run(_health_while(app, "GET", "/api/v1/thing"))
    assert elapsed < SLOW / 2 and stall < SLOW / 2, f"/health {elapsed:.2f}s, loop stall {stall:.2f}s"


def test_session_policy_lookup_runs_off_the_loop(monkeypatch):
    from src.middleware import auth_middleware

    def slow_attach(request, _token):
        time.sleep(SLOW)  # the session-policy tenant lookup inside the attach
        request.state.user_email = "a@example.com"
        request.state.user = {"email": "a@example.com", "role": "admin"}
        return None

    monkeypatch.setattr(auth_middleware._auth_service, "auth_required", lambda: True)
    monkeypatch.setattr(auth_middleware, "_attach_user_reason", slow_attach)
    monkeypatch.setattr(auth_middleware, "_tenant_bind_forbidden", lambda _request: None)
    from services import mfa_enforcement

    monkeypatch.setattr(mfa_enforcement, "gate", lambda *_a, **_k: None)
    app = _with_health(FastAPI())
    app.add_middleware(auth_middleware.AuthMiddleware)

    @app.get("/api/v1/thing")
    async def thing():
        return {"ok": True}

    elapsed, stall = asyncio.run(
        _health_while(app, "GET", "/api/v1/thing", headers={"Authorization": "Bearer t0k"})
    )
    assert elapsed < SLOW / 2 and stall < SLOW / 2, f"/health {elapsed:.2f}s, loop stall {stall:.2f}s"


def test_mcp_invocation_log_is_written_off_the_loop(monkeypatch):
    from services import mcp_invocation_log, mcp_policy
    from src.routers import mcp_router

    def slow_log(**_kwargs):
        time.sleep(SLOW)
        return {"id": "r1"}

    monkeypatch.setattr(mcp_router, "_require_mcp_tool_auth", lambda *_a, **_k: None)
    monkeypatch.setattr(mcp_policy, "policy_denial", lambda _name: "tool disabled by policy")
    monkeypatch.setattr(mcp_invocation_log, "log_mcp_invocation", slow_log)
    app = _with_health(FastAPI())
    app.include_router(mcp_router.router, prefix="/api/v1")
    elapsed, stall = asyncio.run(
        _health_while(app, "POST", "/api/v1/mcp/tools/call", json={"name": "list_jobs", "arguments": {}})
    )
    assert elapsed < SLOW / 2 and stall < SLOW / 2, f"/health {elapsed:.2f}s, loop stall {stall:.2f}s"


def test_shape_draft_save_runs_off_the_loop(monkeypatch):
    from services import shape_draft_store
    from src.routers import shape_router

    def slow_save(**kwargs):
        time.sleep(SLOW)
        return {"route_key": kwargs["route_key"]}

    monkeypatch.setattr(shape_draft_store, "save_shape_draft", slow_save)
    monkeypatch.setattr(shape_router, "resolve_write_workspace", lambda _request, _ws: "ws-1")
    app = _with_health(FastAPI())
    app.include_router(shape_router.router, prefix="/api/v1")
    elapsed, stall = asyncio.run(
        _health_while(app, "PUT", "/api/v1/shape/drafts", json={"route_key": "a->b", "recipe": {}})
    )
    assert elapsed < SLOW / 2 and stall < SLOW / 2, f"/health {elapsed:.2f}s, loop stall {stall:.2f}s"
