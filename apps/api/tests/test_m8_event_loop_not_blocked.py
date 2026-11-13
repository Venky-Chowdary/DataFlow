"""Audit M-8: a slow database call in one handler must not stall the others.

The API runs one uvicorn worker. An ``async`` handler that calls the
synchronous job store directly holds the only event loop for as long as the
store takes, so ``/health`` and every other request wait behind it.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import os
import time

import httpx
import pytest
from fastapi import FastAPI

SLOW_SECONDS = 0.8


class _SlowStore:
    """A job store whose reads block the calling thread, like a slow MongoDB."""

    def __init__(self, job: dict, delay: float = SLOW_SECONDS) -> None:
        self.job = job
        self.delay = delay
        self.reads = 0

    def get_job(self, _job_id):
        self.reads += 1
        time.sleep(self.delay)
        return dict(self.job) if self.job else None

    def get_connector(self, _connector_id):
        self.reads += 1
        time.sleep(self.delay)
        return {"_id": "c1", "id": "c1", "name": "pg", "type": "postgresql", "config": {}, "workspace_id": ""}


@pytest.fixture
def app_and_store(monkeypatch):
    connectors_router = importlib.import_module("src.routers.connectors_router")

    store = _SlowStore({"_id": "job-1", "job_id": "job-1", "status": "running", "workspace_id": ""})
    monkeypatch.setattr(connectors_router, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(connectors_router, "_can_access_job", lambda _request, _job: True)
    app = FastAPI()
    app.include_router(connectors_router.router, prefix="/api/v1")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app, store


async def _health_latency_while(app: FastAPI, slow_path: str) -> tuple[float, bool]:
    """Seconds /health took while ``slow_path`` was in flight, and whether it still was."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:

        async def slow() -> None:
            async with client.stream("GET", slow_path) as response:
                async for _chunk in response.aiter_bytes():
                    break

        # Timed from before the slow request starts: on a blocked loop even
        # the short sleep below cannot return until the store call ends.
        started = time.perf_counter()
        slow_task = asyncio.create_task(slow())
        await asyncio.sleep(0.1)  # the slow handler is now inside its store call
        response = await client.get("/health")
        elapsed = time.perf_counter() - started
        still_running = not slow_task.done()
        assert response.status_code == 200
        slow_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):  # only the timing matters here
            await slow_task
        return elapsed, still_running


@pytest.mark.parametrize(
    "slow_path",
    [
        "/api/v1/connectors/jobs/job-1",
        "/api/v1/connectors/jobs/job-1/stream",
        "/api/v1/connectors/jobs/job-1/quarantine",
        "/api/v1/connectors/c1",
    ],
)
def test_slow_store_call_does_not_stall_an_unrelated_request(app_and_store, slow_path):
    app, _store = app_and_store
    elapsed, still_running = asyncio.run(_health_latency_while(app, slow_path))
    assert still_running, "the slow request finished too early for the test to mean anything"
    assert elapsed < 0.1 + SLOW_SECONDS / 2, (
        f"/health took {elapsed:.2f}s while {slow_path} waited on the store: the event loop was blocked"
    )


# ---------------------------------------------------------------------------
# The job stream
# ---------------------------------------------------------------------------


class _Request:
    def __init__(self, disconnect_after: int | None = None) -> None:
        self.checks = 0
        self.disconnect_after = disconnect_after

    async def is_disconnected(self) -> bool:
        self.checks += 1
        return self.disconnect_after is not None and self.checks > self.disconnect_after


async def _collect(response, limit: int = 50) -> list[str]:
    frames: list[str] = []
    async for chunk in response.body_iterator:
        frames.append(chunk if isinstance(chunk, str) else chunk.decode())
        if len(frames) >= limit:
            break
    return frames


@pytest.fixture
def stream(monkeypatch):
    connectors_router = importlib.import_module("src.routers.connectors_router")

    store = _SlowStore({"_id": "job-1", "job_id": "job-1", "status": "running", "workspace_id": ""}, delay=0.0)
    monkeypatch.setattr(connectors_router, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(connectors_router, "_can_access_job", lambda _request, _job: True)
    monkeypatch.setattr(connectors_router, "JOB_STREAM_POLL_SECONDS", 0.01)
    return connectors_router, store


def test_stream_stops_polling_when_the_viewer_disconnects(stream):
    connectors_router, store = stream
    request = _Request(disconnect_after=3)

    async def run() -> list[str]:
        response = await connectors_router.stream_transfer_job("job-1", request)
        return await asyncio.wait_for(_collect(response), timeout=5)

    frames = asyncio.run(run())
    assert frames, "the first frame is sent before the first disconnect check matters"
    reads_at_disconnect = store.reads
    time.sleep(0.1)
    assert store.reads == reads_at_disconnect, "the store was still polled after the viewer left"
    assert store.reads <= 6


def test_stream_sends_a_frame_on_change_and_a_refresh_while_unchanged(stream, monkeypatch):
    connectors_router, store = stream
    monkeypatch.setattr(connectors_router, "JOB_STREAM_REFRESH_SECONDS", 3600.0)
    request = _Request()
    statuses = iter(["running", "running", "running", "running", "completed"])

    def get_job(_job_id):
        store.reads += 1
        return {"_id": "job-1", "job_id": "job-1", "status": next(statuses, "completed"), "workspace_id": ""}

    monkeypatch.setattr(store, "get_job", get_job)

    async def run() -> list[str]:
        response = await connectors_router.stream_transfer_job("job-1", request)
        return await asyncio.wait_for(_collect(response), timeout=5)

    frames = [f for f in asyncio.run(run()) if f.startswith("data:")]
    seen = [json.loads(f[len("data:"):])["status"] for f in frames]
    assert seen == ["running", "completed"], "an unchanged job must not be re-sent on every poll"

    # With a short refresh period the unchanged frame is repeated, so the web
    # client's 8s silence check does not fall back to polling the REST route.
    monkeypatch.setattr(connectors_router, "JOB_STREAM_REFRESH_SECONDS", 0.0)
    statuses = iter(["running", "running", "running", "running", "completed"])  # the first read is the access check
    frames = [f for f in asyncio.run(run()) if f.startswith("data:")]
    assert [json.loads(f[len("data:"):])["status"] for f in frames] == ["running", "running", "running", "completed"]


def test_stream_ends_after_its_maximum_lifetime(stream, monkeypatch):
    connectors_router, store = stream
    monkeypatch.setattr(connectors_router, "JOB_STREAM_MAX_SECONDS", 0.05)

    async def run() -> list[str]:
        response = await connectors_router.stream_transfer_job("job-1", _Request())
        return await asyncio.wait_for(_collect(response, limit=10_000), timeout=5)

    frames = asyncio.run(run())
    assert frames[-1].startswith("event: timeout"), "a stream nobody closes must end on its own"
    assert store.reads < 200


# ---------------------------------------------------------------------------
# Health: dependency checks are bounded, shared and off the event loop
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("probe_path", ["/health/ready", "/api/v1/status"])
def test_liveness_answers_while_a_dependency_check_is_slow(monkeypatch, probe_path):
    """Readiness opens a database connection; liveness must not wait behind it."""
    from services import health_service
    from src.main import app
    status_module = importlib.import_module("src.routers.status_router")

    def slow_health():
        time.sleep(SLOW_SECONDS)
        return {"status": "healthy", "services": {"api": "up", "mongodb": "up", "storage": "up", "connectors": "ready"}}

    monkeypatch.setattr(health_service, "aggregate_health", slow_health)
    monkeypatch.setattr(health_service, "_health_cache", {"at": 0.0, "payload": None, "probe": None})
    monkeypatch.setattr(status_module, "_cache", {"at": 0.0, "payload": None})

    async def run() -> tuple[float, bool]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await client.get("/health")  # first-request costs are not what is measured
            started = time.perf_counter()
            slow_task = asyncio.create_task(client.get(probe_path))
            await asyncio.sleep(0.1)
            response = await client.get("/health")
            elapsed = time.perf_counter() - started
            still_running = not slow_task.done()
            assert response.status_code == 200
            await slow_task
            return elapsed, still_running

    elapsed, still_running = asyncio.run(run())
    assert still_running
    assert elapsed < 0.1 + SLOW_SECONDS / 2, f"/health took {elapsed:.2f}s behind {probe_path}"


def test_close_readiness_probes_share_one_dependency_check(monkeypatch):
    from services import health_service

    calls = []

    def health():
        calls.append(1)
        time.sleep(0.05)
        return {"status": "healthy", "services": {}}

    monkeypatch.setattr(health_service, "aggregate_health", health)
    monkeypatch.setattr(health_service, "_health_cache", {"at": 0.0, "payload": None, "probe": None})
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _n: health_service.cached_health(), range(8)))
    assert len(calls) == 1, "eight probes at once must not open eight database connections"
    results[0]["status"] = "mutated"
    assert health_service.cached_health()["status"] == "healthy", "callers must not share one mutable payload"
    assert health_service.cached_health(max_age_seconds=0)["status"] == "healthy"
    assert len(calls) == 2


def test_database_check_is_bounded_when_the_server_accepts_and_goes_silent(monkeypatch):
    import socket

    from services import health_service

    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(8)
    try:
        monkeypatch.setenv("MONGODB_URI", f"mongodb://127.0.0.1:{server.getsockname()[1]}/")
        for key in ("MONGO_URL", "MONGO_PRIVATE_URL", "MONGODB_URL", "MONGO_PUBLIC_URL"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("DATAFLOW_HEALTH_PROBE_TIMEOUT_SECONDS", "0.5")
        monkeypatch.delenv("DATAWRAP_HEALTH_PROBE_TIMEOUT_SECONDS", raising=False)
        started = time.perf_counter()
        result = health_service.check_mongodb()
        elapsed = time.perf_counter() - started
    finally:
        server.close()
    assert result == {"status": "down", "detail": "connection failed"}
    assert elapsed < 3.0, f"the probe waited {elapsed:.1f}s on a silent server (it used to wait 5s or more)"


def test_route_table_is_built_at_startup_not_inside_the_first_request(tmp_path):
    """In a fresh process, the first request after the warm-up builds no route."""
    import subprocess
    import sys
    from pathlib import Path

    script = tmp_path / "first_request.py"
    script.write_text(
        "import fastapi.routing as routing\n"
        "from src.main import app\n"
        "from services.route_table_warmup import warm_route_table\n"
        "warm_route_table(app)\n"
        "built = [0]\n"
        "original = routing.get_dependant\n"
        "def counting(*args, **kwargs):\n"
        "    built[0] += 1\n"
        "    return original(*args, **kwargs)\n"
        "routing.get_dependant = counting\n"
        "from fastapi.testclient import TestClient\n"
        "client = TestClient(app)\n"
        "status = client.get('/api/v1/health').status_code\n"
        "client.post('/api/v1/connectors/test', json={})\n"
        "print(f'RESULT status={status} built={built[0]}')\n"
    )
    api_root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, DATAFLOW_JOB_STORE="memory", DATAFLOW_DATA_DIR=str(tmp_path / "data"))
    env["PYTHONPATH"] = os.pathsep.join([str(api_root), env.get("PYTHONPATH", "")])
    env.pop("DATAWRAP_DATA_DIR", None)
    result = subprocess.run(
        [sys.executable, str(script)], env=env, cwd=str(api_root), capture_output=True, text=True, timeout=180
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith("RESULT")]
    assert lines, (result.stdout + result.stderr)[-2000:]
    assert lines[-1] == "RESULT status=200 built=0", lines[-1]
