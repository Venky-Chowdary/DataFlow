"""D181 — one hung connector call must not take the workspace's connector actions down.

Production: from 20:12 ET every connector action (test_connector, run_query,
start_transfer, plan_transfer, sample_connector_object) hung for 34+ minutes
while /health said healthy. ``run_query`` and ``test_connector`` had no deadline
at all, so a source that took the connection and never answered held its
``asyncio.to_thread`` worker forever; enough of them used up the shared pool and
every later call queued behind them.

These tests drive the real chokepoint (``DataPilotTools.execute``) from a
deliberately tiny default asyncio pool.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.ai.copilot.tools import DataPilotTools, ToolResult  # noqa: E402
from services.workspace_filter import bound_workspace_filter  # noqa: E402


@pytest.fixture()
def hang(monkeypatch):
    """A run_query that blocks forever for connector 'hung', answers otherwise."""
    release = threading.Event()

    def fake_run_query(self, connector_id="", connector_name="", **_kw):
        if connector_name == "hung":
            release.wait(60)
            return ToolResult(name="run_query", success=False, output=None, error="released")
        return ToolResult(name="run_query", success=True, output={"rows": [[1]]})

    monkeypatch.setattr(DataPilotTools, "_run_query", fake_run_query)
    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", "1")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_POOL_WORKERS", "4")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", "2")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS", "300")
    try:
        from services import connector_executor

        connector_executor.reset_for_tests()
    except ImportError:  # base branch: the module does not exist yet
        connector_executor = None
    yield release
    release.set()
    if connector_executor is not None:
        connector_executor.reset_for_tests()


def _call(name: str, workspace: str = "ws-a", **args):
    from services.workspace_filter import WorkspaceFilter

    with bound_workspace_filter(WorkspaceFilter.only(workspace), workspace, "tester"):
        return DataPilotTools().execute("run_query", {"connector_name": name, **args})


def test_hung_run_query_returns_a_timeout_error_at_its_deadline(hang):
    started = time.monotonic()
    box: dict = {}
    t = threading.Thread(target=lambda: box.update(r=_call("hung")), daemon=True)
    t.start()
    t.join(8)
    assert not t.is_alive(), "a hung run_query never returned"
    assert 0.9 <= time.monotonic() - started < 5
    result = box["r"]
    assert result.success is False
    assert "did not finish within 1s" in result.error


def test_hung_calls_do_not_stop_a_healthy_call_in_a_tiny_default_pool(hang):
    """Four hung calls fill a 4-thread default pool; another tenant still answers."""

    async def scenario():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=4))
        hung = [asyncio.create_task(asyncio.to_thread(_call, "hung")) for _ in range(4)]
        await asyncio.sleep(0.2)
        healthy = await asyncio.wait_for(
            asyncio.to_thread(_call, "good", "ws-b"), timeout=0.8
        )
        return healthy, hung

    healthy, hung = asyncio.run(asyncio.wait_for(scenario(), 20))
    assert healthy.success is True


def test_same_workspace_is_told_why_while_workers_are_stuck_then_recovers(hang):
    """Abandoned workers keep their slots until they end: a clear refusal, not a hang."""

    async def scenario():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=4))
        hung = [asyncio.create_task(asyncio.to_thread(_call, "hung")) for _ in range(4)]
        await asyncio.sleep(1.6)  # past the 1s deadline
        await asyncio.gather(*hung)
        refused = await asyncio.wait_for(asyncio.to_thread(_call, "good"), timeout=3)
        hang.set()
        await asyncio.sleep(0.5)
        recovered = await asyncio.wait_for(asyncio.to_thread(_call, "good"), timeout=3)
        return refused, recovered

    refused, recovered = asyncio.run(asyncio.wait_for(scenario(), 20))
    assert refused.success is False and "in flight" in refused.error
    assert recovered.success is True


def test_per_workspace_cap_rejects_fast_and_spares_other_workspaces(hang):
    from services import connector_executor

    holders = [
        threading.Thread(target=_call, args=("hung",), daemon=True) for _ in range(2)
    ]
    for t in holders:
        t.start()
    time.sleep(0.3)
    started = time.monotonic()
    third = _call("hung")
    assert time.monotonic() - started < 0.5
    assert third.success is False
    assert "connector calls in flight" in third.error and "ws-a" in third.error
    assert _call("good", "ws-b").success is True
    assert connector_executor.snapshot()["rejected_total"] >= 1


def test_readiness_degrades_while_saturated_and_recovers(hang):
    from services import connector_executor

    assert connector_executor.readiness()["ok"] is True
    # 4 workers; two workspaces at the cap of 2 saturate the pool.
    holders = [
        threading.Thread(target=_call, args=("hung", ws), daemon=True)
        for ws in ("ws-a", "ws-a", "ws-b", "ws-b")
    ]
    for t in holders:
        t.start()
    time.sleep(0.4)
    state = connector_executor.readiness()
    assert state["ok"] is False
    assert "saturated" in state["reason"]
    snap = connector_executor.snapshot()
    assert snap["in_flight"] == 4 and snap["max_workers"] == 4
    # Callers give up at the deadline; the workers are still held (abandoned).
    time.sleep(1.3)
    assert connector_executor.snapshot()["abandoned"] == 4
    assert connector_executor.readiness()["ok"] is False
    hang.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not connector_executor.readiness()["ok"]:
        time.sleep(0.1)
    assert connector_executor.readiness()["ok"] is True
    assert connector_executor.snapshot()["abandoned"] == 0
    assert connector_executor.snapshot()["abandoned_total"] == 4


def test_readiness_degrades_when_the_oldest_call_is_too_old(hang, monkeypatch):
    from services import connector_executor

    monkeypatch.setenv("DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS", "1")
    t = threading.Thread(target=_call, args=("hung",), daemon=True)
    t.start()
    time.sleep(1.4)
    state = connector_executor.readiness()
    assert state["ok"] is False and "oldest" in state["reason"]


def test_health_endpoints_expose_the_pool_numbers(hang):
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    body = client.get("/health").json()
    pool = body["connector_pool"]
    assert {"in_flight", "queued", "abandoned", "max_workers"} <= set(pool)
    ready = client.get("/health/ready").json()
    assert "connector_pool" in ready
