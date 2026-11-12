"""Admin-started training runs never hold the API's shutdown hostage.

PR 410 review M2: ``POST /training-agent/run`` ran the training in a
``BackgroundTasks`` callable with no stop event, so uvicorn's graceful
shutdown waited for the whole run (14 s for a 15 s run in the probe).
``/training-agent/run/sync`` ran the CPU-bound pipeline inline in an
``async def``, freezing the event loop, and ``/copilot/train`` used the
default threadpool. All three now run on the app's ``TrainingRuntime``
thread with its stop event, which the lifespan sets on shutdown. These routes
are live with background training off, which is the production default.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request
from types import SimpleNamespace

import pytest

import tests.r4_route_harness as h
from tests.test_training_shutdown import _SpinAgent


class _GatedSpin(_SpinAgent):
    """Holds the GIL and ignores the stop event until the test opens ``release``.

    The run cannot end on its own (cap 30 s), so "it was still running when
    /health answered" needs no wall-clock threshold: a run on the event loop
    would hold /health until the run ended.
    """

    def __init__(self) -> None:
        super().__init__(30.0, deaf_seconds=30.0)
        self.release = threading.Event()

    def run_full_training(self, include_embedding_tune=False, force=False, stop_event=None):
        self.thread = threading.current_thread()
        self.stop_event = stop_event
        self.calls.append((include_embedding_tune, force))
        self.started.set()
        deadline = time.monotonic() + self.seconds
        try:
            while time.monotonic() < deadline and not self.release.is_set():
                for _ in range(20_000):  # hold the GIL like real synthesis does
                    self.spins += 1
        finally:
            self.exited.set()
        return SimpleNamespace(
            id="spin", status="completed", metrics={"spins": self.spins}, errors=[],
        )


def _use_agent(monkeypatch, agent) -> None:
    import src.ai.training.training_agent as training_agent

    monkeypatch.setattr(training_agent, "get_training_agent", lambda: agent)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _post(url: str, payload: dict) -> tuple[int, dict]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # loopback, no proxy
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"},
    )
    with opener.open(request, timeout=30) as response:
        return response.status, json.loads(response.read().decode())


def _wait(predicate, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_uvicorn_shutdown_during_an_admin_run_returns_quickly(tmp_path, monkeypatch):
    """In-process twin of the review's ``probe_uvicorn_admin.py`` on the real app."""
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")  # production default: admin routes only
    monkeypatch.delenv("DATAWRAP_TRAINING", raising=False)
    agent = _SpinAgent(15.0)
    _use_agent(monkeypatch, agent)

    import uvicorn

    from src.main import app

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on"),
    )
    serving = threading.Thread(target=server.run, name="uvicorn-under-test", daemon=True)
    serving.start()
    try:
        assert _wait(lambda: server.started, 60), "uvicorn never started"
        assert _wait(lambda: getattr(app.state, "ready", False), 60), "warm-up never finished"

        t_post = time.monotonic()
        status, body = _post(f"http://127.0.0.1:{port}/api/v1/training-agent/run", {"force": True})
        assert time.monotonic() - t_post < 5.0
        assert status == 200
        assert body == {
            "run_id": "scheduled",
            "status": "running",
            "metrics": {"message": "Training started in background"},
            "errors": [],
        }
        assert agent.started.wait(10), "the admin run never started"

        t0 = time.monotonic()
        server.should_exit = True  # what SIGTERM does
        serving.join(30)
        exit_seconds = time.monotonic() - t0
        print(f"uvicorn exit took {exit_seconds:.2f}s with an admin run in flight")

        assert not serving.is_alive()
        assert exit_seconds < 2.5, (
            f"uvicorn took {exit_seconds:.1f}s to stop: shutdown waited for the admin run"
        )
        assert agent.exited.wait(2.0), "the admin run kept going after shutdown"
        assert agent.saw_stop, "the admin run never saw the stop request"
        assert agent.thread is not None and agent.thread.name == "datawrap-training"
    finally:
        server.should_exit = True
        serving.join(30)
        agent.exited.wait(20)  # old code: let the abandoned spin end before the next test


@pytest.mark.parametrize("path", ["/api/v1/training-agent/run/sync", "/api/v1/copilot/train"])
def test_sync_admin_run_runs_on_the_runtime_thread_off_the_event_loop(tmp_path, monkeypatch, path):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.delenv("DATAWRAP_TRAINING", raising=False)
    agent = _GatedSpin()
    _use_agent(monkeypatch, agent)

    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as client:
        assert _wait(lambda: getattr(app.state, "ready", False), 60), "warm-up never finished"
        runtime = app.state.training_runtime
        result: dict = {}
        caller = threading.Thread(
            target=lambda: result.update(response=client.post(path, json={"force": True})),
            name="admin-caller", daemon=True,
        )
        caller.start()
        try:
            assert agent.started.wait(10), "the admin run never started"
            t0 = time.monotonic()
            health = client.get("/health")
            health_seconds = time.monotonic() - t0
            run_still_going = not agent.exited.is_set()
        finally:
            agent.release.set()
            caller.join(30)

        assert health.status_code == 200
        # No time threshold (PR 410 review M-T1: a cold first TestClient took
        # 0.7-3 s under load). Only a free event loop answers mid-run.
        assert run_still_going, (
            f"/health answered only after the admin run ended ({health_seconds:.1f}s):"
            " the run blocks the event loop"
        )
        response = result["response"]
        assert response.status_code == 200, response.text
        assert response.json() == {
            "run_id": "spin", "status": "completed", "metrics": {"spins": agent.spins}, "errors": [],
        }
        assert agent.calls == [(False, True)]
        assert agent.thread is not None and agent.thread.name == "datawrap-training"
        assert agent.stop_event is runtime.stop_event, "the run must carry the runtime's stop event"
    assert runtime.stop_event.is_set(), "the lifespan must stop the runtime the admin run used"


def test_background_admin_run_is_tracked_so_shutdown_cancels_it(monkeypatch):
    """``POST /training-agent/run`` returns at once; its task belongs to the runtime (mutant T3)."""
    import asyncio

    from src.ai.training.training_runtime import TrainingRuntime
    from src.ai.training.training_scheduler import start_admin_training

    agent = _GatedSpin()  # ignores the stop event until released
    _use_agent(monkeypatch, agent)
    seen: dict = {}

    async def main() -> None:
        runtime = TrainingRuntime()
        task = start_admin_training(runtime, include_embedding_tune=False, force=True)
        while not agent.started.is_set():
            await asyncio.sleep(0.01)
        await runtime.shutdown(join_seconds=0.2)
        seen["done"], seen["cancelled"] = task.done(), task.cancelled()

    try:
        asyncio.run(main())
    finally:
        agent.release.set()
        agent.exited.wait(10)
    assert seen == {"done": True, "cancelled": True}, (
        f"shutdown left the background admin task running: {seen}"
    )
    assert agent.calls == [(False, True)]
