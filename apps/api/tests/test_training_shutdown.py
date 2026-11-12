"""The Training Agent must never hold the API's shutdown hostage.

CI root cause (PR 407 teardown timeout): the lifespan started training with
``asyncio.to_thread``. Shutdown cancelled only the awaiting task, so the
CPU-bound run kept its default-executor thread, and ``asyncio.Runner.close()``
(``TestClient.__exit__`` / uvicorn exit) blocked in
``shutdown_default_executor(300)`` until it finished. Abandoned runs then kept
the GIL busy after the app had "stopped".

The run now lives on its own daemon thread with a stop event that shutdown
sets; the run checks it between phases and inside its long loops.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import tests.r4_route_harness as h

SPIN_SECONDS = 8.0
# The real pipeline has stretches with no stop check (one answer, one ingest
# batch). The headline spin ignores the stop event this long, so only a run
# on its own daemon thread lets shutdown return before it ends; the loop's
# default executor would make ``asyncio.Runner.close()`` wait it out.
DEAF_SECONDS = 4.0


class _SpinAgent:
    """Stands in for the training agent: a CPU spin that honours the stop
    event once its first ``deaf_seconds`` are over."""

    def __init__(self, seconds: float, deaf_seconds: float = 0.0) -> None:
        self.seconds = seconds
        self.deaf_seconds = deaf_seconds
        self.started = threading.Event()
        self.exited = threading.Event()
        self.saw_stop = False
        self.spins = 0
        self.calls: list[tuple] = []
        self.stop_event: threading.Event | None = None
        self.thread: threading.Thread | None = None

    def run_full_training(self, include_embedding_tune=False, force=False, stop_event=None):
        self.thread = threading.current_thread()
        self.stop_event = stop_event
        self.calls.append((include_embedding_tune, force))
        self.started.set()
        t0 = time.monotonic()
        deadline = t0 + self.seconds
        try:
            while time.monotonic() < deadline:
                listening = time.monotonic() - t0 >= self.deaf_seconds
                if listening and stop_event is not None and stop_event.is_set():
                    self.saw_stop = True
                    break
                for _ in range(20_000):  # hold the GIL like real synthesis does
                    self.spins += 1
        finally:
            self.exited.set()
        return SimpleNamespace(
            id="spin", status="stopped" if self.saw_stop else "completed",
            metrics={"spins": self.spins}, errors=[],
        )


class _FastStartAsyncio:
    """``asyncio`` for the scheduler module, with its 60 s start delay cut to 0.1 s."""

    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    async def sleep(delay, *args, **kwargs):
        return await asyncio.sleep(0.1 if delay == 60 else delay, *args, **kwargs)


def test_app_shutdown_does_not_wait_for_a_running_training_run(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAWRAP_TRAINING", raising=False)
    monkeypatch.setenv("DATAFLOW_TRAINING", "on")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")

    import src.ai.training.training_agent as training_agent
    import src.ai.training.training_scheduler as training_scheduler

    agent = _SpinAgent(SPIN_SECONDS, deaf_seconds=DEAF_SECONDS)
    monkeypatch.setattr(training_agent, "get_training_agent", lambda: agent)
    monkeypatch.setattr(training_scheduler, "asyncio", _FastStartAsyncio())
    monkeypatch.setattr(training_scheduler, "TRAINING_INITIAL_DELAY_SECONDS", 0.1, raising=False)

    from fastapi.testclient import TestClient

    from src.main import app

    try:
        client = TestClient(app)
        client.__enter__()
        try:
            assert agent.started.wait(60), "the training loop never started a run"
        finally:
            t0 = time.monotonic()
            client.__exit__(None, None, None)
            exit_seconds = time.monotonic() - t0
        print(f"TestClient.__exit__ took {exit_seconds:.2f}s with a training run in flight")

        assert exit_seconds < 2.0, (
            f"TestClient.__exit__ took {exit_seconds:.1f}s: shutdown waited for a "
            f"training run that ignores the stop event for {DEAF_SECONDS:.0f}s"
        )
        # The runtime's own thread: a default-executor worker made under
        # TestClient's daemon portal thread is a daemon too, so the flag
        # alone cannot tell them apart.
        assert agent.thread is not None and agent.thread.name == "datawrap-training", (
            f"training ran on {agent.thread!r}, not the runtime's datawrap-training thread"
        )
        assert agent.thread.daemon
        assert agent.exited.wait(DEAF_SECONDS + 2.0), "training thread kept running after shutdown"
        assert agent.saw_stop, "training run never observed the stop request"
        agent.thread.join(2.0)
        assert not agent.thread.is_alive()
    finally:
        # Old code: let the abandoned spin finish before the next test runs.
        agent.exited.wait(SPIN_SECONDS + 5)


# --- run_full_training honours the stop event ------------------------------


def _bare_agent(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from src.ai.training.training_agent import DataTransferTrainingAgent

    return DataTransferTrainingAgent()


def test_stop_event_ends_run_full_training_between_phases(tmp_path, monkeypatch):
    agent = _bare_agent(tmp_path, monkeypatch)
    stop = threading.Event()
    calls: list[str] = []

    def feed_all(*a, **k):
        calls.append("feed")
        return []

    def synthesize_full(*a, **k):
        calls.append("synthesize")
        stop.set()  # shutdown arrives while phase 2 runs
        return []

    def later_phase(*a, **k):
        calls.append("later")
        return {}

    monkeypatch.setattr(agent.feeder, "feed_all", feed_all)
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", synthesize_full)
    monkeypatch.setattr(agent.data_synth, "synthesize_full_dataset", later_phase)
    monkeypatch.setattr(agent, "_ingest_training_documents_batched", later_phase)
    monkeypatch.setattr(agent, "_save_artifacts", later_phase)

    run = agent.run_full_training(False, True, stop_event=stop)

    assert run.status == "stopped"
    assert run.completed_at
    assert calls[-1] == "synthesize" and "later" not in calls, calls
    assert not os.path.exists(agent._checkpoint_path()), "a stopped run must not checkpoint"


def test_stop_event_ends_the_grounded_schema_loop(tmp_path, monkeypatch):
    agent = _bare_agent(tmp_path, monkeypatch)
    stop = threading.Event()
    from src.ai.training.universal_data_feeder import UniversalSchema

    schemas = [UniversalSchema(name=f"s{i}", source="industry", columns=["a"]) for i in range(10)]
    monkeypatch.setattr(agent.feeder, "feed_all", lambda *a, **k: schemas)
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", lambda *a, **k: [])

    analysed: list[str] = []

    class _Analyst:
        def analyze_schema(self, schema):
            analysed.append(schema.name)
            stop.set()
            return {}

        def compose_response(self, insight, q, mode):
            return "ok"

    import src.ai.copilot.data_analyst as data_analyst

    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: _Analyst())

    run = agent.run_full_training(False, True, stop_event=stop)

    assert run.status == "stopped"
    assert analysed == ["s0"], analysed


def test_unstopped_run_still_completes(tmp_path, monkeypatch):
    """No stop event (admin routes, direct callers): the pipeline is unchanged."""
    agent = _bare_agent(tmp_path, monkeypatch)
    monkeypatch.setattr(agent.feeder, "feed_all", lambda *a, **k: [])
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", lambda *a, **k: [])
    monkeypatch.setattr(agent, "_ingest_training_documents_batched", lambda docs, **k: len(docs))
    monkeypatch.setattr("src.services.catalog_service.catalog_training_docs", lambda: [])

    run = agent.run_full_training(False, True)

    assert run.status == "completed", run.errors
    assert json.loads(Path(agent._checkpoint_path()).read_text(encoding="utf-8"))["run_id"] == run.id


# --- artifacts are never half-written --------------------------------------


def test_save_artifacts_never_leaves_a_half_written_file(tmp_path, monkeypatch):
    agent = _bare_agent(tmp_path, monkeypatch)
    conv_path = Path(agent.OUTPUT_DIR) / "conversations.jsonl"
    conv_path.write_text('{"previous": "complete run"}\n', encoding="utf-8")

    class _Boom:
        user_message = "u"
        assistant_message = "a"
        intent = "i"

        @property
        def context(self):
            raise RuntimeError("interrupted mid-write")

    good = SimpleNamespace(user_message="u", assistant_message="a", intent="i", context={})

    with pytest.raises(RuntimeError, match="interrupted mid-write"):
        agent._save_artifacts([good, _Boom()], [])

    assert conv_path.read_text(encoding="utf-8") == '{"previous": "complete run"}\n'
    leftovers = sorted(p.name for p in Path(agent.OUTPUT_DIR).iterdir() if p.name != "conversations.jsonl")
    assert leftovers == [], leftovers


# --- the test suite's own default ------------------------------------------


def test_suite_default_has_training_off():
    """conftest turns training off so a TestClient never starts a CPU-bound run."""
    from services.brand_env import getenv_brand

    assert getenv_brand("TRAINING", "on").strip().lower() in ("off", "0", "false")


def test_ci_api_test_jobs_turn_training_off():
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[3]
    ci = yaml.safe_load((root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    step = next(s for s in ci["jobs"]["api-tests"]["steps"] if s.get("name") == "Run API tests")
    assert str(step["env"]["DATAFLOW_TRAINING"]).lower() == "off"

    full = yaml.safe_load((root / ".github" / "workflows" / "full-suite.yml").read_text(encoding="utf-8"))
    suite_jobs = [
        job for job in full["jobs"].values()
        if any("pytest tests " in str(s.get("run", "")) for s in job.get("steps", []))
    ]
    assert suite_jobs
    for job in suite_jobs:
        assert str(job["env"]["DATAFLOW_TRAINING"]).lower() == "off"
