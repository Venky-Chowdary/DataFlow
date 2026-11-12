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


# --- each lifespan stops its own runtime (review L3) -----------------------


class _OneSpinPerRun:
    """Every ``run_full_training`` call is a separate spin agent."""

    def __init__(self) -> None:
        self.runs: list[_SpinAgent] = []

    def run_full_training(self, include_embedding_tune=False, force=False, stop_event=None):
        spin = _SpinAgent(SPIN_SECONDS)
        self.runs.append(spin)
        return spin.run_full_training(include_embedding_tune, force, stop_event)


def _wait_for(predicate, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_nested_lifespans_each_stop_their_own_run(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAWRAP_TRAINING", raising=False)
    monkeypatch.setenv("DATAFLOW_TRAINING", "on")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")

    import src.ai.training.training_agent as training_agent
    import src.ai.training.training_scheduler as training_scheduler

    agent = _OneSpinPerRun()
    monkeypatch.setattr(training_agent, "get_training_agent", lambda: agent)
    monkeypatch.setattr(training_scheduler, "asyncio", _FastStartAsyncio())
    monkeypatch.setattr(training_scheduler, "TRAINING_INITIAL_DELAY_SECONDS", 0.1, raising=False)

    from fastapi.testclient import TestClient

    from src.main import app

    outer = TestClient(app)
    outer.__enter__()
    try:
        assert _wait_for(lambda: len(agent.runs) >= 1, 60), "the first lifespan never started a run"
        inner = TestClient(app)
        inner.__enter__()
        try:
            assert _wait_for(lambda: len(agent.runs) >= 2, 60), "the second lifespan never started a run"
        finally:
            inner.__exit__(None, None, None)
    finally:
        outer.__exit__(None, None, None)
    try:
        first, second = agent.runs[:2]
        assert second.exited.wait(3.0) and second.saw_stop
        assert first.exited.wait(3.0) and first.saw_stop, (
            "the first lifespan's run was never stopped: its exit stopped the second runtime"
        )
    finally:
        for run in agent.runs:
            run.exited.wait(SPIN_SECONDS + 5)


# --- an empty TRAINING value never enables training (review L4) ------------


@pytest.mark.parametrize(
    ("env", "production", "enabled"),
    [
        ({}, False, True),
        ({}, True, False),
        ({"DATAFLOW_TRAINING": "off", "DATAWRAP_TRAINING": ""}, False, False),
        ({"DATAWRAP_TRAINING": ""}, False, False),
        ({"DATAWRAP_TRAINING": "  "}, False, False),
        ({"DATAFLOW_TRAINING": ""}, False, False),
        ({"DATAFLOW_TRAINING": "on"}, True, True),
        ({"DATAFLOW_TRAINING": "1"}, False, True),
        ({"DATAWRAP_TRAINING": "on", "DATAFLOW_TRAINING": "off"}, False, True),
        ({"DATAWRAP_TRAINING": "off", "DATAFLOW_TRAINING": "on"}, False, False),
        ({"DATAFLOW_TRAINING": "false"}, False, False),
    ],
)
def test_training_enabled_env_matrix(monkeypatch, env, production, enabled):
    for name in ("DATAWRAP_TRAINING", "DATAFLOW_TRAINING"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    import src.ai.training.training_scheduler as training_scheduler

    monkeypatch.setattr("services.platform_config.is_production", lambda: production)

    assert training_scheduler.training_enabled() is enabled


def test_empty_datawrap_training_does_not_start_training(tmp_path, monkeypatch):
    """The review's case: conftest's DATAFLOW_TRAINING=off plus an empty DATAWRAP_TRAINING."""
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setenv("DATAWRAP_TRAINING", "")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")

    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app):
        assert _wait_for(lambda: getattr(app.state, "ready", False), 60), "warm-up never finished"
        started = [t.get_name() for t in app.state.training_runtime._tasks]
    assert started == [], f"training started with an empty DATAWRAP_TRAINING: {started}"


# --- TrainingRuntime contract (PR 410 review round 2, L-T3: mutants T4, T5) ---


def _deaf_until(gate: threading.Event, started: threading.Event, then=None):
    """A run that ignores the stop event until ``gate`` opens."""

    def run():
        started.set()
        gate.wait(10)
        if isinstance(then, BaseException):
            raise then
        return then

    return run


def test_shutdown_waits_briefly_for_a_stopping_run_to_leave_its_thread():
    """A run that sees the stop and needs a moment to unwind is joined, not abandoned."""
    from src.ai.training.training_runtime import TrainingRuntime

    seen: dict = {}

    async def main() -> None:
        runtime = TrainingRuntime()

        def run() -> str:
            seen["thread"] = threading.current_thread()
            runtime.stop_event.wait(10)
            time.sleep(0.3)  # unwinding: close files, drop temp output
            return "stopped"

        task = asyncio.ensure_future(runtime.run(run))
        while "thread" not in seen:
            await asyncio.sleep(0.01)
        t0 = time.monotonic()
        await runtime.shutdown(join_seconds=5.0)
        seen["shutdown_seconds"] = time.monotonic() - t0
        seen["alive_after_shutdown"] = seen["thread"].is_alive()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(main())
    assert not seen["alive_after_shutdown"], "shutdown returned before the stopping run left its thread"
    assert seen["shutdown_seconds"] < 5.0


def test_shutdown_wait_for_a_deaf_run_is_bounded():
    from src.ai.training.training_runtime import TrainingRuntime

    gate, started = threading.Event(), threading.Event()
    seen: dict = {}

    async def main() -> None:
        runtime = TrainingRuntime()
        task = asyncio.ensure_future(runtime.run(_deaf_until(gate, started, "done")))
        while not started.is_set():
            await asyncio.sleep(0.01)
        t0 = time.monotonic()
        await runtime.shutdown(join_seconds=0.3)
        seen["shutdown_seconds"] = time.monotonic() - t0
        seen["thread_alive"] = bool(runtime._threads)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    try:
        asyncio.run(main())
    finally:
        gate.set()
    assert seen["thread_alive"], "the deaf run cannot have left yet"
    assert 0.25 <= seen["shutdown_seconds"] < 2.0, seen


@pytest.mark.parametrize("outcome", ["done", RuntimeError("run failed")], ids=["result", "exception"])
def test_a_run_that_ends_after_its_awaiting_task_was_cancelled_is_dropped_quietly(outcome):
    """A cancelled request leaves a cancelled future; the run's late result must not be set on it."""
    from src.ai.training.training_runtime import TrainingRuntime

    gate, started = threading.Event(), threading.Event()
    loop_errors: list[dict] = []

    async def main() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, ctx: loop_errors.append(ctx))
        runtime = TrainingRuntime()
        task = asyncio.ensure_future(runtime.run(_deaf_until(gate, started, outcome)))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        gate.set()  # the run now finishes and posts its outcome to the loop
        deadline = time.monotonic() + 5
        while runtime._threads and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        for _ in range(5):  # let the posted callback run
            await asyncio.sleep(0.01)

    try:
        asyncio.run(main())
    finally:
        gate.set()
    assert loop_errors == [], [str(ctx.get("exception") or ctx.get("message")) for ctx in loop_errors]
