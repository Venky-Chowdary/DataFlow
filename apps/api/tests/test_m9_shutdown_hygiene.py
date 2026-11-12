"""Audit M-9: background work started by the app stops with the app.

The API starts a mail-outbox sweeper, an API claim loop, an orphan sweeper and
a stall watchdog, plus the schedule and training loops. None of them was
stopped at shutdown, so a sweeper from one TestClient kept mutating the store
of the next test, and a worker killed by SIGTERM left its job to lease expiry.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from pathlib import Path

import pytest

_APP_THREADS = ("df-orphan-sweep", "df-stall-sweep", "df-api-claim", "mail-outbox-sweeper")


def _app_threads() -> list[str]:
    return sorted(t.name for t in threading.enumerate() if t.name in _APP_THREADS and t.is_alive())


@pytest.fixture
def quick_app(monkeypatch):
    """The real app with a warm-up that does not load models, in claim mode so all four threads start."""
    from src.ai.rag import pipeline as rag_pipeline

    class _Pipeline:
        def initialize(self):
            return {"ingested": 0}

    monkeypatch.setattr(rag_pipeline, "get_rag_pipeline", lambda: _Pipeline())
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    for key in ("PROCESS_ROLE", "MULTI_REPLICA", "SCHEDULE_LOOP", "API_ORPHAN_RESUME"):
        monkeypatch.delenv(f"DATAFLOW_{key}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{key}", raising=False)
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    monkeypatch.setenv("DATAFLOW_API_CLAIM_LOOP", "1")
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)
    monkeypatch.delenv("DATAWRAP_API_CLAIM_LOOP", raising=False)
    from src.main import app

    return app


def _wait_ready(app, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if getattr(app.state, "ready", False):
            return
        time.sleep(0.05)
    raise AssertionError("warm-up did not finish")


def test_app_started_and_stopped_twice_leaves_no_background_threads(quick_app):
    from fastapi.testclient import TestClient

    assert _app_threads() == [], "another test left app threads running"
    for round_no in (1, 2):
        with TestClient(quick_app) as client:
            _wait_ready(quick_app)
            assert client.get("/health").status_code == 200
            assert _app_threads() == sorted(_APP_THREADS), f"round {round_no}: not every loop started"
            tasks = list(getattr(quick_app.state, "background_tasks", []))
            assert tasks, "the schedule loop task must be kept so shutdown can cancel it"
        assert _app_threads() == [], f"round {round_no}: threads outlived the app"
        assert all(task.done() for task in tasks), f"round {round_no}: a loop task outlived the app"


def test_stop_functions_join_their_threads():
    from services import job_watchdog, orphan_jobs

    assert orphan_jobs.start_orphan_sweeper(interval_seconds=5) is True
    assert job_watchdog.start_stall_watchdog(interval_seconds=5) is True
    orphan_thread, stall_thread = orphan_jobs._sweeper_thread, job_watchdog._stall_sweeper_thread
    orphan_jobs.stop_orphan_sweeper()
    job_watchdog.stop_stall_watchdog()
    assert not orphan_thread.is_alive(), "stop_orphan_sweeper returned while its thread was still running"
    assert not stall_thread.is_alive()


# ---------------------------------------------------------------------------
# Worker drain
# ---------------------------------------------------------------------------


class _Queue:
    """transfer_job_queue rows the drain may put back."""

    job_a = ""

    def __init__(self, job_ids):
        self.rows = {j: {"_id": j, "job_id": j, "status": "queued", "worker": ""} for j in job_ids}
        self._lock = threading.Lock()

    def claim(self, worker):
        with self._lock:
            for row in self.rows.values():
                if row["status"] == "queued":
                    row.update(status="claimed", worker=worker)
                    return row["_id"]
        return None

    def update_one(self, flt, update):
        with self._lock:
            row = self.rows.get(flt.get("_id"))
            if row is None or ("status" in flt and row["status"] != flt["status"]):
                return
            row.update(update.get("$set") or {})


@pytest.fixture
def fleet(monkeypatch):
    """run_fleet_loop over an in-memory queue and lease store."""
    from services import worker_fleet
    from services.worker_leases import WorkerLeaseStore

    # The in-memory lease table is process-wide: each test gets its own job id.
    job_a = f"job-a-{uuid.uuid4().hex[:8]}"
    queue = _Queue([job_a])
    queue.job_a = job_a
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setattr(worker_fleet, "_queue_coll", lambda: queue)
    monkeypatch.setattr(worker_fleet, "reclaim_stale_claims", lambda **_k: 0)

    def claim(store, ttl_seconds=60):
        job_id = queue.claim(store.worker_id)
        if job_id and not store.acquire(job_id, ttl_seconds=ttl_seconds):
            return None
        return job_id

    monkeypatch.setattr(worker_fleet, "claim_next_job", claim)
    monkeypatch.setattr(worker_fleet, "_transfer_job_cancelled", lambda _job: False)
    superseded: list[str] = []
    monkeypatch.setattr(worker_fleet, "_supersede_local_run", lambda job_id: superseded.append(job_id) or True)
    return worker_fleet, queue, superseded, WorkerLeaseStore


@pytest.mark.parametrize("max_inflight", [1, 4])
def test_drain_lets_a_running_job_finish_and_claims_nothing_new(fleet, max_inflight):
    worker_fleet, queue, superseded, _ = fleet
    queue.rows["job-b"] = {"_id": "job-b", "job_id": "job-b", "status": "queued", "worker": ""}
    stop = threading.Event()
    started = threading.Event()
    finished: list[str] = []

    def handler(job_id):
        started.set()
        stop.set()  # SIGTERM arrives while the first batch is being written
        time.sleep(0.3)
        finished.append(job_id)

    runner = threading.Thread(
        target=worker_fleet.run_fleet_loop,
        kwargs=dict(handler=handler, poll_seconds=0.05, stop_event=stop, max_inflight=max_inflight, drain_seconds=5),
    )
    runner.start()
    runner.join(15)
    assert not runner.is_alive(), "the loop did not return after the drain"
    assert finished == [queue.job_a], "the in-flight job must be allowed to reach its end"
    assert queue.rows[queue.job_a]["status"] == "done"
    assert queue.rows["job-b"]["status"] == "queued", "a draining worker must not claim new work"
    assert superseded == []
    assert worker_fleet.fleet_inflight_job_ids() == frozenset()


@pytest.mark.parametrize("max_inflight", [1, 4])
def test_drain_hands_a_long_job_back_at_a_safe_point_and_releases_its_lease(fleet, max_inflight):
    worker_fleet, queue, superseded, lease_store = fleet
    stop = threading.Event()
    stopped_at_batch_boundary = threading.Event()

    def handler(job_id):
        stop.set()
        # A long transfer: it only stops when the run-attempt fence says a
        # newer attempt owns the job, which the writer checks between batches.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if job_id in superseded:
                stopped_at_batch_boundary.set()
                return
            time.sleep(0.02)

    began = time.monotonic()
    runner = threading.Thread(
        target=worker_fleet.run_fleet_loop,
        kwargs=dict(handler=handler, poll_seconds=0.05, stop_event=stop, max_inflight=max_inflight, drain_seconds=1.0),
    )
    runner.start()
    runner.join(15)
    elapsed = time.monotonic() - began
    assert not runner.is_alive()
    assert stopped_at_batch_boundary.is_set(), "the writer was never told to stop"
    assert elapsed < 6, f"drain took {elapsed:.1f}s for a 1s grace period"
    assert queue.rows[queue.job_a]["status"] == "queued", "the job must go back on the queue, not be marked done or failed"
    assert queue.rows[queue.job_a]["worker"] == ""
    assert lease_store("another-worker").acquire(queue.job_a, ttl_seconds=30), "the lease was not released"


def test_job_that_ignores_the_handover_keeps_its_lease(fleet):
    """A writer that never reaches a safe point is not handed to a second writer."""
    worker_fleet, queue, superseded, lease_store = fleet
    stop = threading.Event()
    release = threading.Event()

    def handler(_job_id):
        stop.set()
        release.wait(20)

    runner = threading.Thread(
        target=worker_fleet.run_fleet_loop,
        kwargs=dict(handler=handler, poll_seconds=0.05, stop_event=stop, max_inflight=4, drain_seconds=0.6),
    )
    runner.start()
    runner.join(10)
    try:
        assert not runner.is_alive(), "the loop must return when the grace period ends"
        assert superseded == [queue.job_a]
        assert queue.rows[queue.job_a]["status"] == "claimed", "still being written: must not be re-queued"
        assert not lease_store("another-worker").acquire(queue.job_a, ttl_seconds=30), "lease released under a live writer"
        assert worker_fleet.fleet_inflight_job_ids() == frozenset({queue.job_a})
    finally:
        release.set()
        # The loop has returned, so nothing reaps this run: leave no process-wide trace.
        with worker_fleet._fleet_running_lock:
            worker_fleet._fleet_running.discard(queue.job_a)


def test_supersede_stamps_a_new_run_attempt_on_the_job(monkeypatch):
    """The handover uses the existing run-attempt fence, not the cancel flag."""
    from services import cancel_check, worker_fleet
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    mongo.connect()
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: mongo)
    mongo.create_transfer_job({"job_id": "job-x", "status": "running"})
    cancel_check.begin_run_attempt(mongo, "job-x")
    try:
        assert cancel_check.attempt_superseded(mongo, "job-x") is False
        assert worker_fleet._supersede_local_run("job-x") is True
        assert cancel_check.attempt_superseded(mongo, "job-x") is True
        assert not mongo.is_cancel_requested("job-x"), "a drain is not an operator cancel"
        assert mongo.get_job("job-x")["status"] == "running"
    finally:
        cancel_check.end_run_attempt()


def test_worker_process_drains_on_sigterm(tmp_path):
    """A real worker process: SIGTERM stops claiming, the loop drains, the process exits 0."""
    script = tmp_path / "worker_under_test.py"
    script.write_text(
        textwrap.dedent(
            """
            import sys, threading
            from services import worker_fleet

            seen = {}

            def fake_loop(handler, *, poll_seconds=2.0, stop_event=None, max_inflight=None, drain_seconds=None):
                seen["stop"] = stop_event
                seen["drain"] = drain_seconds
                print("LOOP_READY", flush=True)
                stopped = stop_event.wait(30)
                print(f"LOOP_STOPPED stop={stopped} drain={drain_seconds}", flush=True)

            worker_fleet.run_fleet_loop = fake_loop
            from src import worker_main

            sys.exit(worker_main.main())
            """
        )
    )
    env = dict(os.environ)
    env.update(
        DATAFLOW_JOB_STORE="memory",
        DATAFLOW_WORKER_DRAIN_SECONDS="7",
        DATAFLOW_DATA_DIR=str(tmp_path / "data"),
        PYTHONPATH=os.pathsep.join([str(Path(__file__).resolve().parents[1]), env.get("PYTHONPATH", "")]),
    )
    for key in ("DATAFLOW_MULTI_REPLICA", "DATAWRAP_MULTI_REPLICA", "DATAWRAP_DATA_DIR", "DATAWRAP_WORKER_DRAIN_SECONDS"):
        env.pop(key, None)
    proc = subprocess.Popen(
        [sys.executable, str(script)], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    try:
        lines: list[str] = []
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            lines.append(line)
            if "LOOP_READY" in line:
                break
        assert any("LOOP_READY" in line for line in lines), "".join(lines)[-2000:]
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=30)
        output = "".join(lines) + out
        assert "LOOP_STOPPED stop=True drain=7.0" in output, output[-2000:]
        assert proc.returncode == 0, output[-2000:]
    finally:
        if proc.poll() is None:
            proc.kill()
