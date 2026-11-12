"""Ops review Medium 12: the API's own transfer pool is drained at shutdown.

A single-node install without ``WORKER_FLEET`` runs transfers on the API's
local pool (``services.transfer_scheduler``). The M-9 teardown stopped the
claim loop and the sweepers but never this pool, so SIGTERM gave its
transfers no drain at all. The pool is now one of the background stoppers:
it takes no new work, lets running transfers finish within the drain period,
hands the rest over through the run-attempt fence, and releases the lease of
every job it will not run.
"""

from __future__ import annotations

import threading
import time

import pytest

from services import background_lifecycle, transfer_scheduler, worker_fleet


class _Leases:
    def __init__(self) -> None:
        self.held: set[str] = set()
        self.released: list[str] = []

    def acquire(self, job_id, ttl_seconds=60):
        self.held.add(job_id)
        return True

    def get_fence(self, job_id):
        return 1

    def release(self, job_id, fence=None):
        self.held.discard(job_id)
        self.released.append(job_id)


@pytest.fixture
def pool(monkeypatch):
    from services import lease_heartbeat

    leases = _Leases()
    heartbeats: dict[str, bool] = {}

    def start_heartbeat(_store, job_id, **_kwargs):
        heartbeats[job_id] = True
        return lambda: heartbeats.__setitem__(job_id, False)

    monkeypatch.setattr(lease_heartbeat, "start_lease_heartbeat", start_heartbeat)
    monkeypatch.setattr(transfer_scheduler, "_lease_store", leases)
    monkeypatch.setattr(transfer_scheduler, "_executor", None)
    monkeypatch.setattr(transfer_scheduler, "_shutdown", False)
    monkeypatch.setenv("DATAFLOW_TRANSFER_WORKERS", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_DRAIN_SECONDS", "1.5")
    yield leases, heartbeats
    executor = transfer_scheduler._executor
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=True)
    transfer_scheduler._executor = None


def test_the_transfer_pool_is_one_of_the_shutdown_stoppers():
    names = [name for name, *_rest in background_lifecycle._stoppers()]
    assert "transfer pool" in names
    assert names.index("transfer pool") > names.index("orphan sweeper"), "sweepers stop first"


def test_shutdown_drains_running_hands_over_the_rest_and_releases_leases(pool, monkeypatch):
    leases, heartbeats = pool
    superseded = threading.Event()
    monkeypatch.setattr(worker_fleet, "_supersede_local_run", lambda job_id: superseded.set() or True)
    # Stoppers other than the pool are not under test here.
    monkeypatch.setattr(
        background_lifecycle, "_stoppers",
        lambda real=background_lifecycle._stoppers: [s for s in real() if s[0] == "transfer pool"],
    )

    def writer_that_stops_when_superseded():
        assert superseded.wait(10), "the running transfer was never handed over"
        return "stopped at a batch boundary"

    running = transfer_scheduler.submit("job-running", writer_that_stops_when_superseded)
    queued = transfer_scheduler.submit("job-queued", lambda: "never started")  # pool of one: waits
    started = time.monotonic()
    failed = background_lifecycle.stop_background_threads()
    took = time.monotonic() - started

    assert failed == []
    assert running.result(timeout=1) == "stopped at a batch boundary"
    assert queued.cancelled(), "a transfer that never started is not started during shutdown"
    assert leases.held == set(), "every lease this pool held is released"
    assert sorted(leases.released) == ["job-queued", "job-running"]
    assert heartbeats == {"job-running": False, "job-queued": False}
    assert transfer_scheduler.local_job_ids() == frozenset()
    assert took < 1.5 + 1.0, f"the drain took {took:.1f}s (drain period 1.5s)"


def test_a_transfer_that_finishes_inside_the_drain_is_not_handed_over(pool, monkeypatch):
    leases, _heartbeats = pool
    handed: list[str] = []
    monkeypatch.setattr(worker_fleet, "_supersede_local_run", lambda job_id: handed.append(job_id) or True)
    quick = transfer_scheduler.submit("job-quick", lambda: time.sleep(0.2) or "done")
    transfer_scheduler.drain()
    assert quick.result(timeout=1) == "done"
    assert handed == []
    assert leases.released == ["job-quick"]
    # The pool takes work again afterwards (an in-process restart starts clean).
    again = transfer_scheduler.submit("job-after", lambda: "ok")
    assert again.result(timeout=5) == "ok"
