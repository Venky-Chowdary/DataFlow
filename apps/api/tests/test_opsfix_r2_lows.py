"""Ops review round 2, Lows (one test group per finding, each with its own fix commit)."""

from __future__ import annotations

import pytest

from services import background_lifecycle, transfer_scheduler


# ---------------------------------------------------------------------------
# The API's transfer pool stays closed after the shutdown drain
# ---------------------------------------------------------------------------


class _Leases:
    def acquire(self, job_id, ttl_seconds=60):
        return True

    def get_fence(self, job_id):
        return 1

    def release(self, job_id, fence=None):
        return None


@pytest.fixture
def pool(monkeypatch):
    from services import lease_heartbeat

    monkeypatch.setattr(lease_heartbeat, "start_lease_heartbeat", lambda *_a, **_k: (lambda: None))
    monkeypatch.setattr(transfer_scheduler, "_lease_store", _Leases())
    monkeypatch.setattr(transfer_scheduler, "_executor", None)
    monkeypatch.setattr(transfer_scheduler, "_shutdown", False)
    monkeypatch.setattr(transfer_scheduler, "_draining", False)
    monkeypatch.setattr(transfer_scheduler, "_closed_for_exit", False, raising=False)
    monkeypatch.setenv("DATAFLOW_TRANSFER_WORKERS", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_DRAIN_SECONDS", "0.5")
    yield
    executor = transfer_scheduler._executor
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=True)
    transfer_scheduler._executor = None
    transfer_scheduler._draining = False


def _received_sigterm(monkeypatch) -> None:
    """Run the handler shutdown_signal.install() chains in front of the server's, as SIGTERM would."""
    import signal

    from services import shutdown_signal

    installed: dict = {}
    monkeypatch.setattr(signal, "getsignal", lambda _sig: (lambda *_a: None))
    monkeypatch.setattr(signal, "signal", lambda sig, handler: installed.__setitem__(sig, handler))
    monkeypatch.setattr(shutdown_signal, "_signalled", type(shutdown_signal._event)(), raising=False)
    assert shutdown_signal.install()
    installed[signal.SIGTERM](signal.SIGTERM, None)


def test_a_transfer_submitted_after_the_shutdown_drain_is_refused(pool, monkeypatch):
    """A slow sync request that finishes after the drain must not start a transfer on a dying process."""
    _received_sigterm(monkeypatch)
    stoppers = {name: stop for name, stop, _wait in background_lifecycle._stoppers()}
    stoppers["transfer pool"](0.5)
    with pytest.raises(RuntimeError, match="draining for shutdown"):
        transfer_scheduler.submit("job-late", lambda: "ran")
    from services import shutdown_signal

    shutdown_signal.reset()  # the end of the lifespan teardown; the process exits next
    with pytest.raises(RuntimeError, match="draining for shutdown"):
        transfer_scheduler.submit("job-later", lambda: "ran")
    transfer_scheduler.start()  # only a new lifespan opens it again
    assert transfer_scheduler.submit("job-next", lambda: "ok").result(timeout=5) == "ok"


def test_an_in_process_restart_without_a_signal_still_reopens_the_pool(pool):
    stoppers = {name: stop for name, stop, _wait in background_lifecycle._stoppers()}
    stoppers["transfer pool"](0.5)
    assert transfer_scheduler.submit("job-next", lambda: "ok").result(timeout=5) == "ok"
