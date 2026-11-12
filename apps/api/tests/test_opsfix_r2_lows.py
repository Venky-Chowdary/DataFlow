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


# ---------------------------------------------------------------------------
# Telemetry flushes run inside the shutdown budget
# ---------------------------------------------------------------------------


def test_a_hanging_telemetry_flush_cannot_outlast_the_shutdown_budget(monkeypatch):
    """OTel and tracing flushes ran after the budgeted joins, unbounded (collector unreachable)."""
    import threading
    import time

    from services import otel_logging, tracing

    release = threading.Event()
    monkeypatch.setattr(tracing, "shutdown_tracing", lambda: release.wait(30))
    monkeypatch.setattr(otel_logging, "shutdown_otel_logging", lambda: release.wait(30))
    quick = [(name, (lambda _timeout: None), wait) for name, _stop, wait in background_lifecycle._stoppers()
             if name != "telemetry flush"]
    real = background_lifecycle._stoppers
    monkeypatch.setattr(background_lifecycle, "_stoppers",
                        lambda: quick + [s for s in real() if s[0] == "telemetry flush"])
    assert any(name == "telemetry flush" for name, _stop, _wait in real()), "the flush is not budgeted"
    started = time.monotonic()
    background_lifecycle.stop_background_threads(budget_seconds=1.0)
    took = time.monotonic() - started
    release.set()
    assert took < 1.5, f"teardown took {took:.1f}s with a 1 s budget"


def test_the_lifespan_does_not_flush_telemetry_after_the_budget():
    from pathlib import Path

    teardown = Path(__file__).resolve().parents[1].joinpath("src", "main.py").read_text(encoding="utf-8")
    teardown = teardown[teardown.index("stop_background_threads)"):teardown.index("_docs = ")]
    assert "shutdown_tracing" not in teardown and "shutdown_otel_logging" not in teardown


# ---------------------------------------------------------------------------
# Connector lists and the name check during a control-plane outage
# ---------------------------------------------------------------------------


@pytest.fixture
def byok_connector(tmp_path, monkeypatch):
    from services import byok_key_manager, connector_store, secret_vault, tenant_store
    from services.shared_record_store import SharedRecordSet

    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    tenant = tenant_store.create_tenant("ws-byok", "BYOK")
    byok_key_manager.create_key(tenant.id, "k0")
    doc = {"_id": "c-byok", "name": "warehouse", "type": "postgresql", "workspace_id": "ws-byok",
           "password": secret_vault.encrypt_secret("s3cret", tenant_id=tenant.id, label="connector-password")}

    class _Coll:
        def find(self, *_args, **_kwargs):
            return [dict(doc)]

    monkeypatch.setattr(connector_store, "_use_mongo", lambda: True)
    monkeypatch.setattr(connector_store, "_mongo_collection", lambda: _Coll())
    assert [c.name for c in connector_store.list_connectors(workspace_id="ws-byok")] == ["warehouse"]

    def down(*_args, **_kwargs):
        from services.control_plane_store import ControlPlaneUnavailable

        raise ControlPlaneUnavailable("the control-plane database is unavailable")

    monkeypatch.setattr(SharedRecordSet, "load", down)
    return connector_store


def test_a_connector_list_during_an_outage_is_a_503_not_a_shorter_list(byok_connector):
    from services.control_plane_store import ControlPlaneUnavailable

    with pytest.raises(ControlPlaneUnavailable):
        byok_connector.list_connectors(workspace_id="ws-byok")


def test_the_name_check_during_an_outage_does_not_answer_free(byok_connector):
    from services.control_plane_store import ControlPlaneUnavailable

    with pytest.raises(ControlPlaneUnavailable):
        byok_connector.connector_name_taken("warehouse", workspace_id="ws-byok")


# ---------------------------------------------------------------------------
# The chart refuses a non-numeric grace or drain value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"api": {"gracefulShutdownSeconds": "10s"}}, "api.gracefulShutdownSeconds"),
        ({"api": {"preStopSleepSeconds": "5s"}}, "api.preStopSleepSeconds"),
        ({"api": {"terminationGracePeriodSeconds": "1m"}}, "api.terminationGracePeriodSeconds"),
        ({"worker": {"drainSeconds": "25s"}}, "worker.drainSeconds"),
        ({"cdcWorker": {"terminationGracePeriodSeconds": "60s"}}, "cdcWorker.terminationGracePeriodSeconds"),
    ],
)
def test_the_chart_refuses_a_duration_string_where_it_needs_seconds(overrides, field):
    """Helm's int turns "10s" into 0, so the grace rule passed and start-api.sh then exited 64."""
    from tests._helm_lite import ChartFail
    from tests.test_helm_chart_static import _lite

    with pytest.raises(ChartFail, match=field.replace(".", r"\.")):
        _lite(overrides)


def test_the_renderer_reads_int_like_helm():
    from tests._helm_lite import _FUNCS

    assert _FUNCS["int"]("10s") == 0 and _FUNCS["int"]("10") == 10 and _FUNCS["int"](10.0) == 10
