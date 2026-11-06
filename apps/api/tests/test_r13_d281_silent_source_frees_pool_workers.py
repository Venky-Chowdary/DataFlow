"""D281/D181/D287: a source that answers the handshake and then goes silent
must not hold a connector pool worker for good, and a pool full of such
workers must say so instead of blaming the next source.

Production (3e258e90f, 2026-11-03 07:27-08:05 ET): ``/health`` showed
``in_flight`` 24 over ``max_workers`` 16 and ``abandoned`` 21 for forty
minutes; ``sample_connector_object`` on a healthy, untunnelled Neon database
"did not answer within 45s"; ``run_query``, ``plan_transfer`` and
``start_transfer`` all timed out; metadata calls that never touch the pool
kept working. The stuck workers were blocked in a driver ``recv`` on tunnel
sockets that had accepted the connection and stopped answering. A server-side
``statement_timeout`` cannot reach that (the server's answer is what never
arrives), the replacement-worker cap filled, and every later call queued in
the pool until its own deadline, then reported the *source* unreachable.

Real PostgreSQL behind a stalling TCP proxy: the proxy relays the handshake
and authentication, then swallows everything the server sends, which is what
a dead tunnel looks like to psycopg2. Every stuck call gets a worker at once
(as many workers as calls), so each one reaches the source and names it; the
discriminating assertion is what happens to those workers afterwards.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

#: Lowered for a quick test; never raised.
DEADLINE = 3
#: One worker per stuck call: none of them queues behind another.
WORKERS = 4
#: Seconds of normal relaying before the proxy goes silent: long enough for
#: the handshake and authentication, shorter than the query below.
RELAY_SECONDS = 1.0


@pytest.fixture
def stalling_proxy():
    """A TCP relay to the local PostgreSQL that stops relaying server bytes
    ``RELAY_SECONDS`` after each connection opened."""
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    upstream = (cfg["host"], int(cfg["port"]))
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(64)
    stop = threading.Event()
    opened: list[socket.socket] = []

    def pump(src: socket.socket, dst: socket.socket, silent_after: float | None) -> None:
        started = time.monotonic()
        try:
            while not stop.is_set():
                data = src.recv(65536)
                if not data:
                    return
                if silent_after is not None and time.monotonic() - started > silent_after:
                    # The tunnel is "up" but nothing from the server gets through.
                    continue
                dst.sendall(data)
        except OSError:
            return

    def serve() -> None:
        server.settimeout(0.2)
        while not stop.is_set():
            try:
                client, _ = server.accept()
            except OSError:
                continue
            try:
                up = socket.create_connection(upstream, timeout=5)
            except OSError:
                client.close()
                continue
            up.settimeout(None)
            opened.extend((client, up))
            threading.Thread(target=pump, args=(client, up, None), daemon=True).start()
            threading.Thread(target=pump, args=(up, client, RELAY_SECONDS), daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    yield server.getsockname()[1]
    stop.set()
    for s in opened:
        try:
            s.close()
        except OSError:
            pass
    server.close()


@pytest.fixture
def world(monkeypatch, stalling_proxy):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    try:
        psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        ).close()
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_POOL_WORKERS", str(WORKERS))
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    connector_executor.reset_for_tests()
    tag = uuid.uuid4().hex[:6]
    good = create_connector({"name": f"good-{tag}", "type": "postgresql", "schema": "public",
                             "workspace_id": "ws-d281", **cfg})
    silent = create_connector({"name": f"silent-{tag}", "type": "postgresql", "schema": "public",
                               "workspace_id": "ws-d281", **{**cfg, "host": "127.0.0.1",
                                                              "port": stalling_proxy}})
    try:
        yield {"good": good.id, "silent": silent.id, "good_host": f"{cfg['host']}:{cfg['port']}",
               "silent_port": stalling_proxy}
    finally:
        # Let the last abandoned worker end before its connector goes away:
        # a worker still unwinding while the store row is deleted raced the
        # teardown once in eleven runs.
        _wait_until(lambda: connector_executor.snapshot()["in_flight"] == 0, 20)
        delete_connector(good.id)
        delete_connector(silent.id)
        connector_executor.reset_for_tests()


def _call(tool: str, args: dict):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only("ws-d281"), "ws-d281", "tester"):
        return DataPilotTools().execute(tool, args)


def _wait_until(predicate, seconds: float) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


def test_workers_blocked_on_a_silent_source_end_at_the_deadline_and_the_pool_recovers(world):
    from services.connector_executor import snapshot

    # As many stuck calls as workers: every call starts at once, reaches the
    # silent source and blocks there. On the old code these workers never end
    # (abandoned stays at 4, in_flight at 4) and the interpreter cannot even
    # exit while they live.
    stuck = {"connector_id": world["silent"],
             "query": "SELECT count(*) AS n FROM generate_series(1, 300000000)"}
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(_call, "run_query", stuck) for _ in range(WORKERS)]
        results = [f.result(timeout=60) for f in futures]
    for r in results:
        assert not r.success, r
        # Every call reached the silent source and names it; the healthy host
        # is never blamed.
        assert f"127.0.0.1:{world['silent_port']}" in str(r.error), r.error
        assert world["good_host"] not in str(r.error), r.error
    snap = snapshot()
    assert snap["abandoned_lifetime"] == WORKERS, snap

    # The abandoned workers are interrupted (their sockets shut), so they end
    # within seconds instead of holding the pool for hours.
    assert _wait_until(lambda: snapshot()["abandoned"] == 0 and snapshot()["in_flight"] == 0, 20), snapshot()
    assert snapshot().get("saturated") is False

    # And a healthy source answers right away afterwards.
    started = time.monotonic()
    healthy = _call("run_query", {"connector_id": world["good"], "query": "SELECT 1 AS one"})
    assert healthy.success, healthy.error
    assert time.monotonic() - started < DEADLINE


def test_a_pool_filled_by_stuck_workers_refuses_at_once_and_names_the_pool():
    """Unit: no source is blamed for a call the pool never started (D281's
    "source unreachable/slow: <healthy Neon host>")."""
    from services import connector_executor

    os.environ["DATAFLOW_CONNECTOR_POOL_WORKERS"] = "2"
    connector_executor.reset_for_tests()
    release = threading.Event()
    try:
        executor = connector_executor.get_executor()
        for i in range(2 * 2):
            with pytest.raises(connector_executor.ConnectorCallTimeout):
                executor.run(
                    "sample_connector_object",
                    release.wait,
                    workspace=f"ws-{i}",
                    seconds=0.1,
                    describe_timeout=lambda: "source unreachable/slow: 10.0.0.9:3306",
                )
        snap = executor.snapshot()
        assert snap["abandoned"] == 4
        assert snap.get("saturated") is True, snap
        assert executor.readiness()["ok"] is False

        started = time.monotonic()
        # The parent class exists on the old code too; there the call waits
        # out its 5 s deadline and raises ConnectorCallTimeout blaming the
        # healthy source, which fails this block for the right reason.
        with pytest.raises(connector_executor.ConnectorCapacityExceeded) as refused:
            executor.run(
                "run_query",
                lambda: "never",
                workspace="ws-neon",
                seconds=5,
                describe_timeout=lambda: "source unreachable/slow: neon.example:5432",
            )
        # Refused at once, not after the 5 s deadline, and the message names
        # the pool, not the healthy source.
        assert time.monotonic() - started < 1
        text = str(refused.value)
        assert "run_query was not started" in text and "connector pool" in text
        assert "neon.example" not in text
        assert executor.snapshot()["rejected_total"] == 1

        # An interruptible stuck call is interrupted when abandoned.
        wake = threading.Event()
        release.set()
        assert _wait_until(lambda: executor.snapshot()["abandoned"] == 0, 5), executor.snapshot()
        with pytest.raises(connector_executor.ConnectorCallTimeout):
            executor.run(
                "test_connector",
                wake.wait,
                workspace="ws-x",
                seconds=0.1,
                describe_timeout=lambda: "x",
                interrupt=wake.set,
            )
        assert _wait_until(lambda: executor.snapshot()["in_flight"] == 0, 5), executor.snapshot()
    finally:
        release.set()
        os.environ.pop("DATAFLOW_CONNECTOR_POOL_WORKERS", None)
        connector_executor.reset_for_tests()


def test_a_probe_that_outlives_its_step_budget_is_abandoned_and_interrupted():
    """``bounded_step`` children used to keep their worker uncounted (no
    replacement, nothing ending them); now they are abandoned like a call."""
    from services import connector_executor
    from src.ai.copilot import tool_deadline

    os.environ["DATAFLOW_CONNECTOR_POOL_WORKERS"] = "2"
    connector_executor.reset_for_tests()
    wake = threading.Event()
    try:
        executor = connector_executor.get_executor()

        class FakeConn:
            closed = False

            def __init__(self) -> None:
                self.interrupted = False

            def cancel(self) -> None:
                self.interrupted = True
                wake.set()

        conn = FakeConn()
        register = getattr(tool_deadline, "register_connection", None)

        def probe() -> str:
            if register is not None:
                register(conn)
            wake.wait()
            return "late"

        with tool_deadline.deadline_context_for_tests(30):
            result = tool_deadline.bounded_step("count rows", probe, seconds=0.2)
        assert result.timed_out is True
        # The overrun probe is counted as an abandoned *probe* (so the pool
        # grows a replacement) and its connection is interrupted, so it ends.
        # It is not an abandoned call: the tool answered. On the old code it
        # stays an uncounted, never-ending worker.
        snap = executor.snapshot()
        assert snap.get("abandoned_probes_lifetime") == 1, snap
        assert snap["abandoned_lifetime"] == 0, snap
        assert _wait_until(lambda: conn.interrupted, 2)
        assert _wait_until(lambda: executor.snapshot()["in_flight"] == 0, 5), executor.snapshot()
    finally:
        wake.set()
        os.environ.pop("DATAFLOW_CONNECTOR_POOL_WORKERS", None)
        connector_executor.reset_for_tests()


def test_an_optional_probe_overrun_is_not_an_abandoned_call_but_still_holds_capacity():
    """CI 90f60d46b (D187 e2e on a slow runner): a plan that *succeeded* left
    ``abandoned_lifetime`` at 1 because an optional probe overran its step
    budget and was counted like a call that missed its deadline. The public
    ``abandoned*`` gauges are abandoned calls (QA's leak gauge); probe
    overruns are counted apart, and a probe still running keeps counting
    toward saturation and readiness."""
    from services import connector_executor
    from src.ai.copilot import tool_deadline

    os.environ["DATAFLOW_CONNECTOR_POOL_WORKERS"] = "2"
    connector_executor.reset_for_tests()
    release = threading.Event()
    try:
        executor = connector_executor.get_executor()
        with tool_deadline.deadline_context_for_tests(30):
            first = tool_deadline.bounded_step("count rows", release.wait, seconds=0.2)
        assert first.timed_out is True
        snap = executor.snapshot()
        # The call answered with a labelled partial result: no call abandoned.
        assert snap["abandoned"] == 0 and snap["abandoned_total"] == 0, snap
        assert snap["abandoned_lifetime"] == 0, snap
        assert snap.get("abandoned_probes") == 1, snap
        assert snap.get("abandoned_probes_lifetime") == 1, snap

        with tool_deadline.deadline_context_for_tests(30):
            second = tool_deadline.bounded_step("probe uniqueness", release.wait, seconds=0.2)
        assert second.timed_out is True
        snap = executor.snapshot()
        # Two stuck probes hold both workers: readiness says so although no
        # call was abandoned.
        assert snap.get("stuck_workers") == 2, snap
        assert executor.readiness()["ok"] is False, executor.readiness()

        release.set()
        assert _wait_until(lambda: executor.snapshot()["in_flight"] == 0, 5), executor.snapshot()
        snap = executor.snapshot()
        assert snap["abandoned_probes"] == 0 and snap["abandoned_lifetime"] == 0, snap
        assert executor.readiness()["ok"] is True
    finally:
        release.set()
        os.environ.pop("DATAFLOW_CONNECTOR_POOL_WORKERS", None)
        connector_executor.reset_for_tests()
