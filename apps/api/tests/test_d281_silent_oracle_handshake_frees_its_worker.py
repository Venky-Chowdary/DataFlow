"""D281: a connector call cut off at its deadline must free its pool worker,
even when the host accepted the connection and never answered the handshake.

Production (Nov 9-10): ``test_connector`` / ``start_transfer`` preflights on
Oracle hosts behind tunnels answered "did not answer within 45s" and their
workers kept running for 400-4100 s, released only when the tunnel was
stopped; fresh Oracle connectors then hung with *zero* inbound connections
on their tunnels.

Reproduced against a black hole (accepts TCP, never sends a byte):

* python-oracledb bounds only the TCP connect (``tcp_connect_timeout``); the
  handshake that follows waits on the socket forever. No DBAPI connection
  exists yet, so the deadline's interrupt had nothing registered to shut and
  the worker stayed blocked until the peer closed.
* Until its first connection succeeds, python-oracledb serializes every
  connect in the process behind the one deciding thin/thick mode
  (``driver_mode.get_manager``). One silent Oracle host therefore parked every
  other Oracle connect on a lock, without dialing (the zero inbound
  connections QA saw), where no socket interrupt can reach them.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

pytest.importorskip("oracledb")

#: Lowered for a quick test, never raised.
DEADLINE = 2
#: The worker must be gone this long after the answer, with the peer still silent.
RECLAIMED_WITHIN = 5.0


class _BlackHole:
    def __init__(self) -> None:
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(64)
        self.server.settimeout(0.1)
        self.port = self.server.getsockname()[1]
        self.held: list[socket.socket] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self.server.accept()
                self.held.append(conn)
            except OSError:
                continue

    def close(self) -> None:
        self._stop.set()
        self._thread.join(2)
        for conn in self.held:
            conn.close()
        self.server.close()


@pytest.fixture
def world(monkeypatch):
    from oracledb import driver_mode

    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    # A fresh process: python-oracledb has not fixed its mode yet.
    monkeypatch.setattr(driver_mode, "manager", driver_mode.DriverModeManager())
    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.delenv("DATAFLOW_CONNECTOR_POOL_WORKERS", raising=False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    connector_executor.reset_for_tests()
    holes = [_BlackHole(), _BlackHole()]
    ids = [
        create_connector(
            {
                "name": f"d281-silent-oracle-{h.port}",
                "type": "oracle",
                "host": "127.0.0.1",
                "port": h.port,
                "database": "ORCL",
                "username": "u",
                "password": "p",
                "workspace_id": "ws-d281",
            }
        ).id
        for h in holes
    ]
    try:
        yield holes, ids
    finally:
        for h in holes:
            h.close()
        end = time.monotonic() + 20
        while time.monotonic() < end and connector_executor.snapshot()["in_flight"]:
            time.sleep(0.1)
        left = connector_executor.snapshot()["in_flight"]
        for cid in ids:
            delete_connector(cid)
        connector_executor.reset_for_tests()
        assert left == 0, f"{left} connector workers still running after the test"


def _test_connector(connector_id: str):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only("ws-d281"), "ws-d281", "tester"):
        return DataPilotTools().execute("test_connector", {"connector_id": connector_id})


def _wait_idle(seconds: float) -> dict:
    from services.connector_executor import snapshot

    end = time.monotonic() + seconds
    while time.monotonic() < end and snapshot()["in_flight"]:
        time.sleep(0.05)
    return snapshot()


def _failed(result) -> str:
    out = result.output if isinstance(result.output, dict) else {}
    assert not result.success or out.get("ok") is False, result
    return str(result.error or out.get("message") or "")


def test_a_silent_oracle_handshake_frees_its_worker_at_the_deadline(world):
    holes, ids = world
    started = time.monotonic()
    message = _failed(_test_connector(ids[0]))
    answered = time.monotonic() - started
    assert answered < DEADLINE + 2, answered
    assert f"127.0.0.1:{holes[0].port}" in message, message
    assert holes[0].held, "the probe never reached the host"

    # The peer is still silent: only the deadline's interrupt can end the
    # blocked handshake.
    pool = _wait_idle(RECLAIMED_WITHIN)
    assert pool["in_flight"] == 0 and pool["stuck_workers"] == 0, pool


def test_one_silent_oracle_host_does_not_park_every_other_oracle_connect(world):
    holes, ids = world
    first = threading.Thread(target=_test_connector, args=(ids[0],), daemon=True)
    first.start()
    end = time.monotonic() + 5
    while time.monotonic() < end and not holes[0].held:
        time.sleep(0.02)
    assert holes[0].held, "the first probe never reached its host"

    # A second Oracle connector, while the first handshake hangs: it must
    # reach its own host, not wait on the driver's mode lock.
    message = _failed(_test_connector(ids[1]))
    assert holes[1].held, f"the second probe never dialed its host: {message}"
    first.join(DEADLINE + RECLAIMED_WITHIN)
    pool = _wait_idle(RECLAIMED_WITHIN)
    assert pool["in_flight"] == 0, pool


@pytest.fixture
def endless_hubspot(monkeypatch):
    """A HubSpot stand-in whose contact list never ends: every page takes a
    moment and names a next page (the 627-2600 s "counting rows in source
    table contacts" calls)."""
    import http.server
    import json

    from services import connector_executor

    monkeypatch.delenv("DATAFLOW_CONNECTOR_POOL_WORKERS", raising=False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    connector_executor.reset_for_tests()
    served = {"pages": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):  # noqa: ANN002 - quiet test server
            return

        def do_GET(self):  # noqa: N802 - http.server API
            if "/crm/v3/properties/" in self.path:
                body = {"results": [{"name": "email", "type": "string"}]}
            else:
                time.sleep(0.3)
                served["pages"] += 1
                n = served["pages"]
                body = {
                    "results": [{"id": str(n), "properties": {"email": f"u{n}@example.com"}}],
                    "paging": {"next": {"after": str(n)}},
                }
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1], served
    finally:
        server.shutdown()
        server.server_close()
        connector_executor.reset_for_tests()


def test_an_abandoned_saas_read_stops_paging_at_the_deadline(endless_hubspot):
    from connectors import hubspot
    from src.ai.copilot.tool_deadline import run_with_deadline

    port, served = endless_hubspot
    cfg = {"host": f"http://127.0.0.1:{port}", "api_key": "k", "table": "contacts"}
    answer = run_with_deadline(
        "aggregate_data",
        lambda: hubspot.read_object(cfg=cfg, limit=10**9),
        lambda message: message,
        seconds=DEADLINE,
    )
    assert isinstance(answer, str) and f"within {DEADLINE}s" in answer, answer
    # The abandoned worker stops at its next request instead of reading the
    # whole object for nobody, and the API stops calling the SaaS.
    pool = _wait_idle(RECLAIMED_WITHIN)
    assert pool["in_flight"] == 0, pool
    pages = served["pages"]
    time.sleep(1.0)
    assert served["pages"] == pages


def test_readiness_names_the_calls_that_hold_the_pool(monkeypatch):
    from services import connector_executor

    monkeypatch.setenv("DATAFLOW_CONNECTOR_POOL_WORKERS", "1")
    connector_executor.reset_for_tests()
    release = threading.Event()
    pool = connector_executor.get_executor()
    try:
        with pytest.raises(connector_executor.ConnectorCallTimeout):
            pool.run(
                "sample_connector_object",
                lambda: release.wait(10),
                workspace="ws-d281",
                seconds=0.2,
                describe_timeout=lambda: "timed out",
                label=lambda: "sample_connector_object: reading rows (source 10.0.0.5:1521)",
            )
        ready = connector_executor.readiness()
        assert ready["ok"] is False
        assert "10.0.0.5:1521" in ready["reason"], ready["reason"]
    finally:
        release.set()
        _wait_idle(5)
        connector_executor.reset_for_tests()


def test_an_abandoned_rest_saas_read_stops_paging_at_the_deadline(endless_hubspot):
    """The generic REST reader (Notion, Airtable, Shopify, ...) pages the same way."""
    from connectors import rest_api
    from src.ai.copilot.tool_deadline import run_with_deadline

    port, served = endless_hubspot
    url = f"http://127.0.0.1:{port}/v1/records"

    def _page_forever() -> None:
        for _ in range(10_000):
            rest_api._request_with_retry("GET", url, timeout=60)

    answer = run_with_deadline(
        "aggregate_data", _page_forever, lambda message: message, seconds=DEADLINE
    )
    assert isinstance(answer, str) and f"within {DEADLINE}s" in answer, answer
    pool = _wait_idle(RECLAIMED_WITHIN)
    assert pool["in_flight"] == 0, pool
    pages = served["pages"]
    time.sleep(1.0)
    assert served["pages"] == pages
