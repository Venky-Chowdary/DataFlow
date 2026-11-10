"""D314: a BigQuery call cut off at its deadline must not keep its worker.

Production (Nov 10 7:52-8:10 ET): ``list_connector_objects`` and
``sample_connector_object`` on a BigQuery sandbox answered "did not answer
within 45s" while their workers kept running for 305-536 s; the same project
answered the QA box in about a second.

google-cloud-bigquery sends every API request with ``timeout=None`` and
retries it, and re-issues query jobs, for 10 to 40 minutes
(``DEFAULT_RETRY`` / ``DEFAULT_JOB_RETRY``). Inside a tool call nothing bounded
either: a request the endpoint never answered blocked in ``recv`` for good, and
a retried error kept the worker for up to ten minutes. The deadline's
interrupt has no socket to shut for an HTTP client.

No internet here: a local stand-in plays the endpoint. One never answers (a
TCP black hole), one answers every request with a retryable ``backendError``.
"""

from __future__ import annotations

import http.server
import json
import os
import socket
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

pytest.importorskip("google.cloud.bigquery")

#: Lowered for a quick test, never raised.
DEADLINE = 2
#: The worker must be gone this long after the answer.
RECLAIMED_WITHIN = 4.0


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


class _BackendErrors:
    """Answers every BigQuery API request with a retryable 500 backendError."""

    def __init__(self) -> None:
        self.requests = 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):  # noqa: ANN002 - quiet test server
                return

            def _answer(self) -> None:
                owner.requests += 1
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                body = json.dumps(
                    {
                        "error": {
                            "code": 500,
                            "message": "Backend error",
                            "errors": [{"reason": "backendError", "message": "Backend error"}],
                        }
                    }
                ).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = _answer  # noqa: N815 - http.server API
            do_POST = _answer  # noqa: N815 - http.server API

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def pool(monkeypatch):
    from services import connector_executor

    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.delenv("DATAFLOW_CONNECTOR_POOL_WORKERS", raising=False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    connector_executor.reset_for_tests()
    yield connector_executor
    connector_executor.reset_for_tests()


@pytest.fixture
def black_hole(pool):
    from services.connector_store import create_connector, delete_connector

    hole = _BlackHole()
    conn = create_connector(
        {
            "name": f"d314-silent-bq-{hole.port}",
            "type": "bigquery",
            "host": "127.0.0.1",
            "port": hole.port,
            "database": "dfqa-project",
            "schema": "dfqa",
            "workspace_id": "ws-d314",
        }
    )
    try:
        yield hole, conn.id
    finally:
        hole.close()
        _wait_idle(10)
        delete_connector(conn.id)
        left = pool.snapshot()["in_flight"]
        assert left == 0, f"{left} connector workers still running after the test"


def _wait_idle(seconds: float) -> dict:
    from services.connector_executor import snapshot

    end = time.monotonic() + seconds
    while time.monotonic() < end and snapshot()["in_flight"]:
        time.sleep(0.05)
    return snapshot()


def _call(tool: str, args: dict):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only("ws-d314"), "ws-d314", "tester"):
        return DataPilotTools().execute(tool, args)


@pytest.mark.parametrize(
    "tool,extra",
    [
        ("list_connector_objects", {}),
        ("sample_connector_object", {"table": "dfqa.dfqa_src", "limit": 30}),
    ],
)
def test_list_and_sample_free_their_worker_when_bigquery_never_answers(black_hole, tool, extra):
    hole, connector_id = black_hole
    started = time.monotonic()
    result = _call(tool, {"connector_id": connector_id, **extra})
    answered = time.monotonic() - started
    assert not result.success, result
    assert answered < DEADLINE + 2, answered
    assert hole.held, "the call never reached the endpoint"
    # The endpoint is still silent: the worker ends on the call's own budget.
    snap = _wait_idle(RECLAIMED_WITHIN)
    assert snap["in_flight"] == 0 and snap["stuck_workers"] == 0, snap


def test_a_retried_bigquery_error_stops_at_the_deadline(pool):
    """The hosted client (default ten-minute retry) against an endpoint that
    keeps answering a retryable error."""
    from google.auth.credentials import AnonymousCredentials
    from google.cloud.bigquery import _http

    from connectors import bigquery_conn
    from src.ai.copilot.tool_deadline import run_with_deadline

    stub = _BackendErrors()
    try:
        endpoint = f"http://127.0.0.1:{stub.port}"
        mp = pytest.MonkeyPatch()
        # The hosted path (service-account key) talks to this endpoint.
        mp.setattr(_http.Connection, "DEFAULT_API_ENDPOINT", endpoint)
        mp.setattr(
            "google.oauth2.service_account.Credentials.from_service_account_info",
            classmethod(lambda cls, info, **kw: AnonymousCredentials()),
        )
        try:
            client = bigquery_conn.get_client(
                project_id="dfqa-project", service_account=json.dumps({"type": "service_account"})
            )

            def _list() -> list:
                return [t.table_id for t in client.list_tables("dfqa-project.dfqa")]

            started = time.monotonic()
            try:
                answer = run_with_deadline(
                    "list_connector_objects", _list, lambda message: message, seconds=DEADLINE
                )
            except Exception as exc:  # noqa: BLE001 - the retry gave up inside the budget
                answer = exc
            assert time.monotonic() - started < DEADLINE + 1
            # Either the deadline answer, or the client's own error once its
            # retries ran out of the call's budget; never a table list.
            assert isinstance(answer, (str, Exception)), answer
            snap = _wait_idle(RECLAIMED_WITHIN)
            assert snap["in_flight"] == 0, snap
            served = stub.requests
            time.sleep(1.0)
            assert stub.requests == served, "the abandoned call kept retrying"
        finally:
            mp.undo()
    finally:
        stub.close()
        _wait_idle(30)
