"""D310: Databricks is Planned here (its driver is not shipped), yet
create_connector accepted it and every call went on to a connection attempt.

Production (Nov 10 7:21-7:37 ET): the catalog tile said Planned with
``driver_available=false``; create_connector type=databricks saved; then
test_connector, list_connector_objects and plan_transfer each waited out the
45 s deadline and a worker was still running at 250 s.

``databricks-sqlalchemy`` is kept off the default install
(requirements-databricks.txt, oauthlib pin), exactly as in production. Here:

* create_connector stages a Databricks connector although nothing can ever
  connect it (with ``test_first`` it says "Could not connect ... with those
  credentials", blaming the token);
* a saved one answers every call with "Authentication failed. Check username,
  password ..." for what is a missing driver.

The calls must refuse at once, name the missing driver, never dial the host
(a black hole here) and leave no worker behind.

Live Databricks was not reachable from this sandbox (no DNS); the hang itself
is covered by the D281 / D321 pool fixes.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

#: Lowered for a quick test, never raised.
DEADLINE = 3


def _driver_shipped() -> bool:
    from src.transfer.connector_capabilities import driver_available, resolve_driver_type

    return driver_available(resolve_driver_type("databricks"), "databricks")


class _BlackHole:
    def __init__(self) -> None:
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(16)
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
    if _driver_shipped():
        pytest.skip("the Databricks driver is installed here")
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    connector_executor.reset_for_tests()
    hole = _BlackHole()
    # A Databricks connector saved before this fix (or through the REST form).
    saved = create_connector(
        {
            "name": f"d310-dbx-{hole.port}",
            "type": "databricks",
            "host": "127.0.0.1",
            "port": hole.port,
            "database": "workspace",
            "schema": "default",
            "username": "token",
            "password": "dapi-not-a-real-token",
            "auth_mode": "pat",
            "workspace_id": "ws-d310",
        }
    )
    try:
        yield hole, saved.id
    finally:
        hole.close()
        delete_connector(saved.id)
        connector_executor.reset_for_tests()


def _call(tool: str, args: dict):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only("ws-d310"), "ws-d310", "tester"):
        return DataPilotTools().execute(tool, args)


def _text(result) -> str:
    out = result.output if isinstance(result.output, dict) else {}
    return f"{result.error or ''} {out.get('message') or ''}"


@pytest.mark.parametrize("test_first", [True, False])
def test_create_connector_refuses_databricks_without_its_driver(world, test_first):
    hole, _ = world
    result = _call(
        "create_connector",
        {
            "name": f"d310-new-{test_first}",
            "type": "databricks",
            "host": "127.0.0.1",
            "port": hole.port,
            "database": "workspace",
            "username": "token",
            "password": "dapi-not-a-real-token",
            "test_first": test_first,
        },
    )
    text = _text(result)
    assert not result.success, result
    assert "ack_id" not in str(result.output or ""), result.output
    assert "not installed" in text.lower(), text
    assert "credentials" not in text.lower(), text
    assert not hole.held, "a connector that cannot work must not dial its host"


@pytest.mark.parametrize(
    "tool,extra",
    [
        ("test_connector", {}),
        ("list_connector_objects", {}),
        ("sample_connector_object", {"table": "dfqa_src", "limit": 30}),
    ],
)
def test_calls_on_a_saved_databricks_connector_name_the_missing_driver_at_once(world, tool, extra):
    from services.connector_executor import snapshot

    hole, connector_id = world
    started = time.monotonic()
    result = _call(tool, {"connector_id": connector_id, **extra})
    answered = time.monotonic() - started
    text = _text(result)
    assert answered < 2.0, answered
    assert "not available" in text.lower() or "not installed" in text.lower(), text
    assert "authentication failed" not in text.lower(), text
    assert not hole.held, "a connector that cannot work must not dial its host"
    assert snapshot()["in_flight"] == 0
