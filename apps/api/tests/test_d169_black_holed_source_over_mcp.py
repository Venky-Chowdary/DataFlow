"""D169 end to end: a black-holed source (TCP accepts, never answers) fails
over MCP JSON-RPC within the stated budget, with the source named, and the
operation itself ends (no worker left stuck behind the answer).

QA: a refused source failed in 1.2s, but a slow / black-holed one was
untested and suspected to hang until the client's -32001. The deadline is the
production default (45s); the source is a saved PostgreSQL connector pointing
at a socket that accepts and never writes; the destination is the real local
PostgreSQL.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


@pytest.fixture
def black_hole():
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(16)
    held: list[socket.socket] = []
    stop = threading.Event()

    def accept() -> None:
        server.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
                held.append(conn)
            except OSError:
                continue

    threading.Thread(target=accept, daemon=True).start()
    yield server.getsockname()[1]
    stop.set()
    for c in held:
        c.close()
    server.close()


@pytest.mark.parametrize("tool", ["plan_transfer", "start_transfer"])
def test_black_holed_source_fails_within_budget_over_mcp(tool, black_hole, monkeypatch):
    from fastapi.testclient import TestClient

    from conftest import pg_test_cfg
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tool_deadline import deadline_seconds
    from src.main import app

    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    budget = deadline_seconds(tool)
    assert budget == 45.0
    sfx = uuid.uuid4().hex[:6]
    src = create_connector({
        "name": f"hung-pg-{sfx}", "type": "postgresql", "host": "127.0.0.1",
        "port": black_hole, "database": "app", "username": "u", "password": "p",
    })
    dst = create_connector({"name": f"pg-{sfx}", "type": "postgresql", "schema": "public", **pg_test_cfg()})
    try:
        client = TestClient(app)  # no lifespan: the data dir stays the test's
        if True:
            started = time.monotonic()
            response = client.post(
                "/api/v1/mcp",
                headers={"Accept": "application/json, text/event-stream"},
                json={
                    "jsonrpc": "2.0", "id": 7, "method": "tools/call",
                    "params": {"name": tool, "arguments": {
                        "source_connector_id": src.id, "source_table": "orders",
                        "dest_connector_id": dst.id, "dest_table": f"d169_{sfx}",
                        "sync_mode": "full_refresh_overwrite",
                    }},
                },
            )
            elapsed = time.monotonic() - started
        assert response.status_code == 200, response.text
        body = response.json()
        # An answer, not the client's -32001.
        assert "error" not in body or body["error"].get("code") != -32001, body
        text = str(body)
        assert elapsed < budget, elapsed
        assert f"127.0.0.1:{black_hole}" in text or "127.0.0.1" in text, text[:600]
        assert "source" in text.lower(), text[:600]
        # The operation itself ended: no worker is left stuck behind the answer.
        end = time.monotonic() + 5
        while time.monotonic() < end and connector_executor.snapshot()["in_flight"]:
            time.sleep(0.1)
        pool = connector_executor.snapshot()
        assert pool["in_flight"] == 0 and pool["abandoned_total"] == 0, pool
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
