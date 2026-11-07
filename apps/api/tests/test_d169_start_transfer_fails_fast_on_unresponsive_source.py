"""D169 — a source that accepts the connection and never answers fails fast.

QA: three ``start_transfer`` calls against a degraded MySQL source all ended in
the MCP client's bare ``-32001 Request timed out`` and staged nothing. The
planner introspects the source and destination on the request path, each driver
step waited out its own socket timeout, and the steps stacked.

The tool call now has a deadline and, past it, says which host:port did not
answer and at which step.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.ai.copilot.tool_deadline import (  # noqa: E402
    describe_target,
    note_step,
    run_with_deadline,
)


class _BlackHole:
    """Accepts TCP connections and never writes a byte (a hung database)."""

    def __init__(self) -> None:
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self._held: list[socket.socket] = []
        self._stop = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        self.sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except (TimeoutError, OSError):
                continue
            self._held.append(conn)

    def close(self) -> None:
        self._stop.set()
        for conn in self._held:
            conn.close()
        self.sock.close()


@pytest.fixture()
def black_hole():
    server = _BlackHole()
    yield server
    server.close()


def _connectors(port: int):
    source = {
        "name": "hung-mysql", "type": "mysql",
        "host": "127.0.0.1", "port": port, "database": "app",
        "username": "u", "password": "p",
    }
    dest = {"name": "local-sqlite", "type": "sqlite", "database": ":memory:"}

    def fake_safe_connector(connector_id, connector_name, tool, *a, **k):
        return (source if "src" in (connector_id or connector_name or "") or "mysql" in (connector_name or "") else dest), None

    return fake_safe_connector


def test_start_transfer_fails_fast_naming_the_unresponsive_source(monkeypatch, black_hole):
    import src.ai.copilot.transfer_tools as transfer_tools
    from src.ai.copilot.tools import get_pilot_tools

    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "3")
    monkeypatch.setattr(transfer_tools, "_safe_connector", _connectors(black_hole.port))

    started = time.monotonic()
    result = get_pilot_tools().execute(
        "start_transfer",
        {
            "source_connector_id": "c-src",
            "source_table": "orders",
            "dest_connector_id": "c-dst",
            "dest_table": "orders_copy",
            "sync_mode": "full_refresh_overwrite",
        },
    )
    elapsed = time.monotonic() - started

    assert not result.success
    assert elapsed < 8, f"the call held for {elapsed:.1f}s"
    assert f"127.0.0.1:{black_hole.port}" in str(result.error)
    assert "source" in str(result.error).lower()
    assert "step" in str(result.error).lower()


def test_deadline_reports_the_step_that_was_waiting():
    def slow():
        note_step("reading source table orders", {"host": "db.internal", "port": 3306}, role="source")
        time.sleep(5)

    out = run_with_deadline("plan_transfer", slow, lambda message: message, seconds=0.3)
    assert "source unreachable/slow: db.internal:3306" in out
    assert "reading source table orders" in out


def test_fast_tool_is_not_delayed_and_errors_propagate():
    assert run_with_deadline("x", lambda: 7, lambda m: m, seconds=5) == 7
    with pytest.raises(ValueError):
        run_with_deadline("x", lambda: (_ for _ in ()).throw(ValueError("boom")), lambda m: m, seconds=5)


def test_describe_target_never_carries_credentials():
    target = describe_target({"connection_string": "mysql://user:secret@db.example:3307/app"})
    assert target == "db.example:3307"
    assert "secret" not in target


@pytest.mark.parametrize("terminal", ["failed", "cancelled", "completed"])
def test_destination_claim_does_not_outlive_a_terminal_holder(terminal):
    """A frozen job settled by the watchdog (or cancelled before it started)
    must free the destination for the next transfer, not hold it for the
    claim's multi-hour lease."""
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    store.update_job_status("holder", "running", transfer_request={})
    claimed, _, _ = store.claim_job_idempotency(key="dest:t", job_id="holder")
    assert claimed
    blocked, holder, _ = store.claim_job_idempotency(key="dest:t", job_id="next")
    assert not blocked and holder == "holder"

    if terminal == "failed":
        # What the stall watchdog does to a job frozen in a driver call.
        from services.job_watchdog import _settle_stalled

        info = {"step": "connect", "stall_seconds": 400, "fail_seconds": 360}
        assert _settle_stalled(store, store.get_job("holder"), info)
    else:
        store.update_job_status("holder", terminal, operator_command=True)
    assert store.get_job("holder")["status"] in {"failed", "cancelled", "completed"}
    claimed, _, _ = store.claim_job_idempotency(key="dest:t", job_id="next")
    assert claimed


def test_unset_connect_timeout_is_bounded_for_generic_sql_drivers():
    import sqlalchemy as sa

    from connectors.generic_sql import _connect_args_for

    url = sa.URL.create("mssql+pymssql", host="h", database="d")
    assert _connect_args_for(url, {}) == {"login_timeout": 15}
