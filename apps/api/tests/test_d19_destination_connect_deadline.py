"""D19 — a job froze at 5% "Connecting to destination..." with no heartbeat.

A destination that accepts the TCP connection and never answers held the
engine's own worker for the sum of every driver timeout it stacked (measured:
181s on PostgreSQL, longer than 200s on MySQL and SQL Server, 75s+ on
Elasticsearch), and the job document did not move while it waited. The orphan
sweeper cannot settle such a job (the worker is alive and holds its lease) and
the step watchdog is silenced by any heartbeat, so the only thing that can end
it is a deadline in the engine itself.

Real SQLite source, real silent socket as the destination, the real engine.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _SilentServer:
    """Accepts every connection and never writes a byte."""

    def __init__(self) -> None:
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.held: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.held.append(conn)

    def close(self) -> None:
        self.sock.close()
        for conn in self.held:
            conn.close()


@pytest.fixture()
def silent():
    server = _SilentServer()
    yield server
    server.close()


@pytest.fixture()
def job_store(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Recording(_FakeMongo):
        def __init__(self) -> None:
            super().__init__()
            self.writes: list[tuple[float, dict]] = []

        def update_job_status(self, job_id, status, **kwargs):
            self.writes.append((time.monotonic(), {"status": status, **kwargs}))
            return super().update_job_status(job_id, status, **kwargs)

    fake = _Recording()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setenv("DATAFLOW_DESTINATION_CONNECT_DEADLINE_SECONDS", "2")
    return fake


def _source(tmp_path: Path) -> Path:
    path = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"v{i}") for i in range(20)])
    conn.close()
    return path


def _run_in_thread(fake, request: TransferRequest, limit: float):
    job_id = "d19" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    box: dict = {}

    def go() -> None:
        box["result"] = UniversalTransferEngine().execute_tracked(request, job_id)

    worker = threading.Thread(target=go, daemon=True)
    started = time.monotonic()
    worker.start()
    worker.join(limit)
    return job_id, box.get("result"), time.monotonic() - started, worker.is_alive()


@pytest.mark.parametrize("fmt", ["postgresql", "mysql", "sqlserver", "mongodb", "elasticsearch", "oracle", "redis", "pgvector", "kafka"])
def test_silent_destination_fails_at_the_deadline_and_keeps_pulsing(
    tmp_path, silent, job_store, fmt
):
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(_source(tmp_path)), table="t"
        ),
        destination=EndpointConfig(
            kind="database",
            format=fmt,
            host="127.0.0.1",
            port=silent.port,
            database="d",
            username="u",
            password="p",
            table="t",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id, result, elapsed, still_running = _run_in_thread(job_store, request, limit=25)

    assert not still_running, "the job is still frozen on a destination that never answers"
    assert result is not None and result.success is False
    assert elapsed < 20, elapsed
    error = str(result.error)
    assert f"127.0.0.1:{silent.port}" in error, error
    assert "did not answer within 2s" in error, error
    assert job_store.jobs[job_id]["status"] == "failed"

    # The job document kept moving while the engine waited.
    pulses = [
        stamp
        for stamp, fields in job_store.writes
        if "Connecting to destination" in str(fields.get("message") or "")
    ]
    assert len(pulses) >= 3, pulses
    gaps = [b - a for a, b in zip(pulses, pulses[1:])]
    assert max(gaps) < 1.5, gaps
