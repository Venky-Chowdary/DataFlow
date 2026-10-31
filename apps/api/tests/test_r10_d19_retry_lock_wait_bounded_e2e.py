"""D19 / D168 (QA round 10) - a retry that queues behind a lock ends, and pulses.

QA on 087ac4bf: the retry of a 40k-row job that failed mid-way froze at 5%
"Connecting to destination..." with no heartbeat.

What the live runs show (MySQL -> SQL Server, REST ``/jobs/{id}/retry``):

* after the failed attempt, pooled SQL Server sessions stay open with an open
  transaction (``open_transaction_count = 1``), and
* SQL Server sessions from the pooled engine ran with ``LOCK_TIMEOUT -1``:
  PostgreSQL and MySQL write sessions already gave up a lock wait after 120 s,
  SQL Server waited forever. A retry whose overwrite clear (backup copy /
  DELETE / TRUNCATE) queued behind any lock on the table therefore never
  ended, and the clear ran outside every job heartbeat, so ``updated_at``
  stood still at 5%.

This test holds a table lock from another session while the operator retries
the failed job through the REST route on real engines. The retry must end
inside the bounded lock wait with a message that names the lock, the job must
pulse while it waits, and once the lock is released the next retry loads every
row. ``DATAFLOW_DEST_LOCK_WAIT_SECONDS`` is lowered for the test; the default
stays 120 s (no budget raised).
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_N = 40_000
_LOCK_WAIT_S = 8


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_DEST_LOCK_WAIT_SECONDS", str(_LOCK_WAIT_S))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connector(cfg: dict, role: str) -> str:
    from services.connector_store import create_connector

    return str(create_connector({
        **cfg, "name": f"r10d19-{role}-{uuid.uuid4().hex[:6]}", "role": role,
        "workspace_id": "", "ssl": False,
    }).id)


def _seed_mysql(table: str) -> None:
    h.execute("mysql", f"DROP TABLE IF EXISTS `{table}`")
    h.execute(
        "mysql",
        f"CREATE TABLE `{table}` (id int primary key, name text, amount decimal(12,2), "
        "note varchar(40)) CHARSET utf8mb4",
    )
    rows = []
    for g in range(1, _N + 1):
        note = "NULL" if g % 4 == 0 else ("''" if g % 4 == 1 else f"'é{g} 山'")
        rows.append(f"({g},'n{g}',{g * 1.25:.2f},{note})")
    for i in range(0, len(rows), 10_000):
        h.execute("mysql", f"INSERT INTO `{table}` VALUES {','.join(rows[i:i + 10_000])}")


def _wait(job_id: str, limit_s: float) -> tuple[dict, list[tuple]]:
    """Terminal job doc and the distinct heartbeat stamps seen while running."""
    from services.mongodb_service import get_mongodb_service

    mongo = get_mongodb_service()
    deadline = time.monotonic() + limit_s
    beats: list[tuple] = []
    job: dict = {}
    while time.monotonic() < deadline:
        job = mongo.get_job(job_id) or {}
        stamp = (str(job.get("updated_at")), job.get("message"))
        if job.get("status") == "running" and (not beats or beats[-1] != stamp):
            beats.append(stamp)
        if job.get("status") in {"completed", "failed", "cancelled", "partial"}:
            return job, beats
        time.sleep(0.25)
    return job, beats


def _stage_and_confirm(client, args: dict) -> str:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, staged
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r10-d19"})
    assert resp.status_code == 200, resp.text
    return str(resp.json()["job_id"])


def _retry(client, job_id: str) -> str:
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/retry?force=true")
    assert resp.status_code == 200, resp.text
    return str(resp.json()["job_id"])


class _TableLock:
    """Another session holding an exclusive lock on the destination table."""

    def __init__(self, table: str) -> None:
        self.conn = h.mssql_conn()
        self.conn.autocommit = False
        cur = self.conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM [{table}] WITH (TABLOCKX, HOLDLOCK)")
        cur.fetchall()

    def release(self) -> None:
        try:
            self.conn.rollback()
        finally:
            self.conn.close()


def test_a_retry_behind_a_held_lock_ends_named_and_pulses_then_loads(client):
    if not h.port_up(3306) or not h.port_up(1433):
        pytest.skip("MySQL :3306 and SQL Server :1433 required")
    src_cfg, dst_cfg = h.dest_config("mysql"), h.dest_config("sqlserver")
    table = f"r10d19_{uuid.uuid4().hex[:8]}"
    dest = f"{table}_d"
    _seed_mysql(table)
    h.drop_table("sqlserver", dest)
    args = {
        "source_connector_id": _connector(src_cfg, "source"), "source_table": table,
        "dest_connector_id": _connector(dst_cfg, "destination"), "dest_table": dest,
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
    }
    lock = None
    try:
        # Run 0 creates the table the way DataFlow does; run 1 fails mid-way.
        job0, _ = _wait(_stage_and_confirm(client, args), 600)
        assert job0.get("status") == "completed", job0.get("error")
        h.execute(
            "sqlserver",
            f"CREATE TRIGGER [{dest}_t] ON [{dest}] AFTER INSERT AS BEGIN IF EXISTS "
            f"(SELECT 1 FROM inserted WHERE id = 25000) THROW 50001, 'r10-d19: row 25000 refused', 1; END",
        )
        failed_id = _stage_and_confirm(client, args)
        job1, _ = _wait(failed_id, 600)
        assert job1.get("status") == "failed", job1.get("status")
        h.execute("sqlserver", f"DROP TRIGGER [{dest}_t]")

        # The operator retries while another session holds the table.
        lock = _TableLock(dest)
        started = time.monotonic()
        retry_id = _retry(client, failed_id)
        job2, beats = _wait(retry_id, 300)
        waited = time.monotonic() - started
        assert job2.get("status") == "failed", {
            "status": job2.get("status"), "message": job2.get("message"), "waited_s": round(waited),
        }
        error = str(job2.get("error") or job2.get("message") or "").lower()
        assert "lock" in error, error
        # It pulsed while it waited: more than one distinct heartbeat stamp.
        assert len({b[0] for b in beats}) >= 3, beats
        lock.release()
        lock = None

        # Released: the next retry loads every row.
        job3, _ = _wait(_retry(client, retry_id), 600)
        assert job3.get("status") == "completed", job3.get("error")
        assert h.query("sqlserver", f"SELECT COUNT(*) FROM [{dest}]") == [(_N,)]
        assert int(job3.get("records_processed") or 0) == _N
    finally:
        if lock is not None:
            lock.release()
        h.drop_table("sqlserver", dest)
        h.drop_table("mysql", table)
