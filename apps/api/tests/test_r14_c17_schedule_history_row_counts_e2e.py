"""QA C17: a completed schedule run's history carries its row counts.

QA: ``GET /schedules/{id}/history`` showed ``rows_written: null`` on a
completed run. The run record now carries ``rows_read`` / ``rows_written`` /
``rows_quarantined`` from the job's ``row_accounting`` (the ledger Gate-8 and
the job page report from), and older records are read the same way.
Live PostgreSQL -> PostgreSQL.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_completed_run_history_carries_the_job_ledger_counts(client):
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest = f"c17_src_{token}", f"c17_dst_{token}"
    rows = [(1, "Ada"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓")]
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        cur.executemany(f"INSERT INTO {src_table} VALUES (%s, %s)", rows)  # nosec B608
        src = create_connector({"name": f"c17-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"c17-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        resp = client.post("/api/v1/schedules/", json={
            "name": f"c17 {token}", "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest, "interval": "hourly",
            "sync_mode": "full_refresh_overwrite",
            "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name")],
            "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        run = _beat(sid)
        job = run["job"]
        assert job.get("status") == "completed", job.get("error")
        ledger = job.get("row_accounting") or {}

        history = client.get(f"/api/v1/schedules/{sid}/history").json()
        latest = history["runs"][0]
        assert latest["status"] == "completed", latest
        assert latest["rows_written"] == len(rows) == ledger.get("rows_written"), latest
        assert latest["rows_read"] == len(rows) == ledger.get("rows_read"), latest
        assert latest["rows_quarantined"] == 0, latest
        assert latest["records_transferred"] == len(rows)
        last = history["status"]["last_skip"] or {}
        assert "rows_written" not in last or last["rows_written"] is None
        # The schedule's own recent-run brief says the same.
        view = client.get(f"/api/v1/schedules/{sid}").json()
        assert view["last_status"] == "completed"
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest)


def test_a_record_written_before_the_counts_existed_reads_them_from_its_ledger():
    from services.schedule_run_lifecycle import run_view

    legacy = {"job_id": "j1", "status": "completed", "records_transferred": 3,
              "row_accounting": {"rows_read": 4, "rows_written": 3, "rows_quarantined": 1}}
    row = run_view(legacy)
    assert (row["rows_read"], row["rows_written"], row["rows_quarantined"]) == (4, 3, 1)
    unknown = run_view({"job_id": "j2", "status": "failed", "row_accounting": {}})
    assert unknown["rows_written"] is None  # unknown stays unknown, never 0
