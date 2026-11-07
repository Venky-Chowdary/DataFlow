"""QA round 10: a failed MySQL -> SQL Server attempt showed 62,528 of 40,000 rows.

Where the rows were counted twice: the MySQL -> SQL Server engine copy reports
every fetch batch it reads to the job's progress meter (8,192 rows a fetch;
four fetches = 32,768 by the time the insert batch holding the refused row
fails). That failure rolls the copy back and declines to the row path, which
re-reads the table from the first row and reports its own cumulative count on
top (29,760 when it failed): 32,768 + 29,760 = 62,528. The earlier fix only
capped the display at ``total_rows``, which hides the double count but still
shows a failed attempt as having processed every row.

The count is now rewound to where the COPY started when the COPY's rows are
rolled back (``services.cancel_check.progress_rewind``), so the row path
counts from the same mark. The cap stays as a safety net.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job on real
MySQL :3306 and SQL Server :1433. Run 0 creates the table the way DataFlow
does; run 1 (overwrite into that table) fails mid-way on a trigger.
"""
from __future__ import annotations

import time
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_N = 12_000
_REFUSED_ID = 9_000


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connector(cfg: dict, role: str) -> str:
    from services.connector_store import create_connector

    return str(create_connector({
        **cfg, "name": f"r10pc-{role}-{uuid.uuid4().hex[:6]}", "role": role,
        "workspace_id": "", "ssl": False,
    }).id)


def _seed_mysql(table: str) -> None:
    h.execute("mysql", f"DROP TABLE IF EXISTS `{table}`")
    h.execute(
        "mysql",
        f"CREATE TABLE `{table}` (id int primary key, name text, note varchar(40)) CHARSET utf8mb4",
    )
    rows = []
    for g in range(1, _N + 1):
        note = "NULL" if g % 4 == 0 else ("''" if g % 4 == 1 else f"'é{g} 山'")
        rows.append(f"({g},'n{g}',{note})")
    for i in range(0, len(rows), 6_000):
        h.execute("mysql", f"INSERT INTO `{table}` VALUES {','.join(rows[i:i + 6_000])}")


def _run(client, args: dict, limit_s: float) -> tuple[dict, int]:
    """Terminal job and the highest records_processed seen while it ran."""
    from services.mongodb_service import get_mongodb_service

    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, staged
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r10-pc"})
    assert resp.status_code == 200, resp.text
    job_id = str(resp.json()["job_id"])
    mongo = get_mongodb_service()
    deadline = time.monotonic() + limit_s
    seen = 0
    job: dict = {}
    while time.monotonic() < deadline:
        job = mongo.get_job(job_id) or {}
        seen = max(seen, int(job.get("records_processed") or 0))
        if job.get("status") in {"completed", "failed", "cancelled", "partial"}:
            return job, seen
        time.sleep(0.2)
    return job, seen


def test_failed_attempt_counts_each_row_once(client):
    if not h.port_up(3306) or not h.port_up(1433):
        pytest.skip("MySQL :3306 and SQL Server :1433 required")
    table = f"r10pc_{uuid.uuid4().hex[:8]}"
    dest = f"{table}_d"
    _seed_mysql(table)
    h.drop_table("sqlserver", dest)
    args = {
        "source_connector_id": _connector(h.dest_config("mysql"), "source"), "source_table": table,
        "dest_connector_id": _connector(h.dest_config("sqlserver"), "destination"), "dest_table": dest,
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
    }
    try:
        job0, seen0 = _run(client, args, 600)
        assert job0.get("status") == "completed", job0.get("error")
        assert int(job0.get("records_processed") or 0) == _N
        assert seen0 <= _N

        h.execute(
            "sqlserver",
            f"CREATE TRIGGER [{dest}_t] ON [{dest}] AFTER INSERT AS BEGIN IF EXISTS "
            f"(SELECT 1 FROM inserted WHERE id = {_REFUSED_ID}) "
            "THROW 50001, 'r10-pc: refused row', 1; END",
        )
        job1, seen1 = _run(client, args, 600)
        assert job1.get("status") == "failed", job1.get("status")
        shown = int(job1.get("records_processed") or 0)
        # Each row is counted once: the copy's rolled-back pass is forgotten,
        # and the row path cannot have processed the refused row's batch or
        # anything after it. (On the base the copy's 12,000 reads plus the
        # row path's own count were capped to 12,000 = every row.)
        assert shown < _REFUSED_ID, {"records_processed": shown, "total": _N}
        assert seen1 <= _N, seen1
    finally:
        h.drop_table("sqlserver", dest)
        h.drop_table("mysql", table)
