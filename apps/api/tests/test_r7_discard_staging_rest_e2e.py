"""Discard staging through the REST route the web app calls, on real PostgreSQL.

QA (demo videos, production 557f556c): Job Theater "Discard staging" ->
``POST /api/v1/transfer/{job_id}/rollback/execute`` -> 409 "Failed to discard
staging table ... fail closed", and the staging table stays.

Root cause: the route rebuilt the destination from ``job["request"]``, a key
the job store never writes (the run is stored under ``transfer_request``), so
the drop ran against an endpoint with no engine, connector or schema and
returned False every time.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _exists(table: str, schema: str = "public") -> bool:
    return bool(h.query(
        "postgresql",
        "SELECT 1 FROM information_schema.tables "  # nosec B608
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'",
    ))


def _run_staged(client, src_id: str, dst_id: str, src_table: str, dest_table: str) -> dict:
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")]
    resp = client.post("/api/v1/transfer/run", data={
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": src_id, "source_table": src_table,
        "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": dst_id, "dest_table": dest_table,
        "dest_database": h.PG["database"],
        "mappings_json": json.dumps(mappings), "sync_mode": "full_refresh_overwrite",
        "validation_mode": "balanced", "write_via_staging": "true",
        "skip_preflight": "true", "async_mode": "false",
    })
    body = resp.json()
    assert resp.status_code == 200 and body.get("success") is True, body
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().get_job(str(body["job_id"])) or {}


@pytest.mark.parametrize("send_plan", [True, False], ids=["ui_sends_plan", "job_plan"])
def test_discard_staging_drops_the_staging_table(client, send_plan):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"r7s_src_{token}", f"r7s_dst_{token}"
    h.execute("postgresql", f"CREATE TABLE {src_table} (id int PRIMARY KEY, note text)")
    h.execute("postgresql", f"INSERT INTO {src_table} VALUES (1, 'a'), (2, NULL), (3, ''), (4, ' '), (5, 'Łukasz 山田')")
    try:
        src = create_connector({"name": f"r7s-s-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("postgresql")})
        dst = create_connector({"name": f"r7s-d-{token}", "role": "destination", "workspace_id": "",
                                "ssl": False, **h.dest_config("postgresql")})
        job = _run_staged(client, src.id, dst.id, src_table, dest_table)
        plan = (job.get("destination_summary") or {}).get("rollback_plan") or {}
        stage = str(plan.get("staging_table") or "")
        assert plan.get("strategy") == "DISCARD_STAGING" and plan.get("executable") is True, plan
        assert stage and _exists(stage), (stage, plan)
        rows_before = h.query("postgresql", f"SELECT id, note FROM {dest_table} ORDER BY id")
        assert len(rows_before) == 5

        body = {"approved_by": "job-theater-operator",
                "reason": "Operator discarded unused staging after migration review"}
        if send_plan:
            body["plan"] = plan  # JobTheater sends the job's signed plan
        resp = client.post(f"/api/v1/transfer/{job['_id']}/rollback/execute", json=body)
        assert resp.status_code == 200, resp.text
        out = resp.json()
        assert out.get("status") == "executed" and out.get("staging_table") == stage, out
        assert not _exists(stage), f"staging table {stage} still exists"
        # Primary rows untouched (population undo is never claimed).
        assert h.query("postgresql", f"SELECT id, note FROM {dest_table} ORDER BY id") == rows_before

        # The job records the execution, and a second discard is not a 409:
        # the staging table is already gone, which is what was asked for.
        from services.mongodb_service import get_mongodb_service

        rec = get_mongodb_service().get_job(str(job["_id"])) or {}
        assert (rec.get("rollback_execution") or {}).get("status") == "executed", rec.get("rollback_execution")
        again = client.post(f"/api/v1/transfer/{job['_id']}/rollback/execute", json=body)
        assert again.status_code == 200, again.text
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("postgresql", dest_table)
        h.drop_table("postgresql", f"{dest_table}_df_staging")
        h.drop_table("postgresql", f"{dest_table}_df_quarantine")
