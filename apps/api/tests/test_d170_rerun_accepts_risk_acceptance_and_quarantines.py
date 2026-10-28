"""D170 — rerun after the source changed can quarantine instead of refusing.

QA: SFTP CSV -> PostgreSQL succeeded; a bad row was added to the file (qty
``NOTANUMBER``, d ``2024-13-45``) and ``rerun_job`` failed with "Sample
transform / cast failures: 2 column(s) (qty, d)", 0 rows and no way forward:
rerun accepted only ``force`` and the error offered no quarantine path.

* REST ``POST /jobs/{id}/rerun`` and the MCP ``rerun_job`` tool take an optional
  ``risk_acceptance`` that signs a continue-policy Risk Contract on the replayed
  mappings, so the failing rows are held out and the valid rows load.
* The cast-failure job error names that remediation.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

ACCEPTANCE = {
    "approved_by": "ops@example.com",
    "reason": "source file now carries a few bad rows; hold them out",
    "execution_policy": "CAST_AND_CONTINUE",
    "on_cast_failure": "quarantine",
    "columns": ["qty", "d"],
}


def _job() -> dict:
    return {
        "job_id": "job-d170",
        "status": "completed",
        "records_processed": 30,
        "transfer_request": {
            "source": {"kind": "database", "type": "sqlite", "table": "src"},
            "destination": {"kind": "database", "type": "sqlite", "table": "dst"},
            "sync_mode": "full_refresh_overwrite",
            "mappings": [
                {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
                {"source": "qty", "target": "qty", "source_type": "TEXT", "target_type": "INTEGER"},
                {"source": "d", "target": "d", "source_type": "TEXT", "target_type": "DATE"},
            ],
        },
    }


def _post(monkeypatch, body: dict | None, job: dict | None = None):
    from services.mongodb_service import get_mongodb_service
    from src.main import app

    mongo = get_mongodb_service()
    job = job or _job()
    captured: dict = {}
    created: dict = {}
    monkeypatch.setattr(mongo, "get_job", lambda jid: job if jid == job["job_id"] else None)
    monkeypatch.setattr(
        mongo,
        "update_job_status",
        lambda jid, status, **kw: created.update({"job_id": jid, "status": status, **kw}) or True,
    )
    import src.transfer.engine as engine_mod

    class _Engine:
        def _create_pending_job(self, req):
            captured["req"] = req
            return "new-job"

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr("src.transfer.background.run_transfer_async", lambda *a, **k: None)
    with TestClient(app) as client:
        if body is None:
            response = client.post("/api/v1/connectors/jobs/job-d170/rerun")
        else:
            response = client.post("/api/v1/connectors/jobs/job-d170/rerun", json=body)
    return response, created, captured


def test_rerun_accepts_risk_acceptance_and_signs_a_quarantine_contract(monkeypatch):
    response, created, captured = _post(monkeypatch, {"risk_acceptance": ACCEPTANCE})
    assert response.status_code == 200, response.text
    contracts = {
        m["source"]: m.get("risk_contract") for m in captured["req"].mappings
    }
    assert contracts["id"] is None
    for column in ("qty", "d"):
        contract = contracts[column]
        assert contract, f"{column} has no contract"
        assert contract["execution_policy"] == "CAST_AND_CONTINUE"
        assert contract["quarantine_policy"] == "holdout_rejected_rows"
        assert contract["approved_by"] == "ops@example.com"
    assert created["confirmation"]["risk_accepted_columns"] == ["qty", "d"]
    assert captured["req"].skip_preflight is False


def test_rerun_without_acceptance_is_unchanged(monkeypatch):
    response, _, captured = _post(monkeypatch, None)
    assert response.status_code == 200, response.text
    assert not any(m.get("risk_contract") for m in captured["req"].mappings)


def test_rerun_acceptance_that_signs_nothing_is_refused(monkeypatch):
    response, _, captured = _post(
        monkeypatch,
        {"risk_acceptance": {**ACCEPTANCE, "columns": ["nope"]}},
    )
    assert response.status_code == 400
    assert "nothing" in response.text.lower()
    assert "req" not in captured


def test_rerun_partial_acceptance_is_refused(monkeypatch):
    response, _, captured = _post(
        monkeypatch, {"risk_acceptance": {"execution_policy": "CAST_AND_CONTINUE"}}
    )
    assert response.status_code == 400
    assert "approved_by" in response.text
    assert "req" not in captured


def test_mcp_rerun_job_stages_the_acceptance(monkeypatch):
    from src.ai.copilot import lifecycle_tools

    monkeypatch.setattr(
        lifecycle_tools,
        "resolve_job",
        lambda job_id, selector: ({**_job(), "job_id": "job-d170"}, ""),
    )
    from src.ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools().execute(
        "rerun_job", {"job_id": "job-d170", "risk_acceptance": ACCEPTANCE}
    )
    assert result.success, result.error
    assert result.output["risk_acceptance"]["execution_policy"] == "CAST_AND_CONTINUE"
    assert "quarantine" in result.output["label"].lower()

    bad = get_pilot_tools().execute(
        "rerun_job", {"job_id": "job-d170", "risk_acceptance": "yes"}
    )
    assert not bad.success


def test_confirmed_rerun_hands_the_acceptance_to_the_route(monkeypatch):
    import importlib

    seen: dict = {}

    async def fake_rerun(job_id, background_tasks, request, force=False, body=None):
        seen["body"] = body
        return {"success": True}

    router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router, "rerun_transfer_job", fake_rerun)
    copilot_router = importlib.import_module("src.routers.copilot_router")

    import asyncio

    class _Req:
        headers: dict = {}

    asyncio.run(
        copilot_router._run_lifecycle_confirm(
            "rerun_job",
            {"job_id": "job-d170", "risk_acceptance": ACCEPTANCE},
            _Req(),
            None,
        )
    )
    assert seen["body"].risk_acceptance == ACCEPTANCE


def _cast_failing_preflight() -> dict:
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["amt"],
        column_types={"amt": "INTEGER"},
        row_count=2,
        mappings=[
            {
                "source": "amt",
                "target": "amt",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "INTEGER",
            }
        ],
        destination_connected=True,
        sample_rows=[{"amt": "NOTANUMBER"}, {"amt": "10"}],
        estimated_bytes=64,
        destination_column_types={"amt": "INTEGER"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        source_kind="database",
        skip_population_fit=True,
    )


def test_cast_failure_job_error_names_the_quarantine_replan():
    from services.mongodb_service import MemoryMongoDBService
    from services.root_cause_engine import CAST_FAILURE_ROOT_TITLE
    from src.transfer import job_failure

    pf = _cast_failing_preflight()
    assert any(
        (b.get("details") or {}).get("title") == CAST_FAILURE_ROOT_TITLE
        for b in pf["blockers"]
    ), [b.get("message") for b in pf["blockers"]]

    import services.lineage_telemetry as lineage

    store = MemoryMongoDBService()
    store.update_job_status("j-d170", "running", transfer_request={})
    message, details = job_failure._fail_job_preflight(
        store, "j-d170", pf, lineage=lineage, rows_read=3, sync_mode="full_refresh_overwrite"
    )
    assert "risk_acceptance" in message and "quarantine" in message.lower()
    assert "CAST_AND_CONTINUE" in message
    assert details["remediation"] in message
    assert "risk_acceptance" in store.get_job("j-d170")["error"]
