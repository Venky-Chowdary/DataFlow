"""Accuracy Track A item 7: Validate verdict vs Execute outcome, per run.

Each terminal job is recorded once with the verdict that predicted it and what
its write did; ``GET /ops/validate-execute`` serves per-workspace counts and
rates; ``dataflow_validate_execute_*_total`` counters reach ``/metrics``. The
record carries no table, column, host, user or message text.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from services import team_store
from services import validate_execute_ledger as ledger
from services.workspace_filter import WorkspaceFilter


@pytest.fixture(autouse=True)
def _own_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger, "_path", lambda: tmp_path / "validate_execute_runs.jsonl")
    monkeypatch.setattr(ledger._LOG, "backend", lambda: None)


def _job(*, pf_passed=True, run_id="", ws="ws-a", recon=None, src="postgresql", dst="mysql"):
    job = {
        "workspace_id": ws,
        "validation_mode": "strict",
        "transfer_request": {
            "source": {"format": src, "host": "db.internal.example", "table": "patients",
                       "username": "alice", "password": "hunter2"},
            "destination": {"format": dst, "table": "patients_copy"},
            "preflight_run_id": run_id,
        },
        "error": "Blocking issues detected: column ssn of patients",
    }
    if pf_passed is not None:
        job["preflight"] = {"passed": pf_passed, "blockers": [{"message": "ssn 123-45-6789"}]}
    if recon is not None:
        job["reconciliation"] = {"passed": recon}
    return job


@pytest.mark.parametrize(
    "validate,execute_pf,status,expected",
    [
        (None, "passed", "completed", ("agreed_pass", None)),
        (None, "passed", "completed_with_quarantine", ("agreed_pass", None)),
        (None, "passed", "failed", ("missed", "write")),
        (None, "blocked", "failed", ("agreed_block", "preflight")),
        ("passed", "blocked", "failed", ("drift", "preflight")),
        ("passed", "passed", "failed", ("missed", "write")),
        ("blocked", "passed", "completed", ("over_block", None)),
        ("blocked", "blocked", "failed", ("agreed_block", "preflight")),
        (None, "skipped", "completed", ("unvalidated", None)),
        (None, "passed", "cancelled", ("cancelled", None)),
    ],
)
def test_classify(validate, execute_pf, status, expected) -> None:
    assert ledger.classify(validate, execute_pf, status) == expected


def test_a_run_is_recorded_once_without_names_or_messages() -> None:
    first = ledger.record_run_outcome("job-1", "completed", _job(recon=True))
    assert first["outcome"] == "agreed_pass" and first["verification"] == "passed"
    assert ledger.record_run_outcome("job-1", "failed", _job(pf_passed=False)) is None
    rows = ledger.list_records()
    assert [r["job_id"] for r in rows] == ["job-1"]
    text = json.dumps(rows)
    for secret in ("patients", "db.internal.example", "alice", "hunter2", "ssn", "123-45-6789",
                   "Blocking issues"):
        assert secret not in text
    assert set(rows[0]) == {
        "job_id", "workspace_id", "recorded_at", "source_type", "destination_type", "validation_mode",
        "validate_run_id", "validate_verdict", "execute_preflight", "execute_status", "failed_at",
        "verification", "outcome",
    }
    assert rows[0]["source_type"] == "postgresql" and rows[0]["destination_type"] == "mysql"


def test_a_linked_validate_run_is_the_predicting_verdict() -> None:
    from services.preflight_run_store import save_preflight_run

    run = save_preflight_run({"passed": True, "gates": [], "blockers": []}, validation_mode="strict")
    rec = ledger.record_run_outcome("job-2", "failed", _job(pf_passed=False, run_id=run["run_id"]))
    assert rec["validate_verdict"] == "passed" and rec["execute_preflight"] == "blocked"
    assert rec["outcome"] == "drift" and rec["failed_at"] == "preflight"


def test_aggregate_rates_and_workspace_scope() -> None:
    for i, (status, pf) in enumerate([("completed", True)] * 7 + [("failed", True)] + [("failed", False)] * 2):
        ledger.record_run_outcome(f"a-{i}", status, _job(pf_passed=pf, ws="ws-a"))
    ledger.record_run_outcome("b-0", "failed", _job(pf_passed=True, ws="ws-b", src="oracle"))
    agg = ledger.aggregate(WorkspaceFilter.only("ws-a"))
    assert agg["runs"] == 10
    assert agg["by_outcome"]["agreed_pass"] == 7 and agg["by_outcome"]["missed"] == 1
    assert agg["by_outcome"]["agreed_block"] == 2
    assert agg["validate_passed_runs"] == 8
    assert agg["validate_predicts_execute_rate"] == 0.875 and agg["miss_rate"] == 0.125
    lo, hi = agg["miss_rate_95ci"]
    assert 0 < lo < 0.125 < hi < 0.6
    assert [r["source_type"] for r in agg["by_route"]] == ["postgresql"]
    assert "b-0" not in json.dumps(agg) and "a-0" not in json.dumps(agg)
    assert ledger.aggregate(WorkspaceFilter.only("ws-b"))["by_outcome"]["missed"] == 1
    assert ledger.aggregate(WorkspaceFilter.nothing())["runs"] == 0


def test_terminal_job_status_write_records_the_run(monkeypatch) -> None:
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    job_id = store.create_transfer_job({"name": "t", "workspace_id": "ws-t", "transfer_request": {
        "source": {"format": "postgresql"}, "destination": {"format": "mysql"}}})
    store.update_job_status(job_id, "running", phase="preflight", preflight={"passed": True})
    assert ledger.list_records() == []
    store.update_job_status(job_id, "failed", error="write failed")
    rows = ledger.list_records()
    assert [(r["job_id"], r["outcome"], r["failed_at"]) for r in rows] == [(job_id, "missed", "write")]


def test_counters_reach_the_metrics_exposition() -> None:
    from services.ops_metrics import prometheus_text

    ledger.record_run_outcome("job-m", "completed", _job())
    text = prometheus_text(deployment_wide=True)
    assert "dataflow_validate_execute_runs_total" in text
    assert "dataflow_validate_execute_agreed_pass_total" in text


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    from src.main import app

    return TestClient(app)


def test_http_aggregate_is_workspace_scoped(client) -> None:
    ws_a = team_store.create_workspace(name="Alpha", created_by="anonymous").id
    ws_b = team_store.create_workspace(name="Beta", created_by="anonymous").id
    ledger.record_run_outcome("job-a", "completed", _job(ws=ws_a))
    ledger.record_run_outcome("job-b", "failed", _job(ws=ws_b))
    a = client.get("/api/v1/ops/validate-execute", headers={"X-Workspace-Id": ws_a})
    assert a.status_code == 200, a.text
    assert a.json()["runs"] == 1 and a.json()["by_outcome"]["agreed_pass"] == 1
    assert "job-a" not in a.text and "job-b" not in a.text
    b = client.get("/api/v1/ops/validate-execute", headers={"X-Workspace-Id": ws_b}).json()
    assert b["by_outcome"]["missed"] == 1 and b["miss_rate"] == 1.0
