"""Review finding 11: explaining writes, so it is permissioned and audited; the
model never sees who accepted what; a read persists no decision state.

Before: explain was job.read and wrote the stored explanation with no audit;
the accepted cause's detail ("Accepted by owner@…: <note>") went to the model in
grounded_facts and in the Pilot tool's output; a GET persisted the keys a re-read
proved equal (``resolved``) without audit.
"""

from __future__ import annotations

import json
import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService

APPROVER = "owner-secret@example.com"
NOTE = "ticket FIN-4411 says this rounding is fine"


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def _accepted_job(svc) -> str:
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    job_id = svc.create_transfer_job({
        "_id": "exp" + uuid.uuid4().hex[:10], "workspace_id": "ws", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
                           "sample_compare": {"rows_compared": 100, "mismatches": [
                               {"key": "7", "source": "amount", "target": "amount", "source_value": "7.1249",
                                "target_value": "7.12"}]}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    report = build_parity_report(svc.get_job(job_id), reread=False, job_id=job_id)
    accept = next(f for f in report["fixes"] if f["action"] == "accept")
    approve_fix(job_id, accept["id"], actor=APPROVER, note=NOTE)
    return job_id


def test_explain_needs_job_run():
    from services.rbac import Permission, _required_permission

    assert _required_permission("POST", "/api/v1/jobs/j1/parity-report/explain") == Permission.JOB_RUN


def test_explaining_is_audited(store):
    from services.audit_log import list_audit_events
    from services.parity_explain import explain_job

    job_id = _accepted_job(store)
    explain_job(job_id, actor="eng@example.com")
    events = [e for e in list_audit_events(limit=50) if e.get("action") == "job.parity.explained"]
    assert events and events[0]["actor"] == "eng@example.com"


def test_the_model_never_sees_the_approver_or_their_note(store):
    from services.parity_explain import grounded_facts
    from services.parity_report import build_parity_report

    job_id = _accepted_job(store)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert any(APPROVER in d["cause"]["detail"] for d in report["differences"]), "the report itself names them"
    for include_values in (False, True):
        facts = json.dumps(grounded_facts(report, include_values=include_values))
        assert APPROVER not in facts and "FIN-4411" not in facts
        assert "accepted_by_operator" in facts


def test_the_pilot_tool_output_never_names_the_approver(store):
    from src.ai.copilot.parity_tools import explain_parity

    job_id = _accepted_job(store)
    out = json.dumps(explain_parity(job_id=job_id).output, default=str)
    assert APPROVER not in out and "FIN-4411" not in out


def test_a_read_persists_no_resolved_keys(store):
    from services.parity_report import build_parity_report
    from services.parity_summary import remember_report

    job_id = _accepted_job(store)
    job = store.get_job(job_id)
    report = build_parity_report(job, reread=False, job_id=job_id)
    report["_tables_ev"][0].resolved_now = ["7"]  # what a re-read would have proved equal
    before = store.get_job(job_id)["parity"]
    remember_report(job_id, job, report)
    after = store.get_job(job_id)["parity"]
    assert after.get("resolved") == before.get("resolved")
    assert after["version"] == before["version"]
