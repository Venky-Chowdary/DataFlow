"""Review finding 12: the dashboard summary must follow every decision.

The compact summary the Migration health dashboard reads was written only by a
GET of the report. Approving, accepting, dismissing or signing left it stale:
a job whose differences were all accepted still showed "unexplained" until
someone happened to open its report again. The summary is now recomputed in
the same compare-and-swap write as the decision.
"""

from __future__ import annotations

import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def _job(svc) -> str:
    job_id = svc.create_transfer_job({
        "_id": "sum" + uuid.uuid4().hex[:10], "name": "payments", "workspace_id": "ws", "status": "completed",
        "reconciliation": {
            "passed": False, "source_rows": 100, "target_rows": 99,
            "sample_compare": {"rows_compared": 100, "mismatches": [
                {"key": "5", "source": "amount", "target": "amount", "source_value": "10.1049",
                 "target_value": "10.10"}]},
            "culprit_keys": {"missing_in_dest": ["42"]},
        },
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def _dashboard_verdict(svc, job_id: str) -> tuple[str, bool]:
    from services.parity_summary import build_summary

    svc.list_jobs = lambda limit=30, workspace_filter=None, **_k: [{"_id": job_id}]
    run = build_summary(None, limit=5)["runs"][0]
    stored = svc.get_job(job_id)["parity"]["summary"]
    return run["verdict"], bool(stored and stored.get("signed"))


def test_accepting_and_signing_refresh_the_dashboard(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.parity_signoff import sign_off
    from services.parity_summary import remember_report

    job_id = _job(store)
    job = store.get_job(job_id)
    report = build_parity_report(job, reread=False, job_id=job_id)
    remember_report(job_id, job, report)  # someone opened the report once
    assert _dashboard_verdict(store, job_id) == ("unexplained", False)

    for fix in [f for f in report["fixes"] if f["action"] == "accept"]:
        approve_fix(job_id, fix["id"], actor="owner@example.com", note="known rounding")
    assert _dashboard_verdict(store, job_id) == ("explained", False)

    for role, who in (("engineering", "e@x.io"), ("data_owner", "d@x.io"), ("compliance", "c@x.io")):
        sign_off(job_id, role=role, actor=who, **_signer(role))
    assert _dashboard_verdict(store, job_id) == ("explained", True)


def _signer(role: str) -> dict:
    import inspect

    from services.parity_signoff import sign_off

    # Sign-off binds roles to authority (review finding 5): sign as a workspace admin.
    return {"signer_role": "admin"} if "signer_role" in inspect.signature(sign_off).parameters else {}
