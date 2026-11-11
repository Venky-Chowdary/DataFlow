"""Re-review 3 (LOW): a late fix outcome never rewrites a signed report.

``settle_fix`` did not check ``not_signed``: a fix settled as stale, then the
report signed, then the real outcome arriving rewrote the signed state. It is
now recorded as a post-signing audit event and the signed state is untouched.
"""

from __future__ import annotations

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
    monkeypatch.delenv("DATAFLOW_PARITY_SIGNOFF_AUTHORITY", raising=False)
    return svc


def test_a_late_outcome_after_signing_is_an_event_not_a_change(store):
    from services.audit_log import list_audit_events
    from services.parity_apply import settle_fix
    from services.parity_signoff import sign_off

    job_id = store.create_transfer_job({
        "name": "clean", "workspace_id": "w",
        "reconciliation": {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "parity": {"version": 1, "fixes": {"f_w": {
            "id": "f_w", "action": "widen_column", "status": "failed", "_stale": True, "difference_ids": [],
            "title": "Widen", "approved_by": "dba@x", "decided_at": "2026-01-01T00:00:00+00:00",
            "applied_at": "2026-01-01T01:00:00+00:00", "result": "never finished"}}},
    })
    store.update_job_status(job_id, "completed")
    for role, user in (("engineering", "e@x"), ("data_owner", "d@x"), ("compliance", "c@x")):
        sign_off(job_id, role=role, actor=user, signer_role="admin")
    before = store.get_job(job_id)["parity"]
    assert before["signed"]

    settle_fix(job_id, "f_w", ok=True, result="Ran: ALTER TABLE t ...")
    after = store.get_job(job_id)["parity"]
    assert after["version"] == before["version"]
    assert after["fixes"]["f_w"]["status"] == "failed"
    assert after["signed"] == before["signed"]
    assert "job.parity.fix_settled_after_signing" in [e.get("action") for e in list_audit_events(limit=50)]
