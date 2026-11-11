"""Re-review 3 (LOW): an approve runs only what the person reviewed.

The widen preview was not bound to the approval: the DDL could change
between the review and the apply (another ALTER, a different source type)
and the approve ran whatever was needed then. A fix now carries
``preview_sha256``; an approve that names another digest is a 409, and a
widen re-checks its DDL right before running it — a different DDL runs
nothing, releases the claim and answers 409.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def widen_job(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log, parity_cache, parity_fixes

    parity_cache.clear()
    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    monkeypatch.setattr(parity_fixes, "_live_types", lambda job, ev, columns, report=None: {
        "source": {"code": "VARCHAR(20)"}, "dest": {"code": "varchar(3)"}, "dialect": "mysql"})
    plan = {"ddl": "ALTER TABLE `orders` MODIFY COLUMN `code` VARCHAR(20) NOT NULL", "ran": []}

    def statements(job, table, column, new_type, *, dry_run):
        if not dry_run:
            plan["ran"].append(plan["ddl"])
        return [plan["ddl"]]

    monkeypatch.setattr(parity_fixes, "widen_statements", statements)
    job_id = svc.create_transfer_job({
        "name": "o", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 10,
                           "sample_compare": {"rows_compared": 10, "mismatches": [
                               {"key": "1", "target": "code", "source_value": "abcdef", "target_value": "abc"}]}},
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"source": {"kind": "database", "format": "mysql", "table": "o"},
                             "destination": {"kind": "database", "format": "mysql", "table": "orders"},
                             "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    yield svc, job_id, plan
    parity_cache.clear()


def _widen(svc, job_id):
    from services.parity_report import build_parity_report

    return next(f for f in build_parity_report(svc.get_job(job_id), job_id=job_id)["fixes"]
                if f["action"] == "widen_column")


def test_a_fix_carries_the_digest_of_its_preview(widen_job):
    import hashlib

    svc, job_id, plan = widen_job
    fix = _widen(svc, job_id)
    assert fix["preview_sha256"] == hashlib.sha256(fix["preview"].encode()).hexdigest()


def test_an_approve_of_another_preview_is_refused(widen_job):
    from services.parity_fixes import ParityConflict, approve_fix

    svc, job_id, plan = widen_job
    fix = _widen(svc, job_id)
    with pytest.raises(ParityConflict, match="changed since you reviewed it"):
        approve_fix(job_id, fix["id"], actor="dba@x", note="go", preview_sha256="0" * 64)
    assert plan["ran"] == []
    assert fix["id"] not in (svc.get_job(job_id).get("parity") or {}).get("fixes", {})


def test_a_widen_whose_ddl_changed_before_it_ran_runs_nothing(widen_job, monkeypatch):
    from services import parity_apply
    from services.parity_fixes import ParityConflict, approve_fix

    svc, job_id, plan = widen_job
    fix = _widen(svc, job_id)
    # Right after the claim is written, the destination catalog changes.
    monkeypatch.setattr(parity_apply, "update_state", _after_claim(parity_apply.update_state, plan))
    with pytest.raises(ParityConflict, match="differs from the DDL you approved"):
        approve_fix(job_id, fix["id"], actor="dba@x", note="go", preview_sha256=fix["preview_sha256"])
    assert plan["ran"] == []
    assert fix["id"] not in (svc.get_job(job_id).get("parity") or {}).get("fixes", {}), "the claim was released"


def _after_claim(update_state, plan):
    calls = {"n": 0}

    def wrapped(*a, **k):
        out = update_state(*a, **k)
        calls["n"] += 1
        if calls["n"] == 1:  # the claim is written: now the destination catalog changes
            plan["ddl"] = "ALTER TABLE `orders` MODIFY COLUMN `code` VARCHAR(40) NOT NULL"
        return out

    return wrapped
