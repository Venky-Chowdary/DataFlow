"""Re-review finding 13(b) (LOW-MEDIUM): no row value reaches a fix result.

A child job's or a database's error text went verbatim into the fix result —
and from there into the report, the signed pack, the Markdown export and the
audit chain. Database errors quote row values (PostgreSQL ``Key (email)=(…)``,
``Failing row contains (…)``, MySQL ``Duplicate entry '…'``), bypassing the
rule that protected columns never show a value. Results are sanitized.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService

SECRET = "alice@example.com"
PG_ERROR = (f'duplicate key value violates unique constraint "u_email"\nDETAIL:  Key (email)=({SECRET}) '
            "already exists.")


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


@pytest.mark.parametrize("text", [
    PG_ERROR,
    f"new row for relation \"t\" violates check constraint\nFailing row contains (1, {SECRET}, 2).",
    f"(1062, \"Duplicate entry '{SECRET}' for key 'u_email'\")",
    f'invalid input syntax for type integer: "{SECRET}"',
    f"Violation of UNIQUE KEY constraint. The duplicate key value is ({SECRET}).",
])
def test_database_errors_lose_their_row_values(text):
    from services.parity_errors import safe_error

    assert SECRET not in safe_error(text)


def test_a_failed_child_job_error_is_sanitized_in_the_fix_and_the_audit(store, tmp_path):
    from services import audit_log
    from services.parity_apply import settle_pending

    child = store.create_transfer_job({"name": "c", "workspace_id": "w"})
    store.update_job_status(child, "failed", error=PG_ERROR)
    job_id = store.create_transfer_job({
        "name": "p", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 9,
                           "culprit_keys": {"missing_in_dest": ["4"]}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "parity": {"version": 1, "fixes": {"f_r": {
            "id": "f_r", "action": "resync_keys", "status": "applying", "approved_by": "ops@example.com",
            "difference_ids": [], "title": "t", "child_job_id": child, "_keys": ["4"], "_table": "t",
            "_key_column": "id", "decided_at": "2026-01-01T00:00:00+00:00",
        }}},
    })
    store.update_job_status(job_id, "completed")
    report = settle_pending(job_id)
    fix = next(f for f in report["fixes"] if f["id"] == "f_r")
    assert fix["status"] == "failed"
    assert SECRET not in str(fix["result"])
    assert SECRET not in audit_log.STORE_PATH.read_text()


def test_a_failed_rerun_start_is_sanitized(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    job_id = store.create_transfer_job({
        "name": "o", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 97, "culprit_census_clean": True},
        "transfer_request": {"source": {"kind": "database", "format": "postgresql", "table": "o"},
                             "destination": {"kind": "database", "format": "postgresql", "table": "o"},
                             "sync_mode": "full_refresh_overwrite"},
        "destination_summary": {"table": "o", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    })
    store.update_job_status(job_id, "completed")
    rerun = next(f for f in build_parity_report(store.get_job(job_id), job_id=job_id)["fixes"]
                 if f["action"] == "rerun")

    def broken() -> str:
        raise RuntimeError(PG_ERROR)

    report = approve_fix(job_id, rerun["id"], actor="ops@example.com", note="go", start_rerun=broken)
    fix = next(f for f in report["fixes"] if f["id"] == rerun["id"])
    assert fix["status"] == "failed" and SECRET not in str(fix["result"])
