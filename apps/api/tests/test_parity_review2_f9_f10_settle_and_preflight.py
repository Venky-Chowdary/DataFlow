"""Re-review findings 9 and 10 (MEDIUM): claims settle on real signals; a re-sync keeps preflight.

9. A fix whose re-sync job stopped moving stayed ``applying`` forever (no stale
   rule once ``child_job_id`` was set), while a long ALTER still running in
   this process was failed after an hour — and its real outcome, arriving
   later, was lost (409 "not being applied"). Now the child job is judged by
   the per-step stall watchdog (and an unknown, quiet status), a claim this
   process is applying is never stale, and a late real outcome replaces the
   stale one.
10. The re-sync job set ``skip_preflight=True``; the existing re-run route
    keeps preflight, and so does the re-sync now.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.mongodb_service import MemoryMongoDBService

_RECON = {"passed": False, "source_rows": 100, "target_rows": 99, "culprit_keys": {"missing_in_dest": ["42"]}}


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def _ago(seconds: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _parent(store, fix: dict) -> str:
    job_id = store.create_transfer_job({
        "name": "p", "workspace_id": "w", "reconciliation": _RECON,
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "parity": {"version": 1, "fixes": {fix["id"]: fix}},
    })
    store.update_job_status(job_id, "completed")
    return job_id


def _claim(fix_id: str, **extra) -> dict:
    return {"id": fix_id, "action": "resync_keys", "difference_ids": [], "title": "t", "detail": "",
            "preview": None, "applicable": True, "not_applicable_reason": None, "status": "applying",
            "approved_by": "ops@example.com", "decided_at": _ago(10), "applied_at": None, "result": None,
            "_table": "t", "_keys": ["42"], "_key_column": "id", **extra}


def _child(store, status: str, quiet_seconds: float, phase: str = "load") -> str:
    child = store.create_transfer_job({"name": "c", "workspace_id": "w"})
    store.update_job_status(child, status)
    store._jobs[child].update(phase=phase, started_at=_ago(quiet_seconds), updated_at=_ago(quiet_seconds))
    return child


def _fix(report: dict, fix_id: str) -> dict:
    return next(f for f in report["fixes"] if f["id"] == fix_id)


def test_a_stalled_resync_job_settles_the_fix_as_failed(store):
    from services.parity_apply import settle_pending

    child = _child(store, "running", quiet_seconds=3 * 3600)
    job_id = _parent(store, _claim("f_a", child_job_id=child))
    settled = settle_pending(job_id)
    assert settled is not None
    fix = _fix(settled, "f_a")
    assert fix["status"] == "failed" and "no progress" in fix["result"], fix["result"]


def test_a_resync_job_stuck_in_an_unknown_state_settles_as_failed(store):
    from services.parity_apply import settle_pending

    child = _child(store, "paused", quiet_seconds=2 * 3600)
    job_id = _parent(store, _claim("f_b", child_job_id=child))
    fix = _fix(settle_pending(job_id), "f_b")
    assert fix["status"] == "failed" and "paused" in fix["result"]


def test_a_running_resync_job_that_moves_is_left_alone(store):
    from services.parity_apply import settle_pending

    child = _child(store, "running", quiet_seconds=30)
    job_id = _parent(store, _claim("f_c", child_job_id=child, decided_at=_ago(5 * 3600)))
    assert settle_pending(job_id) is None
    assert store.get_job(job_id)["parity"]["fixes"]["f_c"]["status"] == "applying"


def test_a_claim_this_process_is_applying_is_never_stale(store):
    from services import parity_apply

    job_id = _parent(store, _claim("f_d", action="widen_column", decided_at=_ago(2 * 3600)))
    parity_apply._APPLYING_HERE.add((job_id, "f_d"))
    try:
        assert parity_apply.settle_pending(job_id) is None
    finally:
        parity_apply._APPLYING_HERE.discard((job_id, "f_d"))
    assert store.get_job(job_id)["parity"]["fixes"]["f_d"]["status"] == "applying"


def test_a_late_real_outcome_replaces_the_stale_one(store):
    from services.parity_apply import settle_fix, settle_pending

    job_id = _parent(store, _claim("f_e", action="widen_column", decided_at=_ago(2 * 3600)))
    stale = _fix(settle_pending(job_id), "f_e")
    assert stale["status"] == "failed"
    late = _fix(settle_fix(job_id, "f_e", ok=True, result="Ran: ALTER TABLE t ..."), "f_e")
    assert late["status"] == "applied"
    assert "finished after" in late["result"]


def test_the_resync_job_keeps_preflight(store):
    from services.parity_apply import _resync_request

    job = {"_id": "p", "workspace_id": "w", "transfer_request": {
        "source": {"kind": "database", "format": "postgresql", "table": "o"},
        "destination": {"kind": "database", "format": "postgresql", "table": "o"},
        "sync_mode": "full_refresh_overwrite",
    }}
    req = _resync_request(job, "p", {"id": "f_x", "_keys": ["1", "2"], "_key_column": "id"}, "ops@example.com")
    assert req.skip_preflight is False
    assert req.source_filter == {"column": "id", "op": "in", "value": ["1", "2"]}
