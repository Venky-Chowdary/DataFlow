"""Run now on a schedule whose last run is still being recorded: a 409, never a 500.

Run now first records a finished job that still holds the claim (its callback
never ran). When that write keeps losing to other writers the store gives up
with ``ScheduleWriteContended`` and keeps the claim for the beat. That used to
reach HTTP as a 500 (the copilot confirm as a 400) and leave a failed run row
for a Run now that never started (review round 2, L-B).

Every door that starts a manual run answers the same way: 409 with
``Retry-After``, the claim untouched, and no failed run row.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

COMPLETED = {"status": "completed", "phase": "completed", "records_processed": 3}


@pytest.fixture
def held(tmp_path, monkeypatch):
    """A schedule whose finished job still holds the claim and cannot be recorded."""
    import services.request_principal as principal
    import services.schedule_approvals as approvals
    import services.schedule_runner as runner
    import services.schedule_store as store
    from services import schedule_run_lifecycle as life

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "still-recording", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_rec", "interval": "daily",
        "sync_mode": "incremental_upsert", "primary_key": "id",
        "mappings": [{"source": "id", "target": "id"}], "max_retries": 0,
    })
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-a", "job-held") is not None
    life.mark_running(sched.id, run_id, "job-held")

    monkeypatch.setattr(runner, "_job_doc", lambda jid: COMPLETED if jid == "job-held" else None)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(store, "_job_dispatch_state", lambda jid: "terminal")
    monkeypatch.setattr(store, "_job_is_live", lambda jid: False)
    monkeypatch.setattr(principal, "assert_bound_principal_may_use_endpoints",
                        lambda *a, **k: None)
    monkeypatch.setattr(approvals, "release_create_new_dest_exists_false_refuse",
                        lambda *a, **k: None)
    attempts: list[int] = []

    def stale(*_a, **_k):
        # Each attempt finds the schedule rewritten after its decision.
        attempts.append(1)
        raise store._OutcomeStale()

    monkeypatch.setattr(store, "_mark_schedule_run_once", stale)
    return store, sched, attempts


def _client(app) -> TestClient:
    # No lifespan: the app's scheduler beat and startup reclaim would race the
    # Run now under test for the same held claim.
    return TestClient(app)


def _assert_untouched(store, sched, attempts) -> None:
    assert attempts, "Run now never tried to record the finished job"
    after = store.get_schedule(sched.id)
    assert after.running is True and after.running_job_id == "job-held"
    assert after.run_count == 0
    failed = [r for r in after.run_history if r.get("status") == "failed"]
    assert failed == [], failed


def _assert_conflict(resp) -> None:
    assert resp.status_code == 409, resp.text
    assert int(resp.headers.get("Retry-After") or 0) > 0, dict(resp.headers)
    assert "still being recorded" in resp.text


def test_rest_run_now(held):
    store, sched, attempts = held
    from src.main import app

    resp = _client(app).post(f"/api/v1/schedules/{sched.id}/run")

    _assert_conflict(resp)
    _assert_untouched(store, sched, attempts)


def test_copilot_confirmed_run(held):
    store, sched, attempts = held
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.main import app

    ack_id = get_ack_ledger().put(
        kind="run_schedule",
        payload={"schedule_id": sched.id, "name": sched.name},
        preview={"confirm_reason": "Run the schedule now"},
    )
    resp = _client(app).post("/api/v1/copilot/confirm",
                             json={"ack_id": ack_id, "reason": "operator confirmed"})

    _assert_conflict(resp)
    _assert_untouched(store, sched, attempts)


def test_destination_rebuild(held, monkeypatch):
    store, sched, attempts = held
    import services.audit_coverage as audit_coverage
    import services.schedule_recovery as recovery
    from src.main import app

    store.update_schedule(sched.id, {"approval_request": {
        "id": "apr_cont", "status": "open", "code": recovery.DESTINATION_CONTINUITY_CODE,
        "job_id": "job-before", "occurrences": 1, "finding": "the destination changed",
    }})
    monkeypatch.setattr(recovery, "_destination_endpoint", lambda s: object())
    audited: list[dict] = []
    real = audit_coverage.record_security_event

    def capture(action, **kw):
        if action == "schedule.destination.recovered":
            audited.append(kw)
        return real(action, **kw)

    monkeypatch.setattr(audit_coverage, "record_security_event", capture)
    resp = _client(app).post(
        f"/api/v1/schedules/{sched.id}/recover", headers={"X-Actor": "qa@example.com"},
        json={"action": "rebuild", "reason": "rebuild it", "confirm_replace": True},
    )

    _assert_conflict(resp)
    _assert_untouched(store, sched, attempts)
    assert store.get_schedule(sched.id).approval_request["status"] == "open"
    assert [a.get("outcome") for a in audited] == ["failed"], audited
