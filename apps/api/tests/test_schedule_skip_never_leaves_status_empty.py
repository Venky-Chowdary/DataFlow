"""A beat that starts nothing never leaves the schedule's status empty.

CI, PR 426 api-tests (6/6): ``test_d119_scheduled_pipeline_lifecycle_e2e``
``[drift_guard]`` failed with ``{'last_status': None, 'last_error': None,
'approval': {}}``. The app's own scheduler loop (started by the TestClient
lifespan) fired the schedule the instant the test made it due; the test's beat
was skipped ("A run of this schedule is already in progress") and the skip
was written only as a ``run_history`` row. A schedule whose records are all
skips derived ``last_status`` None, and the stored mirror stayed None: a
refused beat looked like no beat at all (reproduced on origin/feature/Cleanup
with the same race).

Now the first skip of a schedule that never ran sets the stored mirror and the
derived status to ``skipped`` with its reason; a schedule with an earlier run
keeps that run's status (a skip never hides a failure).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def store(tmp_path, monkeypatch):
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    return schedule_store


def _schedule(store, name="skip"):
    return store.create_schedule({
        "name": name, "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": f"orders_{name}", "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })


IN_PROGRESS = "A run of this schedule is already in progress (job j1)."


def test_first_beat_skipped_records_a_status(store):
    from services import schedule_run_lifecycle as life

    sched = _schedule(store)
    life.record_skip(sched.id, "already_leased", IN_PROGRESS)
    after = store.get_schedule(sched.id)
    assert after.last_status == "skipped"
    view = life.derive_status(after)
    assert view["last_status"] == "skipped"
    assert view["last_status_reason"] == IN_PROGRESS
    assert view["last_status_code"] == "already_leased"


def test_attempt_finished_as_skip_records_a_status(store):
    from services import schedule_run_lifecycle as life

    sched = _schedule(store, "attempt")
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    life.finish_run(sched.id, run_id, life.SKIPPED, reason=IN_PROGRESS,
                    reason_code="already_leased")
    after = store.get_schedule(sched.id)
    assert after.last_status == "skipped"
    assert life.derive_status(after)["last_status"] == "skipped"


def test_skip_after_a_failed_run_keeps_the_failure(store):
    from services import schedule_run_lifecycle as life

    sched = _schedule(store, "failed")
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    life.finish_run(sched.id, run_id, "failed", reason="boom", reason_code="start_error")
    life.record_skip(sched.id, "already_leased", IN_PROGRESS)
    after = store.get_schedule(sched.id)
    assert after.last_status == "failed"
    view = life.derive_status(after)
    assert view["last_status"] == "failed" and view["last_skip"]["reason"] == IN_PROGRESS


def test_skip_on_a_pre_ledger_failure_keeps_the_failure(store):
    """Final review M-c: the stored mirror holds a failure the history no longer has."""
    from services import schedule_run_lifecycle as life

    sched = _schedule(store, "legacy")
    life._mutate(sched.id, lambda s: {"last_status": "failed", "run_history": []})
    life.record_skip(sched.id, "approval_pending", "Waiting on an approval")
    after = store.get_schedule(sched.id)
    assert after.last_status == "failed"
    assert life.derive_status(after)["last_status"] == "failed"


def test_a_failure_then_many_alternating_skips_stays_failed(store):
    """Final review M-c: skips that push the failure out of the history window."""
    from services import schedule_run_lifecycle as life

    sched = _schedule(store, "trim")
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    life.finish_run(sched.id, run_id, "failed", reason="boom", reason_code="start_error")
    for i in range(26):
        code = "source_unchanged" if i % 2 else "approval_pending"
        life.record_skip(sched.id, code, f"reason {code}")
    after = store.get_schedule(sched.id)
    assert after.last_status == "failed"
    assert life.derive_status(after)["last_status"] == "failed"


def test_copilot_still_sees_the_failure(store):
    from services import schedule_run_lifecycle as life
    from src.ai.copilot.schedule_questions import _failed_last

    sched = _schedule(store, "copilot")
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    life.finish_run(sched.id, run_id, "failed", reason="boom", reason_code="start_error")
    for i in range(26):
        life.record_skip(sched.id, "approval_pending" if i % 2 else "source_unchanged", f"r{i}")
    row = {**store.get_schedule(sched.id).to_dict(), **life.derive_status(store.get_schedule(sched.id))}
    assert _failed_last(row)
