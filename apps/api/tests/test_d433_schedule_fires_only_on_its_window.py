"""D433: a schedule starts runs only on its cron window, a declared retry, or Run now.

QA (MySQL -> Oracle and MySQL -> PG incremental_upsert, yearly / weekly cron,
max_retries 0, three occurrences): seconds after a manual run completed, the
scheduler started a run by itself — trigger ``cadence``, attempt 0 — although
the next cron window was a year away and retries were off. The operator's own
Run now was then refused 409 while the unrequested run re-read the source.

Root causes, each proven here on the store and the runner as they run:

1. The beat decides "due" on a snapshot and dispatches outside its fleet lock;
   neither ``_run_schedule`` nor the claim checked again. A Run now that
   finished in between (its record moves the slot to the next window and
   clears any retry) was ignored, and the stale decision started a run. A
   retry cancelled that way was even relabelled ``cadence``, attempt 0.
2. A park the scheduler released by itself (``system:`` actor) re-armed the
   schedule to *now*: an unrequested run seconds after the release.
3. An enabled schedule with no stored slot was "due now".
4. A retry still owed when the schedule was paused fired at once on resume.

And what an operator reads (AC-3/AC-4): every unattended run records why it
started (``fire_reason``) and the window it serves (``scheduled_for``); a
refused Run now names the run holding the claim and how it started; a status
that is bookkeeping ("approved") never turns the next run into a full re-read.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import services.schedule_runner as runner
import services.schedule_store as store
from services import schedule_run_lifecycle as life

YEARLY = "0 3 1 1 *"


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield


@pytest.fixture
def dispatched(monkeypatch):
    """Stand-in for the transfer: records each dispatch and binds a job like the real one."""
    calls: list[dict] = []

    def _fake_dispatch(schedule_id, attempt=0, **kw):
        job_id = f"job-{len(calls) + 1}"
        calls.append({"schedule_id": schedule_id, "attempt": attempt, "job_id": job_id})
        store.set_running_job(schedule_id, job_id)
        life.mark_running(schedule_id, life.current_run_id(schedule_id), job_id)
        return job_id

    monkeypatch.setattr(runner, "_dispatch_transfer", _fake_dispatch)
    monkeypatch.setattr(
        "services.request_principal.assert_bound_principal_may_use_endpoints",
        lambda *a, **k: None,
    )
    return calls


def _schedule(**extra) -> store.PipelineSchedule:
    return store.create_schedule({
        "name": "D433 yearly upsert",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "interval": "cron",
        "cron": YEARLY,
        "timezone": "UTC",
        "sync_mode": "incremental_upsert",
        "primary_key": "id",
        "cursor_column": "updated_at",
        "max_retries": 0,
        "mappings": [{"source": c, "target": c} for c in ("id", "updated_at")],
        **extra,
    })


def _ago(minutes: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _record_manual_success(schedule_id: str, job_id: str = "manual-job") -> store.PipelineSchedule:
    """What a finished Run now writes (``_finalize_run`` -> ``mark_schedule_run``)."""
    updated = store.mark_schedule_run(
        schedule_id, job_id, status="completed",
        run_entry={"job_id": job_id, "status": "completed", "attempt": 0, "trigger": "manual"},
    )
    assert updated is not None
    return updated


def test_a_beat_decided_before_a_manual_run_finished_starts_nothing(temp_store, dispatched):
    sched = _schedule()
    store.update_schedule(sched.id, {"next_run_at": _ago(1)})
    assert sched.id in {s.id for s in store.due_schedules()}  # the beat's snapshot

    # Meanwhile the operator's Run now finishes: its record moves the slot to
    # the next cron window (a year away).
    after = _record_manual_success(sched.id)
    assert store._parse_ts(after.next_run_at) > datetime.now(timezone.utc) + timedelta(days=1)

    # The beat now dispatches the schedule it judged due earlier.
    assert runner._run_schedule(sched.id) is None
    assert dispatched == [], "the scheduler started a run outside its cron window"
    assert not store.get_schedule(sched.id).running


def test_the_claim_itself_refuses_an_unattended_run_that_is_not_owed(temp_store):
    sched = _schedule()
    # A slot a year away: an unattended claim is refused, a Run now is not.
    assert store.mark_schedule_running(sched.id, "beat", unattended=True) is None
    assert store.last_claim_refusal(sched.id)["kind"] == "not_due"
    assert store.mark_schedule_running(sched.id, "run-now") is not None


def test_a_pending_retry_is_cancelled_by_a_manual_success(temp_store, dispatched):
    sched = _schedule(max_retries=2)
    store.update_schedule(sched.id, {"retry_at": _ago(1), "retry_attempt": 1})
    assert sched.id in {s.id for s in store.due_schedules()}

    _record_manual_success(sched.id)
    assert not store.get_schedule(sched.id).retry_at

    assert runner._run_schedule(sched.id) is None
    assert dispatched == [], "a cancelled retry ran anyway, relabelled as a cadence run"


def test_a_due_retry_runs_labelled_retry_with_its_attempt(temp_store, dispatched):
    sched = _schedule(max_retries=2)
    retry_at = _ago(1)
    store.update_schedule(sched.id, {"retry_at": retry_at, "retry_attempt": 1})

    assert runner._run_schedule(sched.id) == "job-1"
    assert dispatched[0]["attempt"] == 1
    run = store.get_schedule(sched.id).run_history[-1]
    assert run["trigger"] == "retry" and run["attempt"] == 1, run
    assert run["fire_reason"] == "retry" and run["scheduled_for"] == retry_at, run


def test_a_cron_window_run_records_its_reason_and_window(temp_store, dispatched):
    sched = _schedule()
    slot = _ago(1)
    store.update_schedule(sched.id, {"next_run_at": slot})

    assert runner._run_schedule(sched.id) == "job-1"
    run = store.get_schedule(sched.id).run_history[-1]
    assert run["trigger"] == "cadence" and run["attempt"] == 0, run
    assert run["fire_reason"] == "cron_window" and run["scheduled_for"] == slot, run
    # The history the UI and MCP read carries both fields.
    view = life.run_view(run)
    assert view["fire_reason"] == "cron_window" and view["scheduled_for"] == slot


def test_a_long_overdue_slot_is_recorded_as_a_missed_window_catch_up(temp_store, dispatched):
    sched = _schedule(cron="0 3 * * *")
    store.update_schedule(sched.id, {"next_run_at": _ago(3 * 24 * 60)})

    assert runner._run_schedule(sched.id) == "job-1"
    run = store.get_schedule(sched.id).run_history[-1]
    assert run["fire_reason"] == "missed_window_catch_up", run


def test_an_unslotted_enabled_schedule_is_armed_not_fired(temp_store):
    sched = _schedule()
    store.update_schedule(sched.id, {"next_run_at": None})
    assert store.get_schedule(sched.id).next_run_at is None

    assert store.due_schedules() == []
    armed = store._parse_ts(store.get_schedule(sched.id).next_run_at)
    assert armed is not None and armed > datetime.now(timezone.utc) + timedelta(days=1)


def test_a_park_the_scheduler_releases_by_itself_keeps_the_cron_window(temp_store, monkeypatch):
    """``system:`` releases (dest-exists after create-new, same-declaration
    narrow, a write that succeeded) re-armed the schedule to *now*."""
    import services.schedule_approvals as approvals

    sched = _schedule()
    slot = sched.next_run_at
    store.update_schedule(sched.id, {
        "last_status": "needs_approval",
        "approval_request": {
            "id": "apr-da", "status": "open", "approvable": False, "code": "RUN_REFUSED",
            "finding": "Decision Artifact diverged from current Map (DDL identity).",
        },
    })
    monkeypatch.setattr(approvals, "create_new_stamp_matches_schedule", lambda *a, **k: True)

    assert approvals.release_create_new_dest_exists_false_refuse() == 1
    released = store.get_schedule(sched.id)
    assert not store.has_open_approval(released)
    assert released.next_run_at == slot, "a system release re-armed the schedule to now"
    assert store.due_schedules() == []


def test_an_operator_plan_change_still_reruns_now_and_says_why(temp_store, dispatched):
    from services.schedule_approvals import resolve_plan_change

    sched = _schedule()
    store.update_schedule(sched.id, {
        "approval_request": {
            "id": "apr-op", "status": "open", "approvable": False, "code": "SOURCE_SCHEMA_DRIFT",
            "finding": "A column changed type.",
        },
    })
    resolve_plan_change(sched.id, actor="ops@example.test", reason="Accepted the new shape.")
    assert runner._run_schedule(sched.id) == "job-1"
    run = store.get_schedule(sched.id).run_history[-1]
    assert run["fire_reason"] == "plan_change", run
    # The claim consumed the decision: the next slot is a cron window again.
    claimed = store.get_schedule(sched.id)
    assert claimed.next_run_reason == ""
    assert store._parse_ts(claimed.next_run_at) > datetime.now(timezone.utc) + timedelta(days=1)


def test_pausing_cancels_a_pending_retry_so_resume_waits_for_the_window(temp_store):
    sched = _schedule(max_retries=2)
    store.update_schedule(sched.id, {"retry_at": _ago(1), "retry_attempt": 1})
    store.update_schedule(sched.id, {"enabled": False})
    resumed = store.update_schedule(sched.id, {"enabled": True})
    assert resumed is not None and not resumed.retry_at
    assert store.due_schedules() == [], "the retry owed before the pause fired on resume"


def test_a_refused_run_now_names_the_run_holding_the_claim(temp_store, dispatched, monkeypatch):
    sched = _schedule()
    store.update_schedule(sched.id, {"next_run_at": _ago(1)})
    assert runner._run_schedule(sched.id) == "job-1"  # the cron window's run holds the claim
    monkeypatch.setattr(store, "_job_is_live", lambda job_id: True)
    monkeypatch.setattr(store, "_job_dispatch_state", lambda job_id: "running")
    with pytest.raises(runner.ScheduleStartError) as exc:
        runner._run_schedule(sched.id, manual=True)
    assert exc.value.http_status == 409
    text = str(exc.value)
    assert "job-1" in text and "started by: Cron window" in text, text


@pytest.mark.parametrize("bookkeeping", ["approved", "rearmed"])
def test_a_bookkeeping_status_after_a_completed_run_is_not_a_failed_run(bookkeeping):
    """Last status "approved" read a completed run as failed: the next run
    became a full pass that re-read the whole source."""
    from services.full_pass_rerun import apply_schedule_full_pass

    sched = SimpleNamespace(
        last_status=bookkeeping, last_job_id="j2",
        run_history=[{"job_id": "j1", "status": "failed"}, {"job_id": "j2", "status": "completed"}],
    )
    req = SimpleNamespace(sync_mode="incremental_upsert", stream_contracts=[])
    assert apply_schedule_full_pass(req, sched, attempt=0) is None
    assert req.sync_mode == "incremental_upsert"


def test_on_mongo_a_beat_claim_loses_when_another_replica_moved_the_slot(monkeypatch):
    """The claim CAS carries the slot it decided on: a run another replica
    recorded between this replica's read and its write makes the claim lose."""
    from tests.test_schedule_claim_concurrency import _Mongo

    svc = _Mongo()
    monkeypatch.setattr(store, "_mongo_backend", lambda: svc)
    getattr(store, "_BASE_DOCS", {}).clear()
    sched = _schedule()
    store.update_schedule(sched.id, {"next_run_at": _ago(1)})
    stale = store.get_schedule(sched.id)
    assert stale is not None and store._parse_ts(stale.next_run_at) < datetime.now(timezone.utc)

    # Another replica's finalizer records a Run now: the slot moves a year on.
    docs = svc.get_database()["pipeline_schedules"].docs
    docs[sched.id]["next_run_at"] = "2027-01-01T03:00:00+00:00"

    claimed = store._claim_running_mongo(
        sched.id, "beat", datetime.now(timezone.utc).isoformat(),
        next_run_at="2027-01-01T03:00:00+00:00", stale=stale,
        expect={"next_run_at": stale.next_run_at, "retry_at": stale.retry_at},
    )
    assert claimed is None, "the beat claimed on a slot another replica had already moved"
    assert not docs[sched.id].get("running")
