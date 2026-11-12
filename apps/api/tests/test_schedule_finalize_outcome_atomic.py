"""A finished scheduled run is never visible without its outcome (finalize race).

CI, api-tests shard 5/6: ``test_d119_scheduled_pipeline_lifecycle_e2e``
``[job_g13]`` read the schedule the moment its claim was released and found no
open finding (``'' == 'open'``); ``test_r12_mongo_incremental_schedule_e2e``
read ``next_run_at`` on a schedule that was about to be parked. The run's
finalizer released the claim in one store write and parked the schedule in a
second one, so a reader between the two saw a finished run with no outcome.

Proven here without timing: every state a store write makes visible is read
back, and none of them may show the claim released without the run's outcome.
The same holds when two finalizers arrive for one job (the job's own terminal
write and the scheduler beat): one of them records the run, once.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

G13 = "1 source column(s) are neither mapped nor declared omitted: currency"
#: A job the in-job G13 gate refused (what ``[job_g13]`` runs into).
REFUSED_BY_G13 = {
    "status": "failed",
    "phase": "failed",
    "error": G13,
    "records_processed": 0,
    "preflight": {
        "blockers": [
            {
                "id": "g13_source_coverage",
                "message": G13,
                "details": {
                    "rule_id": "g13_source_coverage.unaccounted",
                    "unaccounted_sources": ["currency"],
                },
            }
        ]
    },
}
#: A deterministic refusal that is not source drift (parks as RUN_REFUSED).
REFUSED_BY_GATE = {
    "status": "failed",
    "phase": "validate",
    "error": "Transfer refused by validation gate g22: value does not fit",
    "records_processed": 0,
}
TRANSIENT = {
    "status": "failed",
    "phase": "load",
    "error": "connection reset by peer",
    "records_processed": 0,
}
COMPLETED = {"status": "completed", "phase": "completed", "records_processed": 3}


@pytest.fixture(params=["file", "mongo"])
def store(request, tmp_path, monkeypatch):
    """The schedule store on each backend (the Mongo one needs a live MongoDB)."""
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    if request.param == "file":
        monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
        yield schedule_store
        return
    pymongo = pytest.importorskip("pymongo")
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://127.0.0.1:27017")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable: {exc}")
    name = "finalize_race_" + uuid.uuid4().hex[:8]

    class _Svc:
        def __init__(self) -> None:
            self.client = client

        def get_database(self):
            return client[name]

    svc = _Svc()
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: svc)
    try:
        yield schedule_store
    finally:
        client.drop_database(name)
        client.close()


class _Scheduler:
    """``services.schedule_runner`` with the job documents and alerts a test supplies."""

    def __init__(self, module: Any) -> None:
        self.module = module
        self.jobs: dict[str, dict[str, Any]] = {}
        self.notified: list[tuple[str, str]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.module, name)


@pytest.fixture
def runner(monkeypatch):
    from services import schedule_runner

    env = _Scheduler(schedule_runner)
    monkeypatch.setattr(schedule_runner, "_job_doc", lambda jid: env.jobs.get(jid))
    monkeypatch.setattr(
        schedule_runner,
        "_notify_schedule",
        lambda sched, job_id, status, doc: env.notified.append((job_id, status)),
    )
    monkeypatch.setattr(schedule_runner, "_observe_parallel_run", lambda *a, **k: None)
    return env


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _claimed(store, name: str, job_id: str, **fields: Any):
    """A schedule whose run is in flight: run record open, claim bound to the job."""
    from services import schedule_run_lifecycle as life

    sched = store.create_schedule({
        "name": name,
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": f"orders_{name}",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
        "max_retries": 0,
        **fields,
    })
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-a", job_id) is not None
    if job_id:
        life.mark_running(sched.id, run_id, job_id)
    return store.get_schedule(sched.id), run_id


def _watch(store, monkeypatch, schedule_id: str) -> list[Any]:
    """Every state of the schedule that a store write made visible, in order."""
    seen: list[Any] = []
    real = store._save_all

    def save(schedules, **kwargs):
        result = real(schedules, **kwargs)
        seen.append(store.get_schedule(schedule_id))
        return result

    monkeypatch.setattr(store, "_save_all", save)
    return seen


def _released(seen: list[Any]) -> list[Any]:
    states = [s for s in seen if s is not None and not s.running]
    assert states, "the run was finalized without ever releasing its claim"
    return states


def test_refused_run_is_never_visible_released_without_its_finding(store, runner, monkeypatch):
    """D119 ``[job_g13]`` and R12: the claim release carries the parked finding."""
    runner.jobs["job-g13"] = REFUSED_BY_G13
    sched, _run = _claimed(store, "g13", "job-g13")
    seen = _watch(store, monkeypatch, sched.id)

    runner._finalize_run(sched.id, "job-g13", 0, _now())

    for state in _released(seen):
        # What the e2e reads as soon as ``running_job_id`` is empty.
        request = state.approval_request or {}
        assert str(request.get("status") or "") == "open", (state.last_status, request)
        assert state.last_status == "needs_approval"
        assert state.last_job_id == "job-g13" and state.run_count == 1
        # What GET /schedules/{id} shows: parked, not "next run at ...".
        assert store.visible_next_run_at(state) is None
        assert store.schedule_run_state(state)[0] == "stalled"
    final = store.get_schedule(sched.id)
    assert final.approval_request["code"] == "SOURCE_DRIFT"
    assert final.approval_request["job_id"] == "job-g13"
    assert final.approval_request["occurrences"] == 1
    assert final.approval_request["evidence"]["park_reason"] == "deterministic_refusal"
    assert final.approval_request["evidence"]["unmapped_columns"] == ["currency"]
    assert runner.notified == [("job-g13", "failed")]


def test_refusal_before_a_job_exists_parks_in_the_write_that_records_it(
    store, runner, monkeypatch
):
    sched, _run = _claimed(store, "prerun", "")
    seen = _watch(store, monkeypatch, sched.id)

    runner._park_on_decision(
        sched.id, ValueError("Schedule cannot run — the recipe cannot be applied."), attempt=0
    )

    for state in _released(seen):
        assert store.has_open_approval(state), (state.last_status, state.approval_request)
        assert state.last_status == "needs_approval"
    assert store.get_schedule(sched.id).run_count == 1
    assert runner.notified == [("", "failed")]


def test_start_error_that_repeats_parks_in_the_write_that_records_it(store, runner, monkeypatch):
    reason = "The scheduler could not start this run: RuntimeError: boom"
    sched, run_id = _claimed(store, "starterr", "")
    store.record_run_history(sched.id, {
        "run_id": "run_before", "job_id": "job-before", "status": "failed",
        "error": reason, "reason": reason,
    })
    seen = _watch(store, monkeypatch, sched.id)

    runner._fail_unstarted_run(sched.id, run_id, RuntimeError("boom"), attempt=0, manual=False)

    for state in _released(seen):
        assert store.has_open_approval(state), (state.last_status, state.approval_request)
    assert store.get_schedule(sched.id).last_status == "needs_approval"


def test_transient_back_off_lands_with_the_claim_release(store, runner, monkeypatch):
    """A released claim never shows the plain cadence slot the back-off replaces."""
    runner.jobs["job-flaky"] = TRANSIENT
    sched, _run = _claimed(store, "flaky", "job-flaky", interval="", cron="* * * * *")
    for n in range(3):  # failed beats in a row: the back-off is past the next minute
        store.record_run_history(sched.id, {
            "run_id": f"run_{n}", "job_id": f"job-{n}", "status": "failed",
            "error": TRANSIENT["error"],
        })
    seen = _watch(store, monkeypatch, sched.id)
    before = _now()

    runner._finalize_run(sched.id, "job-flaky", 0, _now())

    for state in _released(seen):
        assert not store.has_open_approval(state), state.approval_request
        due = datetime.fromisoformat(str(state.next_run_at))
        assert due >= before + timedelta(seconds=240), (state.next_run_at, before.isoformat())
    assert runner.notified == [("job-flaky", "failed")]


def test_successful_run_closes_a_stale_finding_with_the_claim_release(
    store, runner, monkeypatch
):
    """A completed run is never visible still parked on the finding it disproved."""
    from services.schedule_approvals import build_approval_request, open_approval_request

    sched, _run = _claimed(store, "rearm", "job-ok")
    open_approval_request(sched.id, build_approval_request(
        kind="run_refused", code="RUN_REFUSED", finding="An earlier run was refused.",
        corrective_action="Fix the plan.", binding={"binding_hash": "b"},
    ))
    assert store.has_open_approval(store.get_schedule(sched.id))
    runner.jobs["job-ok"] = COMPLETED
    seen = _watch(store, monkeypatch, sched.id)

    runner._finalize_run(sched.id, "job-ok", 0, _now())

    for state in _released(seen):
        assert not store.has_open_approval(state), state.approval_request
        assert state.last_status == "completed"
        assert store.visible_next_run_at(state)
    final = store.get_schedule(sched.id)
    assert final.approval_request["resolved_by"] == "system:run-succeeded"
    assert final.run_count == 1


def test_two_finalizers_for_one_job_record_one_outcome(store, runner, monkeypatch, caplog):
    """The job's terminal write and the beat both finalize: one run, one finding."""
    runner.jobs["job-twice"] = REFUSED_BY_GATE
    sched, _run = _claimed(store, "twice", "job-twice")
    # Both finalizers are past every "already recorded" check before either writes.
    together = threading.Barrier(2, timeout=20)
    real_entry = runner._run_entry

    def entry(*args):
        try:
            together.wait()
        except threading.BrokenBarrierError:
            pass
        return real_entry(*args)

    monkeypatch.setattr(runner.module, "_run_entry", entry)
    errors: list[BaseException] = []

    def finalize():
        try:
            runner._finalize_run(sched.id, "job-twice", 0, _now())
        except BaseException as exc:  # noqa: BLE001 - reported by the assert below
            errors.append(exc)

    with caplog.at_level("INFO"):
        threads = [threading.Thread(target=finalize) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
    assert not errors, errors
    assert not together.broken, "the two finalizers never overlapped"

    final = store.get_schedule(sched.id)
    assert final.run_count == 1
    assert final.running is False and final.last_status == "needs_approval"
    assert final.approval_request["occurrences"] == 1, final.approval_request
    assert runner.notified == [("job-twice", "failed")]
    events = [getattr(r, "event", None) for r in caplog.records]
    assert events.count("schedule.run.give_up") == 1
    assert events.count("approval.requested") == 1


def test_run_recorded_by_another_replica_is_not_recorded_twice(store, runner, monkeypatch):
    """Mongo store: a peer's finalizer landed between this one's read and write."""
    if store._mongo_backend() is None:
        pytest.skip("cross-process finalizers need the shared (Mongo) store")
    runner.jobs["job-peer"] = REFUSED_BY_GATE
    sched, _run = _claimed(store, "peer", "job-peer")
    coll = store._mongo_backend().get_database()["pipeline_schedules"]
    peer_finding = {"id": "apr_peer", "status": "open", "code": "RUN_REFUSED",
                    "job_id": "job-peer", "occurrences": 1, "finding": "peer"}
    real = store._save_all
    landed: list[bool] = []

    def save(schedules, **kwargs):
        if not landed:
            landed.append(True)
            coll.update_one({"_id": sched.id}, {
                "$set": {"running": False, "running_job_id": "", "running_instance": "",
                         "running_started_at": None, "last_job_id": "job-peer",
                         "last_status": "needs_approval", "run_count": 1,
                         "approval_request": peer_finding},
                "$inc": {"version": 1},
            })
        return real(schedules, **kwargs)

    monkeypatch.setattr(store, "_save_all", save)

    runner._finalize_run(sched.id, "job-peer", 0, _now())

    final = store.get_schedule(sched.id)
    assert final.run_count == 1
    assert final.approval_request["id"] == "apr_peer", final.approval_request
    assert final.approval_request["occurrences"] == 1
    assert runner.notified == [], "the peer already recorded and announced this run"


# --- A write that lands between the finalizer's read and its write ---------------
#
# On the shared (Mongo) store another replica can write the schedule while this
# finalizer is between loading it and storing the run: an operator's decision,
# a mapping edit. The run's outcome was decided on the copy that was loaded, so
# it must be decided again on what is stored now, never merged over it.


def _write_inside_finalize(store, monkeypatch, peer_write, around: str = "mark_schedule_run"):
    """Run ``peer_write()`` once, after ``store.<around>`` has loaded and decided."""
    real_save, real_mark = store._save_all, getattr(store, around)
    state = {"inside": False, "pending": True}

    def mark(*args, **kwargs):
        state["inside"] = True
        try:
            return real_mark(*args, **kwargs)
        finally:
            state["inside"] = False

    def save(schedules, **kwargs):
        if state["inside"] and state["pending"]:
            state["pending"] = False
            peer_write()
        return real_save(schedules, **kwargs)

    monkeypatch.setattr(store, around, mark)
    monkeypatch.setattr(store, "_save_all", save)
    return state


#: A peer replica's write needs the shared (Mongo) store. In one process the
#: store lock leaves no window (test_a_concurrent_writer_waits_for_the_finalize).
mongo_only = pytest.mark.parametrize("store", ["mongo"], indirect=True)


def _open_old_finding(store, schedule_id: str) -> None:
    store.update_schedule(schedule_id, {
        "approval_request": {"id": "apr_old", "status": "open", "code": "RUN_REFUSED",
                             "job_id": "job-old", "occurrences": 1, "finding": "old"},
        "last_status": "needs_approval",
    })


def _operator_rejects(store, schedule_id: str) -> None:
    current = store.get_schedule(schedule_id).approval_request
    store.update_schedule(schedule_id, {"approval_request": {
        **current, "status": "rejected", "resolved_by": "operator@x", "resolved_at": "now",
    }})


@mongo_only
def test_operator_rejection_made_while_a_success_is_recorded_survives(
    store, runner, monkeypatch
):
    """I1: the success closes the finding only if it is still open when stored."""
    sched, _run = _claimed(store, "reject", "job-ok")
    _open_old_finding(store, sched.id)
    runner.jobs["job-ok"] = COMPLETED
    state = _write_inside_finalize(store, monkeypatch, lambda: _operator_rejects(store, sched.id))

    runner._finalize_run(sched.id, "job-ok", 0, _now())

    assert not state["pending"], "the peer write never landed inside the finalize"
    final = store.get_schedule(sched.id)
    assert final.approval_request["status"] == "rejected", final.approval_request
    assert final.approval_request["resolved_by"] == "operator@x"
    assert final.running is False and final.running_job_id == ""
    assert final.run_count == 1 and final.last_status == "completed"
    assert runner.notified == [("job-ok", "completed")]


@mongo_only
def test_mapping_edit_made_while_a_refusal_is_recorded_binds_the_finding_to_the_new_plan(
    store, runner, monkeypatch, caplog
):
    """I3: a finding bound to the plan it replaced could never be approved."""
    from services.standing_authorization import binding_from_schedule

    sched, _run = _claimed(store, "remap", "job-refused")
    runner.jobs["job-refused"] = REFUSED_BY_GATE
    edited = [{"source": "id", "target": "id"}, {"source": "amt", "target": "amt"}]
    state = _write_inside_finalize(
        store, monkeypatch, lambda: store.update_schedule(sched.id, {"mappings": edited})
    )

    with caplog.at_level("INFO"):
        runner._finalize_run(sched.id, "job-refused", 0, _now())

    assert not state["pending"], "the peer write never landed inside the finalize"
    final = store.get_schedule(sched.id)
    assert final.mappings == edited
    request = final.approval_request
    assert request["status"] == "open" and request["job_id"] == "job-refused"
    assert request["binding"]["binding_hash"] == binding_from_schedule(final)["binding_hash"]
    assert final.last_status == "needs_approval" and final.run_count == 1
    assert runner.notified == [("job-refused", "failed")]
    events = [getattr(r, "event", None) for r in caplog.records]
    assert events.count("approval.requested") == 1


@mongo_only
def test_a_schedule_that_keeps_moving_is_never_written_from_a_stale_decision(
    store, runner, monkeypatch
):
    """Past the reload bound the finalize fails loudly; the claim stays for the beat."""
    sched, _run = _claimed(store, "churn", "job-ok")
    _open_old_finding(store, sched.id)
    runner.jobs["job-ok"] = COMPLETED
    coll = store._mongo_backend().get_database()["pipeline_schedules"]
    real_save = store._save_all
    peer_writes: list[int] = []

    def save(schedules, **kwargs):
        # A peer writes before every attempt this finalizer makes.
        peer_writes.append(1)
        coll.update_one({"_id": sched.id},
                        {"$set": {"enabled_note": f"peer {len(peer_writes)}"},
                         "$inc": {"version": 1}})
        return real_save(schedules, **kwargs)

    monkeypatch.setattr(store, "_save_all", save)

    with pytest.raises(store.ScheduleWriteContended):
        runner._finalize_run(sched.id, "job-ok", 0, _now())

    held = store.get_schedule(sched.id)
    assert held.running is True and held.running_job_id == "job-ok", "no blind write"
    assert held.approval_request["status"] == "open" and held.run_count == 0
    assert held.enabled_note == f"peer {len(peer_writes)}"
    assert runner.notified == []
    # The contention ends: the beat that notices the finished job records it once.
    monkeypatch.setattr(store, "_save_all", real_save)
    runner._finalize_run(sched.id, "job-ok", 0, _now())
    final = store.get_schedule(sched.id)
    assert final.run_count == 1 and final.running is False
    assert final.approval_request["resolved_by"] == "system:run-succeeded"
    assert runner.notified == [("job-ok", "completed")]


def test_a_concurrent_writer_waits_for_the_finalize(store, runner, monkeypatch):
    """In one process the store lock leaves no window (the file store's whole CAS)."""
    sched, _run = _claimed(store, "waits", "job-ok")
    _open_old_finding(store, sched.id)
    runner.jobs["job-ok"] = COMPLETED
    seen: dict[str, bool] = {}
    writer = threading.Thread(target=_operator_rejects, args=(store, sched.id))

    def peer_write():
        writer.start()
        writer.join(0.5)
        seen["finished_inside"] = not writer.is_alive()

    _write_inside_finalize(store, monkeypatch, peer_write)

    runner._finalize_run(sched.id, "job-ok", 0, _now())
    writer.join(20)

    assert seen == {"finished_inside": False}, "a writer ran inside the finalize's window"
    final = store.get_schedule(sched.id)
    assert final.run_count == 1 and final.last_status == "completed"
    assert final.approval_request["resolved_by"] == "operator@x"


@mongo_only
def test_a_schedule_deleted_while_its_run_is_recorded_asks_for_no_approval(
    store, runner, monkeypatch, caplog
):
    """I4: a finding is never announced for a schedule that no longer exists."""
    sched, _run = _claimed(store, "gone", "job-gone")
    runner.jobs["job-gone"] = REFUSED_BY_GATE
    state = _write_inside_finalize(store, monkeypatch, lambda: store.delete_schedule(sched.id))

    with caplog.at_level("INFO"):
        runner._finalize_run(sched.id, "job-gone", 0, _now())

    assert not state["pending"], "the delete never landed inside the finalize"
    assert store.get_schedule(sched.id) is None, "a deleted schedule came back"
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "approval.requested" not in events
    assert events.count("schedule.run.give_up") == 1
    # The job did fail: that is still said once (as before this fix).
    assert runner.notified == [("job-gone", "failed")]


@mongo_only
@pytest.mark.parametrize("with_outcome", [True, False])
def test_recording_a_run_on_a_schedule_deleted_meanwhile_reports_no_schedule(
    store, monkeypatch, with_outcome
):
    sched, run_id = _claimed(store, f"gone-{with_outcome}", "job-x")
    decided: list[str] = []

    def decide(after):
        decided.append(after.id)
        return {"approval_request": {"id": "apr_x", "status": "open", "code": "RUN_REFUSED",
                                     "job_id": "job-x", "occurrences": 1}}

    outcome = store.RunOutcome(decide) if with_outcome else None
    _write_inside_finalize(store, monkeypatch, lambda: store.delete_schedule(sched.id))

    result = store.mark_schedule_run(
        sched.id, "job-x", status="failed",
        run_entry={"run_id": run_id, "job_id": "job-x", "status": "failed"}, outcome=outcome,
    )

    assert result is None
    if outcome is not None:
        assert decided == [sched.id] and outcome.recorded is False
    assert store.get_schedule(sched.id) is None


# --- A late finalizer after the next run has claimed --------------------------
#
# The job's own terminal write and the beat both finalize a job. If the slow one
# arrives after the next run claimed the schedule, it may only re-decide this
# job's outcome: the claim, the cadence and the newer run belong to that run.

_NEWER_RUN_FIELDS = ("running", "running_job_id", "running_instance", "running_started_at",
                     "next_run_at", "run_count", "last_run_at", "last_job_id",
                     "retry_at", "retry_attempt", "missed_window_count")


def _next_run_claimed(store, runner, name: str, first: dict, *, approve: bool, **fields):
    """jE1 finished with ``first`` and was recorded; jE2 now holds the claim."""
    from services import schedule_run_lifecycle as life

    runner.jobs["jE1"] = first
    sched, _run = _claimed(store, name, "jE1", **fields)
    runner._finalize_run(sched.id, "jE1", 0, _now())
    if approve:
        current = store.get_schedule(sched.id).approval_request
        store.update_schedule(sched.id, {"approval_request": {**current, "status": "approved"},
                                         "last_status": "failed"})
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-b", "jE2") is not None
    life.mark_running(sched.id, run_id, "jE2")
    runner.notified.clear()
    return store.get_schedule(sched.id)


def _newer_run(sched) -> dict:
    return {
        **{name: getattr(sched, name) for name in _NEWER_RUN_FIELDS},
        "history": [(r.get("job_id"), r.get("status")) for r in sched.run_history],
    }


def test_late_finalizer_with_a_changed_verdict_leaves_the_next_runs_claim(store, runner):
    """Scenario F: jE1 recorded refused, then reported completed while jE2 runs."""
    before = _next_run_claimed(store, runner, "late-ok", REFUSED_BY_GATE, approve=True)
    assert before.running and before.running_job_id == "jE2" and before.run_count == 1

    runner.jobs["jE1"] = COMPLETED
    runner._finalize_run(before.id, "jE1", 0, _now())

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    assert after.approval_request == before.approval_request


def test_late_refusal_after_the_next_run_claimed_parks_without_touching_its_claim(
    store, runner
):
    """jE1 recorded completed, then reported refused while jE2 runs."""
    before = _next_run_claimed(store, runner, "late-refused", COMPLETED, approve=False)

    runner.jobs["jE1"] = REFUSED_BY_GATE
    runner._finalize_run(before.id, "jE1", 0, _now())

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    # The changed verdict is still decided: jE1's refusal parks on one finding.
    assert after.approval_request["status"] == "open"
    assert after.approval_request["job_id"] == "jE1"
    assert after.last_status == "needs_approval"


def test_a_late_back_off_that_is_not_stored_is_not_reported(store, runner, caplog):
    """jE1 recorded completed, then reported as a repeated transient failure."""
    before = _next_run_claimed(store, runner, "late-flaky", COMPLETED, approve=False,
                               interval="", cron="* * * * *")
    for n in range(3):  # failed beats in a row: a back-off would be owed
        store.record_run_history(before.id, {
            "run_id": f"run_{n}", "job_id": f"job-{n}", "status": "failed",
            "error": TRANSIENT["error"],
        })
    before = store.get_schedule(before.id)
    runner.jobs["jE1"] = TRANSIENT

    with caplog.at_level("INFO"):
        runner._finalize_run(before.id, "jE1", 0, _now())

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "schedule.run.backoff" not in events, "a back-off jE2's cadence never got"


def test_a_late_outcome_is_stored_without_the_fields_of_the_next_run(store, runner):
    """Store level: a back-off or retry reset decided for jE1 is not jE2's to lose."""
    before = _next_run_claimed(store, runner, "late-fields", COMPLETED, approve=False)
    finding = {"id": "apr_late", "status": "open", "code": "RUN_REFUSED", "job_id": "jE1",
               "occurrences": 1}
    outcome = store.RunOutcome(lambda after: {
        "approval_request": finding,
        "last_status": "needs_approval",
        "next_run_at": "2099-01-01T00:00:00+00:00",
        "retry_at": None,
        "retry_attempt": 0,
        "running": False,
        "running_job_id": "",
        "run_count": 99,
    })

    store.mark_schedule_run(before.id, "jE1", status="failed",
                            run_entry={"job_id": "jE1", "status": "failed"}, outcome=outcome)

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    assert after.approval_request == finding and after.last_status == "needs_approval"
    assert outcome.recorded is True


# --- Retries and contended writes across replicas ------------------------------


def _retry_claimed_by_peer(store, schedule_id: str, entry: dict) -> None:
    """A peer replica parked the same retry, and the retry then took the claim."""
    store.schedule_retry(schedule_id, retry_at=_now(), attempt=1, run_entry=entry)
    assert store.mark_schedule_running(schedule_id, "beat-b", "job-retry") is not None


@mongo_only
def test_retry_parked_by_another_replica_is_not_parked_twice(store, runner, monkeypatch):
    """A second finalizer's retry write never releases the retry's own claim."""
    runner.jobs["job-1"] = TRANSIENT
    sched, run_id = _claimed(store, "retry-peer", "job-1", max_retries=1,
                             retry_backoff_seconds=0)
    entry = {"run_id": run_id, "job_id": "job-1", "status": "failed", "attempt": 0,
             "error": TRANSIENT["error"], "retry_scheduled": True}
    state = _write_inside_finalize(
        store, monkeypatch, lambda: _retry_claimed_by_peer(store, sched.id, entry),
        around="schedule_retry",
    )

    runner._finalize_run(sched.id, "job-1", 0, _now())

    assert not state["pending"], "the peer write never landed inside the retry"
    final = store.get_schedule(sched.id)
    assert final.running is True and final.running_job_id == "job-retry", (
        final.running, final.running_job_id)
    assert final.retry_attempt == 1
    assert [r.get("job_id") for r in final.run_history] == ["job-1"]


class _ContendedSvc:
    """The Mongo store, where a peer writes the schedule before every CAS update."""

    def __init__(self, svc, peer_write) -> None:
        self.client, self._svc, self._peer_write = svc.client, svc, peer_write

    def get_database(self):
        db, peer_write = self._svc.get_database(), self._peer_write

        class _Db:
            def __getitem__(self, name):
                coll = db[name]
                return _Coll(coll) if name == "pipeline_schedules" else coll

        class _Coll:
            def __init__(self, coll) -> None:
                self._coll = coll

            def find_one_and_update(self, filt, update, **kwargs):
                if "version" in filt:
                    peer_write(self._coll, filt["_id"])
                return self._coll.find_one_and_update(filt, update, **kwargs)

            def __getattr__(self, name):
                return getattr(self._coll, name)

        return _Db()


@mongo_only
@pytest.mark.parametrize("peer_records_the_retry_last", [False, True])
def test_a_contended_retry_write_is_never_written_blind(
    store, monkeypatch, peer_records_the_retry_last
):
    """Past every CAS attempt the write keeps its guard: superseded, or it raises."""
    sched, run_id = _claimed(store, f"contended-{peer_records_the_retry_last}", "job-1",
                             max_retries=1, retry_backoff_seconds=0)
    entry = {"run_id": run_id, "job_id": "job-1", "status": "failed", "attempt": 0,
             "retry_scheduled": True}
    peer_writes: list[int] = []
    real_backend = store._mongo_backend

    def peer_write(coll, schedule_id):
        peer_writes.append(1)
        if peer_records_the_retry_last and len(peer_writes) == store._SAVE_CAS_ATTEMPTS:
            # The last attempt loses to a peer that parked this retry and whose
            # retry then claimed the schedule.
            monkeypatch.setattr(store, "_mongo_backend", real_backend)
            _retry_claimed_by_peer(store, schedule_id, entry)
            monkeypatch.setattr(store, "_mongo_backend", lambda: contended)
            return
        coll.update_one({"_id": schedule_id},
                        {"$set": {"enabled_note": f"peer {len(peer_writes)}"},
                         "$inc": {"version": 1}})

    contended = _ContendedSvc(real_backend(), peer_write)
    monkeypatch.setattr(store, "_mongo_backend", lambda: contended)

    if peer_records_the_retry_last:
        store.schedule_retry(sched.id, retry_at=_now(), attempt=1, run_entry=entry)
        monkeypatch.setattr(store, "_mongo_backend", real_backend)
        final = store.get_schedule(sched.id)
        assert final.running is True and final.running_job_id == "job-retry"
    else:
        with pytest.raises(store.ScheduleWriteContended):
            store.schedule_retry(sched.id, retry_at=_now(), attempt=1, run_entry=entry)
        monkeypatch.setattr(store, "_mongo_backend", real_backend)
        final = store.get_schedule(sched.id)
        # Nothing of this caller's was written: the claim is still held.
        assert final.running is True and final.running_job_id == "job-1"
        assert final.retry_attempt == 0 and final.retry_at is None
    assert len(peer_writes) == store._SAVE_CAS_ATTEMPTS


# --- Duplicates that reach the store ------------------------------------------


def test_a_duplicate_record_after_the_next_run_claimed_changes_nothing(store, runner):
    """Store level, same verdict (scenario E): the run was recorded; jE2 runs now."""
    before = _next_run_claimed(store, runner, "late-dup", REFUSED_BY_GATE, approve=True)
    finding = {"id": "apr_dup", "status": "open", "code": "RUN_REFUSED", "job_id": "jE1",
               "occurrences": 1}
    outcome = store.RunOutcome(lambda after: {"approval_request": finding,
                                              "last_status": "needs_approval"})

    store.mark_schedule_run(before.id, "jE1", status="failed",
                            run_entry={"job_id": "jE1", "status": "failed"}, outcome=outcome)

    after = store.get_schedule(before.id)
    assert outcome.recorded is False
    assert _newer_run(after) == _newer_run(before)
    assert after.approval_request == before.approval_request
    assert after.last_status == before.last_status


def test_a_second_retry_write_for_one_attempt_leaves_the_retry_alone(store):
    """schedule_retry twice for one attempt; the retry claimed in between."""
    sched, run_id = _claimed(store, "retry-twice", "job-1", max_retries=1,
                             retry_backoff_seconds=0)
    entry = {"run_id": run_id, "job_id": "job-1", "status": "failed", "attempt": 0,
             "retry_scheduled": True}
    first = store.schedule_retry(sched.id, retry_at=_now(), attempt=1, run_entry=entry)
    assert first is not None and first.running is False and first.retry_attempt == 1
    assert store.mark_schedule_running(sched.id, "beat-b", "job-retry") is not None
    claimed = store.get_schedule(sched.id)

    store.schedule_retry(sched.id, retry_at=_now() + timedelta(hours=1), attempt=1,
                         run_entry=entry)

    after = store.get_schedule(sched.id)
    assert after.running is True and after.running_job_id == "job-retry"
    assert after.retry_at == claimed.retry_at
    assert after.run_history == claimed.run_history


def _peer_recorded(store, schedule_id: str, job_id: str):
    """A peer replica's finalizer records ``job_id``'s refused run, raw."""
    coll = store._mongo_backend().get_database()["pipeline_schedules"]
    finding = {"id": "apr_peer", "status": "open", "code": "RUN_REFUSED",
               "job_id": job_id, "occurrences": 1, "finding": "peer"}

    def write():
        coll.update_one({"_id": schedule_id}, {
            "$set": {"running": False, "running_job_id": "", "running_instance": "",
                     "running_started_at": None, "last_job_id": job_id,
                     "last_status": "needs_approval", "run_count": 1,
                     "approval_request": finding},
            "$inc": {"version": 1},
        })

    return write, finding


@mongo_only
def test_refusal_before_a_job_recorded_by_another_replica_is_not_announced_twice(
    store, runner, monkeypatch, caplog
):
    sched, _run = _claimed(store, "pre-peer", "job-pre")
    write, finding = _peer_recorded(store, sched.id, "job-pre")
    state = _write_inside_finalize(store, monkeypatch, write)

    with caplog.at_level("INFO"):
        runner._park_on_decision(
            sched.id, ValueError("Schedule cannot run — the recipe cannot be applied."),
            attempt=0, job_id="job-pre",
        )

    assert not state["pending"], "the peer write never landed inside the finalize"
    final = store.get_schedule(sched.id)
    assert final.run_count == 1 and final.approval_request == finding
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "approval.requested" not in events
    assert runner.notified == [], "the peer already recorded and announced this run"


@mongo_only
def test_start_error_recorded_by_another_replica_is_not_announced_twice(
    store, runner, monkeypatch, caplog
):
    reason = "The scheduler could not start this run: RuntimeError: boom"
    sched, run_id = _claimed(store, "start-peer", "job-start")
    store.record_run_history(sched.id, {
        "run_id": "run_before", "job_id": "job-before", "status": "failed",
        "error": reason, "reason": reason,
    })
    write, finding = _peer_recorded(store, sched.id, "job-start")
    state = _write_inside_finalize(store, monkeypatch, write)

    with caplog.at_level("INFO"):
        runner._fail_unstarted_run(sched.id, run_id, RuntimeError("boom"), attempt=0,
                                   manual=False)

    assert not state["pending"], "the peer write never landed inside the finalize"
    final = store.get_schedule(sched.id)
    assert final.run_count == 1 and final.approval_request == finding
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "approval.requested" not in events
    assert runner.notified == [], "the peer already recorded and announced this run"


def test_drift_preflight_parked_is_not_counted_again_by_the_run(store, runner, caplog):
    """Preflight parked job-g13 on drift; the run's own finalizer sees the same refusal."""
    sched, _run = _claimed(store, "drift-once", "job-g13")
    parked = runner._finding_patch(
        sched, G13, attempt=0, job_id="job-g13",
        evidence={"gate_codes": ["g13_source_coverage.unaccounted"],
                  "unmapped_columns": ["currency"]},
    )
    assert parked and parked["approval_request"]["code"] == "SOURCE_DRIFT"
    store.update_schedule(sched.id, parked)
    preflight = store.get_schedule(sched.id).approval_request
    runner.jobs["job-g13"] = REFUSED_BY_G13

    with caplog.at_level("INFO"):
        runner._finalize_run(sched.id, "job-g13", 0, _now())

    final = store.get_schedule(sched.id)
    assert final.approval_request == preflight
    assert final.approval_request["occurrences"] == 1
    assert final.run_count == 1
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "approval.requested" not in events


# --- A parked retry is not a later run (review round 2, M-A) -------------------


def _retry_parked(store, runner, name: str):
    """jR1 failed transiently and its retry is parked: no other run exists."""
    runner.jobs["jR1"] = TRANSIENT
    sched, _run = _claimed(store, name, "jR1", max_retries=2, retry_backoff_seconds=600)
    runner._finalize_run(sched.id, "jR1", 0, _now())
    parked = store.get_schedule(sched.id)
    assert parked.retry_at and parked.retry_attempt == 1 and parked.run_count == 0
    assert parked.running is False and runner.notified == []
    return parked


def test_a_success_reported_after_its_retry_was_parked_is_recorded(store, runner):
    """L3c: the worker-lost settle failed jR1, then the worker finished it.

    No later run exists, only jR1's own parked retry: the success is recorded,
    the retry is disarmed (it would land the rows a second time) and the run is
    announced once, as completed.
    """
    parked = _retry_parked(store, runner, "retry-flip")

    runner.jobs["jR1"] = COMPLETED
    runner._finalize_run(parked.id, "jR1", 0, _now())

    final = store.get_schedule(parked.id)
    assert final.retry_at is None and final.retry_attempt == 0
    assert final.run_count == 1 and final.last_job_id == "jR1"
    assert final.last_status == "completed" and final.running is False
    assert runner.notified == [("jR1", "completed")]


def test_a_success_reported_after_its_retry_ran_leaves_the_retry_alone(store, runner):
    """The retry jR2 ran and was recorded since: that is a later run."""
    from services import schedule_run_lifecycle as life

    parked = _retry_parked(store, runner, "retry-ran")
    run_id = life.begin_run(parked.id, trigger=life.TRIGGER_RETRY, attempt=1)
    assert store.mark_schedule_running(parked.id, "beat-b", "jR2") is not None
    life.mark_running(parked.id, run_id, "jR2")
    runner.jobs["jR2"] = COMPLETED
    runner._finalize_run(parked.id, "jR2", 1, _now())
    before = store.get_schedule(parked.id)
    assert before.run_count == 1 and before.last_job_id == "jR2" and not before.running

    runner.jobs["jR1"] = COMPLETED
    runner._finalize_run(parked.id, "jR1", 0, _now())

    after = store.get_schedule(parked.id)
    assert _newer_run(after) == _newer_run(before)


# --- A finalize write that keeps losing (review round 2, L-B) -------------------


def _contend(store, monkeypatch, schedule_id: str) -> list[int]:
    """Every attempt to record ``schedule_id``'s run loses to a peer write.

    On Mongo a peer really writes the schedule before each guarded save; the
    file store has no peer, so each attempt is told its decision went stale.
    Either way the store gives up with ``ScheduleWriteContended``.
    """
    attempts: list[int] = []
    if store._mongo_backend() is not None:
        coll = store._mongo_backend().get_database()["pipeline_schedules"]
        real_save = store._save_all

        def save(schedules, **kwargs):
            guard = kwargs.get("guard")
            if guard is not None and guard.schedule_id == schedule_id:
                attempts.append(1)
                coll.update_one({"_id": schedule_id},
                                {"$set": {"enabled_note": f"peer {len(attempts)}"},
                                 "$inc": {"version": 1}})
            return real_save(schedules, **kwargs)

        monkeypatch.setattr(store, "_save_all", save)
        return attempts
    real_once = store._mark_schedule_run_once

    def once(sid, *args, **kwargs):
        if sid == schedule_id:
            attempts.append(1)
            raise store._OutcomeStale()
        return real_once(sid, *args, **kwargs)

    monkeypatch.setattr(store, "_mark_schedule_run_once", once)
    return attempts


def _finished_but_held(store, runner, monkeypatch, name: str, job_id: str):
    """``job_id`` ended, its callback never ran: the claim still names it."""
    sched, _run = _claimed(store, name, job_id)
    runner.jobs[job_id] = COMPLETED
    return sched


def _jobs_ended(store, monkeypatch, *job_ids: str) -> None:
    ended = set(job_ids)
    monkeypatch.setattr(store, "_job_dispatch_state",
                        lambda jid: "terminal" if jid in ended else "running")
    monkeypatch.setattr(store, "_job_is_live", lambda jid: jid not in ended)


def _start_stubs(monkeypatch) -> None:
    import services.request_principal as principal
    import services.schedule_approvals as approvals

    monkeypatch.setattr(principal, "assert_bound_principal_may_use_endpoints",
                        lambda *a, **k: None)
    monkeypatch.setattr(approvals, "release_create_new_dest_exists_false_refuse",
                        lambda *a, **k: None)


def test_run_now_on_a_run_still_being_recorded_is_a_conflict(store, runner, monkeypatch):
    """Run now finalizes the finished job first; a contended write is a 409, not a 500."""
    sched = _finished_but_held(store, runner, monkeypatch, "rn-contended", "job-held")
    _jobs_ended(store, monkeypatch, "job-held")
    _start_stubs(monkeypatch)
    attempts = _contend(store, monkeypatch, sched.id)

    with pytest.raises(runner.ScheduleStartError) as raised:
        runner._run_schedule(sched.id, manual=True, triggered_by="operator@x")

    assert attempts, "the held claim was never finalized"
    assert raised.value.http_status == 409 and raised.value.code == "run_still_recording"
    assert raised.value.retry_after > 0
    held = store.get_schedule(sched.id)
    assert held.running is True and held.running_job_id == "job-held" and held.run_count == 0
    # No run row says this Run now failed: it never started. The attempt is a
    # skip with its reason, the way every other refused Run now is recorded.
    rows = [(r.get("trigger"), r.get("job_id"), r.get("status")) for r in held.run_history]
    assert rows == [("cadence", "job-held", "running"), ("manual", "", "skipped")], rows
    assert held.run_history[-1]["reason_code"] == "run_still_recording"
    assert runner.notified == []


def test_a_cadence_start_on_a_run_still_being_recorded_is_a_skip(store, runner, monkeypatch):
    """A stale claim can be due again: the beat's start skips, it does not raise."""
    sched = _finished_but_held(store, runner, monkeypatch, "cad-contended", "job-held")
    _jobs_ended(store, monkeypatch, "job-held")
    _start_stubs(monkeypatch)
    attempts = _contend(store, monkeypatch, sched.id)

    assert runner._run_schedule(sched.id) is None

    assert attempts, "the held claim was never finalized"
    held = store.get_schedule(sched.id)
    assert held.running is True and held.running_job_id == "job-held" and held.run_count == 0
    rows = [(r.get("trigger"), r.get("job_id"), r.get("status")) for r in held.run_history]
    assert rows == [("cadence", "job-held", "running"), ("cadence", "", "skipped")], rows
    assert held.run_history[-1]["reason_code"] == "run_still_recording"
    assert runner.notified == []


def _beat_stubs(runner, monkeypatch) -> dict[str, list]:
    """The beat's neighbours, recorded: every step after the claims must still run."""
    import services.cdc_capture_inventory as inventory
    import services.cdc_capture_release as releases
    import services.schedule_approvals as approvals

    seen: dict[str, list] = {"dispatched": [], "swept": [], "released": []}
    monkeypatch.setattr(runner.module, "_acquire_scheduler_lock", lambda: True)
    monkeypatch.setattr(runner.module, "_release_scheduler_lock", lambda: None)
    monkeypatch.setattr(runner.module, "_release_superseded_queued_claims", lambda: None)
    monkeypatch.setattr(runner.module, "_sweep_orphan_runs",
                        lambda: seen["swept"].append(1))
    monkeypatch.setattr(runner.module, "_run_schedule",
                        lambda sid, **k: seen["dispatched"].append(sid) or None)
    monkeypatch.setattr(approvals, "release_same_declaration_source_drift",
                        lambda *a, **k: None)
    monkeypatch.setattr(approvals, "release_create_new_dest_exists_false_refuse",
                        lambda *a, **k: None)
    monkeypatch.setattr(releases, "retry_pending_capture_releases",
                        lambda *a, **k: seen["released"].append(1))
    monkeypatch.setattr(inventory, "start_source_gone_sweep", lambda *a, **k: None)
    return seen


def test_a_contended_claim_does_not_stop_the_beat(store, runner, monkeypatch, caplog):
    """One schedule's lost write is deferred; every other step of the beat runs."""
    a = _finished_but_held(store, runner, monkeypatch, "beat-a", "job-a")
    c = _finished_but_held(store, runner, monkeypatch, "beat-c", "job-c")
    b = store.create_schedule({
        "name": "beat-b", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_beat_b", "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}], "max_retries": 0,
    })
    store.update_schedule(b.id, {"next_run_at": (_now() - timedelta(minutes=5)).isoformat()})
    _jobs_ended(store, monkeypatch, "job-a", "job-c")
    seen = _beat_stubs(runner, monkeypatch)
    attempts = _contend(store, monkeypatch, a.id)

    with caplog.at_level("INFO"):
        runner._run_due_schedules()

    assert attempts, "schedule A's run was never finalized"
    held = store.get_schedule(a.id)
    assert held.running is True and held.running_job_id == "job-a" and held.run_count == 0
    done = store.get_schedule(c.id)
    assert done.running is False and done.run_count == 1
    assert seen["swept"] and seen["released"], seen
    assert b.id in seen["dispatched"], seen
    deferred = [r for r in caplog.records
                if getattr(r, "event", None) == "schedule.run.finalize_deferred"]
    assert [getattr(r, "schedule_id", None) for r in deferred] == [a.id]


def test_a_contended_claim_does_not_stop_the_startup_reclaim(store, runner, monkeypatch, caplog):
    a = _finished_but_held(store, runner, monkeypatch, "boot-a", "job-a")
    c = _finished_but_held(store, runner, monkeypatch, "boot-c", "job-c")
    _jobs_ended(store, monkeypatch, "job-a", "job-c")
    monkeypatch.setattr(runner.module, "_sweep_orphan_runs", lambda: None)
    attempts = _contend(store, monkeypatch, a.id)

    with caplog.at_level("INFO"):
        runner._clear_stale_running_schedules()

    assert attempts, "schedule A's run was never finalized"
    held = store.get_schedule(a.id)
    assert held.running is True and held.running_job_id == "job-a" and held.run_count == 0
    done = store.get_schedule(c.id)
    assert done.running is False and done.run_count == 1
    deferred = [r for r in caplog.records
                if getattr(r, "event", None) == "schedule.run.finalize_deferred"]
    assert [getattr(r, "schedule_id", None) for r in deferred] == [a.id]


# --- A late outcome after the newer run finished (review round 2, L-C) --------

PLAIN_FAIL = {"status": "failed", "phase": "load", "error": "something odd happened",
              "records_processed": 0}


def _two_runs_recorded(store, runner, name: str, first: dict, second: dict):
    """jE1 then jE2 ran and were recorded; nothing holds the claim."""
    from services import schedule_run_lifecycle as life

    runner.jobs["jE1"] = first
    sched, _run = _claimed(store, name, "jE1")
    runner._finalize_run(sched.id, "jE1", 0, _now())
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-b", "jE2") is not None
    life.mark_running(sched.id, run_id, "jE2")
    runner.jobs["jE2"] = second
    runner._finalize_run(sched.id, "jE2", 0, _now())
    runner.notified.clear()
    after = store.get_schedule(sched.id)
    assert after.running is False and after.last_job_id == "jE2" and after.run_count == 2
    return after


def test_a_late_success_after_the_newer_run_parked_leaves_its_finding(
    store, runner, monkeypatch
):
    """L3a: jE2 was refused and parked; jE1's late success does not re-arm it."""
    import services.schedule_approvals as approvals

    before = _two_runs_recorded(store, runner, "late-l3a", PLAIN_FAIL, REFUSED_BY_GATE)
    assert before.approval_request["status"] == "open"
    assert before.approval_request["job_id"] == "jE2"
    closed: list[str] = []
    monkeypatch.setattr(approvals, "close_dest_exists_park_after_success",
                        lambda sid: closed.append(sid))

    runner.jobs["jE1"] = COMPLETED
    runner._finalize_run(before.id, "jE1", 0, _now())

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    assert after.approval_request == before.approval_request
    assert after.last_status == before.last_status == "needs_approval"
    assert closed == [], "a late success released a park the newer run made"


def test_a_late_refusal_after_the_newer_run_succeeded_parks_nothing(store, runner, caplog):
    """L3b: jE2 completed; jE1's late refusal does not park the schedule."""
    before = _two_runs_recorded(store, runner, "late-l3b", COMPLETED, COMPLETED)

    runner.jobs["jE1"] = REFUSED_BY_GATE
    with caplog.at_level("INFO"):
        runner._finalize_run(before.id, "jE1", 0, _now())

    after = store.get_schedule(before.id)
    assert _newer_run(after) == _newer_run(before)
    assert not (after.approval_request or {}).get("status"), after.approval_request
    assert after.last_status == "completed"
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "approval.requested" not in events


# --- Deleted while a back-off is recorded (review round 2, L-D) ---------------


@mongo_only
def test_a_back_off_on_a_schedule_deleted_meanwhile_is_not_reported(
    store, runner, monkeypatch, caplog
):
    """L1x: the back-off was decided, then the schedule was deleted before it was stored."""
    from services import schedule_run_lifecycle as life

    runner.jobs["job-0"] = TRANSIENT
    sched, _run = _claimed(store, "gone-flaky", "job-0", interval="", cron="* * * * *")
    runner._finalize_run(sched.id, "job-0", 0, _now())
    for n in (1, 2):
        store.record_run_history(sched.id, {
            "run_id": f"run_{n}", "job_id": f"job-{n}", "status": "failed",
            "error": TRANSIENT["error"],
        })
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-b", "job-3") is not None
    life.mark_running(sched.id, run_id, "job-3")
    runner.jobs["job-3"] = TRANSIENT
    runner.notified.clear()
    caplog.clear()
    state = _write_inside_finalize(store, monkeypatch, lambda: store.delete_schedule(sched.id))

    with caplog.at_level("INFO"):
        runner._finalize_run(sched.id, "job-3", 0, _now())

    assert not state["pending"], "the delete never landed inside the finalize"
    assert store.get_schedule(sched.id) is None
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "schedule.run.backoff" not in events, "a back-off for a schedule that is gone"
    # The job did fail: that is still said once (as for a refusal, I4).
    assert events.count("schedule.run.give_up") == 1
    assert runner.notified == [("job-3", "failed")]


# --- Finalizing again after a contended write (review round 2, L-E) -----------


def test_a_rebuild_failure_finalized_again_is_audited_once(store, runner, monkeypatch):
    """The beat re-finalizes a run whose first write lost; the rebuild fails once."""
    import services.audit_coverage as audit_coverage
    import services.schedule_recovery as recovery

    sched, _run = _claimed(store, "rebuild-again", "job-rebuild")
    store.update_schedule(sched.id, {"approval_request": {
        "id": "apr_cont", "status": "open", "code": recovery.DESTINATION_CONTINUITY_CODE,
        "job_id": "job-before", "occurrences": 1, "finding": "the destination changed",
        "recovery": {"action": "rebuild", "job_id": "job-rebuild", "actor": "qa@x",
                     "reason": "rebuild it", "started_at": _now().isoformat()},
    }})
    runner.jobs["job-rebuild"] = REFUSED_BY_GATE
    audited: list[dict] = []
    real = audit_coverage.record_security_event

    def capture(action, **kw):
        if action == "schedule.destination.recovered":
            audited.append(kw)
        return real(action, **kw)

    monkeypatch.setattr(audit_coverage, "record_security_event", capture)
    real_once, real_save = store._mark_schedule_run_once, store._save_all
    attempts = _contend(store, monkeypatch, sched.id)
    with pytest.raises(store.ScheduleWriteContended):
        runner._finalize_run(sched.id, "job-rebuild", 0, _now())
    assert attempts and store.get_schedule(sched.id).running is True
    # The contention ends; the beat finalizes the finished job again.
    monkeypatch.setattr(store, "_mark_schedule_run_once", real_once)
    monkeypatch.setattr(store, "_save_all", real_save)

    runner._finalize_run(sched.id, "job-rebuild", 0, _now())

    final = store.get_schedule(sched.id)
    assert final.run_count == 1 and final.running is False
    assert [a.get("outcome") for a in audited] == ["failed"], audited
    rec = (final.approval_request or {}).get("evidence", {}).get("recovery_context") or {}
    assert rec.get("recovery", {}).get("outcome") == "failed", final.approval_request


def test_a_retry_finalized_again_after_a_lost_write_is_reported_once(
    store, runner, monkeypatch, caplog
):
    """The retry write lost (nothing parked): only the write that parks it reports it."""
    runner.jobs["job-1"] = TRANSIENT
    sched, _run = _claimed(store, "retry-again", "job-1", max_retries=1,
                           retry_backoff_seconds=0)
    real_retry = store.schedule_retry
    calls: list[int] = []

    def contended_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise store.ScheduleWriteContended("lost every attempt")
        return real_retry(*args, **kwargs)

    monkeypatch.setattr(store, "schedule_retry", contended_once)

    with caplog.at_level("INFO"):
        with pytest.raises(store.ScheduleWriteContended):
            runner._finalize_run(sched.id, "job-1", 0, _now())
        assert store.get_schedule(sched.id).running is True
        runner._finalize_run(sched.id, "job-1", 0, _now())

    parked = store.get_schedule(sched.id)
    assert parked.retry_attempt == 1 and parked.running is False
    events = [getattr(r, "event", None) for r in caplog.records]
    assert events.count("schedule.run.retry") == 1, events
