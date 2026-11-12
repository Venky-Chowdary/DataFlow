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


def _write_inside_finalize(store, monkeypatch, peer_write):
    """Run ``peer_write()`` once, after the finalizer has loaded and decided."""
    real_save, real_mark = store._save_all, store.mark_schedule_run
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

    monkeypatch.setattr(store, "mark_schedule_run", mark)
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
