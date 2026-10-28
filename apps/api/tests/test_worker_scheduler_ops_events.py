from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from services.lease_heartbeat import start_lease_heartbeat
from services.worker_fleet import claim_next_job, enqueue_job, reclaim_stale_claims
from services.worker_leases import WorkerLeaseStore


def _events(caplog, name: str):
    return [record for record in caplog.records if getattr(record, "event", None) == name]


class _Queue:
    def __init__(self, doc=None):
        self.doc = doc
        self.rows = []

    def update_one(self, *_args, **_kwargs):
        return None

    def find_one_and_update(self, *_args, **_kwargs):
        doc, self.doc = self.doc, None
        return doc

    def find(self, *_args, **_kwargs):
        return self.rows


def test_queue_enqueue_emits_structured_outcome(monkeypatch, caplog):
    from services import worker_fleet

    monkeypatch.setattr(worker_fleet, "_queue_coll", lambda: _Queue())
    with caplog.at_level("INFO"):
        assert enqueue_job("job-enqueue-event", payload={"workspace_id": "ws-test"})
    event, = _events(caplog, "job.queue.enqueue")
    assert event.outcome == "succeeded"
    assert event.job_id == "job-enqueue-event"


def test_claim_and_reclaim_emit_structured_counts(monkeypatch, caplog):
    from services import worker_fleet

    now = datetime.now(timezone.utc)
    queue = _Queue({"_id": "claim-event", "job_id": "claim-event", "status": "queued"})
    monkeypatch.setattr(worker_fleet, "_queue_coll", lambda: queue)
    monkeypatch.setattr(worker_fleet, "_mark_transfer_job_claimed", lambda *_args: None)

    class _Lease:
        worker_id = "worker-event"

        def acquire(self, *_args, **_kwargs):
            return True

        def get_fence(self, _job_id):
            return 1

        def is_held(self, _job_id):
            return False

    with caplog.at_level("INFO"):
        assert claim_next_job(_Lease()) == "claim-event"
    acquired, = _events(caplog, "job.claim.acquire")
    assert acquired.outcome == "succeeded"
    assert acquired.worker_id == "worker-event"

    queue.rows = [{
        "_id": "stale-event",
        "job_id": "stale-event",
        "status": "claimed",
        "claimed_at": now - timedelta(minutes=10),
    }]
    class _Cursor(list):
        def limit(self, _count):
            return self

    queue.find = lambda *_args, **_kwargs: _Cursor(queue.rows)
    with caplog.at_level("INFO"):
        assert reclaim_stale_claims(lease_store=_Lease()) == 1
    reclaimed, = _events(caplog, "job.claim.reclaim")
    assert reclaimed.outcome == "succeeded"
    assert reclaimed.reclaimed_count == 1


def test_job_handler_failure_emits_type_only_event(monkeypatch, caplog):
    from services import worker_fleet

    job_id = "handler-error-event"
    sentinel = "SENTINEL_HANDLER_EXCEPTION"
    stop = threading.Event()
    claimed = False

    class _Lease:
        def release(self, _job_id):
            return None

    def _claim(_store, **_kwargs):
        nonlocal claimed
        if claimed:
            return None
        claimed = True
        stop.set()
        return job_id

    def _handler(_job_id):
        raise RuntimeError(sentinel)

    monkeypatch.setattr(worker_fleet, "WorkerLeaseStore", lambda _worker_id: _Lease())
    monkeypatch.setattr(worker_fleet, "reclaim_stale_claims", lambda: 0)
    monkeypatch.setattr(worker_fleet, "claim_next_job", _claim)
    monkeypatch.setattr(
        worker_fleet,
        "_run_with_lease_heartbeat",
        lambda _store, claimed_job_id, handler, _ttl: handler(claimed_job_id),
    )
    monkeypatch.setattr(worker_fleet, "_finish_queue_row", lambda *_args, **_kwargs: None)

    with caplog.at_level("ERROR"):
        worker_fleet.run_fleet_loop(_handler, stop_event=stop, max_inflight=1)

    error, = _events(caplog, "job.handler.run")
    assert error.outcome == "errored"
    assert error.job_id == job_id
    assert error.error_type == "RuntimeError"
    assert sentinel not in caplog.text


def test_lease_acquire_reports_held_by_other(monkeypatch, caplog):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    a = WorkerLeaseStore("worker-a-event")
    b = WorkerLeaseStore("worker-b-event")
    monkeypatch.setattr(a, "_mongo_collection", lambda: None)
    monkeypatch.setattr(b, "_mongo_collection", lambda: None)
    with caplog.at_level("INFO"):
        assert a.acquire("lease-event")
        assert not b.acquire("lease-event")
    events = _events(caplog, "job.lease.acquire")
    assert [(event.outcome, event.worker_id) for event in events] == [
        ("succeeded", "worker-a-event"),
        ("skipped", "worker-b-event"),
    ]
    assert events[1].reason_code == "held_by_other"


def test_heartbeat_failure_emits_heartbeat_and_lost_events(caplog):
    lost = threading.Event()

    class _Lease:
        def heartbeat(self, *_args, **_kwargs):
            return False

    with caplog.at_level("INFO"):
        stop = start_lease_heartbeat(
            _Lease(),
            "heartbeat-event",
            ttl_seconds=2,
            interval_seconds=0.01,
            on_lost=lost.set,
        )
        assert lost.wait(2)
        stop()
    assert _events(caplog, "job.lease.heartbeat")
    assert _events(caplog, "job.lease.lost")


def test_transfer_scheduler_lifecycle_and_held_lease(monkeypatch, caplog):
    from services import transfer_scheduler

    class _Lease:
        def acquire(self, *_args, **_kwargs):
            return False

    monkeypatch.setattr(transfer_scheduler, "_executor", None)
    monkeypatch.setattr(transfer_scheduler, "_started", threading.Event())
    monkeypatch.setattr(transfer_scheduler, "_ensure_executor", lambda: object())
    monkeypatch.setattr(transfer_scheduler, "_lease_store", _Lease())
    transfer_scheduler._shutdown = False
    try:
        with caplog.at_level("INFO"):
            transfer_scheduler.start()
            assert transfer_scheduler.submit("already-held-event", lambda: None).result() is None
            transfer_scheduler.shutdown(wait=False)
    finally:
        transfer_scheduler._shutdown = False
    assert _events(caplog, "scheduler.service.start")
    assert _events(caplog, "scheduler.service.stop")
    skipped, = _events(caplog, "schedule.run.skip")
    assert skipped.reason_code == "already_leased"


def test_schedule_fire_started_and_held(monkeypatch, caplog):
    import importlib

    from services import schedule_runner, schedule_store

    schedule = SimpleNamespace(
        id="schedule-fire-event",
        enabled=True,
        source_connector_id="source-event",
        dest_connector_id="destination-event",
    )
    monkeypatch.setattr(schedule_store, "get_schedule", lambda _sid: schedule)
    monkeypatch.setattr(schedule_runner, "_park_on_decision", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(schedule_runner, "_resolve_connector", lambda connector_id: {"id": connector_id})
    monkeypatch.setattr(
        schedule_runner,
        "build_schedule_request",
        lambda *_args, **_kwargs: SimpleNamespace(acknowledgment_actor=None),
    )
    monkeypatch.setattr(schedule_runner, "_guard_source_schema_drift", lambda *_args: False)
    monkeypatch.setattr(schedule_store, "set_running_job", lambda *_args: None)

    class _Engine:
        def _create_pending_job(self, _request):
            return "job-fire-event"

    class _Future:
        def done(self):
            return True

    engine_module = importlib.import_module("src.transfer.engine")
    background_module = importlib.import_module("src.transfer.background")
    monkeypatch.setattr(engine_module, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(background_module, "run_transfer_async", lambda *_args: _Future())
    with caplog.at_level("INFO"):
        assert schedule_runner._dispatch_transfer(schedule.id, attempt=2) == "job-fire-event"
    started, = _events(caplog, "schedule.run.fire")
    assert started.outcome == "started"
    assert started.attempt == 3
    assert started.job_id == "job-fire-event"

    caplog.clear()
    monkeypatch.setattr(
        schedule_runner,
        "build_schedule_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("SENTINEL_PREFLIGHT_DETAIL")),
    )
    with caplog.at_level("INFO"):
        assert schedule_runner._dispatch_transfer(schedule.id) is None
    held, = _events(caplog, "schedule.run.fire")
    assert held.outcome == "held"
    assert held.reason_code == "preflight_refused"
    assert "SENTINEL_PREFLIGHT_DETAIL" not in caplog.text


def test_schedule_skip_connector_missing_and_append_replay(monkeypatch, caplog):
    import importlib

    from services import schedule_runner, schedule_store

    schedule = SimpleNamespace(
        id="schedule-skip-event",
        name="schedule-skip-event",
        enabled=True,
        source_connector_id="source-event",
        dest_connector_id="destination-event",
    )
    monkeypatch.setattr(schedule_store, "get_schedule", lambda _sid: schedule)
    monkeypatch.setattr(schedule_runner, "_resolve_connector", lambda _connector_id: None)
    monkeypatch.setattr(schedule_runner, "_park_on_decision", lambda *_args, **_kwargs: None)
    with caplog.at_level("INFO"):
        assert schedule_runner._dispatch_transfer(schedule.id) is None
    missing, = _events(caplog, "schedule.run.skip")
    assert missing.outcome == "skipped"
    assert missing.reason_code == "connector_missing"

    caplog.clear()
    approvals = importlib.import_module("services.schedule_approvals")
    monkeypatch.setattr(approvals, "release_create_new_dest_exists_false_refuse", lambda **_kwargs: None)
    monkeypatch.setattr(schedule_runner, "_append_replay_blocked", lambda _schedule: "blocked")
    with caplog.at_level("INFO"):
        assert schedule_runner._run_schedule(schedule.id) is None
    replay, = _events(caplog, "schedule.run.skip")
    assert replay.outcome == "skipped"
    assert replay.reason_code == "append_replay_duplicate"


def test_schedule_gate_block_emits_skip(monkeypatch, caplog):
    from services import schedule_store

    now = datetime.now(timezone.utc)
    schedule = schedule_store.PipelineSchedule.from_dict({
        "id": "schedule-gate-event",
        "name": "schedule-gate-event",
        "enabled": True,
        "source_connector_id": "source-event",
        "source_table": "orders",
        "dest_connector_id": "destination-event",
        "dest_table": "orders_copy",
        "interval": "hourly",
        "next_run_at": (now - timedelta(hours=2)).isoformat(),
        "mappings": [{"source": "id", "target": "id"}],
        "approval_request": {"status": "open"},
    })
    monkeypatch.setattr(schedule_store, "_load_all", lambda: [schedule])
    with caplog.at_level("INFO"):
        assert schedule_store.due_schedules(now) == []
    event, = _events(caplog, "schedule.run.skip")
    assert event.outcome == "skipped"
    assert event.reason_code == "gate_blocked"
    assert event.schedule_id == schedule.id


def test_schedule_misfire_events_include_missed_count(tmp_path, monkeypatch, caplog):
    from services import schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    now = datetime.now(timezone.utc)
    schedule = schedule_store.create_schedule({
        "name": "schedule-misfire-event",
        "source_connector_id": "source-event",
        "source_table": "orders",
        "dest_connector_id": "destination-event",
        "dest_table": "orders_copy",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    overdue = (now - timedelta(hours=4)).isoformat()
    schedule_store._save_all([
        schedule_store.PipelineSchedule.from_dict({
            **schedule.to_dict(),
            "next_run_at": overdue,
        })
    ])
    with caplog.at_level("INFO"):
        schedule_store.mark_schedule_run(schedule.id, "job-misfire-event", status="completed")
    completion_event, = _events(caplog, "schedule.run.skip")
    assert completion_event.reason_code == "misfire"
    assert completion_event.missed_count > 0

    caplog.clear()
    claim_schedule = schedule_store.create_schedule({
        "name": "schedule-claim-misfire-event",
        "source_connector_id": "source-event",
        "source_table": "orders",
        "dest_connector_id": "destination-event",
        "dest_table": "orders_copy",
        "shared_destination_acknowledged": True,
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    schedule_store._save_all([
        schedule_store.PipelineSchedule.from_dict({
            **claim_schedule.to_dict(),
            "next_run_at": overdue,
        })
    ])
    with caplog.at_level("INFO"):
        schedule_store.mark_schedule_running(claim_schedule.id, "job-claim-misfire-event")
    claim_event, = _events(caplog, "schedule.run.skip")
    assert claim_event.reason_code == "misfire"
    assert claim_event.missed_count > 0

    caplog.clear()
    pinned_at = (now - timedelta(hours=4)).isoformat()
    schedule_store._save_all([
        schedule_store.PipelineSchedule.from_dict({
            **schedule.to_dict(),
            "last_run_at": pinned_at,
            "next_run_at": pinned_at,
            "running": False,
        })
    ])
    with caplog.at_level("INFO"):
        assert schedule_store.unstick_completion_pinned_schedules(now) == 1
    unstick_event, = _events(caplog, "schedule.run.skip")
    assert unstick_event.reason_code == "misfire"
    assert unstick_event.missed_count > 0


def test_schedule_retry_and_give_up_events(monkeypatch, caplog):
    import importlib

    from services import schedule_runner, schedule_store

    schedule = SimpleNamespace(
        id="schedule-retry-event",
        last_job_id="",
        max_retries=2,
        retry_backoff_seconds=0,
        sync_mode="full_refresh_overwrite",
        primary_key="",
        cursor_column="",
    )
    monkeypatch.setattr(schedule_store, "get_schedule", lambda _sid: schedule)
    monkeypatch.setattr(schedule_runner, "_job_doc", lambda _jid: {"status": "failed"})
    monkeypatch.setattr(schedule_runner, "_run_entry", lambda *_args: {})
    contract = importlib.import_module("services.execution_engine_contract")
    monkeypatch.setattr(contract, "committed_rows_of", lambda _doc: (0, True))
    monkeypatch.setattr(schedule_store, "schedule_retry", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(schedule_store, "mark_schedule_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(schedule_runner, "_park_reason", lambda *_args: None)
    monkeypatch.setattr(schedule_runner, "_notify_schedule", lambda *_args: None)

    monkeypatch.setattr(
        schedule_runner,
        "_retry_decision",
        lambda *_args, **_kwargs: {
            "retry": True,
            "reason": "",
            "failure_class": {"kind": "transient"},
        },
    )
    with caplog.at_level("INFO"):
        schedule_runner._finalize_run(schedule.id, "job-retry", 0, datetime.now(timezone.utc))
    retry, = _events(caplog, "schedule.run.retry")
    assert retry.outcome == "retrying"
    assert retry.attempt == 1
    assert retry.classification == "transient"

    caplog.clear()
    monkeypatch.setattr(
        schedule_runner,
        "_retry_decision",
        lambda *_args, **_kwargs: {
            "retry": False,
            "reason": "budget exhausted",
            "failure_class": {"kind": "deterministic"},
        },
    )
    with caplog.at_level("INFO"):
        schedule_runner._finalize_run(schedule.id, "job-give-up", 1, datetime.now(timezone.utc))
    give_up, = _events(caplog, "schedule.run.give_up")
    assert give_up.outcome == "failed"
    assert give_up.attempt == 2
    assert give_up.classification == "deterministic"


def test_scheduler_lock_acquire_reports_unavailable_and_backend_error(monkeypatch, caplog):
    import importlib

    from services import schedule_runner

    worker_leases = importlib.import_module("services.worker_leases")
    monkeypatch.setattr(worker_leases, "requires_distributed_backend", lambda: True)
    monkeypatch.setattr(schedule_runner, "_mongo_backend", lambda: None)
    with caplog.at_level("INFO"):
        assert schedule_runner._acquire_scheduler_lock() is False
    unavailable, = _events(caplog, "scheduler.lock.acquire")
    assert unavailable.outcome == "failed"
    assert unavailable.reason_code == "backend_unavailable"

    class _Collection:
        def find_one_and_update(self, *_args, **_kwargs):
            raise RuntimeError("SENTINEL_LOCK_DETAIL")

    class _Database:
        def __getitem__(self, _name):
            return _Collection()

    class _Mongo:
        def get_database(self):
            return _Database()

    class _HeldCollection:
        def find_one_and_update(self, *_args, **_kwargs):
            return {"instance": "other-instance"}

    class _HeldDatabase:
        def __getitem__(self, _name):
            return _HeldCollection()

    class _HeldMongo:
        def get_database(self):
            return _HeldDatabase()

    class DuplicateKeyError(Exception):
        pass

    class _RaceCollection:
        def find_one_and_update(self, *_args, **_kwargs):
            raise DuplicateKeyError("duplicate")

    class _RaceDatabase:
        def __getitem__(self, _name):
            return _RaceCollection()

    class _RaceMongo:
        def get_database(self):
            return _RaceDatabase()

    caplog.clear()
    monkeypatch.setattr(schedule_runner, "_mongo_backend", lambda: _HeldMongo())
    with caplog.at_level("INFO"):
        assert schedule_runner._acquire_scheduler_lock() is False
    held, = _events(caplog, "scheduler.lock.acquire")
    assert held.outcome == "failed"
    assert held.reason_code == "held_by_other"

    caplog.clear()
    monkeypatch.setattr(schedule_runner, "_mongo_backend", lambda: _RaceMongo())
    with caplog.at_level("INFO"):
        assert schedule_runner._acquire_scheduler_lock() is False
    race, = _events(caplog, "scheduler.lock.acquire")
    assert race.outcome == "failed"
    assert race.reason_code == "race"

    caplog.clear()
    monkeypatch.setattr(schedule_runner, "_mongo_backend", lambda: _Mongo())
    with caplog.at_level("INFO"):
        assert schedule_runner._acquire_scheduler_lock() is False
    error, = _events(caplog, "scheduler.lock.acquire")
    assert error.outcome == "errored"
    assert error.reason_code == "lock_error"
    assert error.error_type == "RuntimeError"
    assert "SENTINEL_LOCK_DETAIL" not in caplog.text
