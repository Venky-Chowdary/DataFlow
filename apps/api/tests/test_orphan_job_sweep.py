"""A job whose worker died is resumed or settled, never left "running".

DEF-B-025: jobs in flight during a restart or worker kill stayed ``running``
at 5% forever. The startup scan read jobs through the list view, which strips
``transfer_request``, so no orphan was ever resumed.

DEF-B-026: a reclaimed job's status froze at the dead worker's 5% while the
new worker finished the load. The fence check accepted only an equal token,
and a released lease handed the next run fence 1 again, so every write of the
new run was refused. An operator cancel could be refused the same way and was
still reported as cancelled.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from services import worker_leases
from services.mongodb_service import MemoryMongoDBService
from services.orphan_jobs import owner_signal, sweep_orphan_jobs

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
STALE = (NOW - timedelta(minutes=30)).isoformat()

_REQUEST = {
    "source": {"kind": "database", "format": "postgresql", "table": "orders"},
    "destination": {"kind": "database", "format": "mysql", "table": "orders"},
    "sync_mode": "full_refresh_append",
}


class _Leases:
    def __init__(self, held: set[str] | None = None) -> None:
        self.held = held or set()

    def is_held(self, job_id: str) -> bool:
        return job_id in self.held


def _store(*jobs: dict[str, Any]) -> MemoryMongoDBService:
    store = MemoryMongoDBService()
    for job in jobs:
        store.create_transfer_job({"updated_at": STALE, **job})
    return store


def _sweep(store: MemoryMongoDBService, **kwargs: Any) -> tuple[list[dict], list[tuple]]:
    submitted: list[tuple] = []
    out = sweep_orphan_jobs(
        mongo=store,
        lease_store=kwargs.pop("lease_store", _Leases()),
        now=NOW,
        grace_seconds=kwargs.pop("grace_seconds", 60),
        max_reclaims=kwargs.pop("max_reclaims", 2),
        local_ids=kwargs.pop("local_ids", frozenset()),
        queue_status=kwargs.pop("queue_status", lambda _jid: None),
        resubmit=lambda jid, req, resume: submitted.append((jid, req, resume)),
    )
    return out, submitted


def test_list_view_strips_the_request_the_sweep_reads() -> None:
    store = _store({"_id": "j1", "status": "running", "transfer_request": _REQUEST})
    assert "transfer_request" not in store.list_jobs(limit=10)[0]
    assert store.list_unfinished_jobs()[0]["transfer_request"] == _REQUEST


def test_running_job_with_no_owner_is_resumed_from_its_checkpoint() -> None:
    store = _store(
        {
            "_id": "j1",
            "status": "running",
            "phase": "load",
            "progress_pct": 5,
            "records_processed": 1200,
            "checkpoint": {"chunk_index": 2, "rows_processed": 1200},
            "transfer_request": _REQUEST,
        }
    )
    out, submitted = _sweep(store)

    assert out == [{"job_id": "j1", "action": "resumed", "reason": "checkpoint"}]
    assert [(jid, resume) for jid, _req, resume in submitted] == [("j1", True)]
    job = store.get_job("j1")
    assert job["status"] == "pending"
    assert job["orphan_reclaims"] == 1


def test_a_job_with_an_owner_is_left_alone() -> None:
    store = _store(
        {"_id": "leased", "status": "running", "transfer_request": _REQUEST},
        {"_id": "local", "status": "running", "transfer_request": _REQUEST},
        {"_id": "queued", "status": "pending", "transfer_request": _REQUEST},
        {
            "_id": "fresh",
            "status": "running",
            "transfer_request": _REQUEST,
            "updated_at": (NOW - timedelta(seconds=5)).isoformat(),
        },
        {"_id": "paused", "status": "paused", "transfer_request": _REQUEST},
        {"_id": "done", "status": "completed", "transfer_request": _REQUEST},
    )
    out, submitted = _sweep(
        store,
        lease_store=_Leases({"leased"}),
        local_ids=frozenset({"local"}),
        queue_status=lambda jid: "queued" if jid == "queued" else None,
    )
    assert out == [] and submitted == []
    assert store.get_job("leased")["status"] == "running"


def test_orphan_without_a_request_fails_with_its_committed_rows() -> None:
    store = _store(
        {"_id": "j1", "status": "running", "phase": "validate", "records_processed": 5}
    )
    out, submitted = _sweep(store)

    assert out[0]["action"] == "failed"
    assert submitted == []
    job = store.get_job("j1")
    assert job["status"] == "failed"
    assert job["error_code"] == "job_orphaned"
    assert job["failed_at_phase"] == "validate"
    assert "5 row(s) were committed" in job["error"]
    assert "not rolled back" in job["error"]


def test_reclaim_limit_fails_instead_of_looping() -> None:
    store = _store(
        {
            "_id": "j1",
            "status": "running",
            "orphan_reclaims": 2,
            "transfer_request": _REQUEST,
        }
    )
    out, submitted = _sweep(store, max_reclaims=2)
    assert out[0]["action"] == "failed"
    assert submitted == []
    assert "limit is 2 automatic resume" in store.get_job("j1")["error"]


def test_orphan_that_asked_to_cancel_is_recorded_cancelled() -> None:
    store = _store(
        {
            "_id": "j1",
            "status": "running",
            "cancel_requested": True,
            "transfer_request": _REQUEST,
        }
    )
    out, submitted = _sweep(store)
    assert out[0]["action"] == "cancelled"
    assert submitted == []
    assert store.get_job("j1")["status"] == "cancelled"


def test_one_bad_request_does_not_stop_the_sweep() -> None:
    store = _store(
        {"_id": "bad", "status": "running", "transfer_request": {"source": "not-a-dict"}},
        {"_id": "good", "status": "running", "transfer_request": _REQUEST},
    )
    out, _ = _sweep(store)
    actions = {row["job_id"]: row["action"] for row in out}
    assert actions == {"bad": "failed", "good": "resumed"}


def test_owner_signal_names_why_a_job_is_not_an_orphan() -> None:
    job = {"status": "running", "updated_at": STALE}
    common = dict(now=NOW, grace_seconds=60)
    assert owner_signal(job, lease_held=True, local=False, queue_status=None, **common) == "lease_held"
    assert owner_signal(job, lease_held=False, local=True, queue_status=None, **common) == "running_in_this_process"
    assert owner_signal(job, lease_held=False, local=False, queue_status="claimed", **common) == "queue_claimed"
    assert owner_signal(job, lease_held=False, local=False, queue_status="done", **common) is None


def test_a_newer_fence_supersedes_the_dead_workers_fence() -> None:
    store = _store({"_id": "j1", "status": "running", "progress_pct": 5, "lease_fence": 1})

    assert store.update_job_status("j1", "running", progress_pct=60, lease_fence=2)
    assert store.get_job("j1")["progress_pct"] == 60
    assert store.get_job("j1")["lease_fence"] == 2
    assert not store.update_job_status("j1", "running", progress_pct=7, lease_fence=1)
    assert store.get_job("j1")["progress_pct"] == 60


def test_operator_cancel_is_not_fenced() -> None:
    store = _store({"_id": "j1", "status": "running", "lease_fence": 9})
    assert store.update_job_status("j1", "cancelled", lease_fence=3, operator_command=True)
    assert store.get_job("j1")["status"] == "cancelled"


def test_acquire_never_hands_out_a_fence_below_the_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store({"_id": "fenced-job", "status": "running", "lease_fence": 5})
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: store
    )
    leases = worker_leases.WorkerLeaseStore("worker-b")
    leases._memory.pop("fenced-job", None)
    try:
        assert leases.acquire("fenced-job", ttl_seconds=30)
        assert leases.get_fence("fenced-job") == 6
    finally:
        leases.release("fenced-job")


def test_a_job_waiting_for_a_pool_thread_keeps_its_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading

    from services import transfer_scheduler

    started: list[str] = []
    monkeypatch.setattr(
        "services.lease_heartbeat.start_lease_heartbeat",
        lambda store, job_id, **kw: (started.append(job_id), lambda: None)[1],
    )
    release = threading.Event()
    future = transfer_scheduler.submit("waiting-job", release.wait, 5)
    try:
        assert started == ["waiting-job"]
        assert "waiting-job" in transfer_scheduler.local_job_ids()
    finally:
        release.set()
        future.result(timeout=5)
    assert "waiting-job" not in transfer_scheduler.local_job_ids()
