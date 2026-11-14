"""Ops review: tests for the three behaviours no test caught (mutants M10, M16, M20).

- M10: a job superseded for the drain that completed in the same instant was
  re-queued, so a completed append would run twice.
- M16: ``stop_api_claim_loop`` without its join returned while the loop
  thread was still draining.
- M20: a read cache that never expires kept a revoked setting in force; the
  2 s window was untested.
"""

from __future__ import annotations

import json
import threading
import time
import uuid

import pytest

from services import shared_doc_store, tenant_store, worker_fleet
from tests._control_plane_fakes import install_shared_store


class _Queue:
    def __init__(self, job_id: str) -> None:
        self.rows = {job_id: {"_id": job_id, "job_id": job_id, "status": "queued", "worker": ""}}
        self._lock = threading.Lock()

    def claim(self, worker):
        with self._lock:
            for row in self.rows.values():
                if row["status"] == "queued":
                    row.update(status="claimed", worker=worker)
                    return row["_id"]
        return None

    def update_one(self, flt, update):
        with self._lock:
            row = self.rows.get(flt.get("_id"))
            if row is None or ("status" in flt and row["status"] != flt["status"]):
                return
            row.update(update.get("$set") or {})


@pytest.mark.parametrize("max_inflight", [1, 4])
def test_a_completed_job_is_not_requeued_by_the_drain(monkeypatch, max_inflight):
    """M10: superseded for the hand-over, but the append had already completed."""
    from services import mongodb_service

    store = mongodb_service.MemoryMongoDBService()
    store.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", store)
    job_id = f"job-done-{uuid.uuid4().hex[:8]}"
    store.create_transfer_job({"job_id": job_id, "status": "running"})
    queue = _Queue(job_id)
    monkeypatch.setattr(worker_fleet, "_queue_coll", lambda: queue)
    monkeypatch.setattr(worker_fleet, "reclaim_stale_claims", lambda **_k: 0)
    monkeypatch.setattr(worker_fleet, "_transfer_job_cancelled", lambda _job: False)

    def claim(lease_store, ttl_seconds=60):
        claimed = queue.claim(lease_store.worker_id)
        return claimed if claimed and lease_store.acquire(claimed, ttl_seconds=ttl_seconds) else None

    monkeypatch.setattr(worker_fleet, "claim_next_job", claim)
    superseded = threading.Event()
    monkeypatch.setattr(worker_fleet, "_supersede_local_run", lambda _job: superseded.set() or True)
    stop = threading.Event()

    def handler(jid):
        stop.set()  # SIGTERM while the last batch is written
        assert superseded.wait(10)
        store.update_job_status(jid, "completed")  # the append finished at the same instant

    runner = threading.Thread(
        target=worker_fleet.run_fleet_loop,
        kwargs=dict(handler=handler, poll_seconds=0.05, stop_event=stop, max_inflight=max_inflight, drain_seconds=0.9),
    )
    runner.start()
    runner.join(15)
    assert not runner.is_alive()
    assert queue.rows[job_id]["status"] == "done", "a completed append was put back on the queue to run again"


def test_stop_api_claim_loop_waits_for_the_loop_to_return(monkeypatch):
    """M16: the stop joins the claim loop (bounded by its timeout)."""
    stop = threading.Event()
    returned = threading.Event()

    def loop():
        stop.wait(10)
        time.sleep(0.3)  # the drain after the stop flag
        returned.set()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    monkeypatch.setattr(worker_fleet, "_api_claim_stop", stop)
    monkeypatch.setattr(worker_fleet, "_api_claim_thread", thread)
    worker_fleet.stop_api_claim_loop(timeout=5)
    assert returned.is_set() and not thread.is_alive(), "returned while the claim loop was still draining"
    assert worker_fleet._api_claim_thread is None and worker_fleet._api_claim_stop is None


def test_the_read_cache_expires_within_its_two_second_bound(tmp_path, monkeypatch):
    """M20: a change saved on another replica is enforced here within 2 s (the default)."""
    db = install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant = tenant_store.create_tenant("ws-ttl", "TTL", sso_enforced=False)
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        monkeypatch.delenv(f"{prefix}CONTROL_STORE_CACHE_SECONDS", raising=False)
    assert shared_doc_store._cache_seconds() == 2.0
    assert tenant_store.get_tenant(tenant.id).sso_enforced is False  # cached from here

    item = db["control_plane_items"].find_one({"_id": f"tenant_settings:{tenant.id}"})
    body = json.loads(item["body"])
    body["sso_enforced"] = True
    db["control_plane_items"].update_one({"_id": item["_id"]}, {"$set": {"body": json.dumps(body)}, "$inc": {"rev": 1}})
    changed_at = time.monotonic()
    assert tenant_store.get_tenant(tenant.id).sso_enforced is False, "the cache must serve within its window"
    time.sleep(max(0.0, 2.05 - (time.monotonic() - changed_at)))
    assert tenant_store.get_tenant(tenant.id).sso_enforced is True, "the cache outlived its 2 s bound"


def test_the_cache_window_cannot_be_configured_beyond_its_bound(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "3600")
    assert shared_doc_store._cache_seconds() <= shared_doc_store.MAX_CACHE_SECONDS == 10.0
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "not-a-number")
    assert shared_doc_store._cache_seconds() == 2.0
