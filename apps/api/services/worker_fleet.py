"""Optional transfer worker fleet — pull jobs from a Mongo queue.

When ``DATAFLOW_WORKER_FLEET=1``, API replicas enqueue job ids and dedicated
worker processes (or API threads) claim them via the same lease/fencing store.
This separates HTTP capacity from execution capacity after leases are correct.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from services.worker_leases import WorkerLeaseStore, requires_distributed_backend, worker_id

_logger = logging.getLogger(__name__)


def _queue_coll():  # type: ignore[no-untyped-def]
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if getattr(mongo, "client", None):
            return mongo.get_database()["transfer_job_queue"]
    except Exception:
        return None
    return None


def fleet_enabled() -> bool:
    return os.getenv("DATAFLOW_WORKER_FLEET", "").lower() in ("1", "true", "yes")


def enqueue_job(job_id: str, *, payload: dict[str, Any] | None = None) -> bool:
    """Enqueue a job for a fleet worker. Returns False if queue unavailable."""
    coll = _queue_coll()
    if coll is None:
        if requires_distributed_backend() and fleet_enabled():
            _logger.error("Fleet enabled but Mongo queue unavailable; refuse enqueue for %s", job_id)
            return False
        return False
    try:
        coll.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "job_id": job_id,
                    "status": "queued",
                    "payload": payload or {},
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
        return True
    except Exception:
        _logger.exception("Failed to enqueue job %s", job_id)
        return False


def claim_next_job(lease_store: WorkerLeaseStore | None = None, ttl_seconds: int = 60) -> str | None:
    """Claim the oldest queued job under a worker lease. Returns job_id or None."""
    coll = _queue_coll()
    if coll is None:
        return None
    store = lease_store or WorkerLeaseStore(worker_id())
    try:
        doc = coll.find_one_and_update(
            {"status": "queued"},
            {"$set": {"status": "claimed", "claimed_at": datetime.now(timezone.utc), "worker": store.worker_id}},
            sort=[("created_at", 1)],
            return_document=True,
        )
        if not doc:
            return None
        job_id = str(doc.get("job_id") or doc.get("_id"))
        if not store.acquire(job_id, ttl_seconds=ttl_seconds):
            coll.update_one({"_id": doc["_id"]}, {"$set": {"status": "queued", "worker": ""}})
            return None
        return job_id
    except Exception:
        _logger.exception("claim_next_job failed")
        return None


def run_fleet_loop(
    handler: Callable[[str], None],
    *,
    poll_seconds: float = 2.0,
    stop_event: threading.Event | None = None,
) -> None:
    """Blocking loop for a worker process: claim → handle → release."""
    stop = stop_event or threading.Event()
    store = WorkerLeaseStore(worker_id())
    while not stop.is_set():
        job_id = claim_next_job(store)
        if not job_id:
            stop.wait(poll_seconds)
            continue
        try:
            handler(job_id)
            coll = _queue_coll()
            if coll is not None:
                coll.update_one({"_id": job_id}, {"$set": {"status": "done", "finished_at": datetime.now(timezone.utc)}})
        except Exception:
            _logger.exception("Fleet handler failed for %s", job_id)
            coll = _queue_coll()
            if coll is not None:
                coll.update_one({"_id": job_id}, {"$set": {"status": "failed"}})
        finally:
            store.release(job_id)
        time.sleep(0.05)
