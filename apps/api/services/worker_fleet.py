"""Durable transfer job queue — Mongo claim + worker leases (Phase F5).

When claim mode is on (``SCHEDULER_MODE=claim`` / ``auto`` on multi-replica, or
``WORKER_FLEET=1``), API replicas enqueue job ids into ``transfer_job_queue``.
Workers (dedicated ``src.worker_main`` and/or the API claim loop) pull under
``worker_leases`` fencing so two replicas cannot execute the same job.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.scheduler_mode import api_claim_loop_enabled, claim_queue_enabled
from services.worker_leases import WorkerLeaseStore, requires_distributed_backend, worker_id

_logger = logging.getLogger(__name__)

_api_claim_stop: threading.Event | None = None
_api_claim_thread: threading.Thread | None = None


def _queue_coll():  # type: ignore[no-untyped-def]
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection("transfer_job_queue")
    except Exception:
        return None


def fleet_enabled() -> bool:
    """True when transfers should enqueue to the Mongo worker queue (Phase F5).

    Resolved via :func:`services.scheduler_mode.claim_queue_enabled`.
    """
    return claim_queue_enabled()


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
        try:
            from pymongo import ReturnDocument

            return_doc = ReturnDocument.AFTER
        except Exception:
            return_doc = True  # type: ignore[assignment]
        doc = coll.find_one_and_update(
            {"status": "queued"},
            {
                "$set": {
                    "status": "claimed",
                    "claimed_at": datetime.now(timezone.utc),
                    "worker": store.worker_id,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            sort=[("created_at", 1)],
            return_document=return_doc,
        )
        if not doc:
            return None
        job_id = str(doc.get("job_id") or doc.get("_id"))
        if not store.acquire(job_id, ttl_seconds=ttl_seconds):
            coll.update_one(
                {"_id": doc["_id"], "status": "claimed", "worker": store.worker_id},
                {"$set": {"status": "queued", "worker": "", "updated_at": datetime.now(timezone.utc)}},
            )
            return None
        _mark_transfer_job_claimed(job_id, store)
        return job_id
    except Exception:
        _logger.exception("claim_next_job failed")
        return None


def _mark_transfer_job_claimed(job_id: str, store: WorkerLeaseStore) -> None:
    """Best-effort CAS on transfer_jobs for operator-visible ownership."""
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        fence = store.get_fence(job_id)
        # Prefer atomic pending/queued → running when the helper exists.
        updater = getattr(mongo, "claim_job_for_execution", None)
        if callable(updater):
            updater(job_id, worker_id=store.worker_id, lease_fence=fence)
            return
        mongo.update_job_status(
            job_id,
            "running",
            phase="claimed",
            message=f"Claimed by worker {store.worker_id}",
            lease_fence=fence,
        )
    except Exception as exc:
        _logger.debug("transfer_jobs claim stamp skipped for %s: %s", job_id, exc)


def reclaim_stale_claims(*, older_than_seconds: int = 120) -> int:
    """Re-queue claimed jobs whose worker died before finishing."""
    coll = _queue_coll()
    if coll is None:
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - max(30, int(older_than_seconds))
    try:
        # claimed_at may be datetime; compare loosely via updated_at when present.
        stale = list(coll.find({"status": "claimed"}).limit(200))
        n = 0
        for doc in stale:
            claimed = doc.get("claimed_at") or doc.get("updated_at")
            ts = None
            if isinstance(claimed, datetime):
                ts = claimed.replace(tzinfo=timezone.utc).timestamp() if claimed.tzinfo is None else claimed.timestamp()
            elif isinstance(claimed, (int, float)):
                ts = float(claimed)
            if ts is None or ts > cutoff:
                continue
            coll.update_one(
                {"_id": doc["_id"], "status": "claimed"},
                {"$set": {"status": "queued", "worker": "", "updated_at": datetime.now(timezone.utc)}},
            )
            n += 1
        return n
    except Exception:
        _logger.exception("reclaim_stale_claims failed")
        return 0


def _finish_queue_row(job_id: str, *, status: str) -> None:
    coll = _queue_coll()
    if coll is None:
        return
    coll.update_one(
        {"_id": job_id},
        {
            "$set": {
                "status": status,
                "finished_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )


def run_fleet_loop(
    handler: Callable[[str], None],
    *,
    poll_seconds: float = 2.0,
    stop_event: threading.Event | None = None,
    max_inflight: int | None = None,
) -> None:
    """Blocking loop for a worker process: reclaim → claim → handle → release.

    When ``max_inflight`` > 1, claims feed a local thread pool so one worker
    process can run several transfers concurrently (bounded by TRANSFER_WORKERS).
    """
    stop = stop_event or threading.Event()
    store = WorkerLeaseStore(worker_id())
    try:
        inflight_cap = int(
            max_inflight
            if max_inflight is not None
            else (getenv_brand("TRANSFER_WORKERS", "8") or "8")
        )
    except ValueError:
        inflight_cap = 8
    inflight_cap = max(1, inflight_cap)
    inflight: dict[str, Future[Any]] = {}

    def _reap() -> None:
        done = [jid for jid, fut in list(inflight.items()) if fut.done()]
        for jid in done:
            fut = inflight.pop(jid)
            try:
                fut.result()
                _finish_queue_row(jid, status="done")
            except Exception:
                _logger.exception("Fleet handler failed for %s", jid)
                _finish_queue_row(jid, status="failed")
            finally:
                store.release(jid)

    while not stop.is_set():
        reclaim_stale_claims()
        _reap()
        if len(inflight) >= inflight_cap:
            stop.wait(min(poll_seconds, 0.5))
            continue
        job_id = claim_next_job(store)
        if not job_id:
            stop.wait(poll_seconds)
            continue
        if inflight_cap == 1:
            try:
                handler(job_id)
                _finish_queue_row(job_id, status="done")
            except Exception:
                _logger.exception("Fleet handler failed for %s", job_id)
                _finish_queue_row(job_id, status="failed")
            finally:
                store.release(job_id)
            time.sleep(0.05)
            continue
        # Concurrent path — submit onto the durable transfer scheduler pool.
        # The lease was already acquired in claim_next_job; transfer_scheduler.submit
        # would try to acquire again and skip. Run handler in a bare thread via
        # the executor without a second lease: use a private pool.
        from concurrent.futures import ThreadPoolExecutor

        if not hasattr(run_fleet_loop, "_pool"):
            run_fleet_loop._pool = ThreadPoolExecutor(  # type: ignore[attr-defined]
                max_workers=inflight_cap, thread_name_prefix="df-fleet"
            )
        pool: ThreadPoolExecutor = run_fleet_loop._pool  # type: ignore[attr-defined]
        inflight[job_id] = pool.submit(handler, job_id)
        time.sleep(0.05)

    _reap()


def start_api_claim_loop(*, poll_seconds: float | None = None) -> bool:
    """Start a daemon claim loop inside the API process (Phase F5).

    Returns True when the loop was started (or already running).
    """
    global _api_claim_stop, _api_claim_thread
    if not api_claim_loop_enabled():
        return False
    if _api_claim_thread is not None and _api_claim_thread.is_alive():
        return True
    from src.transfer.background import run_fleet_job

    stop = threading.Event()
    _api_claim_stop = stop

    def _run() -> None:
        try:
            secs = float(
                poll_seconds
                if poll_seconds is not None
                else (getenv_brand("WORKER_POLL", "2") or "2")
            )
        except ValueError:
            secs = 2.0
        _logger.info(
            "API claim loop starting (worker_id=%s, mode=claim)",
            worker_id(),
        )
        run_fleet_loop(run_fleet_job, poll_seconds=secs, stop_event=stop, max_inflight=1)

    _api_claim_thread = threading.Thread(
        target=_run, name="df-api-claim", daemon=True
    )
    _api_claim_thread.start()
    return True


def stop_api_claim_loop() -> None:
    global _api_claim_stop, _api_claim_thread
    if _api_claim_stop is not None:
        _api_claim_stop.set()
    _api_claim_thread = None
    _api_claim_stop = None
