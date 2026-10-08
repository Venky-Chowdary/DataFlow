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
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.scheduler_mode import api_claim_loop_enabled, claim_queue_enabled
from services.worker_leases import WorkerLeaseStore, requires_distributed_backend, worker_id

_logger = logging.getLogger(__name__)

_api_claim_stop: threading.Event | None = None
_api_claim_thread: threading.Thread | None = None
# One pool for the process. Created at the claim cap before the loop so a
# later larger TRANSFER_WORKERS is not stuck on the first cap that ran.
_fleet_pool: ThreadPoolExecutor | None = None
_fleet_pool_cap: int = 0


def fleet_executor(inflight_cap: int) -> ThreadPoolExecutor:
    """Thread pool sized to the claim cap.

    The pool used to be created on the first concurrent job and then reused
    forever, so a loop that started at 1 worker never grew when
    ``TRANSFER_WORKERS`` was 8. A larger cap replaces the smaller pool. A
    smaller later cap keeps the larger pool. ``inflight_cap == 1`` does not
    call this — that path stays serial.
    """
    global _fleet_pool, _fleet_pool_cap
    cap = max(1, int(inflight_cap))
    if _fleet_pool is not None and _fleet_pool_cap >= cap:
        return _fleet_pool
    previous = _fleet_pool
    pool = ThreadPoolExecutor(max_workers=cap, thread_name_prefix="df-fleet")
    _fleet_pool = pool
    _fleet_pool_cap = cap
    if previous is not None:
        previous.shutdown(wait=False, cancel_futures=False)
    return pool


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


def _workload_of(payload: dict[str, Any] | None) -> str:
    raw = str((payload or {}).get("workload") or "batch").strip().lower()
    if raw not in ("cdc", "batch"):
        return "batch"
    return raw


def enqueue_job(job_id: str, *, payload: dict[str, Any] | None = None) -> bool:
    """Enqueue a job for a fleet worker. Returns False if queue unavailable.

    ``workload`` is stamped on the queue document so a batch worker can skip
    CDC rows without reading the transfer payload.
    """
    coll = _queue_coll()
    if coll is None:
        if requires_distributed_backend() and fleet_enabled():
            _logger.error("Fleet enabled but Mongo queue unavailable; refuse enqueue for %s", job_id)
            return False
        return False
    body = dict(payload or {})
    workload = _workload_of(body)
    body["workload"] = workload
    try:
        coll.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "job_id": job_id,
                    "status": "queued",
                    "payload": body,
                    "workload": workload,
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


def cancel_queued_job(job_id: str) -> dict[str, Any]:
    """Stop the queue from starting or restarting this job.

    Cancel on the transfer document alone left the queue row ``queued``,
    so the worker claimed it and started the write. A ``claimed`` row has
    to leave the queue too: once its lease expires, reclaim would start a
    second writer while the first is still inside the write. The writer
    that already holds the row observes ``cancel_requested`` and stops.
    """
    coll = _queue_coll()
    if coll is None or not job_id:
        return {"queue": "unavailable"}
    try:
        result = coll.update_one(
            {"_id": job_id, "status": {"$in": ["queued", "claimed"]}},
            {
                "$set": {
                    "status": "cancelled",
                    "worker": "",
                    "finished_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
    except Exception:
        _logger.exception("Failed to cancel queued job %s", job_id)
        return {"queue": "unavailable"}
    if getattr(result, "modified_count", 0):
        return {"queue": "cancelled"}
    return {"queue": "not_waiting"}


def _transfer_job_cancelled(job_id: str) -> bool:
    """True when the operator already cancelled this transfer."""
    if not job_id:
        return False
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if mongo.is_cancel_requested(job_id):
            return True
        job = mongo.get_job(job_id) or {}
        return str(job.get("status") or "") == "cancelled"
    except Exception:
        _logger.debug("cancel check failed for %s", job_id, exc_info=True)
        return False


def _queued_claim_filter() -> dict[str, Any]:
    """Oldest-queued filter limited to this process's workloads.

    Unset ``WORKER_MODE`` (or both tokens) keeps ``{status: queued}`` so a
    single worker still drains the whole queue. Batch-only uses ``$ne: cdc``
    so a row written before workloads existed is still batch work.
    """
    from services.process_role import worker_workloads

    loads = worker_workloads()
    filt: dict[str, Any] = {"status": "queued"}
    if loads == frozenset({"batch"}):
        filt["workload"] = {"$ne": "cdc"}
    elif loads == frozenset({"cdc"}):
        filt["workload"] = "cdc"
    return filt


def _can_reclaim_workload(doc: dict[str, Any]) -> bool:
    from services.process_role import worker_workloads

    loads = worker_workloads()
    workload = str(doc.get("workload") or "batch")
    if workload == "cdc":
        return "cdc" in loads
    return "batch" in loads


def claim_next_job(lease_store: WorkerLeaseStore | None = None, ttl_seconds: int = 60) -> str | None:
    """Claim the oldest queued job this worker is allowed to run."""
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
        skipped = 0
        while skipped < 20:
            doc = coll.find_one_and_update(
                _queued_claim_filter(),
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
            if _transfer_job_cancelled(job_id):
                coll.update_one(
                    {"_id": doc["_id"], "status": "claimed", "worker": store.worker_id},
                    {
                        "$set": {
                            "status": "cancelled",
                            "worker": "",
                            "finished_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )
                skipped += 1
                continue
            break
        else:
            return None
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


def reclaim_stale_claims(
    *,
    older_than_seconds: int = 120,
    lease_store: WorkerLeaseStore | None = None,
) -> int:
    """Re-queue claimed jobs whose worker died before finishing.

    A live lease is not stale, even when ``claimed_at`` is old — CDC holds
    that lease for the life of the slot. A batch worker also leaves CDC rows
    alone so a batch scale-down cannot requeue a capture it does not own.
    """
    coll = _queue_coll()
    if coll is None:
        return 0
    store = lease_store or WorkerLeaseStore(worker_id())
    cutoff = datetime.now(timezone.utc).timestamp() - max(30, int(older_than_seconds))
    try:
        # claimed_at may be datetime; compare loosely via updated_at when present.
        stale = list(coll.find({"status": "claimed"}).limit(200))
        n = 0
        for doc in stale:
            if not _can_reclaim_workload(doc):
                continue
            job_id = str(doc.get("job_id") or doc.get("_id") or "")
            if job_id and _transfer_job_cancelled(job_id):
                coll.update_one(
                    {"_id": doc["_id"], "status": "claimed"},
                    {
                        "$set": {
                            "status": "cancelled",
                            "worker": "",
                            "finished_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )
                continue
            if job_id and store.is_held(job_id):
                continue
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


def _mark_fleet_lease_lost(job_id: str) -> None:
    """Stop the writer when its lease heartbeat is refused.

    The flag is what the write loop reads. A status write can lose the
    fence to the worker that took the lease; the flag does not.
    """
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        mongo.request_job_cancel(job_id)
        mongo.update_job_status(
            job_id,
            "cancelled",
            phase="cancelled",
            error="Lease lost to another worker; aborting to prevent dual writes",
            message="Lease lost — cooperative cancel",
        )
    except Exception:  # noqa: BLE001 - lease loss must still stop the writer
        _logger.exception("Failed to mark job %s cancelled after lease loss", job_id)


def _run_with_lease_heartbeat(
    store: WorkerLeaseStore,
    job_id: str,
    handler: Callable[[str], None],
    ttl_seconds: int | None = None,
) -> None:
    """Run ``handler`` while this process still owns the lease."""
    from services.lease_heartbeat import start_lease_heartbeat

    stop = start_lease_heartbeat(
        store,
        job_id,
        ttl_seconds=ttl_seconds,
        on_lost=lambda: _mark_fleet_lease_lost(job_id),
    )
    try:
        handler(job_id)
    finally:
        stop()


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
    from services.lease_heartbeat import lease_ttl_seconds

    # Claim and heartbeat must share one TTL. A 60s claim with a longer
    # heartbeat interval expires before the first extension, and reclaim
    # starts a second overwrite.
    lease_ttl = lease_ttl_seconds()
    inflight: dict[str, Future[Any]] = {}
    # Size the pool before the first claim. Creating it inside the loop left
    # every later call on the first cap that happened to run.
    pool = fleet_executor(inflight_cap) if inflight_cap > 1 else None

    def _reap() -> None:
        done = [jid for jid, fut in list(inflight.items()) if fut.done()]
        for jid in done:
            fut = inflight.pop(jid)
            try:
                fut.result()
                _finish_queue_row(
                    jid,
                    status="cancelled" if _transfer_job_cancelled(jid) else "done",
                )
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
        job_id = claim_next_job(store, ttl_seconds=lease_ttl)
        if not job_id:
            stop.wait(poll_seconds)
            continue
        if inflight_cap == 1:
            try:
                _run_with_lease_heartbeat(store, job_id, handler, lease_ttl)
                _finish_queue_row(
                    job_id,
                    status="cancelled" if _transfer_job_cancelled(job_id) else "done",
                )
            except Exception:
                _logger.exception("Fleet handler failed for %s", job_id)
                _finish_queue_row(job_id, status="failed")
            finally:
                store.release(job_id)
            time.sleep(0.05)
            continue
        # Concurrent path. The lease was already acquired in claim_next_job;
        # transfer_scheduler.submit would try to acquire again and skip.
        # Heartbeat here — submit's heartbeat does not run on this path.
        if pool is None:
            raise RuntimeError("Fleet pool was not opened for a concurrent claim loop")
        inflight[job_id] = pool.submit(
            _run_with_lease_heartbeat, store, job_id, handler, lease_ttl
        )
        time.sleep(0.05)

    _reap()


def api_claim_inflight() -> int:
    """How many transfers one API process may run at once.

    The claim loop used to pin this at 1, so every job waited behind the
    previous one and a schedule fire stuck in that queue was cancelled when
    the next slot arrived. ``TRANSFER_WORKERS`` is the same cap the local
    scheduler already uses.
    """
    try:
        return max(1, int(getenv_brand("TRANSFER_WORKERS", "8") or "8"))
    except ValueError:
        return 8


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
        run_fleet_loop(
            run_fleet_job,
            poll_seconds=secs,
            stop_event=stop,
            max_inflight=api_claim_inflight(),
        )

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
