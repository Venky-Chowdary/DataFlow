"""Durable, process-level transfer scheduler.

Replaces ad-hoc daemon threads with a bounded `ThreadPoolExecutor`. Jobs submitted
through the scheduler survive the originating request/response cycle and are
tracked through `job_store` / MongoDB. On startup the API scans persisted jobs and
resubmits orphans so transfers can resume from checkpoints after a restart.
"""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
import os
import threading
from typing import Any, Callable

from services.worker_leases import WorkerLeaseStore, worker_id

_logger = logging.getLogger(__name__)

_executor: concurrent.futures.ThreadPoolExecutor | None = None
_started = threading.Event()
_shutdown = False
_worker_id = worker_id()
_lease_store = WorkerLeaseStore(_worker_id)


def _ensure_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Lazily create the shared thread pool."""
    global _executor, _shutdown
    if _shutdown:
        raise RuntimeError("Transfer scheduler has been shut down")
    if _executor is None or _executor._shutdown:
        max_workers = max(1, int(os.getenv("DATAFLOW_TRANSFER_WORKERS", "4")))
        _executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="df-transfer-",
        )
        atexit.register(shutdown, wait=False)
    return _executor


def start() -> None:
    """Start the scheduler and make it ready to accept jobs."""
    _ensure_executor()
    _started.set()
    _logger.info("Transfer scheduler started")


def shutdown(wait: bool = True) -> None:
    """Gracefully stop accepting new work and optionally wait for in-flight jobs."""
    global _executor, _shutdown
    _shutdown = True
    _started.clear()
    if _executor:
        _executor.shutdown(wait=wait, cancel_futures=False)
        _executor = None
    _logger.info("Transfer scheduler shut down")


def running() -> bool:
    return _started.is_set()


def submit(job_id: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> concurrent.futures.Future:
    """Schedule a transfer job on the durable thread pool.

    A short-lived worker lease is acquired for ``job_id`` so multiple Railway
    replicas do not run the same transfer concurrently.  If another replica
    already holds the lease, the duplicate submission is ignored.

    While the job runs, a background thread heartbeats the lease every half TTL
    so long-running transfers are not stolen by another replica.
    """
    executor = _ensure_executor()
    if not _started.is_set():
        _started.set()

    ttl_seconds = int(os.getenv("DATAFLOW_WORKER_LEASE_TTL", "60"))
    if not _lease_store.acquire(job_id, ttl_seconds=ttl_seconds):
        _logger.warning("Transfer job %s is already leased by another worker; skipping", job_id)
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_result(None)
        return future

    _logger.info("Scheduling transfer job %s", job_id)

    def _leased_fn(*a: Any, **kw: Any) -> Any:
        stop_event = threading.Event()
        interval = max(5, ttl_seconds // 2)

        def _heartbeat() -> None:
            while not stop_event.wait(interval):
                if not _lease_store.heartbeat(job_id, ttl_seconds=ttl_seconds):
                    _logger.warning("Lease heartbeat failed for job %s; another worker may have taken over", job_id)
                    break

        beat_thread = threading.Thread(target=_heartbeat, name=f"df-lease-{job_id}", daemon=True)
        beat_thread.start()
        try:
            return fn(*a, **kw)
        finally:
            stop_event.set()
            beat_thread.join(timeout=interval * 2)
            _lease_store.release(job_id)

    return executor.submit(_leased_fn, *args, **kwargs)


def resubmit_orphan_jobs() -> int:
    """On startup, resume any persisted jobs that were queued or running.

    For now this is a hook called from `main.py` orphan-resume logic. The scheduler
    itself is process-local, so true cross-process durability relies on the
    persisted job record + checkpoint being re-submitted by the orchestrator.
    """
    _ensure_executor()
    _started.set()
    return 0
