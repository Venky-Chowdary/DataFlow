"""Durable, process-level transfer scheduler.

Replaces ad-hoc daemon threads with a bounded `ThreadPoolExecutor`. Jobs submitted
through the scheduler survive the originating request/response cycle and are
tracked through `job_store` / MongoDB. ``services.orphan_jobs`` finds a job
whose worker died and resumes it from its checkpoint or fails it.
"""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
import os
from services.brand_env import getenv_brand
import threading
from typing import Any, Callable

from services.worker_leases import WorkerLeaseStore, worker_id

_logger = logging.getLogger(__name__)

_executor: concurrent.futures.ThreadPoolExecutor | None = None
_started = threading.Event()
_shutdown = False
_worker_id = worker_id()
_lease_store = WorkerLeaseStore(_worker_id)
_inflight: set[str] = set()
_inflight_lock = threading.Lock()


def _ensure_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Lazily create the shared thread pool."""
    global _executor, _shutdown
    if _shutdown:
        raise RuntimeError("Transfer scheduler has been shut down")
    if _executor is None or _executor._shutdown:
        # Phase F6 — default concurrent jobs raised 4 → 8 (still process-local pool).
        max_workers = max(1, int(getenv_brand("TRANSFER_WORKERS", "8")))
        _executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="df-transfer-",
        )
        atexit.register(shutdown, wait=False, log=False)
    return _executor


def start() -> None:
    """Start the scheduler and make it ready to accept jobs."""
    _ensure_executor()
    _started.set()
    _logger.info("Transfer scheduler started")


def shutdown(wait: bool = True, log: bool = True) -> None:
    """Gracefully stop accepting new work and optionally wait for in-flight jobs.

    ``log`` is off for the interpreter-exit path: the logging handlers' streams
    are already closed by then, so the shutdown line was emitted as a
    ``--- Logging error --- ValueError: I/O operation on closed file`` traceback
    on every process exit rather than as a message anyone could read.
    """
    global _executor, _shutdown
    _shutdown = True
    _started.clear()
    if _executor:
        _executor.shutdown(wait=wait, cancel_futures=False)
        _executor = None
    if log:
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

    ttl_seconds = int(getenv_brand("WORKER_LEASE_TTL", "60"))
    if not _lease_store.acquire(job_id, ttl_seconds=ttl_seconds):
        _logger.warning("Transfer job %s is already leased by another worker; skipping", job_id)
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_result(None)
        return future

    _logger.info("Scheduling transfer job %s", job_id)

    # Capture the inbound request's OTel context so the transfer root span
    # nests under the HTTP span that scheduled it. ThreadPoolExecutor does
    # not propagate contextvars / OTel context on its own.
    try:
        from services.tracing import capture_context

        parent_ctx = capture_context()
    except Exception:
        parent_ctx = None

    fence = _lease_store.get_fence(job_id)

    def _mark_lease_lost() -> None:
        """Cooperative cancel so the transfer aborts on next checkpoint poll."""
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
                lease_fence=fence,
            )
        except Exception:
            _logger.exception("Failed to mark job %s cancelled after lease loss", job_id)

    from services.lease_heartbeat import start_lease_heartbeat

    # The heartbeat starts at submit, not when a pool thread is free. A job
    # queued behind TRANSFER_WORKERS running transfers otherwise loses its
    # lease while it waits and looks ownerless to another replica.
    stop_heartbeat = start_lease_heartbeat(
        _lease_store,
        job_id,
        ttl_seconds=ttl_seconds,
        on_lost=_mark_lease_lost,
    )
    with _inflight_lock:
        _inflight.add(job_id)

    def _leased_fn(*a: Any, **kw: Any) -> Any:
        detach_token = None
        try:
            from services.tracing import attach_context, detach_context

            detach_token = attach_context(parent_ctx)
        except Exception:
            detach_token = None

        # Bind the job id on the worker thread. Contextvars do not cross a
        # ThreadPoolExecutor boundary, so without this the lease/heartbeat logs
        # for this job would be anonymous even though the engine's are not.
        log_token = None
        try:
            from services.logging_config import reset_job_id, set_job_id

            log_token = set_job_id(job_id)
        except Exception:
            log_token = None

        try:
            return fn(*a, **kw)
        finally:
            stop_heartbeat()
            _lease_store.release(job_id)
            with _inflight_lock:
                _inflight.discard(job_id)
            if detach_token is not None:
                try:
                    from services.tracing import detach_context

                    detach_context(detach_token)
                except Exception:
                    pass
            if log_token is not None:
                try:
                    from services.logging_config import reset_job_id

                    reset_job_id(log_token)
                except Exception:
                    pass

    try:
        return executor.submit(_leased_fn, *args, **kwargs)
    except Exception:
        stop_heartbeat()
        _lease_store.release(job_id)
        with _inflight_lock:
            _inflight.discard(job_id)
        raise


def local_job_ids() -> frozenset[str]:
    """Jobs this process has submitted and not yet finished."""
    with _inflight_lock:
        return frozenset(_inflight)
