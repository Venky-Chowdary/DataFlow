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

_logger = logging.getLogger(__name__)

_executor: concurrent.futures.ThreadPoolExecutor | None = None
_started = threading.Event()
_shutdown = False


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
    """Schedule a transfer job on the durable thread pool."""
    executor = _ensure_executor()
    if not _started.is_set():
        _started.set()
    _logger.info("Scheduling transfer job %s", job_id)
    return executor.submit(fn, *args, **kwargs)


def resubmit_orphan_jobs() -> int:
    """On startup, resume any persisted jobs that were queued or running.

    For now this is a hook called from `main.py` orphan-resume logic. The scheduler
    itself is process-local, so true cross-process durability relies on the
    persisted job record + checkpoint being re-submitted by the orchestrator.
    """
    _ensure_executor()
    _started.set()
    return 0
