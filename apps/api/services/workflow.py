"""Local transfer workflow — phase tracking backed by the durable job store.

This module previously held an in-memory phase map and started daemon threads.
Phases are now persisted through `job_store` so progress survives process
restarts, and background work is scheduled on the durable transfer executor.
"""

from __future__ import annotations

import threading
from enum import Enum
from typing import Any, Callable

from services.transfer_scheduler import submit as _submit_to_scheduler


class WorkflowPhase(str, Enum):
    QUEUED = "queued"
    PREFLIGHT = "preflight"
    MAPPING = "mapping"
    TRANSFER = "transfer"
    RECONCILE = "reconcile"
    COMPLETED = "completed"
    FAILED = "failed"


# Lightweight in-memory cache for callers that still inspect workflow state
# locally. Writes also propagate to the persisted job store.
_states: dict[str, dict[str, Any]] = {}
_lock = threading.Lock()


def _persist_phase(job_id: str, phase: WorkflowPhase, message: str) -> None:
    """Write the phase/message to the configured job store if available."""
    try:
        from services.jobs import job_store

        job_store.set_workflow_phase(job_id, phase.value)
        if message:
            job_store.set_message(job_id, message)
    except Exception:
        # Job store may not be available during tests or early import.
        pass


def set_phase(job_id: str, phase: WorkflowPhase, message: str = "") -> None:
    msg = message or phase.value
    with _lock:
        _states[job_id] = {"phase": phase.value, "message": msg}
    _persist_phase(job_id, phase, msg)


def get_phase(job_id: str) -> dict[str, Any] | None:
    with _lock:
        cached = _states.get(job_id)
    if cached:
        return dict(cached)
    try:
        from services.jobs import job_store

        job = job_store.get(job_id)
        if job:
            return {"phase": job.workflow_phase, "message": job.message}
    except Exception:
        pass
    return None


def run_in_background(fn: Callable[[], Any], job_id: str) -> Any:
    """Schedule work on the durable transfer executor and return a Future."""
    return _submit_to_scheduler(job_id, fn)


def simulate_chunk_delay(chunk: int, total_chunks: int) -> None:
    """Brief pause so UI polling can observe checkpoint progress (dev only)."""
    import time

    if total_chunks <= 1:
        return
    try:
        from services.platform_config import is_production

        if is_production():
            return
    except Exception:
        pass
    time.sleep(0.15)
