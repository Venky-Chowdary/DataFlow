"""Local transfer workflow — Temporal-compatible state machine (Phase 2 stub)."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class WorkflowPhase(str, Enum):
    QUEUED = "queued"
    PREFLIGHT = "preflight"
    MAPPING = "mapping"
    TRANSFER = "transfer"
    RECONCILE = "reconcile"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class WorkflowState:
    job_id: str
    phase: WorkflowPhase = WorkflowPhase.QUEUED
    message: str = "Queued"


# In-memory workflow registry — replaced by Temporal in production
_states: dict[str, WorkflowState] = {}
_lock = threading.Lock()


def set_phase(job_id: str, phase: WorkflowPhase, message: str = "") -> None:
    with _lock:
        _states[job_id] = WorkflowState(job_id=job_id, phase=phase, message=message or phase.value)


def get_phase(job_id: str) -> WorkflowState | None:
    with _lock:
        return _states.get(job_id)


def run_in_background(fn: Callable[[], None]) -> None:
    thread = threading.Thread(target=fn, daemon=True)
    thread.start()


def simulate_chunk_delay(chunk: int, total_chunks: int) -> None:
    """Brief pause so UI polling can observe checkpoint progress (dev only)."""
    if total_chunks <= 1:
        return
    try:
        from services.platform_config import is_production

        if is_production():
            return
    except Exception:
        pass
    time.sleep(0.15)
