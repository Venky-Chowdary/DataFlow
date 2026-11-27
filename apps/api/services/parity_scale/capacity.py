"""How many verifications run at once, and who waits (review 2, N4).

A process runs at most :data:`MAX_CONCURRENT` scans, and a workspace at most
:data:`MAX_PER_WORKSPACE` of them, so one workspace's long scans cannot starve
the others. The rest wait in one first-come queue (a waiting scan whose
workspace is full lets the next workspace's scan go first). A waiting scan is
**queued**, not running: the Trust Report says "Queued" with its position, and
a schedule waits on it only up to its own limit. The queue is bounded: beyond
:data:`MAX_QUEUED` a verification is not started ("verification capacity
reached") and can be re-verified later.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

MAX_CONCURRENT = 2
MAX_PER_WORKSPACE = 1
MAX_QUEUED = 20


class CapacityReached(RuntimeError):
    """The queue is full: the verification is not started."""


_COND = threading.Condition()
_QUEUE: list[tuple[str, str]] = []  # (job_id, workspace_id), first come first
_RUNNING: dict[str, str] = {}  # job_id -> workspace_id


def _admissible(job_id: str) -> bool:
    if len(_RUNNING) >= MAX_CONCURRENT:
        return False
    busy: dict[str, int] = {}
    for ws in _RUNNING.values():
        busy[ws] = busy.get(ws, 0) + 1
    for queued, ws in _QUEUE:
        if busy.get(ws, 0) < MAX_PER_WORKSPACE:
            return queued == job_id  # the first waiting scan whose workspace has room goes next
    return False


def position(job_id: str) -> int:
    """1-based place in the queue, or 0 when not waiting."""
    with _COND:
        for i, (queued, _ws) in enumerate(_QUEUE):
            if queued == job_id:
                return i + 1
    return 0


def admit(job_id: str, workspace_id: str, *, keep_alive: Callable[[], None],
          on_position: Callable[[int], None], beat_seconds: float = 30.0) -> None:
    """Wait for a slot (keeping the lease alive and reporting the position); raises when the queue is full."""
    with _COND:
        if not any(q == job_id for q, _ws in _QUEUE):
            if len(_QUEUE) >= MAX_QUEUED:
                raise CapacityReached("verification capacity reached")
            _QUEUE.append((job_id, workspace_id))
    shown = -1
    try:
        while True:
            with _COND:
                if _admissible(job_id):
                    _QUEUE[:] = [(q, ws) for q, ws in _QUEUE if q != job_id]
                    _RUNNING[job_id] = workspace_id
                    _COND.notify_all()
                    return
                now = next(i + 1 for i, (q, _ws) in enumerate(_QUEUE) if q == job_id)
            if now != shown:
                on_position(now)
                shown = now
            keep_alive()
            with _COND:
                _COND.wait(timeout=beat_seconds)
    except BaseException:
        with _COND:
            _QUEUE[:] = [(q, ws) for q, ws in _QUEUE if q != job_id]
            _COND.notify_all()
        raise


def done(job_id: str) -> None:
    with _COND:
        _RUNNING.pop(job_id, None)
        _QUEUE[:] = [(q, ws) for q, ws in _QUEUE if q != job_id]
        _COND.notify_all()


def snapshot() -> dict[str, Any]:
    with _COND:
        return {"running": dict(_RUNNING), "queued": list(_QUEUE)}
