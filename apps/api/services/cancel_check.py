"""Shared cancellation probe for transfer write paths (D102).

The engine's ``on_checkpoint`` callback fires only after a batch commits and
is further throttled — a fast copy can finish before the first effective
check runs, so a cancelled job still lands every row. This module is the
single cancel probe both the row writers and the COPY fast paths call
*between* batches (and inside long writes where the engine allows), so a
cancel stops further writes instead of being noticed at the end.
"""

from __future__ import annotations

import contextvars
import logging
from typing import Any, Callable

from services.error_handling import TransferCancelled

logger = logging.getLogger(__name__)

# The transfer engine registers the active job's probe here so COPY fast paths
# and writers can poll it without threading a callback through ~30 call sites.
# A ContextVar, not a thread-local: transfers run on a worker thread and keep
# concurrent jobs isolated, but the chunk writers run on a pool that copies the
# caller's context (ChunkDispatcher) - a thread-local was invisible there, so a
# cancel never reached a writer already past its first chunk (D168).
_probe: contextvars.ContextVar[Callable[[int | None], None] | None] = (
    contextvars.ContextVar("df_cancel_probe", default=None)
)


def set_cancel_probe(fn: Callable[[int | None], None] | None) -> None:
    _probe.set(fn)


def get_cancel_probe() -> Callable[[int | None], None] | None:
    return _probe.get()


def cancel_checkpoint(rows_written: int | None = None) -> None:
    """Poll the active probe; raise ``TransferCancelled`` when cancelled.

    Called between batches and inside long writes. A no-op when no engine
    run owns this thread (e.g. tools calling a copy helper directly).
    """
    fn = _probe.get()
    if fn is not None:
        fn(rows_written)


def job_cancel_requested(mongo: Any, job_id: str) -> bool:
    """True when the operator (or a lost lease) flagged this job to stop."""
    if mongo is None or not job_id:
        return False
    try:
        job = mongo.get_job(job_id)
    except Exception as exc:  # noqa: BLE001 - a failed read must not hide the transfer
        logger.warning("Cancellation check failed: %s", exc, exc_info=exc)
        return False
    return bool(
        job
        and (
            job.get("cancel_requested")
            or str(job.get("status") or "") == "cancelled"
        )
    )


def raise_if_job_cancelled(
    mongo: Any, job_id: str, *, rows_written: int | None = None
) -> None:
    """Raise ``TransferCancelled`` when the job was cancelled.

    ``rows_written`` is whatever this chunk already committed, so the
    cancelled job does not report 0 rows for a table that now holds them.
    """
    if job_cancel_requested(mongo, job_id):
        raise TransferCancelled(
            "Transfer cancelled by user", rows_written=rows_written
        )


def cancel_check_for(mongo: Any, job_id: str) -> Callable[[int | None], None]:
    """Callable ``check(rows_written)`` for threading into write/copy loops."""

    def _check(rows_written: int | None = None) -> None:
        raise_if_job_cancelled(mongo, job_id, rows_written=rows_written)

    return _check
