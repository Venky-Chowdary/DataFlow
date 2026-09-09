"""Canonical transfer-job status vocabulary.

A run that completed but altered or dropped data must NOT be reported as a clean
``completed``. ``completed_with_quarantine`` is a *successful terminal* status
(the transfer finished, data landed) that also signals rejected rows and/or
values coerced to NULL, so dashboards, polling, and reconciliation stay honest.
"""

from __future__ import annotations

COMPLETED = "completed"
COMPLETED_WITH_QUARANTINE = "completed_with_quarantine"

# Statuses that mean "the transfer succeeded" (data is in the destination).
COMPLETED_STATUSES = frozenset({COMPLETED, COMPLETED_WITH_QUARANTINE})

# Statuses that mean "the job will not change again".
TERMINAL_STATUSES = frozenset({COMPLETED, COMPLETED_WITH_QUARANTINE, "failed", "cancelled"})

# A cancel request must not be rewritten to success or live progress. ``failed``
# is still allowed (the run actually broke). ``cancelled`` is the requested terminal.
CANCEL_BLOCKS_STATUSES = frozenset(
    {"running", "pending", COMPLETED, COMPLETED_WITH_QUARANTINE}
)


def refuse_job_status_write(
    *,
    previous_status: str | None,
    next_status: str,
    cancel_requested: bool = False,
    allow_terminal_exit: bool = False,
) -> str | None:
    """Return why a status write must be dropped, or None to allow it.

    Terminal statuses are sticky: ``cancelled`` must not become ``completed``
    when a fast COPY finishes after the operator clicked Cancel. Resume is
    the documented exit (``allow_terminal_exit=True``).

    Does not claim the wire is interruptible — only that the control-plane
    status cannot be rewritten to success after cancel.
    """
    if allow_terminal_exit:
        return None
    prev = (previous_status or "").strip()
    nxt = (next_status or "").strip()
    if prev in TERMINAL_STATUSES and nxt != prev:
        return f"already terminal ({prev})"
    if cancel_requested and nxt in CANCEL_BLOCKS_STATUSES:
        return "cancel_requested blocks success/progress rewrite"
    return None


def terminal_status_for(rejected_rows: int = 0, coerced_null_rows: int = 0) -> str:
    """Pick the success terminal status based on data-integrity accounting."""
    if int(rejected_rows or 0) > 0 or int(coerced_null_rows or 0) > 0:
        return COMPLETED_WITH_QUARANTINE
    return COMPLETED


def is_completed(status: str | None) -> bool:
    return (status or "") in COMPLETED_STATUSES


def is_terminal(status: str | None) -> bool:
    return (status or "") in TERMINAL_STATUSES
