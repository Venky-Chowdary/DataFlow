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


def terminal_status_for(rejected_rows: int = 0, coerced_null_rows: int = 0) -> str:
    """Pick the success terminal status based on data-integrity accounting."""
    if int(rejected_rows or 0) > 0 or int(coerced_null_rows or 0) > 0:
        return COMPLETED_WITH_QUARANTINE
    return COMPLETED


def is_completed(status: str | None) -> bool:
    return (status or "") in COMPLETED_STATUSES


def is_terminal(status: str | None) -> bool:
    return (status or "") in TERMINAL_STATUSES
