"""Bounds on a rehearsal's verification: progress, a total time cap, and statement timeouts.

A verification re-reads both whole populations, so it may run for hours; it
must still end (review 2, N4):

* **Progress, not a timer, keeps the claim.** The verifier counts every row it
  reads (:func:`tick`); its lease is refreshed only while that count moves. A
  verifier blocked on a dead connection stops refreshing, its claim goes stale
  after ``VERIFY_STALE`` and the next reader takes it over.
* **A total cap.** A verification (all its attempts together, measured from
  the first claim) gets at most :func:`max_verify_seconds`
  (``DATAFLOW_EXIT_VERIFY_MAX_SECONDS``, default 6 h). Past it the next row
  read raises :class:`VerificationTimedOut`, blocked sockets are interrupted,
  and the rehearsal closes ``not_proven`` with that reason instead of being
  verified again.
* **Statement timeouts.** The verification runs inside the copilot deadline
  scope (:func:`src.ai.copilot.tool_deadline.deadline_scope`), so every pooled
  SQL session it checks out carries the remaining time as its server-side
  statement timeout (``services.statement_budget``) and is registered for
  interruption.
"""

from __future__ import annotations

import contextlib
import contextvars
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

MAX_VERIFY_ENV = "EXIT_VERIFY_MAX_SECONDS"
_MAX_VERIFY_DEFAULT = 6 * 3600.0


def max_verify_seconds() -> float:
    """The most a rehearsal's verification may take, all attempts together."""
    from services.brand_env import getenv_brand

    raw = str(getenv_brand(MAX_VERIFY_ENV, "") or "").strip()
    try:
        value = float(raw) if raw else _MAX_VERIFY_DEFAULT
    except ValueError:
        value = _MAX_VERIFY_DEFAULT
    return value if value > 0 else _MAX_VERIFY_DEFAULT


class VerificationTimedOut(BaseException):
    """The verification ran past its cap.

    A ``BaseException`` on purpose: the per-table read-back handlers catch
    ``Exception`` and would turn the cap into a "could not be re-read" gap.
    """


@dataclass
class Progress:
    """Rows the verifier has read, and when it must stop (``time.monotonic`` clock)."""

    deadline: float
    rows: int = 0
    cancelled: threading.Event = field(default_factory=threading.Event)
    #: The deadline scope's own progress (registered connections to interrupt).
    scope: Any = None
    #: Stopped without a result: the claim was taken over (``lost``) or the process is stopping (``abandoned``).
    lost: bool = False
    abandoned: bool = False

    def expired(self) -> bool:
        return time.monotonic() >= self.deadline

    def cancel(self) -> None:
        """Stop the verifier: the next row read raises, and a blocked socket is interrupted."""
        self.cancelled.set()
        if self.scope is not None:
            from src.ai.copilot.tool_deadline import interrupt_connections

            interrupt_connections(self.scope)


_current: contextvars.ContextVar[Progress | None] = contextvars.ContextVar("exit_verify_progress", default=None)


def tick(rows: int = 1) -> None:
    """The verifier read ``rows`` more rows (a no-op outside a verification)."""
    progress = _current.get()
    if progress is None:
        return
    progress.rows += rows
    if progress.cancelled.is_set() or progress.expired():
        progress.cancelled.set()
        raise VerificationTimedOut()


@contextlib.contextmanager
def tracking(seconds: float) -> Iterator[Progress]:
    """Count the rows read on this thread, with ``seconds`` left before the cap."""
    from src.ai.copilot.tool_deadline import deadline_scope

    progress = Progress(deadline=time.monotonic() + max(0.0, seconds))
    with deadline_scope(max(1.0, seconds)) as scope:
        progress.scope = scope
        token = _current.set(progress)
        try:
            yield progress
        finally:
            _current.reset(token)


def timed_out_reason(seconds: float) -> str:
    """Why a table is not proven when the verification hit its cap."""
    from services.exit_rehearsal import _hours

    return (f"The verification did not finish within its {_hours(seconds)} limit (the source or the stored part "
            "stopped answering or was too slow), so this table is not proven; rehearse it again.")
