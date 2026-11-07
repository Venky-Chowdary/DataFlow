"""Job status fence — Cancel cannot be rewritten to success.

One owner: ``services.job_status.refuse_job_status_write``. Used by both
MongoDBService and MemoryMongoDBService. Does not claim COPY is interruptible.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_refuse_cancelled_to_completed() -> None:
    from services.job_status import refuse_job_status_write

    assert (
        refuse_job_status_write(previous_status="cancelled", next_status="completed")
        == "already terminal (cancelled)"
    )


def test_refuse_cancelled_to_running() -> None:
    from services.job_status import refuse_job_status_write

    assert refuse_job_status_write(
        previous_status="cancelled", next_status="running"
    )


def test_allow_same_terminal_rewrite() -> None:
    from services.job_status import refuse_job_status_write

    assert (
        refuse_job_status_write(previous_status="cancelled", next_status="cancelled")
        is None
    )
    assert (
        refuse_job_status_write(previous_status="completed", next_status="completed")
        is None
    )


def test_allow_resume_terminal_exit() -> None:
    from services.job_status import refuse_job_status_write

    assert (
        refuse_job_status_write(
            previous_status="cancelled",
            next_status="pending",
            allow_terminal_exit=True,
        )
        is None
    )


def test_cancel_requested_blocks_completed_and_running() -> None:
    from services.job_status import refuse_job_status_write

    assert refuse_job_status_write(
        previous_status="running",
        next_status="completed",
        cancel_requested=True,
    )
    assert refuse_job_status_write(
        previous_status="running",
        next_status="running",
        cancel_requested=True,
    )
    assert refuse_job_status_write(
        previous_status="running",
        next_status="completed_with_quarantine",
        cancel_requested=True,
    )


def test_cancel_requested_allows_cancelled_and_failed() -> None:
    from services.job_status import refuse_job_status_write

    assert (
        refuse_job_status_write(
            previous_status="running",
            next_status="cancelled",
            cancel_requested=True,
        )
        is None
    )
    assert (
        refuse_job_status_write(
            previous_status="running",
            next_status="failed",
            cancel_requested=True,
        )
        is None
    )


def test_live_progress_without_cancel_is_allowed() -> None:
    from services.job_status import refuse_job_status_write

    assert (
        refuse_job_status_write(previous_status="running", next_status="running")
        is None
    )
    assert (
        refuse_job_status_write(previous_status="running", next_status="completed")
        is None
    )
