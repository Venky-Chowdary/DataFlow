"""What happened to an overwrite's pre-run backup, carried onto the job (review N1).

``settle_single_overwrite_backup`` used to catch every restore error and only
log it, so the exact recovery statements a restore gives (Snowflake: which
table holds the previous rows and the SWAP / DROP to run; BigQuery: rows
restored, backup kept, what to compare; Elasticsearch: the alias JSON) stayed
in the API log. The job failed with a plain message, and the next run was
refused with a generic "one of them holds the pre-run data".

The outcome is recorded on ``destination.extra`` by the settle, moved onto the
run's exception (``overwrite_restore``) by ``_settle_backup_after_error``,
and written into the job's error and ``error_details`` by
``job_failure._fail_runtime_job``.
"""

from __future__ import annotations

from typing import Any

OUTCOME_KEY = "overwrite_restore_outcome"


def record_settle_failure(
    extra: dict[str, Any],
    *,
    engine: str,
    table: str,
    backup: str,
    restore: bool,
    exc: BaseException,
) -> None:
    """Record a failed restore (or discard) on ``extra`` for the job's failure."""
    detail = str(getattr(exc, "cause", None) or exc).strip()
    if restore:
        message = (
            f"The previous destination could not be put back automatically ({engine}, "
            f"backup {backup}): {detail}"
        )
    else:
        message = (
            f"This run's backup {backup} of {table} could not be dropped ({engine}): {detail}. "
            f"Drop {backup} once you have checked {table}; the next overwrite is refused while both exist."
        )
    extra[OUTCOME_KEY] = {
        "status": "restore_failed" if restore else "discard_failed",
        "engine": engine,
        "table": table,
        "backup": backup,
        "message": message,
    }


def carry_onto_exception(destination: Any, exc: BaseException) -> dict[str, Any] | None:
    """Move a recorded outcome from ``destination.extra`` onto ``exc``."""
    extra = dict(getattr(destination, "extra", None) or {})
    outcome = extra.pop(OUTCOME_KEY, None)
    destination.extra = extra
    if not isinstance(outcome, dict):
        return None
    setattr(exc, "overwrite_restore", dict(outcome))
    setattr(exc, "failed_batch_undo_note", str(outcome.get("message") or ""))
    return outcome


def apply_to_job_failure(exc: BaseException, display: str, error_details: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """The job's error text and details with the restore outcome in them."""
    outcome = getattr(exc, "overwrite_restore", None)
    if not isinstance(outcome, dict):
        return display, error_details
    message = str(outcome.get("message") or "")
    if message and message not in display:
        display = f"{display} {message}"
    return display, {**(error_details or {}), "overwrite_restore": dict(outcome)}
