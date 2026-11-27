"""What happened to an overwrite's pre-run backup, carried onto the job (review N1).

``settle_single_overwrite_backup`` used to catch every restore error and only
log it, so the exact recovery statements a restore gives (Snowflake: which
table holds the previous rows and the SWAP / DROP to run; BigQuery: rows
restored, backup kept, what to compare; Elasticsearch: where the alias definitions are) stayed
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
    add_outcome(extra, {
        "status": "restore_failed" if restore else "discard_failed",
        "engine": engine,
        "table": table,
        "backup": backup,
        "message": message,
    })


def _items(outcome: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(o) for o in outcome.get("outcomes") or [outcome] if isinstance(o, dict)]


def add_outcome(extra: dict[str, Any], outcome: Any) -> None:
    """Add ``outcome`` to the one on ``extra``: a multi-table run keeps every table's.

    One outcome is stored as is; several as ``{"status", "message", "outcomes":
    [...]}`` with every message joined (review round 3: a parked table's
    restore outcome was recorded on a copy of the endpoint and lost).
    """
    if not isinstance(outcome, dict):
        return
    prior = extra.get(OUTCOME_KEY)
    if not isinstance(prior, dict):
        extra[OUTCOME_KEY] = dict(outcome)
        return
    items = _items(prior) + _items(outcome)
    statuses = {str(o.get("status") or "") for o in items}
    extra[OUTCOME_KEY] = {
        "status": "restore_failed" if "restore_failed" in statuses else "discard_failed",
        "engine": ", ".join(dict.fromkeys(str(o.get("engine") or "") for o in items)),
        "table": ", ".join(str(o.get("table") or "") for o in items),
        "backup": ", ".join(str(o.get("backup") or "") for o in items),
        "message": " ".join(str(o.get("message") or "") for o in items),
        "outcomes": items,
    }


def pop_discard_warning(destination: Any) -> dict[str, Any] | None:
    """A successful run's failed backup drop, as a job warning (review round 3)."""
    from datetime import datetime, timezone

    extra = dict(getattr(destination, "extra", None) or {})
    outcome = extra.get(OUTCOME_KEY)
    if not isinstance(outcome, dict) or outcome.get("status") != "discard_failed":
        return None
    extra.pop(OUTCOME_KEY, None)
    destination.extra = extra
    return {
        "step": "overwrite_backup_discard",
        "error": str(outcome.get("message") or "")[:2000],
        "at": datetime.now(timezone.utc).isoformat(),
        "overwrite_backup": dict(outcome),
    }


def carry_onto_exception(destination: Any, exc: BaseException) -> dict[str, Any] | None:
    """Move a recorded outcome from ``destination.extra`` onto ``exc``."""
    extra = dict(getattr(destination, "extra", None) or {})
    outcome = extra.pop(OUTCOME_KEY, None)
    destination.extra = extra
    if not isinstance(outcome, dict):
        return None
    earlier = getattr(exc, "overwrite_restore", None)
    if isinstance(earlier, dict):
        # Carried once already in this run (a multi-table stream): add to it.
        merged: dict[str, Any] = {OUTCOME_KEY: dict(earlier)}
        add_outcome(merged, outcome)
        setattr(exc, "overwrite_restore", merged[OUTCOME_KEY])
    else:
        setattr(exc, "overwrite_restore", dict(outcome))
    # Added to what the run already says (the multi-table undo's note, the
    # stream's own undo), never in place of it (review round 4).
    own = str(getattr(exc, "failed_batch_undo_note", "") or "")
    message = str(outcome.get("message") or "")
    setattr(exc, "failed_batch_undo_note", own if message in own else f"{own} {message}".strip())
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
