"""Where a resumed run actually restarts from.

Split out of the engine: deciding the restart point is a safety decision with
one rule behind it — never restart an append from a point the control plane
cannot vouch for. A checkpoint blob, a job document's committed-row count and
"the store did not answer" are three different states, and collapsing the third
into "zero rows written" duplicates a load.

The refuse/allow verdict itself belongs to
``services.execution_engine_contract``; this only gathers the evidence.
"""

from __future__ import annotations

import logging
from typing import Any

from services.checkpoint_service import Checkpoint

logger = logging.getLogger(__name__)


def restarts_from_start(sync_mode: str | None) -> bool:
    """Whether a resume of this sync mode runs as a fresh start.

    Clear-and-reload converges, so an overwrite never seeks past a checkpoint.
    Its checkpoint can count rows of a load transaction the cancel rolled back,
    and a resumed pass skipped the rows it believed an earlier pass committed
    after the clear had removed them (D261 is the reclaim side of this rule).
    """
    from services.sync_cursor import is_overwrite_sync

    return is_overwrite_sync(sync_mode)


#: Attribute on the checkpoint a resumed run starts from (C5): a sequential
#: multi-table pass skips only the tables a pass of *this* resume chain finished.
RESUMED_PASS_ATTR = "resumed_pass"


def resolve_resume_checkpoint(
    *,
    job_id: str,
    mongo: Any,
    checkpoint_service: Any,
    has_progress: Any,
    sync_mode: str | None,
) -> Checkpoint:
    """The checkpoint a resumed run starts from (marked resumed), or refuse to start."""
    checkpoint = _resolve_resume_checkpoint(
        job_id=job_id, mongo=mongo, checkpoint_service=checkpoint_service,
        has_progress=has_progress, sync_mode=sync_mode,
    )
    setattr(checkpoint, RESUMED_PASS_ATTR, True)
    return checkpoint


def _resolve_resume_checkpoint(
    *,
    job_id: str,
    mongo: Any,
    checkpoint_service: Any,
    has_progress: Any,
    sync_mode: str | None,
) -> Checkpoint:
    """The checkpoint a resumed run starts from, or refuse to start.

    Raises ``ValueError`` when resuming would duplicate committed rows: the
    engine surfaces that as a failed job with the operator's next action.
    """
    from services.execution_engine_contract import (
        ExecutionContractError,
        assert_resume_allowed,
    )
    if restarts_from_start(sync_mode):
        logger.info("resume job=%s: overwrite restarts from the first row", job_id)
        return Checkpoint(job_id=job_id)

    try:
        checkpoint = checkpoint_service.load(job_id)
    except Exception as exc:
        logger.warning("resume checkpoint load failed: %s", exc, exc_info=exc)
        checkpoint = None

    prior_rows = 0
    # An unreadable or absent job document does not mean "nothing was written".
    prior_rows_known = True
    if not has_progress(checkpoint):
        # Prefer job.records_processed when the checkpoint blob was cleared
        # after a completed partial wave (Studio Resume / multi-batch upsert).
        try:
            job_doc = mongo.get_job(job_id)
            prior_rows_known = isinstance(job_doc, dict) and bool(job_doc)
            prior_rows = int((job_doc or {}).get("records_processed") or 0)
        except Exception as exc:
            logger.warning(
                "resume job=%s: committed-row count unreadable: %s",
                job_id,
                exc,
                exc_info=exc,
            )
            prior_rows = 0
            prior_rows_known = False
        if prior_rows > 0:
            checkpoint = Checkpoint(
                job_id=job_id, rows_processed=prior_rows, offset=prior_rows
            )

    if has_progress(checkpoint):
        _note_resumed(job_id, checkpoint)
        return checkpoint

    try:
        decision = assert_resume_allowed(
            resume_requested=True,
            checkpoint_has_progress=False,
            sync_mode=sync_mode,
            rows_committed=prior_rows,
            rows_committed_known=prior_rows_known,
        )
    except ExecutionContractError as exc:
        raise ValueError(str(exc)) from exc
    logger.warning(
        "resume job=%s without durable checkpoint — %s",
        job_id,
        decision.get("reason"),
    )
    return Checkpoint(job_id=job_id)


def _note_resumed(job_id: str, checkpoint: Any) -> None:
    """Tell the reversibility backstop which committed rows this run resumes after (review 6).

    The run's written count starts at the checkpoint's rows, which this attempt
    never checked; the backstop compares only the rows written after them.
    """
    from services.reversibility_enforce import note_resumed
    from services.sync_cursor import checkpoint_stream_name

    note_resumed(job_id, rows=getattr(checkpoint, "rows_processed", 0),
                 stream=checkpoint_stream_name(checkpoint) or "")


def reapplies_population(job_id: str) -> int:
    """The buffered route resumes by re-applying every row it holds: its count starts at 0 (review 6).

    Returns the rows its pass skips (none), and tells the reversibility
    backstop that this resume seeded no written count from the checkpoint.
    """
    from services.reversibility_enforce import note_resumed_reapplied

    note_resumed_reapplied(job_id)
    return 0


#: Checkpoint phase stamped once every batch of the write pass is committed.
WRITE_PASS_COMPLETE_PHASE = "reconcile"
RECONCILE_SAMPLE_KEEP = 50


def write_pass_complete(checkpoint: Any) -> bool:
    """True when the checkpoint says every batch was committed.

    A run restarted from here has no interrupted batch to re-deliver: only
    verification is left. Re-probing its own committed rows as "already at
    rest" (or refusing a keyless resume) failed a load that had landed (D263).
    """
    return bool(
        checkpoint is not None
        and str(getattr(checkpoint, "phase", "") or "") == WRITE_PASS_COMPLETE_PHASE
        and int(getattr(checkpoint, "rows_processed", 0) or 0)
        + int(getattr(checkpoint, "offset", 0) or 0)
        > 0
    )


def resume_read_cursor(resume: bool, checkpoint: Any) -> dict[str, Any]:
    """The keyset cursor a resumed read seeks past (preflight keyword args)."""
    if not resume or checkpoint is None:
        return {}
    column = str(getattr(checkpoint, "cursor_column", "") or "")
    value = getattr(checkpoint, "cursor_value", None)
    if not column or value is None:
        return {}
    return {"resume_cursor_column": column, "resume_cursor_value": value}


def resumes_at_reconcile(resume: bool, checkpoint: Any) -> bool:
    """A resumed run whose write pass already committed every batch."""
    return bool(resume and write_pass_complete(checkpoint))


def mark_write_pass_complete(
    job_id: str, checkpoint_service: Any, dest_summary: Any = None
) -> None:
    """Stamp the saved checkpoint before verification starts (best effort).

    A worker stopped during reconcile then resumes at reconcile instead of
    re-running the write. A failed stamp leaves the checkpoint at "writing",
    which resumes exactly as before.
    """
    if not job_id or checkpoint_service is None:
        return
    try:
        checkpoint = checkpoint_service.load(job_id)
        if checkpoint is None or not (
            int(checkpoint.rows_processed or 0) or int(checkpoint.offset or 0)
        ):
            return
        checkpoint.phase = WRITE_PASS_COMPLETE_PHASE
        sample = dest_summary.get("reconcile_sample") if isinstance(dest_summary, dict) else None
        if isinstance(sample, list) and sample:
            checkpoint.reconcile_sample = list(sample[:RECONCILE_SAMPLE_KEEP])
        checkpoint_service.save(checkpoint)
    except Exception as exc:  # noqa: BLE001 — never fail a landed load here
        logger.warning(
            "job=%s: write-pass-complete checkpoint stamp failed: %s",
            job_id,
            exc,
            exc_info=exc,
        )
