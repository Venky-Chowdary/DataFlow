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


def resolve_resume_checkpoint(
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
