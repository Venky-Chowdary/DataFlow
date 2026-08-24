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
