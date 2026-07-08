"""Background transfer runner with live job progress."""

from __future__ import annotations

import logging

from ..services.mongodb_service import get_mongodb_service
from .engine import get_transfer_engine
from .models import TransferRequest

logger = logging.getLogger(__name__)


def run_transfer_async(job_id: str, request: TransferRequest) -> None:
    """Execute transfer in background thread — updates MongoDB job progress."""
    try:
        get_transfer_engine().execute_tracked(request, job_id)
    except Exception as exc:
        logger.exception("Background transfer failed job=%s", job_id)
        mongo = get_mongodb_service()
        mongo.update_job_status(
            job_id,
            "failed",
            progress_pct=0,
            phase="failed",
            error=str(exc),
            message=str(exc),
        )
