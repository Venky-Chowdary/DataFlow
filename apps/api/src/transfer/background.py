"""Background transfer runner with live job progress."""

from __future__ import annotations

import logging

try:
    from services.mongodb_service import get_mongodb_service
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service
from .engine import get_transfer_engine
from .models import TransferRequest

logger = logging.getLogger(__name__)


def run_transfer_async(job_id: str, request: TransferRequest, resume: bool = False, resume_from_job_id: str | None = None) -> None:
    """Execute transfer in background thread — updates MongoDB job progress."""
    try:
        mongo = get_mongodb_service()
        if resume_from_job_id and resume:
            old = mongo.get_job(resume_from_job_id)
            if old and old.get("checkpoint"):
                mongo.update_job_status(job_id, "pending", checkpoint=old["checkpoint"])
        get_transfer_engine().execute_tracked(request, job_id, resume=resume)
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
