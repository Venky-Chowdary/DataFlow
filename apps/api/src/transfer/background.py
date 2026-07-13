"""Background transfer runner with live job progress."""

from __future__ import annotations

import logging
import threading

try:
    from services.mongodb_service import get_mongodb_service
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service
from .engine import get_transfer_engine
from .models import TransferRequest

logger = logging.getLogger(__name__)


def _run_transfer(
    job_id: str,
    request: TransferRequest,
    resume: bool = False,
    resume_from_job_id: str | None = None,
) -> None:
    """Synchronous body that runs the transfer and updates job status."""
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


def run_transfer_async(job_id: str, request: TransferRequest, resume: bool = False, resume_from_job_id: str | None = None) -> None:
    """Execute transfer in a background thread without blocking the caller.

    FastAPI BackgroundTasks runs added tasks sequentially.  By starting a daemon
    thread and returning immediately, multiple transfers can run concurrently while
    the HTTP response is still sent first.
    """
    thread = threading.Thread(
        target=_run_transfer,
        args=(job_id, request),
        kwargs={"resume": resume, "resume_from_job_id": resume_from_job_id},
        daemon=True,
        name=f"transfer-{job_id[:8]}",
    )
    thread.start()
