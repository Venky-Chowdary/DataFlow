"""Background transfer runner with live job progress and durable scheduling."""

from __future__ import annotations

import logging
from typing import Any

try:
    from services.mongodb_service import get_mongodb_service
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service

from services.transfer_scheduler import submit as _submit_transfer

from .engine import get_transfer_engine
from .models import TransferRequest


def _log_transfer_exception(fut: Any) -> None:
    try:
        fut.result()
    except Exception as exc:
        logger.exception("Background transfer raised an unhandled exception: %s", exc)

logger = logging.getLogger(__name__)


def _notify_failure(request: TransferRequest, job_id: str, error: str, records_transferred: int = 0) -> None:
    """Fire workspace notifications for an exception-level transfer failure."""
    try:
        from services.notification_service import (
            build_job_payload,
            log_job_notifications,
            notify_workspace,
        )
        from services.platform_config import public_url, web_url

        payload = build_job_payload(
            job_id=job_id,
            status="failed",
            source=request.source.kind or "unknown",
            destination=request.destination.kind or "unknown",
            records_transferred=records_transferred,
            rejected_rows=0,
            error=error,
            retry_url=f"/api/v1/connectors/jobs/{job_id}/resume",
            workspace_id=request.workspace_id or "",
            base_url=public_url(),
            web_url=web_url(),
        )
        results = notify_workspace(request.workspace_id or "", payload)
        log_job_notifications(job_id, results)
    except Exception:
        logger.exception("Failed to send job failure notification")


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
        _notify_failure(request, job_id, str(exc))


def run_transfer_async(job_id: str, request: TransferRequest, resume: bool = False, resume_from_job_id: str | None = None) -> Any:
    """Execute transfer on the durable scheduler and return immediately.

    The scheduler uses a process-wide thread pool and persists phase/status through
    the job store / MongoDB so in-flight work can be recovered after a restart.
    """
    future = _submit_transfer(
        job_id,
        _run_transfer,
        job_id,
        request,
        resume=resume,
        resume_from_job_id=resume_from_job_id,
    )
    future.add_done_callback(_log_transfer_exception)
    return future
