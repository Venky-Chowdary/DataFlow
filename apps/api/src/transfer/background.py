"""Background transfer runner with live job progress and durable scheduling."""

from __future__ import annotations

import concurrent.futures
import logging
from typing import Any

try:
    from services.mongodb_service import get_mongodb_service
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service

from services.transfer_scheduler import submit as _submit_transfer

from .engine import get_transfer_engine
from .models import TransferRequest, transfer_request_from_dict

logger = logging.getLogger(__name__)


def _log_transfer_exception(fut: Any) -> None:
    try:
        fut.result()
    except Exception as exc:
        logger.exception("Background transfer raised an unhandled exception: %s", exc)


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


def run_fleet_job(job_id: str) -> None:
    """Worker-fleet handler: reconstruct TransferRequest from the Mongo job and execute."""
    mongo = get_mongodb_service()
    job = mongo.get_job(job_id)
    if not job:
        raise ValueError(f"Unknown job {job_id}")
    payload = job.get("transfer_request")
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"Job {job_id} has no transfer_request — cannot execute on worker")
    if payload.get("requires_file_reupload"):
        mongo.update_job_status(
            job_id,
            "failed",
            error="File re-upload required after restart — open Transfer Studio",
            message="File re-upload required after restart — open Transfer Studio",
        )
        return
    request = transfer_request_from_dict(payload)
    resume = bool(job.get("checkpoint")) or str(job.get("status") or "") in {
        "paused",
        "retrying",
        "running",
    }
    _run_transfer(job_id, request, resume=resume)


def run_transfer_async(
    job_id: str,
    request: TransferRequest,
    resume: bool = False,
    resume_from_job_id: str | None = None,
) -> Any:
    """Execute transfer on the durable scheduler and return immediately.

    When ``DATAFLOW_WORKER_FLEET=1`` and Mongo queue is available, the job is
    enqueued for a dedicated Railway worker process. Otherwise it runs on the
    local API thread pool (dev / single-service mode).
    """
    if resume_from_job_id and resume:
        try:
            mongo = get_mongodb_service()
            old = mongo.get_job(resume_from_job_id)
            if old and old.get("checkpoint"):
                mongo.update_job_status(job_id, "pending", checkpoint=old["checkpoint"])
        except Exception:
            logger.exception("Could not copy checkpoint from %s onto %s", resume_from_job_id, job_id)

    try:
        from services.worker_fleet import enqueue_job, fleet_enabled
        from services.platform_config import is_production

        if fleet_enabled():
            ok = enqueue_job(
                job_id,
                payload={"resume": resume, "resume_from_job_id": resume_from_job_id or ""},
            )
            if ok:
                logger.info("Enqueued transfer job %s to worker fleet", job_id)
                future: concurrent.futures.Future[Any] = concurrent.futures.Future()
                future.set_result(None)
                return future
            # Production + fleet: fail closed — do not silently run on API (hides missing Worker).
            if is_production():
                mongo = get_mongodb_service()
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error="Worker fleet enabled but enqueue failed — check Mongo queue and Worker service",
                    message="Worker fleet enqueue failed",
                )
                raise RuntimeError(
                    f"DATAFLOW_WORKER_FLEET enabled but could not enqueue job {job_id}. "
                    "Deploy the Worker service and verify MONGODB_URI."
                )
            logger.warning(
                "DATAFLOW_WORKER_FLEET=1 but enqueue failed for %s — falling back to local executor",
                job_id,
            )
    except RuntimeError:
        raise
    except Exception:
        logger.exception("Fleet enqueue path failed for %s — falling back to local executor", job_id)

    future = _submit_transfer(
        job_id,
        _run_transfer,
        job_id,
        request,
        resume=resume,
        resume_from_job_id=None,
    )
    future.add_done_callback(_log_transfer_exception)
    return future
