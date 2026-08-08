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
    request = transfer_request_from_dict(payload)
    from services.transfer_file_staging import (
        file_source_bytes_available,
        hydrate_file_source,
    )

    hydrate_file_source(request)
    if request.source.kind == "file" and not file_source_bytes_available(request):
        mongo.update_job_status(
            job_id,
            "failed",
            error="File re-upload required after restart — open Transfer Studio",
            message="File re-upload required after restart — open Transfer Studio",
        )
        return
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

    Phase F5 — when claim-queue mode is active (``SCHEDULER_MODE=claim`` /
    ``auto`` on multi-replica, or ``WORKER_FLEET=1``), the job is enqueued to
    Mongo ``transfer_job_queue`` for claim under worker leases. Otherwise it
    runs on the local API thread pool (single-replica / demo).
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
        from services.platform_config import is_production
        from services.scheduler_mode import scheduler_mode
        from services.worker_fleet import enqueue_job, fleet_enabled
        from services.worker_leases import requires_distributed_backend

        if fleet_enabled():
            ok = enqueue_job(
                job_id,
                payload={"resume": resume, "resume_from_job_id": resume_from_job_id or ""},
            )
            if ok:
                logger.info(
                    "Enqueued transfer job %s (scheduler_mode=%s)",
                    job_id,
                    scheduler_mode(),
                )
                future: concurrent.futures.Future[Any] = concurrent.futures.Future()
                future.set_result(None)
                return future
            # Multi-replica / production: fail closed — never dual-run on API.
            if is_production() or requires_distributed_backend():
                mongo = get_mongodb_service()
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=(
                        "Claim-queue scheduler enabled but enqueue failed — "
                        "check Mongo transfer_job_queue and worker/API claim loop"
                    ),
                    message="Scheduler enqueue failed",
                )
                raise RuntimeError(
                    f"Claim-queue scheduler could not enqueue job {job_id}. "
                    "Verify MONGODB_URI and that an API claim loop or Worker is running."
                )
            logger.warning(
                "Claim-queue enqueue failed for %s — falling back to local executor",
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
