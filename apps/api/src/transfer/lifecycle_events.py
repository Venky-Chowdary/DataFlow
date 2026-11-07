from __future__ import annotations

import logging
import time
from typing import Any

from services.connector_telemetry import log_retry
from services.ops_events import CANCELLATION_ERRORS, emit_event

from .models import TransferRequest


def _run_event_fields(
    request: TransferRequest, job_id: str, resume: bool
) -> dict[str, Any]:
    """Bounded descriptors for transfer lifecycle events — never object names."""
    return {
        "job_id": job_id,
        "workspace_id": request.workspace_id or None,
        "operation": request.operation or None,
        "source_kind": request.source.kind or None,
        "source_type": request.source.format or None,
        "destination_kind": request.destination.kind or None,
        "destination_type": request.destination.format or None,
        "sync_mode": request.sync_mode or None,
        "resume": bool(resume),
    }


def emit_run_started(
    logger: logging.Logger, request: TransferRequest, job_id: str, resume: bool
) -> dict[str, Any]:
    fields = _run_event_fields(request, job_id, resume)
    emit_event(
        logger,
        "transfer.run.started",
        outcome="started",
        **fields,
    )
    return fields


def emit_run_exception(
    logger: logging.Logger,
    fields: dict[str, Any],
    started: float,
    error: BaseException,
) -> None:
    cancelled = isinstance(error, CANCELLATION_ERRORS)
    emit_event(
        logger,
        "transfer.run.cancelled" if cancelled else "transfer.run.failed",
        outcome="cancelled" if cancelled else "errored",
        duration_ms=(time.monotonic() - started) * 1000.0,
        error=error,
        **fields,
    )


def emit_run_finished(
    logger: logging.Logger,
    fields: dict[str, Any],
    started: float,
    result: Any,
) -> None:
    succeeded = bool(getattr(result, "success", False))
    emit_event(
        logger,
        "transfer.run.succeeded" if succeeded else "transfer.run.failed",
        outcome="succeeded" if succeeded else "failed",
        duration_ms=(time.monotonic() - started) * 1000.0,
        records_transferred=int(getattr(result, "records_transferred", 0) or 0),
        **fields,
    )


def job_requested_fields(request: TransferRequest) -> dict[str, Any]:
    fields = _run_event_fields(request, "", False)
    fields.pop("job_id")
    fields.pop("resume")
    return fields


def emit_job_requested_skipped(
    logger: logging.Logger, fields: dict[str, Any]
) -> None:
    emit_event(
        logger,
        "job.requested",
        outcome="skipped",
        reason="duplicate_submission",
        **fields,
    )


def emit_job_requested_succeeded(
    logger: logging.Logger,
    job_id: str,
    triggered_by_set: bool,
    fields: dict[str, Any],
) -> None:
    emit_event(
        logger,
        "job.requested",
        outcome="succeeded",
        job_id=str(job_id),
        triggered_by_set=triggered_by_set,
        **fields,
    )


def source_read_retry_callback(request: TransferRequest, budget: Any):
    return log_retry(
        "source_read",
        driver_family=request.source.format or request.source.kind,
        budget=budget,
    )
