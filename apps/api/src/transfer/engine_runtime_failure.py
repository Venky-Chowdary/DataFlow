"""The engine's answer to a run that raised: the job record and the API agree.

Split out of ``engine`` (module size budget). ``_fail_runtime_job`` writes the
job record; this returns the matching :class:`TransferResult`. A run whose
failure handler settled it as ``completed_with_warnings`` (a balanced run that
held its watermark tie group back, D335) is a success with an explanation,
never ``success=False`` beside a completed job.
"""

from __future__ import annotations

from typing import Any, Callable

from .models import TransferResult


def runtime_failure_result(
    fail_runtime_job: Callable[..., tuple[str, dict[str, Any]]],
    mongo: Any,
    job_id: str,
    exc: Exception,
    *,
    lineage: Any,
    request: Any,
    contract_id: str,
) -> TransferResult:
    display, error_details = fail_runtime_job(mongo, job_id, exc, lineage=lineage, request=request)
    undo_note = getattr(exc, "failed_batch_undo_note", "")
    if undo_note and undo_note not in display:
        display = f"{display} {undo_note}"
    if getattr(exc, "completes_with_warnings", False):
        return TransferResult(
            success=True,
            job_id=job_id,
            records_transferred=0,
            explanation=display,
            error_details={**(error_details or {}), "status": "completed_with_warnings"},
            operation=request.operation,
            contract_id=contract_id,
        )
    return TransferResult(
        success=False,
        job_id=job_id,
        error=display,
        error_details=error_details,
        row_accounting=error_details.get("row_accounting") or {},
        operation=request.operation,
        contract_id=contract_id,
    )
