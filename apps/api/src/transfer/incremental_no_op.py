"""A cursor-bounded run that finds no new rows is a success, not an empty read.

The buffered path used to report ``No records to transfer`` when the watermark
left nothing to send, so a healthy incremental schedule failed on every run in
which the source had not changed. The streaming path already treats this as a
no-op; this keeps both surfaces answering the same way.
"""

from __future__ import annotations

from .models import TransferRequest, TransferResult


def incremental_no_op_result(
    request: TransferRequest,
    job_id: str,
    watermark: str | None,
) -> TransferResult:
    """Close the job as a successful zero-row run at the current watermark."""
    try:
        from services.mongodb_service import get_mongodb_service
    except ImportError:  # pragma: no cover - packaging variant
        from src.services.mongodb_service import get_mongodb_service  # type: ignore

    summary = {
        "sync_mode": request.sync_mode,
        "source_row_count": 0,
        "source_row_count_source": "incremental_no_new_rows",
        "rejected_rows": 0,
        "incremental_watermark": watermark or "",
    }
    mongo = get_mongodb_service()
    mongo.update_job_status(
        job_id,
        "completed",
        phase="completed",
        progress_pct=100,
        total_rows=0,
        records_processed=0,
        message="No new rows past the incremental watermark — nothing to send.",
    )
    return TransferResult(
        success=True,
        job_id=job_id,
        records_transferred=0,
        operation=request.operation,
        destination_summary=summary,
    )
