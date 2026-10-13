"""Side effects that belong to a job becoming terminal.

Status writes are the only place every completion path meets. Schedule
``run_count`` and connector health both waited on a later beat, so a
finished manual run stayed at 0 and a connector stayed "failed" after
the job that used it had succeeded.
"""

from __future__ import annotations

import logging

from services.job_status import COMPLETED_STATUSES, is_terminal

_logger = logging.getLogger(__name__)


def apply_job_terminal_effects(job_id: str, status: str) -> None:
    if not job_id or not is_terminal(status):
        return
    try:
        from services.schedule_runner import record_schedule_for_finished_job

        record_schedule_for_finished_job(job_id)
    except Exception:  # noqa: BLE001 - bookkeeping must not fail the transfer
        _logger.exception("Schedule run was not recorded for job %s", job_id)
    if status not in COMPLETED_STATUSES:
        return
    try:
        from services.connector_store import note_transfer_succeeded
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id) or {}
        note_transfer_succeeded(
            job.get("source_connector_id"),
            job.get("dest_connector_id"),
        )
    except Exception:  # noqa: BLE001 - health must not fail the transfer
        _logger.exception("Connector transfer stamp failed for job %s", job_id)
