"""Put a session-security note on the job that is running (QA MySQL D10).

A dial that had to use TLS although the operator asked for plaintext is a
fact about the run, not just a log line: the job record carries it in
``security_notes`` so the job page and the API say how the data travelled.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def record_job_tls_note(note: str) -> bool:
    """Append ``note`` to the current job's ``security_notes`` once. False when no job."""
    from services.logging_config import current_job_id

    job_id = current_job_id()
    if not job_id or not note:
        return False
    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    job = svc.get_job(job_id) or {}
    notes = [str(n) for n in (job.get("security_notes") or [])]
    if note in notes:
        return True
    ok = bool(svc.update_job_fields(job_id, {"security_notes": [*notes, note]},
                                    touch_updated_at=False))
    if not ok:
        logger.warning("could not record the TLS note on job %s", job_id)
    return ok
