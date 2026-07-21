"""Durable job event_log is appended on meaningful status updates."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mongodb_service import MemoryMongoDBService  # noqa: E402


def test_inmemory_job_appends_event_log_on_phase_and_message():
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"source_name": "demo", "name": "demo → out"})
    assert mongo.update_job_status(job_id, "running", phase="writing", message="Writing 1000 rows…")
    assert mongo.update_job_status(job_id, "running", phase="writing", message="Writing batch 2/10…")
    assert mongo.update_job_status(job_id, "running", records_processed=25_000, message="Writing batch 2/10…")
    job = mongo.get_job(job_id)
    assert job is not None
    log = job.get("event_log") or []
    assert any("Entered writing phase" in line for line in log)
    assert any("Writing 1000 rows" in line for line in log)
    assert any("25,000 rows processed" in line or "25000 rows processed" in line for line in log)
    # Same message should not spam endlessly
    before = len(log)
    assert mongo.update_job_status(job_id, "running", phase="writing", message="Writing batch 2/10…")
    job2 = mongo.get_job(job_id)
    assert len(job2.get("event_log") or []) <= before + 1
