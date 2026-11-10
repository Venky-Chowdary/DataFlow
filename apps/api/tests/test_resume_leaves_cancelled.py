"""Resume is the documented exit from ``cancelled``; a fresh cancel still wins.

QA 2026-11-10 / CI on feature/Cleanup: the cancel guard added for the
"canceled badges" fix (status != cancelled on every pending/running write)
also refused resume's own write, made with ``allow_terminal_exit=True`` after
``clear_job_cancel``. Every resumed job stayed ``cancelled`` (test_r8_d213,
test_d166, test_r12_d5, test_memory_job_cancel).
"""

from __future__ import annotations

from services.job_status import cancel_status_write_filter
from services.mongodb_service import MemoryMongoDBService


def test_resume_filter_still_refuses_a_cancel_that_lands_after_the_clear() -> None:
    worker = cancel_status_write_filter("running")
    assert worker == {"cancel_requested": {"$ne": True}, "status": {"$ne": "cancelled"}}
    resume = cancel_status_write_filter("pending", allow_terminal_exit=True)
    assert resume == {"cancel_requested": {"$ne": True}}
    assert cancel_status_write_filter("failed", allow_terminal_exit=True) is None


def _cancelled_job() -> tuple[MemoryMongoDBService, str]:
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "resume-1", "status": "running"})
    assert mongo.request_job_cancel(job_id)
    assert mongo.update_job_status(job_id, "cancelled", message="Cancelled")
    return mongo, job_id


def test_resume_moves_a_cancelled_job_back_to_running() -> None:
    mongo, job_id = _cancelled_job()
    assert mongo.clear_job_cancel(job_id)
    assert mongo.update_job_status(job_id, "running", allow_terminal_exit=True)
    assert mongo.get_job(job_id)["status"] == "running"


def test_resume_without_the_terminal_exit_stays_cancelled() -> None:
    mongo, job_id = _cancelled_job()
    assert mongo.clear_job_cancel(job_id)
    assert mongo.update_job_status(job_id, "running") is False
    assert mongo.get_job(job_id)["status"] == "cancelled"


def test_a_cancel_after_the_clear_blocks_the_resume_write() -> None:
    mongo, job_id = _cancelled_job()
    assert mongo.clear_job_cancel(job_id)
    assert mongo.request_job_cancel(job_id)  # operator cancels again at once
    assert mongo.update_job_status(job_id, "running", allow_terminal_exit=True) is False
    assert mongo.get_job(job_id)["status"] == "cancelled"
