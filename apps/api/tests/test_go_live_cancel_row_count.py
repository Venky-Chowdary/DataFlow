"""A cancelled job must report the rows that already committed.

DEF-C-023: cancel set the job to cancelled with records_processed 0 while the
writer had already inserted the table. The checkpoint saw the cancel before
it stored the count, and the later progress write was refused because cancel
blocks a ``running`` update.
"""

from __future__ import annotations

import pytest

from services.error_handling import TransferCancelled
from services.mongodb_service import MemoryMongoDBService
from src.transfer.engine import _progress_write_or_cancel, _raise_if_job_cancelled
from src.transfer.job_failure import _fail_runtime_job


@pytest.fixture
def quiet_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "services.cdc_catchup.release_finished_cdc_slot",
        lambda *args, **kwargs: {"released": False, "reason": "test"},
    )


def test_cancel_after_a_committed_chunk_keeps_the_row_count(quiet_slot: None) -> None:
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job(
        {"_id": "cancel-rows", "status": "running", "records_processed": 0}
    )
    mongo.request_job_cancel(job_id)
    assert mongo.update_job_status(
        job_id, "cancelled", phase="cancelled", message="Transfer cancelled by user"
    )

    with pytest.raises(TransferCancelled) as raised:
        _raise_if_job_cancelled(mongo, job_id, rows_written=90)
    assert raised.value.rows_written == 90
    assert "90" in str(raised.value)
    assert "not removed" in str(raised.value)

    _fail_runtime_job(mongo, job_id, raised.value, request=None)
    job = mongo.get_job(job_id)
    assert job is not None
    assert job["status"] == "cancelled"
    assert job["records_processed"] == 90


def test_a_refused_progress_write_carries_the_committed_count(quiet_slot: None) -> None:
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job(
        {"_id": "cancel-progress", "status": "running", "records_processed": 0}
    )
    mongo.request_job_cancel(job_id)
    mongo.update_job_status(job_id, "cancelled", phase="cancelled")

    with pytest.raises(TransferCancelled) as raised:
        _progress_write_or_cancel(mongo, job_id, records_processed=90)
    assert raised.value.rows_written == 90
    _fail_runtime_job(mongo, job_id, raised.value, request=None)
    job = mongo.get_job(job_id)
    assert job is not None
    assert job["records_processed"] == 90
    assert job["status"] == "cancelled"


def test_cancel_before_any_commit_does_not_invent_rows() -> None:
    exc = TransferCancelled("Transfer cancelled by user")
    assert not hasattr(exc, "rows_written")
    assert str(exc) == "Transfer cancelled by user"
