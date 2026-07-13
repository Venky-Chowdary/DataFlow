"""Tests for checkpoint persistence and resume helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.checkpoint_service import (
    Checkpoint,
    CheckpointService,
    resume_or_create_checkpoint,
)


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


def test_checkpoint_round_trip():
    mongo = _FakeMongo()
    cp = Checkpoint(
        job_id="job-cp-1",
        chunk_index=3,
        offset=1500,
        cursor_value="2025-01-01",
        cursor_column="updated_at",
        rows_processed=1500,
    )
    service = CheckpointService(mongo)
    assert service.save(cp)
    loaded = service.load("job-cp-1")
    assert loaded is not None
    assert loaded.chunk_index == 3
    assert loaded.cursor_value == "2025-01-01"


def test_resume_or_create_initializes_when_missing():
    service = CheckpointService(_FakeMongo())
    cp = resume_or_create_checkpoint("job-cp-2", service)
    assert cp.job_id == "job-cp-2"
    assert cp.chunk_index == 0


def test_resume_or_create_loads_existing():
    mongo = _FakeMongo()
    mongo.update_job_status("job-cp-3", "running", checkpoint=Checkpoint(job_id="job-cp-3", chunk_index=7).to_dict())
    service = CheckpointService(mongo)
    cp = resume_or_create_checkpoint("job-cp-3", service)
    assert cp.chunk_index == 7


def test_mark_failed_sets_checkpoint_and_status():
    mongo = _FakeMongo()
    service = CheckpointService(mongo)
    cp = Checkpoint(job_id="job-cp-4", chunk_index=2)
    assert service.mark_failed("job-cp-4", "connection lost", cp)
    assert mongo.jobs["job-cp-4"]["status"] == "failed"
    assert mongo.jobs["job-cp-4"]["checkpoint"]["chunk_index"] == 2
