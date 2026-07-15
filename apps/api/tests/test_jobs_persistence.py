"""Job store persistence tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.jobs import JsonFileJobStore, MemoryJobStore


def test_json_file_job_store_persists_and_reloads(tmp_path: Path):
    path = tmp_path / "jobs.json"
    store = JsonFileJobStore(path)
    rec = store.create(operation="upload", source="s.csv", destination="mongodb", total_rows=10)
    store.set_running(rec.job_id, total_rows=10, table_name="imported")
    store.update_progress(rec.job_id, current_chunk=1, total_chunks=2, rows_processed=5)
    store.add_checkpoint(rec.job_id, chunk=1, total=2, rows=5)
    store.complete(rec.job_id, 10, reconciliation={"passed": True, "message": "ok"})

    # Simulate restart by loading from disk.
    store2 = JsonFileJobStore(path)
    job = store2.get(rec.job_id)
    assert job is not None
    assert job.status == "completed"
    assert job.rows_processed == 10
    assert len(job.checkpoints) == 1
    assert job.reconciliation == {"passed": True, "message": "ok"}


def test_json_file_job_store_survives_failed_job(tmp_path: Path):
    path = tmp_path / "jobs.json"
    store = JsonFileJobStore(path)
    rec = store.create(operation="upload", source="s.csv", destination="mongodb")
    store.fail(rec.job_id, "connection lost")

    store2 = JsonFileJobStore(path)
    job = store2.get(rec.job_id)
    assert job is not None
    assert job.status == "failed"
    assert job.message == "connection lost"


def test_json_file_job_store_atomic_write(tmp_path: Path):
    path = tmp_path / "jobs.json"
    store = JsonFileJobStore(path)
    store.create(operation="upload", source="s.csv", destination="mongodb")
    assert path.exists()
    assert not (tmp_path / "jobs.json.tmp").exists()


def test_json_file_job_store_empty_file_ignored(tmp_path: Path):
    path = tmp_path / "jobs.json"
    path.write_text("", encoding="utf-8")
    store = JsonFileJobStore(path)
    assert store.stats()["total_jobs"] == 0


def test_json_file_job_store_invalid_json_ignored(tmp_path: Path):
    path = tmp_path / "jobs.json"
    path.write_text("not json", encoding="utf-8")
    store = JsonFileJobStore(path)
    assert store.stats()["total_jobs"] == 0


def test_json_file_job_store_uses_memory_backend_interface(tmp_path: Path):
    path = tmp_path / "jobs.json"
    store = JsonFileJobStore(path)
    assert isinstance(store, MemoryJobStore)
