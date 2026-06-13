"""Job store unit tests (in-memory backend)."""

from services.jobs import MemoryJobStore


def test_create_and_complete():
    store = MemoryJobStore()
    job = store.create(operation="upload", source="a.csv", destination="pg://db", total_rows=100)
    assert job.status == "queued"
    store.set_running(job.job_id, total_rows=100, table_name="df_test")
    store.add_checkpoint(job.job_id, chunk=1, total=2, rows=50)
    store.update_progress(job.job_id, current_chunk=1, total_chunks=2, rows_processed=50)
    done = store.complete(job.job_id, 100, reconciliation={"passed": True, "message": "OK"})
    assert done is not None
    assert done.status == "completed"
    assert len(done.checkpoints) == 1


def test_fail_job():
    store = MemoryJobStore()
    job = store.create(operation="upload", source="x", destination="y")
    store.fail(job.job_id, "Connection refused")
    updated = store.get(job.job_id)
    assert updated is not None
    assert updated.status == "failed"
    assert updated.workflow_phase == "failed"
