"""UI pass 2 item 3: the Run screen shows which table is loading.

A multi-table run recorded a table in ``stream_progress`` only once it had
finished, so the Run screen could not tell the table being read and written
now from the ones still queued. Each table now records ``running`` (with its
start time and the job's row counter at that moment) before it loads; the
finish overwrites it with the completed record, and a resume still skips only
completed tables. Real engine path, SQLite to SQLite, two tables.
"""

from __future__ import annotations

import uuid

from tests.test_c5_resume_reversibility import _request, _seed
from tests.test_reversibility_review3 import fake  # noqa: F401
from tests.test_reversibility_review6 import _resumable  # noqa: F401


def test_each_table_is_recorded_running_before_it_loads(tmp_path, _resumable, monkeypatch):  # noqa: F811
    import services.multi_stream_resume as resume_mod
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine

    src, dst, specs = _seed(tmp_path)
    engine, job = UniversalTransferEngine(), "ptp" + uuid.uuid4().hex[:16]
    store = get_mongodb_service()
    snapshots: list[dict] = []
    real_started = resume_mod.record_stream_started

    def spy(job_id, stream_name, **kwargs):
        real_started(job_id, stream_name, **kwargs)
        snapshots.append({k: dict(v) for k, v in ((store.get_job(job_id) or {}).get("stream_progress") or {}).items()})

    monkeypatch.setattr(resume_mod, "record_stream_started", spy)
    result = engine.execute_tracked(_request(src, dst, specs), job)
    assert result.success is True, result.error
    assert [sorted(s) for s in snapshots] == [["people"], ["orders", "people"]]
    first, second = snapshots
    assert first["people"]["status"] == "running" and first["people"]["started_at"]
    assert isinstance(first["people"]["counter_at_start"], int)
    # When orders starts, people has finished with its own row count.
    assert second["people"]["status"] == "completed" and second["people"]["rows"] == 3
    assert second["orders"]["status"] == "running"


def test_a_running_entry_is_never_skipped_on_resume(monkeypatch):
    import services.multi_stream_resume as resume_mod
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService()
    monkeypatch.setattr(resume_mod, "_store", lambda: svc)
    job_id = svc.create_transfer_job({"_id": "ptp-" + uuid.uuid4().hex[:8], "status": "running", "records_processed": 40})
    resume_mod.record_stream_completed(job_id, "people", rows=3, sync_mode="upsert")
    resume_mod.record_stream_started(job_id, "orders")
    progress = svc.get_job(job_id)["stream_progress"]
    assert progress["orders"]["status"] == "running" and progress["orders"]["counter_at_start"] == 40
    assert set(resume_mod.completed_streams(job_id)) == {"people"}, "a table that was loading is read again"
    resume_mod.record_stream_started("", "orders")  # no job: nothing recorded, nothing raised


def test_a_table_that_stops_leaves_only_finished_tables_recorded(monkeypatch):
    import services.multi_stream_resume as resume_mod
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService()
    monkeypatch.setattr(resume_mod, "_store", lambda: svc)
    job_id = svc.create_transfer_job({"_id": "ptp-" + uuid.uuid4().hex[:8], "status": "running"})
    resume_mod.record_stream_completed(job_id, "people", rows=3, sync_mode="upsert")
    resume_mod.record_stream_started(job_id, "orders")
    resume_mod.forget_stream_started(job_id, "orders")
    resume_mod.forget_stream_started(job_id, "people")  # a finished table is never dropped this way
    assert sorted(svc.get_job(job_id)["stream_progress"]) == ["people"]
