"""Pilot job reads must survive Mongo being down.

Transfers already write to the engine job_store in that case. Asking
"status of my last transfer" and getting "MongoDB unavailable" was the
product claiming it could not see a run that was sitting in the local store.
"""

from __future__ import annotations

from src.ai.copilot.job_reads import list_transfer_jobs, read_transfer_job
from src.ai.copilot.tools import DataPilotTools


class _DownMongo:
    def list_jobs(self, limit=10, workspace_id=None):
        raise ConnectionError("MongoDB unavailable at mongodb://localhost:27017")

    def count_jobs(self, workspace_id=None):
        raise ConnectionError("MongoDB unavailable at mongodb://localhost:27017")

    def get_job(self, job_id: str):
        raise ConnectionError("MongoDB unavailable at mongodb://localhost:27017")


def test_list_jobs_reads_engine_store_when_mongo_is_down(monkeypatch):
    from services.jobs import MemoryJobStore

    store = MemoryJobStore()
    rec = store.create(
        operation="copy",
        source="Audit SQLite",
        destination="warehouse",
        total_rows=12,
    )
    store.complete(rec.job_id, 12, reconciliation={"passed": True})

    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: _DownMongo())
    monkeypatch.setattr("src.ai.copilot.job_reads.job_store", store)

    jobs, counts, source = list_transfer_jobs(limit=5)
    assert source == "job_store"
    assert counts["total"] >= 1
    assert any(j["id"] == rec.job_id for j in jobs)
    assert any(j["status"] == "completed" for j in jobs)

    tools = DataPilotTools()
    result = tools.execute("list_jobs", {"limit": 5})
    assert result.success, result.error
    assert result.output["store"] == "job_store"
    assert result.output["total"] >= 1
    assert "MongoDB unavailable" not in (result.error or "")


def test_list_jobs_skips_a_mongo_client_that_already_failed_to_connect(monkeypatch):
    """Startup already pinged Mongo. Do not pay the 5s timeout again per turn."""
    from services.jobs import MemoryJobStore
    from src.ai.copilot.job_reads import list_transfer_jobs

    store = MemoryJobStore()
    rec = store.create(operation="copy", source="a", destination="b", total_rows=1)
    store.complete(rec.job_id, 1)

    class MongoDBService:
        client = None

        def list_jobs(self, limit=10, workspace_id=None):
            raise AssertionError("must not re-ping Mongo after a failed connect")

        def count_jobs(self, workspace_id=None):
            raise AssertionError("must not re-ping Mongo after a failed connect")

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: MongoDBService()
    )
    monkeypatch.setattr("src.ai.copilot.job_reads.job_store", store)

    jobs, _counts, source = list_transfer_jobs(limit=5)
    assert source == "job_store"
    assert any(j["id"] == rec.job_id for j in jobs)


def test_get_job_reads_engine_store_when_mongo_misses(monkeypatch):
    from services.jobs import MemoryJobStore

    store = MemoryJobStore()
    rec = store.create(
        operation="copy",
        source="pg",
        destination="mysql",
        total_rows=4,
    )
    store.fail(rec.job_id, "destination refused the write")

    class _EmptyMongo:
        def get_job(self, job_id: str):
            return None

    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: _EmptyMongo())
    monkeypatch.setattr("src.ai.copilot.job_reads.job_store", store)

    job = read_transfer_job(rec.job_id)
    assert job is not None
    assert job["status"] == "failed"
    assert "refused" in str(job.get("error") or "")
