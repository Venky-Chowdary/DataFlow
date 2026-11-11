"""Re-review finding 15 (reliability): live reads are cached per version; the summary batches.

Every GET ran a live keyed re-read and catalog introspection, the UI polled it
every 5 s, and the summary read every listed job one by one (up to 200
round trips). The page's reads are now reused for the same parity-state
version for a short window (decisions always read fresh and bump the
version), and the summary reads its page of jobs in one batch.
"""

from __future__ import annotations

import pytest

from services import parity_cache
from services import parity_report as pr


@pytest.fixture(autouse=True)
def _fresh_cache():
    parity_cache.clear()
    yield
    parity_cache.clear()


def _job(version: int = 0) -> dict:
    return {
        "_id": "cache-1", "status": "completed", "updated_at": "2026-11-11T00:00:00+00:00",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "parity": {"version": version},
    }


def _counting(monkeypatch) -> list[int]:
    reads: list[int] = []

    def fake(job, ev, keys):
        reads.append(1)
        return {"7": {"raw": {"id": "7", "v": "a"}, "expected": {"id": "7", "v": "a"},
                      "dest": {"id": "7", "v": "b"}, "held_out": False}}

    monkeypatch.setattr(pr, "_bounded_reread", fake)
    return reads


def test_the_page_reuses_its_reread_within_the_window(monkeypatch):
    reads = _counting(monkeypatch)
    for _ in range(5):
        report = pr.build_parity_report(_job(), cache="use")
    assert len(reads) == 1
    assert report["evidence"]["reread"]["t"]["status"] == "ok" and "cached_at" in report["evidence"]["reread"]["t"]


def test_a_new_version_and_a_decision_read_fresh(monkeypatch):
    reads = _counting(monkeypatch)
    pr.build_parity_report(_job(0), cache="use")
    pr.build_parity_report(_job(1), cache="use")  # a decision bumped the version
    assert len(reads) == 2
    pr.build_parity_report(_job(1))  # decisions and re-checks: always fresh
    assert len(reads) == 3


def test_the_window_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PARITY_READ_CACHE_SECONDS", "0")
    reads = _counting(monkeypatch)
    pr.build_parity_report(_job(), cache="use")
    pr.build_parity_report(_job(), cache="use")
    assert len(reads) == 2


def test_a_failed_reread_is_not_retried_by_every_poll(monkeypatch):
    calls: list[int] = []

    def down(job, ev, keys):
        calls.append(1)
        raise ConnectionError("source down")

    monkeypatch.setattr(pr, "_bounded_reread", down)
    for _ in range(3):
        report = pr.build_parity_report(_job(), cache="use")
    assert len(calls) == 1 and report["evidence"]["reread"]["t"]["status"] == "failed"


def test_the_summary_reads_its_jobs_in_one_batch(monkeypatch):
    from services import parity_summary

    jobs = {f"j{i}": {"_id": f"j{i}", "status": "completed", "reconciliation": {
        "passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"]}} for i in range(30)}
    counts = {"get_job": 0, "get_jobs": 0}

    class Store:
        def list_jobs(self, limit, workspace_filter):
            return [{"_id": k} for k in list(jobs)[:limit]]

        def get_job(self, job_id):
            counts["get_job"] += 1
            return jobs.get(job_id)

        def get_jobs(self, ids):
            counts["get_jobs"] += 1
            return {i: jobs[i] for i in ids if i in jobs}

    monkeypatch.setattr(parity_summary, "job_store", lambda: Store())
    summary = parity_summary.build_summary(None, limit=30)
    assert summary["kpis"]["runs"] == 30
    assert counts == {"get_job": 0, "get_jobs": 1}


@pytest.mark.parametrize("kind", ["memory"])
def test_the_memory_store_reads_a_batch(kind):
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService()
    a = svc.create_transfer_job({"name": "a"})
    b = svc.create_transfer_job({"name": "b"})
    got = svc.get_jobs([a, b, "missing"])
    assert set(got) == {a, b} and got[a]["name"] == "a"


def test_the_mongo_store_reads_a_batch():
    import os
    import uuid

    import pymongo

    from services.mongodb_service import MongoDBService

    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"
    try:
        pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500).admin.command("ping")
    except Exception:  # noqa: BLE001 - no server: skipped
        pytest.skip("MongoDB not reachable")
    svc = MongoDBService(uri)
    svc.db_name = "parity_batch_" + uuid.uuid4().hex[:8]
    svc.connect()
    try:
        a = svc.create_transfer_job({"name": "a"})
        b = svc.create_transfer_job({"_id": "custom-id-b", "name": "b"})
        got = svc.get_jobs([a, b, "missing"])
        assert set(got) == {a, b} and got[b]["name"] == "b"
    finally:
        svc.client.drop_database(svc.db_name)


def test_a_decision_reads_both_sides_once_not_again_in_its_compare_and_swap(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log
    from services.mongodb_service import MemoryMongoDBService
    from services.parity_fixes import approve_fix

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    reads = _counting(monkeypatch)
    job_id = svc.create_transfer_job({
        "name": "n", "workspace_id": "w",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    report = pr.build_parity_report(svc.get_job(job_id), job_id=job_id, cache="use")
    accept = next(f for f in report["fixes"] if f["action"] == "accept")
    reads.clear()
    approve_fix(job_id, accept["id"], actor="ops@example.com", note="known")
    assert len(reads) == 1, "the decision re-read once; its compare-and-swap reused that read"
