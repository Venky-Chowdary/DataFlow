"""Jobs grid: the list carries what its Verification column and filters need.

The redesigned Jobs page shows a Verification column, a "Needs attention"
pivot (failed runs plus runs whose Gate-8 verification failed) and engine /
sync-mode / verification filters. All of it reads the job documents the list
query already returns — no per-row lookup — and the filters run server-side
so paging over a long history stays honest.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import mongodb_service as mongodb_service_mod
from services.job_list_filters import job_needs_attention, memory_job_matches, mongo_job_list_query
from services.job_list_view import slim_job_for_list, verification_summary_for_list
from services.mongodb_service import get_mongodb_service
from src.main import app


def test_verification_summary_keeps_verdict_scalars_and_drops_heavy_rows():
    job = {
        "_id": "j1",
        "status": "completed",
        "reconciliation": {
            "passed": True,
            "coverage": "full_checksum",
            "checksum_scope": "whole_table",
            "source_checksum": "abc",
            "target_checksum": "abc",
            "source_checksum_provenance": "independent_source_reread",
            "source_rows": 10,
            "target_rows": 10,
            "message": "m" * 5000,
            "identity": {"column": "id", "proven": True, "reason": "pk"},
            "sample_compare": {"passed": True, "compared": 5, "mismatches": [{"row": 1}] * 50},
            "mismatches": [{"row": 1}] * 500,
            "dest_readback": {"dest_count": 10},
            "streams": [{"name": "a"}],
        },
        "destination_summary": {"multi_stream": True, "streams": [{"name": "a"}, {"name": "b"}]},
    }
    slim = slim_job_for_list(job)
    summary = slim["verification_summary"]
    assert summary["passed"] is True
    assert summary["coverage"] == "full_checksum"
    assert summary["source_checksum_provenance"] == "independent_source_reread"
    assert summary["identity"] == {"proven": True}
    assert summary["sample_compare"] == {"passed": True, "compared": 5}
    assert len(summary["message"]) == 1000
    for heavy in ("mismatches", "dest_readback", "streams"):
        assert heavy not in summary
    assert slim["multi_stream"] is True
    # The full report stays on the detail endpoint only; the optimistic detail
    # hydrate from a list row must never mistake the summary for it.
    assert "reconciliation" not in slim
    assert "destination_summary" not in slim


def test_no_recorded_verdict_ships_no_summary():
    assert verification_summary_for_list({"status": "failed"}) is None
    assert verification_summary_for_list({"reconciliation": {}}) is None
    assert verification_summary_for_list({"reconciliation": "passed"}) is None
    assert "verification_summary" not in slim_job_for_list({"_id": "x", "status": "running"})
    assert "multi_stream" not in slim_job_for_list({"_id": "x", "destination_summary": {"multi_stream": False}})


def test_needs_attention_is_failed_or_failed_verification():
    assert job_needs_attention({"status": "failed"})
    assert job_needs_attention({"status": "completed", "reconciliation": {"passed": False}})
    assert not job_needs_attention({"status": "completed", "reconciliation": {"passed": True}})
    assert not job_needs_attention({"status": "completed"})
    assert not job_needs_attention({"status": "running", "reconciliation": None})


def test_mongo_query_for_attention_and_facets():
    query = mongo_job_list_query(
        {"workspace_id": "w"},
        status="attention",
        source_type="postgresql",
        destination_type="snowflake",
        sync_mode="upsert",
        verification="failed",
    )
    clauses = query["$and"]
    assert {"$or": [{"status": "failed"}, {"reconciliation.passed": False}]} in clauses
    assert {"source_type": "postgresql"} in clauses
    assert {"destination_type": "snowflake"} in clauses
    assert {"sync_mode": "upsert"} in clauses
    assert {"reconciliation.passed": False} in clauses
    assert mongo_job_list_query(verification="passed") == {"reconciliation.passed": True}
    assert mongo_job_list_query(verification="none") == {"reconciliation.passed": None}
    # Unknown or empty facets never narrow the query.
    assert mongo_job_list_query(verification="bogus", source_type="  ") == {}


def test_memory_matches_apply_the_same_facets():
    job = {
        "status": "completed",
        "source_type": "postgresql",
        "destination_type": "snowflake",
        "sync_mode": "upsert",
        "reconciliation": {"passed": False},
    }
    assert memory_job_matches(job, status="attention")
    assert memory_job_matches(job, source_type="postgresql", destination_type="snowflake", sync_mode="upsert")
    assert not memory_job_matches(job, source_type="mysql")
    assert memory_job_matches(job, verification="failed")
    assert not memory_job_matches(job, verification="passed")
    assert not memory_job_matches(job, verification="none")
    assert memory_job_matches({"status": "completed"}, verification="none")
    assert not memory_job_matches({"status": "completed"}, status="attention")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setattr(mongodb_service_mod, "_mongodb_service", None)
    with TestClient(app) as c:
        yield c


def _seed(name: str, status: str, *, day: int, source: str = "postgresql", dest: str = "mysql",
          mode: str = "full_refresh", passed: bool | None = None) -> str:
    mongo = get_mongodb_service()
    job_id = mongo.create_transfer_job({
        "name": name,
        "source_name": name,
        "source_type": source,
        "destination_type": dest,
        "destination_database": "warehouse",
        "destination_collection": name,
        "sync_mode": mode,
    })
    record = mongo._jobs[job_id]
    record["created_at"] = datetime(2026, 11, day, tzinfo=timezone.utc)
    if passed is not None:
        record["reconciliation"] = {"passed": passed, "source_rows": 3, "target_rows": 3 if passed else 2,
                                    "mismatches": [{"row": 1}]}
    mongo.update_job_status(job_id, status)
    return job_id


@pytest.mark.parametrize("path", ["/api/v1/connectors/jobs", "/api/v1/jobs"])
def test_list_counts_attention_and_filters_server_side(client, path):
    _seed("ok_proven", "completed", day=1, passed=True)
    _seed("ok_differs", "completed", day=2, source="mysql", dest="snowflake", mode="upsert", passed=False)
    _seed("broken", "failed", day=3)
    _seed("broken_after_verify", "failed", day=4, passed=False)
    _seed("live", "running", day=5)

    body = client.get(path).json()
    assert body["total"] == 5
    # Two failed runs plus one completed run whose verification failed — the
    # failed run that also failed verification is counted once.
    assert body["attention_count"] == 3
    rows = {job["name"]: job for job in body["jobs"]}
    assert rows["ok_differs"]["verification_summary"]["passed"] is False
    assert "mismatches" not in rows["ok_differs"]["verification_summary"]
    assert "verification_summary" not in rows["live"]

    attention = client.get(f"{path}?status=attention").json()
    assert sorted(job["name"] for job in attention["jobs"]) == ["broken", "broken_after_verify", "ok_differs"]
    assert attention["filtered_total"] == 3
    assert attention["total"] == 5

    faceted = client.get(f"{path}?source_type=mysql&destination_type=snowflake&sync_mode=upsert").json()
    assert [job["name"] for job in faceted["jobs"]] == ["ok_differs"]
    assert faceted["filtered_total"] == 1

    unverified = client.get(f"{path}?verification=none").json()
    assert sorted(job["name"] for job in unverified["jobs"]) == ["broken", "live"]
    assert client.get(f"{path}?verification=passed").json()["filtered_total"] == 1
    assert client.get(f"{path}?verification=bogus").status_code == 422
