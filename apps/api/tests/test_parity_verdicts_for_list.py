"""UI pass 2 item 1: the Jobs grid shows the Trust Report's verdict word.

The list payload drops the evidence a verdict is decided from (mismatch rows),
so the grid reads ``GET /parity/verdicts`` for its page. These tests pin the
service on the memory store (no MongoDB needed): the verdict equals the one
the Trust Report states for the same job, hidden and live jobs are left out,
and one broken job does not blank the page. The route itself, with workspace
isolation and the 100-id cap, is covered on live MongoDB in test_parity_api.
"""

from __future__ import annotations

import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch):
    import services.mongodb_service as mongodb_service

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    return svc


def _job(svc, *, status: str = "completed", workspace: str = "ws", **over) -> str:
    doc = {
        "_id": "v" + uuid.uuid4().hex[:10], "workspace_id": workspace, "status": status,
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 99, "assurance_level": "none",
                           "sample_compare": {"rows_compared": 100, "mismatches": [
                               {"key": "5", "source": "amount", "target": "amount",
                                "source_value": "10.1049", "target_value": "10.10"}]},
                           "culprit_keys": {"missing_in_dest": ["42"]}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    }
    doc.update(over)
    job_id = svc.create_transfer_job(doc)
    svc.update_job_status(job_id, status)
    return job_id


def test_verdict_is_the_trust_reports_and_hidden_or_live_jobs_are_left_out(store):
    from services.parity_report import build_parity_report
    from services.parity_summary import job_verdicts

    bad = _job(store)
    clean = _job(store, reconciliation={"passed": True, "source_rows": 10, "target_rows": 10,
                                        "sample_compare": {"rows_compared": 10, "mismatches": []}})
    none = _job(store, reconciliation={})
    theirs = _job(store, workspace="other")
    live = _job(store, status="running")

    got = job_verdicts([bad, clean, none, theirs, live, "missing"], lambda job: job.get("workspace_id") == "ws")
    assert set(got) == {bad, clean, none}
    for job_id in (bad, clean, none):
        report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
        assert got[job_id]["verdict"] == report["verdict"], job_id
        assert got[job_id]["unexplained"] == report["parity"]["unexplained"]
    assert got[bad]["verdict"] == "unexplained" and got[bad]["unexplained"] == 2
    assert got[clean]["verdict"] == "no_difference_found"
    assert got[none]["verdict"] == "unverifiable"  # no reconciliation: nothing was compared


def test_one_unreadable_job_does_not_blank_the_page(store, monkeypatch):
    import services.parity_summary as parity_summary

    good, broken = _job(store), _job(store)
    real = parity_summary._job_summary

    def flaky(job, job_id):
        if job_id == broken:
            raise ValueError("corrupt evidence")
        return real(job, job_id)

    monkeypatch.setattr(parity_summary, "_job_summary", flaky)
    assert set(parity_summary.job_verdicts([good, broken], lambda job: True)) == {good}


def test_route_needs_only_job_read():
    from services.rbac import Permission, _required_permission

    assert _required_permission("GET", "/api/v1/parity/verdicts") == Permission.JOB_READ


def test_the_list_row_carries_a_stamp_that_changes_with_parity_decisions(store):
    """Review UI2 #2: parity writes do not touch updated_at, so the grid needs this stamp."""
    from services.job_list_view import parity_stamp, slim_job_for_list

    job_id = _job(store)
    doc = store.get_job(job_id)
    assert parity_stamp(doc) == "" and "parity_stamp" not in slim_job_for_list(doc)
    doc["parity"] = {"version": 2, "summary": {"verdict": "explained", "at": "2026-11-21T10:00:00Z"}}
    assert slim_job_for_list(doc)["parity_stamp"] == "2:2026-11-21T10:00:00Z"
    doc["parity"]["summary"]["at"] = "2026-11-21T10:05:00Z"
    assert slim_job_for_list(doc)["parity_stamp"] == "2:2026-11-21T10:05:00Z", "a re-read moves the stamp"
