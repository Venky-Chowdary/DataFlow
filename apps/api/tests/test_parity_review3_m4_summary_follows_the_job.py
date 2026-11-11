"""Re-review 3, M4: the dashboard summary follows the job, not the first read of its report.

A GET while the job ran stored the compact summary "unverifiable"; the job's
completion does not bump the parity version, so the dashboard kept that
verdict until someone opened the report. A running job's summary is no
longer stored, and a stored summary carries the job status and update time
it was computed for — any other state is recomputed.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch):
    import services.mongodb_service as mongodb_service
    from services import parity_state, parity_summary

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(parity_state, "job_store", lambda: svc)
    monkeypatch.setattr(parity_summary, "job_store", lambda: svc)
    return svc


def _running(store) -> str:
    jid = store.create_transfer_job({"name": "n", "workspace_id": "w", "destination_summary": {
        "table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"}})
    store.update_job_status(jid, "running")
    return jid


def _complete(store, jid: str) -> None:
    store.update_job_fields(jid, {"status": "completed", "reconciliation": {
        "passed": False, "source_rows": 1000, "target_rows": 990, "culprit_census_clean": True}})


def test_a_running_jobs_report_is_not_stored(store):
    from services.parity_report import build_parity_report
    from services.parity_summary import remember_report

    jid = _running(store)
    job = store.get_job(jid)
    remember_report(jid, job, build_parity_report(job, reread=False, job_id=jid))
    assert (store.get_job(jid).get("parity") or {}).get("summary") is None


def test_a_summary_stored_for_another_job_state_is_recomputed(store):
    from services.parity_summary import _job_summary, build_summary

    jid = _running(store)
    # A summary some older code stored while the job ran (no stamp) ...
    store.update_job_fields(jid, {"parity.summary": {"verdict": "unverifiable", "method": "none",
                                                      "proven_equal_pct": None, "unexplained": 0, "tables": []}})
    _complete(store, jid)
    assert _job_summary(store.get_job(jid), jid)["verdict"] == "unexplained"
    [run] = build_summary(None, limit=10)["runs"]
    assert run["verdict"] == "unexplained" and run["unexplained"] == 10


def test_a_current_stamped_summary_is_used(store):
    from services.parity_report import build_parity_report
    from services.parity_summary import _job_summary, remember_report

    jid = _running(store)
    _complete(store, jid)
    job = store.get_job(jid)
    remember_report(jid, job, build_parity_report(job, reread=False, job_id=jid))
    stored = store.get_job(jid)["parity"]["summary"]
    assert stored["job_status"] == "completed"
    assert _job_summary(store.get_job(jid), jid) == stored
