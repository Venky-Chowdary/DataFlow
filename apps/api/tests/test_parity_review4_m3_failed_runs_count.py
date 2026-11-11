"""Re-review 4, finding 3 (Medium): a run whose Gate-8 failed is not left out of the dashboard.

One proven run and nine runs Gate-8 failed (nothing localized) read ``fully_proven_pct: 100.0`` and
``runs_with_unexplained: 0``, and the heatmap drew the failed runs as "not checked": the
decided-runs denominator (contract 11) and the unverifiable verdict (contract 12) together removed
failed runs from every KPI.
"""

from __future__ import annotations

import pytest

import services.parity_state as ps
import services.parity_summary as psum
from services.mongodb_service import MemoryMongoDBService


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    mem = MemoryMongoDBService()
    monkeypatch.setattr(ps, "job_store", lambda: mem)
    monkeypatch.setattr(psum, "job_store", lambda: mem)
    return mem


def _run(store, recon: dict) -> str:
    jid = store.create_transfer_job({
        "name": "nightly", "status": "completed", "workspace_id": "w", "reconciliation": recon,
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    store.update_job_status(jid, "completed")
    return jid


FAILED = {"passed": False, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True,
          "verification_ladder": {"layers": {"L3": {"passed": False}}}}


def test_gate8_failed_runs_are_not_fully_proven(store):
    _run(store, {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True})
    for _ in range(9):
        _run(store, FAILED)
    summary = psum.build_summary(None, limit=30)
    kpis = summary["kpis"]
    assert kpis["fully_proven_pct"] == 10.0, kpis
    assert kpis["runs_failed_verification"] == 9
    assert summary["heatmap"]["cells"] == [["proven"] + ["failed"] * 9]


def test_a_run_without_evidence_stays_out_of_the_denominator(store):
    _run(store, {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True})
    _run(store, {"passed": True, "source_rows": 10, "target_rows": 10})  # counts only: unverifiable
    kpis = psum.build_summary(None, limit=30)["kpis"]
    assert kpis["fully_proven_pct"] == 100.0
    assert kpis["runs_failed_verification"] == 0


def test_a_stored_summary_without_the_failure_flag_is_recomputed(store):
    jid = _run(store, FAILED)
    from services.parity_report import summary_stamp

    job = store.get_job(jid)
    stale = {**summary_stamp(job), "verdict": "unverifiable", "method": "full", "unexplained": 0,
             "tables": [{"name": "orders", "status": "unverifiable", "check": "key_diff", "unexplained": 0}]}
    store.update_job_fields(jid, {"parity.summary": stale})
    assert store.get_job(jid)["parity"]["summary"] == stale
    assert psum.build_summary(None, limit=30)["heatmap"]["cells"] == [["failed"]]
