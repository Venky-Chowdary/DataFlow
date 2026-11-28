"""Train 2: a run still verified after the write does not lower fully_proven_pct.

p3's 'verifying' verdict (flag-gated Parity at Scale) reached the dashboard's
decided-runs denominator (contract 11) as a run that was not proven.
"""

from __future__ import annotations

import pytest

import services.parity_state as ps
import services.parity_summary as psum
from services.mongodb_service import MemoryMongoDBService

PROVEN = {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True}


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    mem = MemoryMongoDBService()
    monkeypatch.setattr(ps, "job_store", lambda: mem)
    monkeypatch.setattr(psum, "job_store", lambda: mem)
    return mem


def _run(store) -> str:
    jid = store.create_transfer_job({
        "name": "nightly", "status": "completed", "workspace_id": "w", "reconciliation": PROVEN,
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    store.update_job_status(jid, "completed")
    return jid


def test_a_verifying_run_waits_outside_the_denominator(store, monkeypatch):
    _run(store)
    verifying = _run(store)
    real = psum._job_summary

    def summary(job, job_id):
        out = real(job, job_id)
        if job_id == verifying:
            # What build_parity_report states while every row is still being compared.
            out = {**out, "verdict": "verifying",
                   "tables": [{**t, "status": "verifying", "check": "verifying"} for t in out.get("tables") or []]}
        return out

    monkeypatch.setattr(psum, "_job_summary", summary)
    built = psum.build_summary(None, limit=30)
    assert [r["verdict"] for r in built["runs"]].count("verifying") == 1
    assert built["kpis"]["fully_proven_pct"] == 100.0, built["kpis"]
    assert built["kpis"]["runs"] == 2


def test_a_gate8_failed_run_still_counts_while_verification_runs(store, monkeypatch):
    """A run Gate-8 failed stays in the denominator even while verification runs:
    dropping it would raise fully_proven_pct above what the runs proved."""
    _run(store)
    failing = _run(store)
    real = psum._job_summary

    def summary(job, job_id):
        out = real(job, job_id)
        if job_id == failing:
            out = {**out, "verdict": "verifying",
                   "tables": [{**t, "status": "verifying", "check": "verifying", "failed_verification": True}
                              for t in out.get("tables") or []]}
        return out

    monkeypatch.setattr(psum, "_job_summary", summary)
    built = psum.build_summary(None, limit=30)
    assert built["kpis"]["fully_proven_pct"] == 50.0, built["kpis"]
