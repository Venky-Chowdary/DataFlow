"""Review finding 9: a clean sample is not proof.

A clean 2,000-row sample of a large table was verdict "proven" and the ring read
"99.85% rows proven equal". "Proven" now needs every table compared in full; a
sample that found nothing is ``no_difference_found`` with its bound, and the
dashboard heatmap marks it ``sampled``, not ``proven``.
"""

from __future__ import annotations

from services.parity_report import build_parity_report, compact_summary
from services.parity_summary import _cell


def _job(recon: dict) -> dict:
    return {
        "_id": "job-1", "status": "completed", "reconciliation": recon,
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    }


def test_a_clean_sample_is_no_difference_found_without_a_bound():
    # Re-review 3, H4 (coordinator decision): Gate-8's sample is not a random draw, so it
    # bounds nothing: the verdict states the rows checked, and sign-off waits for a full compare.
    report = build_parity_report(_job({
        "passed": True, "source_rows": 5_000_000, "target_rows": 5_000_000,
        "sample_compare": {"rows_compared": 2000, "mismatches": []},
    }), reread=False)
    assert report["verdict"] == "no_difference_found", report["verdict_reason"]
    assert "2,000 row(s) checked" in report["verdict_reason"] and "%" not in report["verdict_reason"]
    assert report["confidence"]["method"] == "sampled"
    assert report["parity"]["proven_equal_pct"] is None
    assert report["signoff"]["status"] == "blocked"
    summary = compact_summary(report)
    assert summary["verdict"] == "no_difference_found"
    assert _cell(summary["tables"][0]) == "sampled"


def test_a_full_compare_with_no_difference_is_still_proven():
    report = build_parity_report(_job({
        "passed": True, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
    }), reread=False)
    assert report["verdict"] == "proven"
    assert _cell(compact_summary(report)["tables"][0]) == "proven"


def test_summary_kpi_does_not_count_a_sample_as_fully_proven(monkeypatch):
    from services import parity_summary

    jobs = {
        "a": _job({"passed": True, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True}),
        "b": _job({"passed": True, "source_rows": 10**6, "target_rows": 10**6,
                   "sample_compare": {"rows_compared": 2000, "mismatches": []}}),
    }

    class _Store:
        def list_jobs(self, limit, workspace_filter):
            return [{"_id": k} for k in jobs]

        def get_job(self, job_id):
            return {**jobs[job_id], "_id": job_id}

    monkeypatch.setattr(parity_summary, "job_store", lambda: _Store())
    body = parity_summary.build_summary(None, limit=10)
    assert {r["verdict"] for r in body["runs"]} == {"proven", "no_difference_found"}
    assert body["kpis"]["fully_proven_pct"] == 50.0
