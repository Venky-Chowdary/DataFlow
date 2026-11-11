"""Re-review 3, H4 (coordinator decision): a Gate-8 sample is not random, so it bounds nothing.

Gate-8 compares the first ``_RECONCILE_SAMPLE_CAP`` rows the run read, cut
to the lowest keys (``keyed_sorted``), the first rows (``positional_sorted``)
or a seeded per-stratum pick among them (``stratified``). None is a uniform
random draw over the population, so a Clopper-Pearson bound on the population
does not hold. The report now states only the rows checked and how they were
chosen, shows no percentage, blocks sign-off until every row is compared, and
offers the existing re-run as "Run full verification" where Gate-8's
verification ladder can compare every row.
"""

from __future__ import annotations

import pytest

from services import parity_evidence
from services.parity_report import build_parity_report, compact_summary


def _job(rows: int, *, method: str = "keyed_sorted", mismatches: list | None = None, **extra) -> dict:
    job = {
        "_id": "h4", "status": "completed",
        "reconciliation": {"passed": True, "source_rows": rows, "target_rows": rows,
                           "sample_compare": {"rows_compared": 50, "mismatches": mismatches or [],
                                              "sample_seed": {"method": method, "sort_key": "id",
                                                              "stratify_by": "region"}}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"source": {"kind": "database", "format": "postgresql", "table": "s"},
                             "destination": {"kind": "database", "format": "postgresql", "table": "t"},
                             "sync_mode": "full_refresh_overwrite"},
    }
    job.update(extra)
    return job


def test_no_gate8_sample_method_is_a_random_draw():
    assert parity_evidence.UNIFORM_RANDOM_SAMPLE_METHODS == frozenset()
    for method in ("keyed_sorted", "positional_sorted", "stratified", ""):
        assert parity_evidence.sample_bound_valid(method) is False


@pytest.mark.parametrize("method,how", [
    ("keyed_sorted", "the lowest id values among the first rows the run read"),
    ("positional_sorted", "the first rows the run read"),
    ("stratified", "a pick across region's values among the first rows the run read"),
])
def test_a_clean_sample_states_the_rows_checked_and_no_bound(method, how):
    report = build_parity_report(_job(10_000, method=method), reread=False)
    conf, table = report["confidence"], report["tables"][0]
    assert report["verdict"] == "no_difference_found"
    assert conf["statement"] == f"No difference in the 50 row(s) checked ({how}); the other 9,950 row(s) were not checked."
    assert "%" not in conf["statement"] + report["verdict_reason"]
    assert conf["upper_bound_rate"] is None and conf["bound_valid"] is False
    assert conf["rows_checked"] == 50 and conf["rows_total"] == 10_000 and conf["sample_description"] == how
    assert table["proven_equal_pct"] is None and table["upper_bound_rate"] is None and table["rows_checked"] == 50
    assert report["parity"]["proven_equal_pct"] is None


def test_sign_off_waits_for_a_full_compare():
    report = build_parity_report(_job(10_000), reread=False)
    assert report["signoff"]["status"] == "blocked"
    assert "Only 50 of 10,000 rows of t were checked" in report["signoff"]["blocked_reason"]
    assert "Run full verification" in report["signoff"]["blocked_reason"]


def test_run_full_verification_is_the_existing_rerun_where_the_ladder_can_compare_every_row():
    [fix] = [f for f in build_parity_report(_job(10_000), reread=False)["fixes"] if f["title"] == "Run full verification"]
    assert fix["action"] == "rerun" and fix["applicable"], fix["not_applicable_reason"]
    assert fix["preview"] == "POST /api/v1/connectors/jobs/h4/rerun"


def test_run_full_verification_is_refused_where_a_rerun_would_sample_again():
    from services.verification_ladder import MAX_LADDER_ROWS

    big = build_parity_report(_job(MAX_LADDER_ROWS + 1), reread=False)
    [fix] = [f for f in big["fixes"] if f["title"] == "Run full verification"]
    assert fix["applicable"] is False and "VERIFICATION_LADDER_MAX_ROWS" in fix["not_applicable_reason"]
    unsupported = _job(10_000)
    unsupported["reconciliation"]["ladder_declined_reason"] = "destination_engine_not_in_ladder"
    [fix] = [f for f in build_parity_report(unsupported, reread=False)["fixes"] if f["title"] == "Run full verification"]
    assert fix["applicable"] is False and "would sample again" in fix["not_applicable_reason"]


def test_differences_in_the_sample_are_counted_not_bounded():
    mm = [{"key": str(i), "target": "v", "source_value": f"x{i}", "target_value": f"y{i}"} for i in range(3)]
    report = build_parity_report(_job(10_000, mismatches=mm), reread=False)
    assert report["verdict"] == "unexplained"
    assert report["confidence"]["statement"].startswith("3 of the 50 row(s) checked hold an unexplained difference")
    assert report["confidence"]["upper_bound_rate"] is None


def test_the_dashboard_run_carries_its_method_and_no_figure(monkeypatch):
    from services import parity_summary

    summary = compact_summary(build_parity_report(_job(10_000), reread=False))
    assert summary["method"] == "sampled" and summary["proven_equal_pct"] is None
    assert summary["rows_checked"] == 50 and summary["rows_total"] == 10_000

    class Store:
        def list_jobs(self, limit, workspace_filter):
            return [{"_id": "h4"}]

        def get_jobs(self, ids):
            return {"h4": {"_id": "h4", "status": "completed", "parity": {"summary": summary}}}

    monkeypatch.setattr(parity_summary, "job_store", lambda: Store())
    [run] = parity_summary.build_summary(None, limit=5)["runs"]
    assert run["method"] == "sampled" and run["proven_equal_pct"] is None
    assert run["rows_checked"] == 50 and run["rows_total"] == 10_000


def test_a_stored_summary_without_the_method_is_recomputed():
    from services.parity_summary import _job_summary

    job = _job(10_000)
    job["parity"] = {"summary": {"verdict": "no_difference_found", "proven_equal_pct": 99.4,
                                 "tables": [{"name": "t", "check": "sampled"}]}}
    assert _job_summary(job, "h4")["proven_equal_pct"] is None
