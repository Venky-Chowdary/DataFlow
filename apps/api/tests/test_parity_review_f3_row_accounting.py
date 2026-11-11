"""Review finding 3: row accounting must not hide missing rows.

(a) An upsert into an occupied table set the count gap to ``None``, so a
destination holding fewer rows than this run wrote showed no shortfall.
(b) ``missing = gap - keyed_missing - drift_rows`` subtracted the write
witness's count of rows that are *in* the destination (a value changed at the
source after the write moves no count): 10 lost rows of an overwrite vanished
behind 10 drifted ones. Witnessed losses were subtracted as named missing rows
and turned into phantom extras.
"""

from __future__ import annotations

from services.parity_report import build_parity_report


def _job(recon: dict, dest: dict | None = None) -> dict:
    return {
        "_id": "job-1", "status": "completed", "reconciliation": recon,
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite", **(dest or {})},
    }


def _missing(report: dict) -> int:
    return sum(d["rows"] for d in report["differences"]
               if d["kind"] == "missing_in_destination" and not d["cause"]["explained"])


def test_upsert_into_an_occupied_table_reports_a_shortfall():
    # 1000 source rows upserted into a table of 500: whatever the overlap, at
    # least 1000 rows must be there; 700 means at least 300 never landed.
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1000, "target_rows": 700, "target_rows_before": 500,
        "sample_compare": {"rows_compared": 200, "mismatches": []},
    }, {"sync_mode": "upsert"}), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert _missing(report) == 300


def test_the_reviewers_upsert_case_is_not_explained():
    # 1200 is consistent with 300 overlapping keys, so counts alone cannot name
    # a loss — but Gate-8 failed and nothing accounts for it: never "explained".
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1000, "target_rows": 1200, "target_rows_before": 500,
        "sample_compare": {"rows_compared": 200, "mismatches": []},
    }, {"sync_mode": "upsert"}), reread=False)
    assert report["verdict"] not in {"proven", "explained"}, report["verdict_reason"]
    assert report["signoff"]["status"] == "blocked"


def test_drifted_rows_do_not_hide_lost_rows():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1000, "target_rows": 990, "culprit_census_clean": True,
        "concurrent_source_drift": {"count": 10, "sample_keys": ["1", "2"]},
    }), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert _missing(report) == 10
    drifted = [d for d in report["differences"] if d["cause"]["code"] == "concurrent_source_update"]
    assert drifted and drifted[0]["rows"] == 10 and drifted[0]["cause"]["explained"] is True


def test_deleted_at_source_after_the_write_makes_no_phantom_rows():
    # The reader counted 1000 and wrote 1000; the source deleted 5 afterwards.
    # Both counts are this run's, so nothing is missing or extra.
    report = build_parity_report(_job({
        "passed": True, "source_rows": 1000, "target_rows": 1000, "culprit_census_clean": True,
        "concurrent_source_drift": {"count": 0, "deleted_count": 5, "deleted_sample_keys": ["4"]},
    }), reread=False)
    kinds = {d["kind"] for d in report["differences"] if not d["cause"]["explained"]}
    assert kinds == set(), report["differences"]
    assert report["verdict"] == "explained"


def test_witnessed_losses_are_not_also_phantom_extras():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 5000, "target_rows": 5000, "culprit_census_clean": True,
        "concurrent_source_drift": {"count": 0, "loss_count": 3, "losses": [{"id": 1}, {"id": 2}, {"id": 3}]},
    }), reread=False)
    assert not [d for d in report["differences"] if d["kind"] == "extra_in_destination"], report["differences"]
    assert report["parity"]["unexplained"] == 3


def test_a_key_named_by_both_l5_and_the_census_counts_once():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 100, "target_rows": 99,
        "culprit_keys": {"missing_in_dest": ["42"]},
        "verification_ladder": {
            "layers": {"L5": {"details": {"pk_column": "id", "mismatch_count": 1}}},
            "localization": {"rows": [{"pk": "42", "reason": "missing_side", "source_present": True}]},
        },
    }), reread=False)
    assert _missing(report) == 1
    assert not [d for d in report["differences"] if d["kind"] == "extra_in_destination"]


def test_a_quarantined_key_named_by_the_census_is_not_subtracted_twice():
    report = build_parity_report(_job({
        "passed": True, "source_rows": 100, "target_rows": 99, "rejected_rows": 1,
        "culprit_keys": {"missing_in_dest": ["7"]},
    }, {"rejected_details": [{"row": {"id": 7}, "reason": "bad"}]}), reread=False)
    assert not [d for d in report["differences"] if d["kind"] == "extra_in_destination"], report["differences"]
    assert report["parity"]["unexplained"] == 0
