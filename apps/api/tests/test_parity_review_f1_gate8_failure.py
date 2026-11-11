"""Review finding 1: a Gate-8 failure must never read as "proven".

The verdict never looked at ``reconciliation.passed``, and the extraction
dropped the witness's ``loss_count``/``losses``, the unattributed discrepancy
(``value_discrepancy_unattributed``) and the ladder's L2/L4 column failures. A
failed run with a clean sample was "proven" and sign-off opened.
"""

from __future__ import annotations

from services.parity_report import build_parity_report


def _job(recon: dict, **over) -> dict:
    job = {
        "_id": "job-1", "status": "completed", "reconciliation": recon,
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    }
    job.update(over)
    return job


def test_l2_l4_column_failure_with_a_clean_sample_is_not_proven():
    report = build_parity_report(_job({
        "passed": False, "checksum_match": False, "source_rows": 1_000_000, "target_rows": 1_000_000,
        "sample_compare": {"rows_compared": 1000, "mismatches": []},
        "verification_ladder": {
            "layers": {"L2": {"passed": False}, "L4": {"passed": False, "details": {"mismatched_columns": ["amount"]}}},
            "localization": {"columns": ["amount"], "rows": []},
        },
    }), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    amount = [d for d in report["differences"] if d["column"] == "amount"]
    assert amount and amount[0]["cause"]["explained"] is False
    assert report["signoff"]["status"] == "blocked"


def test_witnessed_losses_and_unattributed_discrepancy_are_unexplained():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 5000, "target_rows": 5000,
        "sample_compare": {"rows_compared": 500, "mismatches": []},
        "value_discrepancy_unattributed": {"columns": ["amount"], "l2": "failed", "l3": "skipped"},
        "concurrent_source_drift": {"count": 0, "loss_count": 3, "losses": [{"id": 1}, {"id": 2}, {"id": 3}]},
    }), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    lost = [d for d in report["differences"] if d["kind"] == "missing_in_destination"]
    assert lost and lost[0]["rows"] == 3 and sorted(lost[0]["keys_sample"]) == ["1", "2", "3"]
    assert any(d["column"] == "amount" and not d["cause"]["explained"] for d in report["differences"])
    assert report["signoff"]["status"] == "blocked"


def test_a_failed_gate8_that_names_nothing_is_not_proven():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
    }), reread=False)
    assert report["verdict"] == "unverifiable", report["verdict_reason"]
    assert "Gate-8" in report["verdict_reason"]
    assert report["tables"][0]["proven_equal_pct"] is None
    assert report["signoff"]["status"] == "blocked"
    assert any(f["action"] == "rerun" for f in report["fixes"])


def test_engine_profile_ladder_l2_failure_names_the_column():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 2_000_000, "target_rows": 2_000_000,
        "verification_ladder": {
            "passed": False,
            "layers": {"L1": {"passed": True},
                       "L2": {"passed": False, "details": {"mismatched_columns": ["note"]}}},
        },
    }), reread=False)
    assert report["verdict"] == "unexplained"
    assert [d["column"] for d in report["differences"] if not d["cause"]["explained"]] == ["note"]


def test_a_passed_gate8_with_a_clean_census_stays_proven():
    report = build_parity_report(_job({
        "passed": True, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
    }), reread=False)
    assert report["verdict"] == "proven"
