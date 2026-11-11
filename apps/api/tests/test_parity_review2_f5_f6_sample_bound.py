"""Re-review findings 5 and 6 (MEDIUM): the sample bound counts what it saw and what the counts prove.

5. Sample mismatches Gate-8 stored without a key were left out of ``k``: 50
   keyless differences in a 1,000-row sample read "at most 0.30%" (the true
   bound is about 6.3%).
6. A sampled table's proven-equal share ignored rows the counts prove
   missing: 1,000 source rows, 500 in the destination and a clean sample read
   ">= 99.40%".
"""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report
from services.parity_stats import clopper_pearson_upper


def _job(recon: dict) -> dict:
    return {"_id": "j", "status": "completed", "reconciliation": recon,
            "destination_summary": {"table": "t", "sync_mode": "full_refresh_overwrite"}}


def test_keyless_sample_mismatches_count_in_the_bound():
    mm = [{"target": "v", "source_value": f"x{i}", "target_value": f"y{i}"} for i in range(50)]
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1_000_000, "target_rows": 1_000_000,
        "sample_compare": {"rows_compared": 1000, "mismatches": mm},
    }), reread=False)
    t = report["tables"][0]
    assert report["confidence"]["differences_in_sample"] == 50
    assert t["upper_bound_rate"] == pytest.approx(clopper_pearson_upper(50, 1000))
    assert t["upper_bound_rate"] > 0.06
    assert t["proven_equal_pct"] < 94.0
    assert "6.29%" in report["confidence"]["statement"]


def test_a_clean_sample_does_not_hide_rows_the_counts_prove_missing():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1000, "target_rows": 500,
        "sample_compare": {"rows_compared": 500, "mismatches": []},
    }), reread=False)
    t = report["tables"][0]
    assert t["unexplained"] == 500
    assert t["proven_equal_pct"] <= 50.0
    assert report["parity"]["proven_equal_pct"] <= 50.0
    assert t["upper_bound_rate"] >= 0.5
    assert "at most 0.60%" not in report["confidence"]["statement"]


def test_an_acceptance_of_keyless_sampled_rows_keeps_them_in_k():
    # Re-review 3, H3 reversed round 2's rule: an acceptance covers the rows a person
    # saw, never the unseen population, so the accepted sampled rows stay in k.
    mm = [{"target": "v", "source_value": f"x{i}", "target_value": f"y{i}"} for i in range(4)]
    job = _job({"passed": False, "source_rows": 10_000, "target_rows": 10_000,
                "sample_compare": {"rows_compared": 1000, "mismatches": mm}})
    did = build_parity_report(job, reread=False)["differences"][0]["id"]
    job["parity"] = {"version": 1, "acceptances": {did: {"by": "a", "note": "ok", "keys": [], "keyless_rows": 4,
                                                         "rows": 4}}}
    report = build_parity_report(job, reread=False)
    assert report["confidence"]["differences_in_sample"] == 4
    assert report["verdict"] == "no_difference_found"
