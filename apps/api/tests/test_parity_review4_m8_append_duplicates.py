"""Re-review 4, finding 8 (Low-Medium): rows an append wrote twice are not rows the destination
"already held".

An append into an occupied table: 500 rows before, the run wrote 1,000, the destination holds
1,600. Only 500 were there before; the other 100 extra rows came from this run (or something
else). The report explained all 600 as ``out_of_scope_population``.
"""

from __future__ import annotations

from services.parity_report import build_parity_report


def _report(passed: bool) -> dict:
    return build_parity_report({
        "_id": "j", "status": "completed",
        "reconciliation": {"passed": passed, "source_rows": 1000, "target_rows": 1600, "target_rows_before": 500,
                           "culprit_census_clean": True},
        "destination_summary": {"table": "orders", "primary_key_columns": [], "sync_mode": "append"},
    }, reread=False)


def test_only_the_rows_held_before_are_out_of_scope():
    report = _report(True)
    scope = [d for d in report["differences"] if d["cause"]["code"] == "out_of_scope_population"]
    assert sum(d["rows"] for d in scope) == 500
    extra = [d for d in report["differences"] if not d["cause"]["explained"]]
    assert [(d["kind"], d["rows"]) for d in extra] == [("extra_in_destination", 100)]
    assert report["verdict"] == "unexplained", report["verdict_reason"]


def test_a_failed_gate8_is_accounted_for_by_the_extra_rows():
    report = _report(False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
