"""A reconcile verdict must be readable and actionable, not two hex strings.

The field complaint was precise: "what proof are we giving, how can he fix the
bad data, how much percentage". A failed Gate-8 now carries the denominators the
run actually holds and an ordered remediation list — without softening any
verdict, and without calling a sample percentage population proof.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.reconcile_step import (  # noqa: E402
    _attach_match_summary,
    _localize_checksum_mismatch,
)


def _failed_report(**over) -> dict:
    report = {
        "passed": False,
        "checksum_match": False,
        "source_rows": 710_000,
        "target_rows": 710_000,
        "rejected_rows": 0,
        "message": "Checksum mismatch (strict): source aaa vs target bbb.",
        "sample_compare": {
            "passed": True,
            "compared": 5_000,
            "cells_compared": 5_000,
            "rows_compared": 500,
            "mismatches": [],
        },
    }
    report.update(over)
    return report


def test_match_percent_names_its_denominator() -> None:
    out = _attach_match_summary(_failed_report(), {"target_rows_before": 0, "rows_written": 710_000})
    match = out["match_summary"]
    assert match["sample_match_percent"] == 100.0
    assert match["sample_rows_compared"] == 500
    assert match["sample_cells_compared"] == 5_000
    # A sample percentage must never read as population proof.
    assert "not population proof" in match["denominator"]
    assert match["dest_rows_before"] == 0
    assert match["rows_moved_this_run"] == 710_000
    assert out["passed"] is False


def test_differing_cells_lower_the_percent_and_point_at_the_mapping() -> None:
    report = _failed_report(
        sample_compare={
            "passed": False,
            "compared": 200,
            "cells_compared": 200,
            "rows_compared": 20,
            "mismatches": [
                {
                    "row": "3",
                    "source": "amount",
                    "target": "amount",
                    "source_value": "10.25",
                    "target_value": "10",
                }
            ],
        }
    )
    out = _attach_match_summary(report, {})
    assert out["match_summary"]["sample_match_percent"] == 99.5
    actions = [a["action"] for a in out["remediation"]]
    assert actions[0] == "open_map"
    assert "amount" in out["remediation"][0]["label"]


def test_basis_mismatch_offers_a_comparable_population() -> None:
    """The 710k append case: nothing differs, so the fix is scope, not data."""
    localized = _localize_checksum_mismatch(
        _failed_report(), {"checksum_mode": "source_reread"}
    )
    out = _attach_match_summary(localized, {"target_rows_before": 0})
    actions = [a["action"] for a in out["remediation"]]
    assert "overwrite_or_keyed_resync" in actions
    assert out["passed"] is False


def test_quarantine_and_resume_are_offered_only_when_they_exist() -> None:
    plain = _attach_match_summary(_failed_report(), {})
    assert "replay_quarantine" not in [a["action"] for a in plain.get("remediation", [])]

    with_holdouts = _attach_match_summary(
        _failed_report(rejected_rows=42), {"resumed_from": 140}
    )
    actions = [a["action"] for a in with_holdouts["remediation"]]
    assert "replay_quarantine" in actions
    assert "resume" in actions


def test_a_passing_report_gets_the_summary_but_no_remediation() -> None:
    out = _attach_match_summary(
        _failed_report(passed=True, checksum_match=True), {"rows_written": 10}
    )
    assert out["match_summary"]["sample_match_percent"] == 100.0
    assert "remediation" not in out


def test_no_comparable_cells_reports_unknown_rather_than_a_number() -> None:
    out = _attach_match_summary(
        _failed_report(sample_compare={"passed": True, "compared": 0, "mismatches": []}),
        {},
    )
    assert out["match_summary"]["sample_match_percent"] is None
    assert "no key-aligned cells" in out["match_summary"]["denominator"]
