"""M17: a count-only / non-comparable run must not carry "Gate-8 still fails"
wording or census-driver refusal text on a *passing* report.

Root cause: ``checksum_match=False`` is stamped whenever no digest comparison
ran (e.g. a kafka source has no census driver — a topic is not re-readable as
a table). Downstream, ``_localize_checksum_mismatch`` and the gate-8
culprit-naming block treat that as "measured mismatch" and append fail-clause
wording to a completed job's message. Both helpers are documented as
failure-path tools — they must not run on reports whose verdict passed.
"""

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
    sys.path.insert(0, str(_API_ROOT / "src"))

from services.reconcile_coverage import (  # noqa: E402
    WHOLE_TABLE_NOT_COMPARABLE,
    row_count_scope_stamp,
)
from services.reconciliation import stamp_post_write_phase  # noqa: E402
from transfer.reconcile_step import _localize_checksum_mismatch  # noqa: E402


def _passed_count_report() -> dict:
    return {
        "passed": True,
        "source_rows": 100,
        "target_rows": 100,
        "source_checksum": "",
        "target_checksum": "deadbeef",
        "checksum_scope": WHOLE_TABLE_NOT_COMPARABLE,
        "checksum_match": False,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "rows_skipped": 0,
        "sample_compare": {
            "passed": True,
            "compared": 12,
            "rows_compared": 12,
            "mismatches": [],
        },
        "message": "Row count verified: 100 row(s) on the destination for 100 source row(s).",
    }


def test_localize_does_not_append_fail_wording_to_passing_report():
    stamped = stamp_post_write_phase(_passed_count_report())
    out = _localize_checksum_mismatch(stamped, dest_summary=None)
    # The digest-difference fail clause must not appear on a completed run.
    assert "Gate-8 still fails" not in out["message"]
    assert "not proof" not in out["message"]


def test_passing_report_keeps_count_verified_message():
    stamped = stamp_post_write_phase(_passed_count_report())
    out = _localize_checksum_mismatch(stamped, dest_summary=None)
    assert out["passed"] is True
    assert "Row count verified" in out["message"]


def test_measured_mismatch_still_gets_fail_wording():
    """A real digest divergence on a failing run must keep the fail text."""
    report = {
        "passed": False,
        "source_rows": 100,
        "target_rows": 100,
        "source_checksum": "aaa",
        "target_checksum": "bbb",
        "checksum_match": False,
        "rejected_rows": 0,
        "sample_compare": {
            "passed": True,
            "compared": 50,
            "rows_compared": 50,
            "mismatches": [],
        },
        "message": "checksums differ",
    }
    out = _localize_checksum_mismatch(report, dest_summary=None)
    assert "Gate-8 still fails" in out["message"]


def test_failed_count_report_still_names_rows():
    """Failed count reports keep checksum_match=False so row-naming can run."""
    stamped = row_count_scope_stamp({**_passed_count_report(), "passed": False})
    assert stamped["checksum_match"] is False
