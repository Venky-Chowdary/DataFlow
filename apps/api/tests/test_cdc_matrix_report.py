"""Honest CDC matrix report parsing — skip ≠ pass; errors count as failures."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cdc_matrix_report import parse_pytest_summary  # noqa: E402


def test_parse_pytest_summary_passed_skipped():
    out = ".......ss\n5 passed, 2 skipped in 1.23s\n"
    passed, failed, skipped, line = parse_pytest_summary(out)
    assert passed == 5
    assert failed == 0
    assert skipped == 2
    assert "5 passed" in line


def test_parse_pytest_summary_errors_count_as_failed():
    out = "E\n==== ERRORS ====\n1 error in 0.50s\n"
    passed, failed, skipped, line = parse_pytest_summary(out)
    assert passed == 0
    assert failed == 1
    assert skipped == 0
    assert "error" in line


def test_parse_pytest_summary_mixed_failed_and_errors():
    out = "FF E\n2 failed, 1 error, 3 passed in 2.0s\n"
    passed, failed, skipped, _ = parse_pytest_summary(out)
    assert passed == 3
    assert failed == 3  # 2 failed + 1 error
    assert skipped == 0


def test_parse_pytest_summary_empty_stays_zero():
    passed, failed, skipped, line = parse_pytest_summary("")
    assert (passed, failed, skipped) == (0, 0, 0)
    assert line == ""
