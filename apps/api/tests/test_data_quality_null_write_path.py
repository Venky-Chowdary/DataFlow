"""Integrity-audit nulls use is_null_evidence, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present string, so
required-null gates missed it and two NULL PKs were a duplicate-key hit
on the sentinel spelling.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality import _is_null, run_integrity_audit  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_is_null_matches_reader_wire():
    assert _is_null(None) is True
    assert _is_null("") is True
    assert _is_null("   ") is True
    assert _is_null(SQL_NULL_SENTINEL) is True
    assert _is_null("0") is False
    assert _is_null(0) is False
    assert _is_null("kept") is False


def test_required_column_reader_null_is_blocked():
    report = run_integrity_audit(
        headers=["id", "email"],
        rows=[["1", "a@x.com"], ["2", SQL_NULL_SENTINEL]],
        required_targets=["email"],
        primary_key="id",
    )
    assert not report.passed
    assert any("null/empty" in issue for issue in report.issues)
    assert not any(SQL_NULL_SENTINEL in issue for issue in report.issues)


def test_two_null_pks_are_required_null_not_sentinel_duplicate():
    report = run_integrity_audit(
        headers=["id", "amount"],
        rows=[[SQL_NULL_SENTINEL, "1"], [SQL_NULL_SENTINEL, "2"], [None, "3"]],
        column_types={"id": "INTEGER", "amount": "DECIMAL"},
        dest_kind="postgresql",
        sync_mode="incremental_deduped",
        primary_key="id",
    )
    assert not report.passed
    assert any("null/empty" in issue for issue in report.issues)
    assert not any("Duplicate primary key" in issue for issue in report.issues)
    assert not any(SQL_NULL_SENTINEL in issue for issue in report.issues)


def test_real_duplicate_pk_still_blocks():
    report = run_integrity_audit(
        headers=["id", "amount"],
        rows=[["1", "100"], ["1", "200"]],
        dest_kind="postgresql",
        sync_mode="incremental_deduped",
        primary_key="id",
    )
    assert not report.passed
    assert any("Duplicate primary key" in issue for issue in report.issues)
