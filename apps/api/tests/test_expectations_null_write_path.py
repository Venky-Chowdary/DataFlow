"""Expectation unique / not_null / accepted / between / regex / pair
use is_null_evidence, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present string, so
two NULL PKs were a duplicate-key hit on the sentinel spelling and
between scored the sentinel as not_numeric. Empty / whitespace stay
blank. True and dest "true" share one unique key.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.expectations_engine import (  # noqa: E402
    _is_absent,
    _present_text,
    expect_column_accepted_values,
    expect_column_not_null,
    expect_column_pair_values_equal,
    expect_column_unique,
    expect_column_values_between,
    expect_column_values_match_regex,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_absent_matches_reader_wire():
    assert _is_absent(None) is True
    assert _is_absent("") is True
    assert _is_absent("   ") is True
    assert _is_absent(SQL_NULL_SENTINEL) is True
    assert _is_absent(DF_MISSING_SENTINEL) is True
    assert _is_absent(Missing) is True
    assert _is_absent("__df_ddb_null__") is True
    assert _is_absent("0") is False
    assert _is_absent(0) is False
    assert _is_absent(False) is False
    assert _is_absent("kept") is False
    assert _present_text(SQL_NULL_SENTINEL) is None
    assert _present_text("") is None
    assert _present_text(True) == "true"
    assert _present_text(True) != str(True)
    assert _present_text(0) == "0"


def test_two_null_pks_are_not_sentinel_duplicates():
    rows = [
        {"id": SQL_NULL_SENTINEL},
        {"id": SQL_NULL_SENTINEL},
        {"id": None},
        {"id": ""},
        {"id": "kept"},
    ]
    r = expect_column_unique(rows, "id")
    assert r.passed is True
    assert r.failing_count == 0
    assert r.details["distinct"] == 1
    assert r.details["total_non_empty"] == 1


def test_real_duplicate_pk_still_blocks():
    rows = [{"id": "1"}, {"id": "1"}, {"id": "2"}]
    r = expect_column_unique(rows, "id")
    assert r.passed is False
    assert r.failing_count == 1


def test_bool_and_dest_true_share_unique_key():
    rows = [{"id": True}, {"id": "true"}]
    r = expect_column_unique(rows, "id")
    assert r.passed is False
    assert r.failing_count == 1
    assert r.failing_samples[0]["value"] == "true"


def test_not_null_counts_reader_sentinel():
    rows = [
        {"id": SQL_NULL_SENTINEL},
        {"id": None},
        {"id": ""},
        {"id": "   "},
        {"id": "kept"},
        {"id": 0},
    ]
    r = expect_column_not_null(rows, "id", max_null_rate=0.0)
    assert r.passed is False
    assert r.failing_count == 4
    assert r.details["null_rate"] == 0.6667


def test_accepted_skips_reader_null_not_unexpected():
    rows = [
        {"status": SQL_NULL_SENTINEL},
        {"status": None},
        {"status": ""},
        {"status": "paid"},
    ]
    r = expect_column_accepted_values(rows, "status", {"paid", "pending"})
    assert r.passed is True
    assert r.failing_count == 0


def test_accepted_still_flags_unexpected_token():
    rows = [{"status": "paid"}, {"status": "void"}]
    r = expect_column_accepted_values(rows, "status", {"paid", "pending"})
    assert r.passed is False
    assert r.failing_count == 1


def test_regex_skips_reader_null():
    rows = [
        {"email": SQL_NULL_SENTINEL},
        {"email": ""},
        {"email": "a@x.com"},
    ]
    r = expect_column_values_match_regex(
        rows, "email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", mostly=1.0
    )
    assert r.passed is True
    assert r.failing_count == 0


def test_between_skips_reader_null_not_not_numeric():
    rows = [
        {"amount": SQL_NULL_SENTINEL},
        {"amount": None},
        {"amount": DF_MISSING_SENTINEL},
        {"amount": "50"},
    ]
    r = expect_column_values_between(rows, "amount", min_value=0, max_value=100)
    assert r.passed is True
    assert r.failing_count == 0
    assert not any(f.get("reason") == "not_numeric" for f in r.failing_samples)


def test_pair_null_vs_sentinel_is_same_absence():
    rows = [
        {"a": None, "b": SQL_NULL_SENTINEL},
        {"a": "", "b": None},
        {"a": SQL_NULL_SENTINEL, "b": DF_MISSING_SENTINEL},
    ]
    r = expect_column_pair_values_equal(rows, "a", "b")
    assert r.passed is True
    assert r.failing_count == 0


def test_pair_null_vs_present_is_mismatch():
    rows = [{"a": SQL_NULL_SENTINEL, "b": "kept"}]
    r = expect_column_pair_values_equal(rows, "a", "b")
    assert r.passed is False
    assert r.failing_count == 1


def test_pair_bool_matches_dest_true():
    rows = [{"a": True, "b": "true"}]
    r = expect_column_pair_values_equal(rows, "a", "b")
    assert r.passed is True
    assert r.failing_count == 0
