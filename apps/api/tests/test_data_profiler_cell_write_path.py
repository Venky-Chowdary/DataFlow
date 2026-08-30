"""Profiler samples use cell_to_string, not a second stringify.

Decimal scientific text, bool True, and reader-wired SQL NULL used to
invent a second spelling. Null-rate still counts absence, not the
sentinel token.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_profiler import _as_str, profile_column  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402

BLOB = bytes([0xFF, 0xFE, 0x00])


def test_as_str_matches_transfer_wire():
    assert _as_str(Decimal("1E+2")) == "100"
    assert _as_str(Decimal("1E+2")) != "1E+2"
    assert _as_str(True) == "true"
    assert _as_str(False) == "false"
    assert _as_str(BLOB) == cell_to_string(BLOB)
    assert _as_str(1.5) == "1.5"


def test_as_str_nulls_collapse_for_null_rate():
    assert _as_str(None) == ""
    assert _as_str("") == ""
    assert _as_str(SQL_NULL_SENTINEL) == ""
    assert _as_str("   ") == ""


def test_profile_decimal_scientific_is_one_value():
    prof = profile_column("amt", [Decimal("1E+2"), Decimal("100"), "100"])
    assert prof["distinct_count"] == 1
    assert prof["top_values"][0]["value"] == "100"
    assert prof["null_rate"] == 0


def test_profile_reader_null_is_absence_not_token():
    prof = profile_column("note", [None, SQL_NULL_SENTINEL, "", "kept"])
    assert prof["null_rate"] == 0.75
    assert prof["non_empty_count"] == 1
    assert prof["distinct_count"] == 1
    assert prof["top_values"][0]["value"] == "kept"
    assert not any(v["value"] == SQL_NULL_SENTINEL for v in prof["top_values"])
