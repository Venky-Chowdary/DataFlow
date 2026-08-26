"""Iceberg leftover PK variants use integer_wire_value, not float(text).

float('1.000') is_integer invented 1. Locale money $1,234 / €1.234
the leftover long column stores must still expand.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import _pk_predicate_variants  # noqa: E402


def test_plain_int_string_still_expands():
    assert 42 in _pk_predicate_variants("42")
    assert -7 in _pk_predicate_variants("-7")


def test_locale_money_expands_to_int():
    assert 1234 in _pk_predicate_variants("$1,234")
    assert 1234 in _pk_predicate_variants("€1.234")


def test_auto_three_digit_group_does_not_invent_int():
    for token in ("1.000", "1.234", "1,234"):
        got = _pk_predicate_variants(token)
        assert 1 not in got
        assert 1234 not in got
