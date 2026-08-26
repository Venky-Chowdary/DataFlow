"""Pilot cell/predicate parses must use decimal_wire_value — never comma-strip."""

from __future__ import annotations

from src.ai.copilot.query_tools import _try_float
from src.ai.copilot.transfer_rules import _utterance_row_limit


def test_try_float_fails_closed_on_lone_grouping():
    assert _try_float("1,234") is None
    assert _try_float("1.234") is None


def test_try_float_parses_currency_and_both_separators():
    assert _try_float("$1,234.56") == 1234.56
    assert _try_float("€1.234,56") == 1234.56
    assert _try_float("1234") == 1234.0


def test_try_float_does_not_invent_from_native_numbers():
    assert _try_float(12) == 12.0
    assert _try_float(True) is None


def test_utterance_row_limit_reads_english_thousands():
    assert _utterance_row_limit("1,000") == 1000
    assert _utterance_row_limit("100") == 100
    assert _utterance_row_limit("abc") == 0
