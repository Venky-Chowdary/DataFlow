"""Pilot cell/predicate parses must use the write-path locale parsers."""

from __future__ import annotations

from datetime import date, datetime

from decimal import Decimal

from src.ai.copilot.query_tools import _infer_kind, _try_bool, _try_datetime, _try_float
from src.ai.copilot.transfer_rules import _utterance_row_limit


def test_try_float_fails_closed_on_lone_grouping():
    assert _try_float("1,234") is None
    assert _try_float("1.234") is None


def test_try_float_parses_currency_and_both_separators():
    assert _try_float("$1,234.56") == Decimal("1234.56")
    assert _try_float("€1.234,56") == Decimal("1234.56")
    assert _try_float("1234") == 1234.0


def test_try_float_does_not_invent_from_native_numbers():
    assert _try_float(12) == 12.0
    assert _try_float(True) is None


def test_utterance_row_limit_reads_english_thousands():
    assert _utterance_row_limit("1,000") == 1000
    assert _utterance_row_limit("100") == 100
    assert _utterance_row_limit("abc") == 0


def test_try_datetime_fails_closed_on_auto_ambiguous_slash_date():
    assert _try_datetime("01/02/2024") is False
    assert _try_datetime("01/02/2024 00:00:00") is False


def test_try_datetime_accepts_iso_unambiguous_and_native_dates():
    assert _try_datetime("2024-03-05") is True
    assert _try_datetime("31/12/2024") is True
    assert _try_datetime("2024-03-05T12:00:00") is True
    assert _try_datetime(date(2024, 3, 5)) is True
    assert _try_datetime(datetime(2024, 3, 5, 12, 0, 0)) is True


def test_try_datetime_honors_date_locale():
    from services.transform_engine import reset_active_date_locale, set_active_date_locale

    token = set_active_date_locale("MDY")
    try:
        assert _try_datetime("01/02/2024") is True
    finally:
        reset_active_date_locale(token)


def test_try_bool_fails_closed_on_informal_yes():
    assert _try_bool("yes") is None
    assert _try_bool("on") is None
    assert _try_bool("y") is None
    assert _try_bool("true") is True
    assert _try_bool("false") is False
    assert _try_bool(True) is True


def test_infer_kind_does_not_call_yes_no_boolean():
    assert _infer_kind(["yes", "no", "yes"]) == "string"
    assert _infer_kind(["true", "false", "true"]) == "boolean"


def test_infer_kind_does_not_call_ambiguous_slash_dates_datetime():
    assert _infer_kind(["01/02/2024", "03/04/2024"]) == "string"
    assert _infer_kind(["31/12/2024", "30/11/2024"]) == "datetime"
    assert _infer_kind(["2024-03-05", "2024-03-06"]) == "datetime"
