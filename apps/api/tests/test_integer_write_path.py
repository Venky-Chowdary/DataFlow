"""INTEGER bind uses integer_wire_value — no Decimal(text) invent.

Auto 1.000 used to land as 1. Locale money the write path stores must
still bind. Wordy true/false stay refused (0/1 digits still bind).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_integer_wire, normalize_sql_bind_value  # noqa: E402


def test_auto_three_digit_group_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_integer_wire("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_integer_wire("1.000")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_integer_wire("1.234")


def test_locale_money_and_integral_scale_land():
    assert coerce_integer_wire("$1,234") == 1234
    assert coerce_integer_wire("€1.234") == 1234
    assert coerce_integer_wire("1.00") == 1
    assert coerce_integer_wire("1000") == 1000
    assert coerce_integer_wire("42") == 42


def test_bool_tokens_and_empty_still_refuse():
    with pytest.raises(ValueError, match="boolean token"):
        coerce_integer_wire("true")
    with pytest.raises(ValueError, match="boolean token"):
        coerce_integer_wire("false")
    assert coerce_integer_wire("1") == 1
    assert coerce_integer_wire("0") == 0
    with pytest.raises(ValueError, match="empty string"):
        coerce_integer_wire("")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_integer_wire("not-an-int")


def test_normalize_sql_bind_integer_carriers():
    assert normalize_sql_bind_value("$1,234", "INTEGER", engine="postgresql") == 1234
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value("1.000", "BIGINT", engine="postgresql")
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value("1,234", "INT", engine="sqlite")
