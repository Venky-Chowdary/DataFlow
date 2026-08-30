"""History column profiles use write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — min/max/mean stay unset. Locale money the write
path stores still sets the range.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality_history import (  # noqa: E402
    _coerce_datetime,
    _rehydrate_stat,
    load_historical_profile,
    profile_batch,
    profile_column,
    save_profile,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_locale_money_minmax_are_decimals():
    p = profile_column(["$10.00", "$1,234.56", "€2.000,00"], "amount", "decimal")
    assert p.min_value == "10.00"
    assert p.max_value == "2000.00"
    assert p.mean == Decimal("1081.52")
    assert isinstance(p.mean, Decimal)


def test_auto_grouping_does_not_invent_stats():
    p = profile_column(["1,234", "1.000", "1.234"], "amount", "decimal")
    assert p.min_value is None
    assert p.max_value is None
    assert p.mean is None
    assert p.std is None


def test_auto_grouping_does_not_invent_integer_range():
    p = profile_column([10, 11, 12, "1,234"], "id", "integer")
    assert p.min_value == "10"
    assert p.max_value == "12"
    assert p.mean == Decimal("11")


def test_locale_money_survives_history_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    src = {"kind": "database", "format": "postgresql", "table": "orders"}
    dst = {"kind": "database", "format": "snowflake", "table": "orders"}
    rows = [
        {"amount": "$10.00"},
        {"amount": "$1,234.56"},
        {"amount": "€2.000,00"},
    ]
    save_profile(src, dst, profile_batch(rows, {"amount": "decimal"}))
    historical = load_historical_profile(src, dst)
    assert historical is not None
    assert historical["amount"].min_value == "10.00"
    assert historical["amount"].max_value == "2000.00"
    assert historical["amount"].mean == Decimal("1081.52")
    assert isinstance(historical["amount"].mean, Decimal)


def test_string_profile_minmax_uses_numeric_order_not_lexicographic_wire():
    p = profile_column(["9", "10", "100"], "code", "string")
    assert p.min_value == "9"
    assert p.max_value == "100"
    assert p.min_value != "10"


def test_reader_null_is_absence_not_a_token():
    p = profile_column(
        [None, SQL_NULL_SENTINEL, "", "kept", "null"],
        "note",
        "string",
    )
    assert p.count == 5
    assert p.null_count == 4
    assert p.distinct_count == 1
    assert p.min_value == "kept"
    assert p.max_value == "kept"
    assert not any(v == SQL_NULL_SENTINEL for v, _ in p.top_values)


def test_rehydrate_and_datetime_skip_reader_null():
    assert _rehydrate_stat(None) is None
    assert _rehydrate_stat("") is None
    assert _rehydrate_stat("   ") is None
    assert _rehydrate_stat(SQL_NULL_SENTINEL) is None
    assert _rehydrate_stat(True) is None
    assert _rehydrate_stat("1.234") == Decimal("1.234")
    assert _coerce_datetime(None) is None
    assert _coerce_datetime(SQL_NULL_SENTINEL) is None
    assert _coerce_datetime("") is None
