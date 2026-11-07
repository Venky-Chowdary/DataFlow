"""Iceberg leftover/CDC delete keys bind through the write-path parsers.

fromisoformat missed 31/12/2024. Decimal(text) invented Auto 1.234.
Informal yes invented TRUE and could delete the wrong boolean identity.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.iceberg_writer import _iceberg_typed_literal  # noqa: E402


class StringType:
    pass


class LongType:
    pass


class DateType:
    pass


class TimestampType:
    pass


class DecimalType:
    pass


class BooleanType:
    pass


class DoubleType:
    pass


class _Schema:
    def __init__(self, fields: dict[str, object]) -> None:
        self._fields = fields

    def find_field(self, name: str, case_sensitive: bool = True) -> object:
        lookup = {k.lower(): v for k, v in self._fields.items()}
        return lookup[name.lower() if not case_sensitive else name]


class _Table:
    def __init__(self, field_type: object, column: str = "id") -> None:
        self._schema = _Schema({column: SimpleNamespace(field_type=field_type)})

    def schema(self) -> _Schema:
        return self._schema


def test_string_and_long_identity_still_bind():
    assert _iceberg_typed_literal(_Table(StringType()), "id", "99") == "99"
    assert _iceberg_typed_literal(_Table(LongType()), "id", "9") == 9


def test_date_binds_unambiguous_slash_and_refuses_auto():
    tbl = _Table(DateType())
    assert _iceberg_typed_literal(tbl, "id", "31/12/2024") == date(2024, 12, 31)
    assert _iceberg_typed_literal(tbl, "id", "12/31/2024") == date(2024, 12, 31)
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "01/02/2024")


def test_timestamp_binds_slash_and_epoch():
    tbl = _Table(TimestampType())
    assert _iceberg_typed_literal(tbl, "id", "31/12/2024") == datetime(2024, 12, 31, 0, 0, 0)
    epoch_s = _iceberg_typed_literal(tbl, "id", "1704451800")
    epoch_ms = _iceberg_typed_literal(tbl, "id", "1704451800000")
    assert epoch_s == epoch_ms
    assert epoch_s == datetime.fromtimestamp(1704451800, tz=timezone.utc).replace(
        tzinfo=None
    )
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "01/02/2024")


def test_decimal_locale_money_and_auto_refuse():
    tbl = _Table(DecimalType())
    assert _iceberg_typed_literal(tbl, "id", "$1,234.56") == Decimal("1234.56")
    assert _iceberg_typed_literal(tbl, "id", "€1.234,56") == Decimal("1234.56")
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "1.234")
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "1,234")


def test_long_locale_money_and_auto_refuse():
    tbl = _Table(LongType())
    assert _iceberg_typed_literal(tbl, "id", "$1,234") == 1234
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "1,234")


def test_boolean_canonical_only_yes_does_not_invent_true():
    tbl = _Table(BooleanType())
    assert _iceberg_typed_literal(tbl, "id", "true") is True
    assert _iceberg_typed_literal(tbl, "id", "false") is False
    assert _iceberg_typed_literal(tbl, "id", "1") is True
    assert _iceberg_typed_literal(tbl, "id", "0") is False
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "yes")
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "2")


def test_double_refuses_auto_three_digit_group():
    tbl = _Table(DoubleType())
    assert _iceberg_typed_literal(tbl, "id", "1.2345") == pytest.approx(1.2345)
    with pytest.raises(ValueError, match="does not bind"):
        _iceberg_typed_literal(tbl, "id", "1.234")
