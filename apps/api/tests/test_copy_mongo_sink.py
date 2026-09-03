"""Canonical Mongo dest writer — DATE midnight law, dest COUNT, no estimatedDocumentCount."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from services.copy_fast_path import FastPathUnavailable
from services.copy_mongo_sink import bson_to_python, sql_value_to_bson


def test_sql_value_to_bson_midnight_date_is_utc():
    assert sql_value_to_bson(date(2024, 1, 2)) == datetime(
        2024, 1, 2, tzinfo=timezone.utc
    )
    assert sql_value_to_bson(datetime(2024, 1, 2, 0, 0, 0)) == datetime(
        2024, 1, 2, tzinfo=timezone.utc
    )


def test_sql_value_to_bson_datetime_with_time_declines():
    with pytest.raises(FastPathUnavailable, match="time component"):
        sql_value_to_bson(datetime(2024, 1, 2, 12, 0, 0))


def test_sql_value_to_bson_null_and_empty_string():
    assert sql_value_to_bson(None) is None
    assert sql_value_to_bson("") == ""
    assert sql_value_to_bson(1) == 1


def test_sql_value_to_bson_binary_declines():
    with pytest.raises(FastPathUnavailable, match="binary"):
        sql_value_to_bson(b"abc")


def test_sql_value_to_bson_decimal():
    pytest.importorskip("bson")
    from bson.decimal128 import Decimal128

    out = sql_value_to_bson(Decimal("1.50"))
    assert isinstance(out, Decimal128)


def test_bson_to_python_nested_and_binary_decline():
    with pytest.raises(FastPathUnavailable, match="nested"):
        bson_to_python({"a": 1}, "VARCHAR2(32)")
    with pytest.raises(FastPathUnavailable, match="nested"):
        bson_to_python([1, 2], "VARCHAR2(32)")
    with pytest.raises(FastPathUnavailable, match="binary"):
        bson_to_python(b"x", "BLOB")


def test_bson_to_python_date_ddl_strips_utc_midnight():
    value = datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert bson_to_python(value, "DATE") == date(2024, 1, 2)
