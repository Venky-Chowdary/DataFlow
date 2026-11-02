"""Mongo Query JSON uses parse_float=Decimal, not json_util IEEE invent.

json_util.loads → json.loads, so a long fraction in a find predicate
collapsed before the server compared it. $date / $oid still rebuild.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.routers.query_router import _parse_mongodb_json  # noqa: E402

LONG = "1.234567890123456789"
IEEE_LOSSY = 9007199254740993


def test_long_fraction_stays_decimal():
    parsed = _parse_mongodb_json(f'{{"amt": {LONG}}}')
    assert parsed["amt"] == Decimal(LONG)
    stock = json.loads(f'{{"amt": {LONG}}}')["amt"]
    assert stock != parsed["amt"]


def test_ieee_exact_fraction_stays_float():
    parsed = _parse_mongodb_json('{"amt": 1.5}')
    assert parsed["amt"] == 1.5
    assert isinstance(parsed["amt"], float)


def test_int_past_ieee_mantissa_stays_int():
    parsed = _parse_mongodb_json(f'{{"big": {IEEE_LOSSY}}}')
    assert parsed["big"] == IEEE_LOSSY
    assert type(parsed["big"]) is int
    assert parsed["big"] != float(IEEE_LOSSY)


def test_extended_json_date_still_datetime():
    pytest.importorskip("bson")
    parsed = _parse_mongodb_json(
        '{"ordered_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}'
    )
    value = parsed["ordered_at"]["$gte"]
    assert isinstance(value, datetime)
    assert value.year == 2024


def test_extended_json_oid_still_objectid():
    pytest.importorskip("bson")
    from bson import ObjectId

    oid = "507f1f77bcf86cd799439011"
    parsed = _parse_mongodb_json(f'{{"_id": {{"$oid": "{oid}"}}}}')
    assert parsed["_id"] == ObjectId(oid)


def test_number_decimal_wrapper_still_decimal128():
    pytest.importorskip("bson")
    from bson.decimal128 import Decimal128

    parsed = _parse_mongodb_json(f'{{"amt": {{"$numberDecimal": "{LONG}"}}}}')
    assert isinstance(parsed["amt"], Decimal128)
    assert parsed["amt"].to_decimal() == Decimal(LONG)


def test_numeric_date_millis_still_datetime():
    pytest.importorskip("bson")
    parsed = _parse_mongodb_json('{"ts": {"$date": 1700000000000}}')
    assert isinstance(parsed["ts"], datetime)
