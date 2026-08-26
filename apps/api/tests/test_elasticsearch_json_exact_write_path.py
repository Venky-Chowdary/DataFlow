"""Elasticsearch _source JSON uses json_loads_exact, not stdlib float().

elastic_transport.JsonSerializer.json_loads collapsed long fractions.
default(Decimal) was float(data). The shared _client pins both.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("elasticsearch")
pytest.importorskip("elastic_transport")

from connectors.elasticsearch_reader import (  # noqa: E402
    _cell,
    _client,
    _exact_es_serializers,
)
from elastic_transport import JsonSerializer  # noqa: E402

LONG = "1.234567890123456789"
IEEE_LOSSY = 9007199254740993


def _json_ser():
    return _exact_es_serializers()["application/json"]


def test_source_long_fraction_stays_decimal():
    collapsed = JsonSerializer().loads(f'{{"amt": {LONG}}}'.encode())["amt"]
    parsed = _json_ser().loads(f'{{"amt": {LONG}}}'.encode())
    assert parsed["amt"] == Decimal(LONG)
    assert collapsed != parsed["amt"]
    assert _cell(parsed["amt"]) == LONG


def test_ieee_exact_fraction_stays_float():
    parsed = _json_ser().loads(b'{"amt": 1.5}')
    assert parsed["amt"] == 1.5
    assert isinstance(parsed["amt"], float)


def test_int_past_ieee_mantissa_stays_int():
    parsed = _json_ser().loads(f'{{"big": {IEEE_LOSSY}}}'.encode())
    assert parsed["big"] == IEEE_LOSSY
    assert type(parsed["big"]) is int
    assert parsed["big"] != float(IEEE_LOSSY)


def test_dump_decimal_does_not_float():
    ser = _json_ser()
    dumped = ser.dumps({"amt": Decimal(LONG)})
    text = dumped.decode("utf-8") if isinstance(dumped, bytes) else dumped
    assert LONG in text
    stock = JsonSerializer().dumps({"amt": Decimal(LONG)})
    stock_text = stock.decode("utf-8") if isinstance(stock, bytes) else stock
    assert json.loads(stock_text)["amt"] != Decimal(LONG)
    assert json.loads(text)["amt"] == LONG


def test_ndjson_long_fraction_stays_decimal():
    ser = _exact_es_serializers()["application/x-ndjson"]
    parsed = ser.loads(f'{{"amt": {LONG}}}\n{{"amt": 1.5}}\n'.encode())
    assert parsed[0]["amt"] == Decimal(LONG)
    assert parsed[1]["amt"] == 1.5


def test_client_uses_exact_serializers():
    client = _client({"host": "localhost", "port": 9200})
    try:
        ser = client.transport.serializers.get_serializer("application/json")
        parsed = ser.loads(f'{{"amt": {LONG}}}'.encode())
        assert parsed["amt"] == Decimal(LONG)
    finally:
        client.close()
