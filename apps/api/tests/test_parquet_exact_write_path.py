"""Parquet ingest uses Arrow to_pylist, not pandas float64 invent.

table.to_pandas() + Series.item() invented nullable integers as 1.0 and
collapsed long DECIMAL identity before Map / write. Streaming peek already
uses to_pylist; the non-streaming FileParser path must match.
"""

from __future__ import annotations

import io
import math
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from services.file_parser import FileParser  # noqa: E402
from src.transfer.adapters import parse_file_content, parse_file_route_sample  # noqa: E402

LONG = "1.234567890123456789"
IEEE_LOSSY = 9007199254740993


def _parquet_bytes(table: object) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _pandas_first(table: object, col: str):
    val = table.to_pandas()[col].tolist()[0]
    if hasattr(val, "item"):
        val = val.item()
    return val


def test_decimal_long_fraction_stays_identity():
    table = pa.table(
        {"amt": pa.array([Decimal(LONG)], type=pa.decimal128(38, 18))}
    )
    result = FileParser.parse_parquet(_parquet_bytes(table))
    assert result.success is True
    assert result.data[0]["amt"] == Decimal(LONG)
    assert result.schema_map["amt"] == "DECIMAL(38,18)"
    pandas_val = _pandas_first(table, "amt")
    if isinstance(pandas_val, float):
        assert pandas_val != result.data[0]["amt"]


def test_int64_past_ieee_mantissa_stays_int():
    table = pa.table({"big": pa.array([IEEE_LOSSY], type=pa.int64())})
    result = FileParser.parse_parquet(_parquet_bytes(table))
    assert result.success is True
    assert result.data[0]["big"] == IEEE_LOSSY
    assert type(result.data[0]["big"]) is int
    assert result.data[0]["big"] != float(IEEE_LOSSY)
    assert result.schema_map["big"] == "BIGINT"


def test_nullable_int_does_not_become_float():
    table = pa.table({"maybe_int": pa.array([1, None], type=pa.int64())})
    result = FileParser.parse_parquet(_parquet_bytes(table))
    assert result.success is True
    assert result.data[0]["maybe_int"] == 1
    assert type(result.data[0]["maybe_int"]) is int
    assert result.data[1]["maybe_int"] is None
    pandas_val = _pandas_first(table, "maybe_int")
    if isinstance(pandas_val, float):
        assert pandas_val == 1.0
        assert type(pandas_val) is float
        assert type(result.data[0]["maybe_int"]) is int


def test_ieee_float_and_nan_stay_ieee():
    table = pa.table(
        {
            "amt": pa.array([1.5, float("nan")], type=pa.float64()),
        }
    )
    result = FileParser.parse_parquet(_parquet_bytes(table))
    assert result.success is True
    assert result.data[0]["amt"] == 1.5
    assert isinstance(result.data[0]["amt"], float)
    assert math.isnan(result.data[1]["amt"])
    assert result.schema_map["amt"] == "DOUBLE"


def test_parse_filename_and_adapter_keep_decimal():
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "amt": pa.array([Decimal(LONG)], type=pa.decimal128(38, 18)),
        }
    )
    raw = _parquet_bytes(table)
    parsed = FileParser.parse(raw, "ledger.parquet")
    assert parsed.success is True
    assert parsed.data[0]["amt"] == Decimal(LONG)
    rows, columns, schema = parse_file_content(raw, "ledger.parquet")
    assert columns == ["id", "amt"]
    assert rows[0]["amt"] == Decimal(LONG)
    assert schema["amt"] == "DECIMAL(38,18)"
    assert schema["id"] == "BIGINT"
    _cols, route_schema, n = parse_file_route_sample(raw, "ledger.parquet")
    assert n == 1
    assert route_schema["amt"] == "DECIMAL(38,18)"


def test_over_max_rows_keeps_writer_schema():
    table = pa.table(
        {"amt": pa.array([Decimal("1.5"), Decimal("2.5")], type=pa.decimal128(10, 1))}
    )
    result = FileParser.parse_parquet(_parquet_bytes(table), max_rows=1)
    assert result.success is False
    assert result.row_count == 2
    assert result.data == []
    assert result.schema_map == {"amt": "DECIMAL(10,1)"}
    assert "streaming" in (result.error or "").lower()
