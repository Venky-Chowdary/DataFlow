"""Iceberg reader cells use cell_to_string, not default=str / str(value).

json.dumps(..., default=str) invented nested BINARY as a Python b'...' repr,
scientific Decimal as 1E+2, and timestamps as a space instead of ISO T.
Leaf Decimal / datetime used str(value) for the same invent. PostgreSQL
already uses cell_to_string; Iceberg must match.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_reader import _stringify  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402

LONG = "1.234567890123456789"
BLOB = bytes([0xFF, 0xFE, 0x00])
TS = datetime(2024, 1, 2, 3, 4, 5)


def _stock_nested(value: object) -> str:
    return json.dumps(value, default=str)


def test_matches_sql_reader_cell_to_string():
    assert _stringify(None) == SQL_NULL_SENTINEL
    assert _stringify(BLOB) == cell_to_string(BLOB, preserve_sql_null=True)
    assert _stringify(Decimal(LONG)) == cell_to_string(Decimal(LONG), preserve_sql_null=True)
    assert _stringify(TS) == cell_to_string(TS, preserve_sql_null=True)
    assert _stringify(True) == "true"


def test_leaf_decimal_keeps_fixed_digits_not_scientific_str():
    scientific = Decimal("1E+2")
    assert str(scientific) == "1E+2"
    assert _stringify(scientific) == "100"
    assert _stringify(Decimal(LONG)) == LONG
    assert _stringify(Decimal("1.50")) == "1.50"


def test_leaf_datetime_is_iso_t_not_space():
    assert " " in str(TS)
    assert "T" not in str(TS)
    assert _stringify(TS) == "2024-01-02T03:04:05"
    assert " " not in _stringify(TS)


def test_nested_bytes_are_base64_not_python_repr():
    nested = {"blob": BLOB, "amt": Decimal(LONG)}
    invented = json.loads(_stock_nested(nested))
    assert invented["blob"] == str(BLOB)
    assert invented["blob"].startswith("b'")
    wire = json.loads(_stringify(nested))
    assert wire["blob"] == cell_to_string(BLOB)
    assert wire["blob"] != str(BLOB)
    assert wire["amt"] == LONG


def test_nested_datetime_is_iso_not_default_str():
    nested = {"ts": TS}
    invented = _stock_nested(nested)
    assert "2024-01-02 03:04:05" in invented
    wire = _stringify(nested)
    assert "2024-01-02T03:04:05" in wire
    assert "2024-01-02 03:04:05" not in wire


def test_nested_list_keeps_long_fraction():
    wire = _stringify([Decimal(LONG), Decimal("1.5"), 1])
    parsed = json.loads(wire)
    assert parsed[0] == LONG
    assert parsed[1] == "1.5"
    assert parsed[2] == 1
