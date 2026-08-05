"""Iceberg write quarantine matrix — decimal / fixed(L) / integer honesty."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.iceberg_writer import (  # noqa: E402
    _apply_iceberg_write_quarantine,
    _decimal_target_types_for_iceberg_write,
    _iceberg_type_to_logical_carrier,
    _logical_to_arrow_type,
)
from services.type_system import ddl_type, normalize_logical_type, parse_binary_carrier_width  # noqa: E402


def test_iceberg_fixed_is_binary_not_decimal():
    assert normalize_logical_type("fixed(16)") == "binary"
    assert parse_binary_carrier_width("fixed(16)") == 16
    assert ddl_type("iceberg", "BINARY(16)") == "fixed(16)"


def test_iceberg_type_carrier_preserves_fixed_and_int():
    assert _iceberg_type_to_logical_carrier({"type": "fixed", "length": 16}) == "BINARY(16)"
    assert _iceberg_type_to_logical_carrier("fixed[16]") == "BINARY(16)"
    assert _iceberg_type_to_logical_carrier("int") == "INT"
    assert _iceberg_type_to_logical_carrier("long") == "BIGINT"


def test_iceberg_arrow_fixed_binary():
    pa = __import__("pyarrow")
    assert pa.types.is_fixed_size_binary(_logical_to_arrow_type("BINARY(16)", pa))
    assert _logical_to_arrow_type("BINARY(16)", pa).byte_width == 16


def test_quarantine_types_prefer_arrow_decimal_and_fixed():
    pa = __import__("pyarrow")
    schema = pa.schema(
        [
            ("amount", pa.decimal128(10, 2)),
            ("blob", pa.binary(4)),
            ("label", pa.large_string()),
        ]
    )
    types = _decimal_target_types_for_iceberg_write(
        ["amount", "blob", "label"],
        {"amount": "DECIMAL(38,10)", "blob": "BINARY", "label": "string"},
        arrow_schema=schema,
        pa_mod=pa,
    )
    assert types[0] == "DECIMAL(10,2)"
    assert types[1] == "BINARY(4)"
    assert types[2] == "STRING"


def test_iceberg_quarantine_holds_decimal_overflow_and_bad_binary():
    details: list[dict] = []
    rows = [
        ("999999999999.99", "AQIDBA=="),  # decimal overflow for (10,2); 4-byte b64 ok
        ("1.50", "not-valid-base64!!!"),
        ("2.00", "AQID"),  # 3 bytes — overflow fixed(4)? AQID is 3 bytes decoded
    ]
    # AQIDBA== → 4 bytes; AQID → 3 bytes
    out = _apply_iceberg_write_quarantine(
        rows,
        ["amount", "blob"],
        ["DECIMAL(10,2)", "BINARY(4)"],
        details,
        policy="quarantine",
    )
    # Row0 amount overflows DECIMAL(10,2) integer digits → held out
    # Row1 invalid base64 → held out
    # Row2 amount ok, blob 3 bytes fits BINARY(4) → kept
    assert len(out) == 1
    assert out[0][0] == "2.00"
    assert details
    assert any("decimal" in d["reason"].lower() or "Iceberg decimal" in d["reason"] for d in details)
    assert any("base64" in d["reason"].lower() or "binary" in d["reason"].lower() for d in details)


def test_iceberg_quarantine_types_from_mapped_fixed():
    types = _decimal_target_types_for_iceberg_write(
        ["blob"],
        {"blob": "VARBINARY(32)"},
    )
    assert types == ["fixed(32)"]
