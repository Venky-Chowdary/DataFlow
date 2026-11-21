"""BIT(n>1) / VARBIT bitstring fidelity — not BYTEA/base64 invent."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_bitstring_wire, normalize_sql_bind_value  # noqa: E402
from connectors.writer_common import quarantine_unfit_bitstrings  # noqa: E402
from services.schema_introspect import _pg_to_logical  # noqa: E402
from services.type_system import (  # noqa: E402
    bitstring_width_would_narrow,
    ddl_type,
    is_bitstring_carrier,
    is_precision_collapse_coercion,
    normalize_logical_type,
    parse_bitstring_width,
)


def test_pg_bitstring_carriers_preserved():
    assert _pg_to_logical("bit(32)") == "BIT(32)"
    assert _pg_to_logical("bit varying(16)") == "BIT VARYING(16)"
    assert _pg_to_logical("varbit(8)") == "VARBIT(8)"
    assert normalize_logical_type("BIT(32)") == "binary"
    assert normalize_logical_type("BIT(1)") == "boolean"
    assert is_bitstring_carrier("BIT(32)") is True
    # DW-D320: sized BIT(1) is a one-bit string on MySQL/PG, not bare BIT boolean.
    assert is_bitstring_carrier("BIT(1)") is True
    assert parse_bitstring_width("BIT VARYING(16)") == 16


def test_bitstring_ddl_not_bytea():
    assert ddl_type("postgresql", "BIT(32)") == "BIT(32)"
    assert ddl_type("postgresql", "BIT VARYING(16)") == "BIT VARYING(16)"
    assert ddl_type("mysql", "BIT(32)") == "BIT(32)"
    assert ddl_type("mysql", "BIT(1)") == "BIT(1)"
    assert ddl_type("mysql", "BIT VARYING(16)") == "BIT(16)"
    # Engines without bitstring → VARCHAR of 0/1 digits, never BYTEA invent.
    assert "BYTEA" not in ddl_type("snowflake", "BIT(32)").upper()
    assert "BINARY" not in ddl_type("snowflake", "BIT(32)").upper()


def test_bitstring_g3_narrow():
    assert bitstring_width_would_narrow("BIT(32)", "BIT(8)") is True
    assert bitstring_width_would_narrow("BIT(8)", "BIT(32)") is False
    assert is_precision_collapse_coercion("BIT(32)", "BIT(8)") is True
    assert bitstring_width_would_narrow("BYTEA", "BIT(8)") is True


def test_bitstring_wire_and_quarantine():
    assert coerce_bitstring_wire("1010", width=4) == "1010"
    assert coerce_bitstring_wire("B'1010'", width=4) == "1010"
    try:
        coerce_bitstring_wire("YWJj", width=4)  # base64 of 'abc'
        raise AssertionError("expected refuse")
    except ValueError as exc:
        assert "0/1" in str(exc)
    assert normalize_sql_bind_value("1010", "BIT(4)", engine="postgresql") == "1010"

    assert coerce_bitstring_wire("10", width=4) == "0010"
    assert coerce_bitstring_wire("10101010", width=8) == "10101010"
    assert coerce_bitstring_wire(b"101", width=16) == "0000000000000101"
    details: list[dict] = []
    out = quarantine_unfit_bitstrings(
        [("1010",), ("YWJj",), ("10",)],
        ["flags"],
        ["BIT(4)"],
        details,
        policy="quarantine",
    )
    assert out == [("1010",), ("0010",)]
    assert len(details) == 1
