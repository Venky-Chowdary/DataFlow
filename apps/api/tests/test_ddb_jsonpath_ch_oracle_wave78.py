"""Wave 78: DynamoDB AttributeValue / PG JSONPATH / CH FixedString / Oracle INTERVAL.

Research anchors
----------------
- AWS DynamoDB AttributeValue: S/N/B/BOOL/NULL/M/L/SS/NS/BS — never invent S
  for Map/List (Document path honesty).
- PostgreSQL jsonpath is a specialty path type (not TEXT invent).
- ClickHouse FixedString(n) is fixed bytes; Decimal128(S) is scale-only typmod.
- Oracle INTERVAL DAY(d) TO SECOND(s) preserves leading-field precision.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_dynamodb_attribute_value_codes():
    from services.type_system import ddl_type, normalize_logical_type

    assert normalize_logical_type("M") == "json"
    assert normalize_logical_type("L") == "array"
    assert normalize_logical_type("N") == "decimal"
    assert normalize_logical_type("S") == "string"
    assert normalize_logical_type("B") == "binary"
    assert normalize_logical_type("BOOL") == "boolean"
    assert normalize_logical_type("SS") == "array"
    assert normalize_logical_type("NS") == "array"
    assert normalize_logical_type("BS") == "array"

    assert ddl_type("dynamodb", "M") == "M"
    assert ddl_type("dynamodb", "L") == "L"
    assert ddl_type("dynamodb", "N") == "N"
    assert ddl_type("dynamodb", "JSON") == "M"
    assert ddl_type("dynamodb", "ARRAY") == "L"
    # Must not invent S for maps/lists.
    assert ddl_type("dynamodb", "M") != "S"
    assert ddl_type("postgresql", "M") == "JSONB"


def test_pg_jsonpath_specialty():
    from connectors.sql_bind import coerce_jsonpath_wire, normalize_sql_bind_value
    from services.schema_introspect import _pg_to_logical
    from services.type_system import ddl_type, specialty_carrier_would_collapse

    assert _pg_to_logical("jsonpath") == "JSONPATH"
    assert ddl_type("postgresql", "JSONPATH") == "JSONPATH"
    assert specialty_carrier_would_collapse("JSONPATH", "TEXT") is True
    assert coerce_jsonpath_wire("$.a.b") == "$.a.b"
    assert normalize_sql_bind_value("$.items[*]", "JSONPATH") == "$.items[*]"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_jsonpath_wire(123)


def test_ch_fixedstring_and_decimal128_scale():
    from services.schema_introspect import _ch_to_logical
    from services.type_system import ddl_type

    assert _ch_to_logical("FixedString(16)") == "BINARY(16)"
    assert _ch_to_logical("Decimal128(10)") == "DECIMAL(38,10)"
    assert _ch_to_logical("Decimal64(4)") == "DECIMAL(18,4)"
    assert ddl_type("clickhouse", "FixedString(16)") == "FixedString(16)"
    assert ddl_type("clickhouse", "BINARY(16)") == "FixedString(16)"
    # Raw CH Decimal128(S) or mapped DECIMAL(38,10) → PG NUMERIC with scale.
    assert ddl_type("postgresql", "Decimal128(10)") == "NUMERIC(38,10)"
    assert ddl_type("postgresql", "DECIMAL(38,10)") == "NUMERIC(38,10)"


def test_oracle_interval_precision_preserved():
    from services.schema_introspect import _oracle_to_logical
    from services.type_system import ddl_type

    assert _oracle_to_logical("INTERVAL DAY(3) TO SECOND(6)") == (
        "INTERVAL DAY(3) TO SECOND(6)"
    )
    assert _oracle_to_logical("INTERVAL YEAR(4) TO MONTH") == (
        "INTERVAL YEAR(4) TO MONTH"
    )
    assert ddl_type("oracle", "INTERVAL DAY(3) TO SECOND(6)") == (
        "INTERVAL DAY(3) TO SECOND(6)"
    )
    assert ddl_type("oracle", "INTERVAL YEAR(4) TO MONTH") == (
        "INTERVAL YEAR(4) TO MONTH"
    )
    # Cross-engine keeps family, may drop Oracle typmod.
    assert "INTERVAL" in ddl_type("postgresql", "INTERVAL DAY(3) TO SECOND(6)").upper()
