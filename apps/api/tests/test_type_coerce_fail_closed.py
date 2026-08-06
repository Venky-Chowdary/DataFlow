"""Fail-closed boolean / decimal / timezone coerce — no invent."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_sqlite_boolean_false_string_is_zero_not_true():
    from connectors.sqlite_writer import _to_sqlite_value

    assert _to_sqlite_value("false", "BOOLEAN") == 0
    assert _to_sqlite_value("no", "BOOLEAN") == 0
    assert _to_sqlite_value("true", "BOOLEAN") == 1
    with pytest.raises(ValueError, match="unrecognized|BOOLEAN"):
        _to_sqlite_value("maybe", "BOOLEAN")


def test_dynamo_boolean_refuses_unknown_token():
    from connectors.dynamodb_writer import _to_dynamo_value

    assert _to_dynamo_value("false", "BOOLEAN") is False
    with pytest.raises(ValueError, match="unrecognized|BOOLEAN"):
        _to_dynamo_value("maybe", "BOOLEAN")


def test_generic_sql_decimal_refuses_nan():
    from connectors.generic_sql import _to_sa_value
    from services.type_system import LOGICAL_DECIMAL

    with pytest.raises(ValueError, match="NaN|Inf|finite|decimal"):
        _to_sa_value(float("nan"), LOGICAL_DECIMAL)


def test_snowflake_timestamptz_keeps_utc_offset_on_wire():
    from connectors.warehouse_temporal import format_snowflake_bind

    out = format_snowflake_bind("2024-06-01T12:00:00-05:00", "TIMESTAMP_TZ")
    assert "+00:00" in str(out)
    assert "17:00:00" in str(out)
    with pytest.raises(ValueError, match="naive|offset"):
        format_snowflake_bind("2024-06-01 12:00:00", "TIMESTAMP_TZ")


def test_generic_sql_boolean_integer_refuse_passthrough_invent():
    from connectors.generic_sql import _to_sa_value

    assert _to_sa_value("false", "boolean") is False
    with pytest.raises(ValueError, match="BOOLEAN refused|refuse invent"):
        _to_sa_value("maybe", "boolean")
    assert _to_sa_value("42", "integer") == 42
    with pytest.raises(ValueError, match="refuse invent integer"):
        _to_sa_value("not-an-int", "integer")
    with pytest.raises(ValueError, match="refuse invent integer|fractional"):
        _to_sa_value("12.5", "integer")


def test_dynamo_json_and_numeric_key_refuse_invent():
    from decimal import Decimal

    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_dynamo_value

    assert _to_dynamo_value('{"a":1}', "JSON") == {"a": 1}
    with pytest.raises(ValueError, match="JSON refused"):
        _to_dynamo_value("not-json{", "JSON")
    with pytest.raises(ValueError, match="JSON refused"):
        _to_dynamo_value("{not:valid}", "JSON")
    assert _coerce_dynamo_cell("9", col="pk", logical_type="INTEGER", key_types={"pk": "N"}) == Decimal(
        "9"
    )
    with pytest.raises(ValueError, match="key type N refused"):
        _coerce_dynamo_cell("x", col="pk", logical_type="INTEGER", key_types={"pk": "N"})


def test_iceberg_integer_boolean_refuse_invent():
    import pyarrow as pa
    from connectors.iceberg_writer import _coerce_arrow_cell

    with pytest.raises(ValueError, match="INTEGER|invent"):
        _coerce_arrow_cell("not-an-int", pa.int64(), pa)
    with pytest.raises(ValueError, match="boolean|refuse invent"):
        _coerce_arrow_cell("maybe", pa.bool_(), pa)
    assert _coerce_arrow_cell("false", pa.bool_(), pa) is False
