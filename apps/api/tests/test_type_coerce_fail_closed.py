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
