"""Wave 71: MySQL BIT(n) introspect + Snowflake LTZ vs TZ polarity."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_mysql_bit_and_mediumint_carriers():
    from services.schema_introspect import _mysql_to_logical
    from services.type_system import ddl_type, normalize_logical_type

    assert _mysql_to_logical("bit(1)") == "BIT(1)"
    assert _mysql_to_logical("bit(8)") == "BIT(8)"
    assert normalize_logical_type("BIT(1)") == "boolean"
    assert normalize_logical_type("BIT(8)") == "binary"
    assert ddl_type("postgresql", "BIT(8)") == "BIT(8)"
    assert _mysql_to_logical("mediumint") == "MEDIUMINT"
    assert _mysql_to_logical("mediumint unsigned") == "MEDIUMINT UNSIGNED"


def test_timestamp_with_local_time_zone_not_string():
    from services.type_system import (
        datetime_timezone_polarity,
        ddl_type,
        is_timezone_polarity_loss,
        normalize_logical_type,
    )

    assert normalize_logical_type("TIMESTAMP WITH LOCAL TIME ZONE") == "datetime"
    assert datetime_timezone_polarity("TIMESTAMP WITH LOCAL TIME ZONE") == "ltz"
    assert datetime_timezone_polarity("TIMESTAMP_LTZ") == "ltz"
    assert datetime_timezone_polarity("TIMESTAMP_TZ") == "tz"
    assert datetime_timezone_polarity("TIMESTAMPTZ") == "ltz"
    assert datetime_timezone_polarity("DATETIMEOFFSET") == "tz"

    assert ddl_type("snowflake", "TIMESTAMP_LTZ") == "TIMESTAMP_LTZ"
    assert ddl_type("snowflake", "TIMESTAMP_TZ") == "TIMESTAMP_TZ"
    assert ddl_type("snowflake", "TIMESTAMP WITH LOCAL TIME ZONE") == "TIMESTAMP_LTZ"
    assert ddl_type("snowflake", "TIMESTAMPTZ") == "TIMESTAMP_LTZ"
    assert ddl_type("oracle", "TIMESTAMP WITH LOCAL TIME ZONE") == (
        "TIMESTAMP WITH LOCAL TIME ZONE"
    )

    assert is_timezone_polarity_loss("TIMESTAMP_LTZ", "TIMESTAMP_NTZ") is True
    assert is_timezone_polarity_loss("TIMESTAMP_LTZ", "TIMESTAMP_TZ") is True
    assert is_timezone_polarity_loss("TIMESTAMP_TZ", "TIMESTAMP_LTZ") is True
    assert is_timezone_polarity_loss("TIMESTAMP_LTZ", "TIMESTAMPTZ") is False


def test_snowflake_introspect_preserves_ltz_tz():
    from services.schema_introspect import _sf_to_logical

    assert _sf_to_logical("TIMESTAMP_LTZ") == "TIMESTAMP_LTZ"
    assert _sf_to_logical("TIMESTAMP_TZ") == "TIMESTAMP_TZ"
    assert _sf_to_logical("TIMESTAMP_LTZ", datetime_precision=6) == "TIMESTAMP_LTZ(6)"
    assert _sf_to_logical("TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"
