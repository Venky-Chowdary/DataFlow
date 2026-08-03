"""Wave 65/7: bare datetime → wall-clock NTZ (never invent TIMESTAMPTZ)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_bare_datetime_uses_platform_wall_clock_default():
    from services.type_system import (
        DDL_TYPES,
        LOGICAL_DATETIME,
        datetime_timezone_polarity,
        ddl_type,
    )

    assert datetime_timezone_polarity("datetime") is None
    assert datetime_timezone_polarity("DATETIME") is None
    # Explicit NTZ from MySQL introspect stays naive.
    assert datetime_timezone_polarity("TIMESTAMP_NTZ") == "ntz"
    assert datetime_timezone_polarity("DATETIME(6)") == "ntz"

    # Bare datetime → wall-clock (wave 7); explicit TIMESTAMPTZ stays aware.
    assert ddl_type("postgresql", "datetime") == DDL_TYPES["postgresql"][LOGICAL_DATETIME]
    assert DDL_TYPES["postgresql"][LOGICAL_DATETIME] == "TIMESTAMP"
    assert ddl_type("postgresql", "TIMESTAMP_NTZ") == "TIMESTAMP"
    assert ddl_type("postgresql", "TIMESTAMPTZ") == "TIMESTAMPTZ"
    assert ddl_type("snowflake", "datetime") == DDL_TYPES["snowflake"][LOGICAL_DATETIME]
    assert DDL_TYPES["snowflake"][LOGICAL_DATETIME] == "TIMESTAMP_NTZ"
    assert ddl_type("snowflake", "TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"
    # PG-class timestamptz → Snowflake LTZ (session-relative instant), not invent NTZ.
    assert ddl_type("snowflake", "TIMESTAMPTZ") in {"TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
