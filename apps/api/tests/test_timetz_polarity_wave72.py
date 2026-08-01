"""Wave 72: PostgreSQL TIMETZ / TIME WITH TIME ZONE polarity SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_pg_timetz_introspect_and_ddl():
    from services.schema_introspect import _pg_to_logical
    from services.type_system import (
        ddl_type,
        is_precision_collapse_coercion,
        time_timezone_polarity,
        time_timezone_polarity_loss,
    )

    assert _pg_to_logical("timetz") == "TIMETZ"
    assert _pg_to_logical("time with time zone") == "TIMETZ"
    assert _pg_to_logical("time without time zone") == "TIME"
    assert _pg_to_logical("time(3)") == "TIME(3)"

    assert time_timezone_polarity("TIMETZ") == "tz"
    assert time_timezone_polarity("TIME WITH TIME ZONE") == "tz"
    assert time_timezone_polarity("TIME") == "ntz"

    assert ddl_type("postgresql", "TIMETZ") == "TIME WITH TIME ZONE"
    assert ddl_type("postgresql", "TIMETZ(3)") == "TIME(3) WITH TIME ZONE"
    assert ddl_type("snowflake", "TIMETZ") == "TIME"  # no native TIMETZ
    assert time_timezone_polarity_loss("TIMETZ", "TIME") is True
    assert is_precision_collapse_coercion("TIMETZ", "TIME") is True
    assert time_timezone_polarity_loss("TIMETZ", "TIME WITH TIME ZONE") is False
