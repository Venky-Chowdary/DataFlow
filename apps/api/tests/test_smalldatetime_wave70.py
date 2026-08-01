"""Wave 70: SQL Server SMALLDATETIME minute accuracy + TIMESTAMP(0) create-new."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_smalldatetime_introspect_and_ddl():
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type, temporal_precision_would_narrow

    assert _sqlserver_to_logical("smalldatetime") == "SMALLDATETIME"
    assert _sqlserver_to_logical("datetime2(7)") == "TIMESTAMP_NTZ(7)"
    assert ddl_type("sqlserver", "SMALLDATETIME") == "SMALLDATETIME"
    assert ddl_type("postgresql", "SMALLDATETIME") == "TIMESTAMP(0)"
    assert ddl_type("snowflake", "SMALLDATETIME") == "TIMESTAMP_NTZ(0)"

    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(7)", "SMALLDATETIME") is True
    assert temporal_precision_would_narrow("SMALLDATETIME", "SMALLDATETIME") is False


def test_round_to_smalldatetime_microsoft_rules():
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.sql_temporal import round_to_smalldatetime

    # ≤ 29.998 → floor
    assert round_to_smalldatetime(datetime(2024, 5, 9, 12, 30, 29, 998_000)) == datetime(
        2024, 5, 9, 12, 30
    )
    # ≥ 29.999 → ceil
    assert round_to_smalldatetime(datetime(2024, 5, 9, 12, 30, 29, 999_000)) == datetime(
        2024, 5, 9, 12, 31
    )
    # 23:59:59 rounds to next day midnight (Microsoft example class)
    assert round_to_smalldatetime(datetime(2024, 5, 9, 23, 59, 59)) == datetime(
        2024, 5, 10, 0, 0
    )

    assert normalize_sql_bind_value(
        "2024-05-09T12:30:45", "SMALLDATETIME", engine="sqlserver"
    ) == datetime(2024, 5, 9, 12, 31)
