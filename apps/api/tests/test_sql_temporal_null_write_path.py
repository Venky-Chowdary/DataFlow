"""Temporal bind treats reader-null as SQL NULL via absent_sql_bind.

Direct coerce_sql_temporal / warehouse formatters used to only pass None
through. After extract emits SQL_NULL_SENTINEL, DATE/TIMESTAMP wrappers
returned the sentinel spelling (generic_sql / sqlite / Snowflake DATE)
or raised naive-TZ (BigQuery TIMESTAMP / Snowflake TZ). Missing stays
Missing. Empty string still refuses — that is not extract NULL.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.hubspot_writer import (  # noqa: E402
    coerce_hubspot_date_wire,
    coerce_hubspot_datetime_wire,
)
from connectors.sql_temporal import coerce_sql_temporal, wire_check_temporal  # noqa: E402
from connectors.warehouse_temporal import (  # noqa: E402
    format_bigquery_bind,
    format_snowflake_bind,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


_TEMPORAL_DDL = (
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
    "TIMESTAMPTZ",
    "DATETIMEOFFSET",
)

_NULL_WIRES = (None, SQL_NULL_SENTINEL, "__df_ddb_null__")


def test_coerce_reader_null_is_sql_null():
    for ddl in _TEMPORAL_DDL:
        for wire in _NULL_WIRES:
            assert coerce_sql_temporal(wire, ddl) is None, (ddl, wire)
        assert coerce_sql_temporal(Missing, ddl) is Missing
        assert coerce_sql_temporal(DF_MISSING_SENTINEL, ddl) == DF_MISSING_SENTINEL


def test_wire_check_reader_null_is_ok():
    for ddl in _TEMPORAL_DDL:
        for wire in _NULL_WIRES:
            check = wire_check_temporal(wire, ddl, engine="mysql")
            assert check["ok"] is True, (ddl, wire, check)
            assert check["wire_value"] is None


def test_warehouse_reader_null_is_sql_null():
    for ddl in ("DATE", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIME"):
        for wire in _NULL_WIRES:
            assert format_snowflake_bind(wire, ddl) is None, (ddl, wire)
        assert format_snowflake_bind(Missing, ddl) is Missing
    for ddl in ("DATE", "TIMESTAMP", "DATETIME", "TIME"):
        for wire in _NULL_WIRES:
            assert format_bigquery_bind(wire, ddl) is None, (ddl, wire)
        assert format_bigquery_bind(Missing, ddl) is Missing


def test_hubspot_reader_null_is_sql_null():
    for wire in _NULL_WIRES:
        assert coerce_hubspot_date_wire(wire) is None
        assert coerce_hubspot_datetime_wire(wire) is None
    assert coerce_hubspot_date_wire(Missing) is Missing
    assert coerce_hubspot_datetime_wire(Missing) is Missing


def test_empty_string_still_refuses_temporal_null_invent():
    with pytest.raises(ValueError, match="empty string"):
        coerce_sql_temporal("", "DATE")
    with pytest.raises(ValueError, match="empty string"):
        coerce_sql_temporal("", "TIMESTAMP", engine="mysql")
    with pytest.raises(ValueError, match="empty HubSpot"):
        coerce_hubspot_date_wire("")
    with pytest.raises(ValueError, match="empty HubSpot"):
        coerce_hubspot_datetime_wire("")


def test_present_calendar_still_binds():
    assert coerce_sql_temporal("2024-08-09", "DATE") == date(2024, 8, 9)
    assert isinstance(coerce_sql_temporal("2024-08-09T01:58:42", "DATETIME"), datetime)
