"""Wave 55: LSEG/LINE/PATH/POLYGON bind + SaaS datetime→TIMESTAMPTZ + epoch int."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_lseg_wire():
    from connectors.sql_bind import coerce_lseg_wire, normalize_sql_bind_value

    assert coerce_lseg_wire("[(0,0),(1,1)]") == "[(0,0),(1,1)]"
    assert coerce_lseg_wire({"x1": 0, "y1": 0, "x2": 2, "y2": 3}) == (
        "[(0.0,0.0),(2.0,3.0)]"
    )
    assert coerce_lseg_wire([(0, 0), (1, 1)]) == "[(0.0,0.0),(1.0,1.0)]"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_lseg_wire(1)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_lseg_wire([(0, 0)])
    assert normalize_sql_bind_value("0,0,1,1", "LSEG") == "[(0,0),(1,1)]"


def test_coerce_line_wire():
    from connectors.sql_bind import coerce_line_wire, normalize_sql_bind_value

    assert coerce_line_wire("{1,2,3}") == "{1,2,3}"
    assert coerce_line_wire({"a": 1, "b": -1, "c": 0}) == "{1.0,-1.0,0.0}"
    assert coerce_line_wire([(0, 0), (1, 1)]) == "((0.0,0.0),(1.0,1.0))"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_line_wire(True)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_line_wire("{0,0,1}")  # A and B both zero
    assert normalize_sql_bind_value("{2,3,4}", "LINE") == "{2,3,4}"


def test_coerce_path_wire():
    from connectors.sql_bind import coerce_path_wire, normalize_sql_bind_value

    assert coerce_path_wire("[(0,0),(1,1),(2,0)]") == "[(0,0),(1,1),(2,0)]"
    assert coerce_path_wire([(0, 0), (1, 1)], closed=False) == "[(0.0,0.0),(1.0,1.0)]"
    assert coerce_path_wire(
        {"points": [(0, 0), (1, 0), (0, 1)], "closed": True}
    ) == "((0.0,0.0),(1.0,0.0),(0.0,1.0))"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_path_wire(3.14)
    assert normalize_sql_bind_value("(0,0),(1,1)", "PATH").startswith("(")


def test_coerce_polygon_wire():
    from connectors.sql_bind import coerce_polygon_wire, normalize_sql_bind_value

    assert coerce_polygon_wire("((0,0),(1,0),(0,1))") == "((0,0),(1,0),(0,1))"
    assert coerce_polygon_wire([(0, 0), (1, 0), (0, 1)]) == (
        "((0.0,0.0),(1.0,0.0),(0.0,1.0))"
    )
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_polygon_wire([(0, 0), (1, 1)])  # need ≥3
    assert normalize_sql_bind_value("0,0,1,0,0,1", "POLYGON") == (
        "((0,0),(1,0),(0,1))"
    )


def test_parse_sql_datetime_epoch_int_float():
    from connectors.sql_temporal import parse_sql_datetime

    # 2024-01-01T00:00:00Z
    dt = parse_sql_datetime(1704067200, aware_utc=True)
    assert dt == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # millis
    dt_ms = parse_sql_datetime(1704067200000, aware_utc=True)
    assert dt_ms == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert parse_sql_datetime(True) is None  # refuse bool invent


def test_airtable_notion_shopify_datetime_timestamptz():
    from connectors.airtable_writer import airtable_field_to_carrier
    from connectors.notion_writer import notion_property_to_carrier
    from connectors.saas_write_carriers import (
        shopify_core_field_carriers,
        shopify_metafield_type_to_carrier,
    )

    assert airtable_field_to_carrier({"type": "dateTime"}) == "TIMESTAMPTZ"
    assert notion_property_to_carrier("date") == "TIMESTAMPTZ"
    assert shopify_metafield_type_to_carrier("date_time") == "TIMESTAMPTZ"
    draft = shopify_core_field_carriers("draft_orders")
    assert draft["invoice_sent_at"] == "TIMESTAMPTZ"
    assert draft["completed_at"] == "TIMESTAMPTZ"
    # Stripe reverse-ETL still expects unix int carriers — do not invent TZ strings.
    from connectors.saas_write_carriers import stripe_field_carriers

    assert stripe_field_carriers("customers").get("created") == "INTEGER"
