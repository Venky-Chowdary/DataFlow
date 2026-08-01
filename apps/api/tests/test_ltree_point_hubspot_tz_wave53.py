"""Wave 53: LTREE/TSVECTOR/POINT bind + HubSpot datetime→TIMESTAMPTZ."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_ltree_labels():
    from connectors.sql_bind import coerce_ltree_wire, normalize_sql_bind_value

    assert coerce_ltree_wire("Top.Science.Astronomy.Stars") == (
        "Top.Science.Astronomy.Stars"
    )
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_ltree_wire("bad label")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_ltree_wire(42)
    assert normalize_sql_bind_value("a.b_c-d", "LTREE") == "a.b_c-d"


def test_coerce_tsvector_and_point():
    from connectors.sql_bind import (
        coerce_point_wire,
        coerce_tsvector_wire,
        normalize_sql_bind_value,
    )

    assert coerce_tsvector_wire("fat cat") == "fat cat"
    assert coerce_tsvector_wire(["fat", "cat"]) == "fat cat"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_tsvector_wire({"a": 1})

    assert coerce_point_wire("(1.5,-2)") == "(1.5,-2)"
    assert coerce_point_wire({"x": 1, "y": 2}) == "(1.0,2.0)"
    assert coerce_point_wire([3, 4]) == "(3.0,4.0)"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_point_wire(1)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_point_wire([1, 2, 3])
    assert normalize_sql_bind_value("(0,0)", "POINT") == "(0,0)"


def test_hubspot_datetime_is_timestamptz():
    from connectors.hubspot_writer import hubspot_property_to_carrier

    assert hubspot_property_to_carrier({"type": "datetime"}) == "TIMESTAMPTZ"
    assert hubspot_property_to_carrier({"type": "date"}) == "DATE"
