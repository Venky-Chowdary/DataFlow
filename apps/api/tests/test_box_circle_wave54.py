"""Wave 54: PostgreSQL BOX / CIRCLE geometric bind (Fivetran geometric class)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_box_wire():
    from connectors.sql_bind import coerce_box_wire, normalize_sql_bind_value

    assert coerce_box_wire("((0,0),(1,1))") == "((0,0),(1,1))"
    assert coerce_box_wire({"x1": 0, "y1": 0, "x2": 2, "y2": 3}) == (
        "((0.0,0.0),(2.0,3.0))"
    )
    assert coerce_box_wire([(0, 0), (1, 1)]) == "((0.0,0.0),(1.0,1.0))"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_box_wire(1)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_box_wire("not-a-box")
    assert normalize_sql_bind_value("0,0,1,1", "BOX") == "((0,0),(1,1))"


def test_coerce_circle_wire():
    from connectors.sql_bind import coerce_circle_wire, normalize_sql_bind_value

    assert coerce_circle_wire("<(0,0),2>") == "<(0,0),2>"
    assert coerce_circle_wire({"x": 1, "y": 2, "r": 3}) == "<(1.0,2.0),3.0>"
    assert coerce_circle_wire([(0, 0), 1.5]) == "<(0.0,0.0),1.5>"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_circle_wire({"x": 0, "y": 0, "r": -1})
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_circle_wire(42)
    assert normalize_sql_bind_value("1,2,3", "CIRCLE") == "<(1,2),3>"
