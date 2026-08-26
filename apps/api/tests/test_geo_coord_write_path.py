"""POINT/BOX/CIRCLE coords use write-path IEEE, not float(token).

float() invented Auto 1.234 and collapsed 2**53+1 onto 2**53.
Locale money the write path binds still lands. Text (1.5,-2) stays identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import (  # noqa: E402
    coerce_circle_wire,
    coerce_point_wire,
    geo_coord_carrier,
)


def test_plain_and_text_point_still_bind():
    assert coerce_point_wire("(1.5,-2)") == "(1.5,-2)"
    assert coerce_point_wire({"x": 1, "y": 2}) == "(1.0,2.0)"


def test_locale_money_coord_binds():
    assert geo_coord_carrier("$1.50") == pytest.approx(1.5)
    assert coerce_point_wire({"x": "$1.50", "y": 0}) == "(1.5,0.0)"


def test_auto_grouping_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        geo_coord_carrier("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_point_wire({"x": "1,234", "y": 0})


def test_ieee_lossy_mantissa_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        geo_coord_carrier("9007199254740993")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_circle_wire({"x": 0, "y": 0, "r": "9007199254740993"})


def test_bool_is_not_a_magnitude():
    with pytest.raises(ValueError, match="refuse invent"):
        geo_coord_carrier(True)
