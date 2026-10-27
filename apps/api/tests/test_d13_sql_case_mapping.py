"""D13 — upper/lower follow SQL UPPER/LOWER: one character in, one out.

Python's full Unicode case mapping expands ß -> SS, ﬁ -> FI and İ -> i + U+0307.
PostgreSQL, MySQL, SQL Server and Oracle UPPER/LOWER use the simple
(one-character) mapping: ß and ﬁ stay, İ lowers to i. The expansion also
lengthened a value past the VARCHAR(n) its source fitted. The transform and
the shape step agree.
"""

from __future__ import annotations

import pytest

from services.transform_engine import apply_transform


@pytest.mark.parametrize(
    ("value", "upper"),
    [
        ("straße", "STRAßE"),
        ("ﬁle", "ﬁLE"),
        ("äöü", "ÄÖÜ"),
        ("  Alice  ", "  ALICE  "),
    ],
)
def test_upper_is_one_to_one(value, upper):
    out, err = apply_transform(value, "upper")
    assert err is None
    assert out == upper
    assert len(out) == len(value)


@pytest.mark.parametrize(("value", "lower"), [("İstanbul", "istanbul"), ("ÄRGER", "ärger")])
def test_lower_is_one_to_one(value, lower):
    out, err = apply_transform(value, "lower")
    assert err is None
    assert out == lower
    assert len(out) == len(value)


def test_shape_case_step_matches_the_transform():
    from services.shape_engine import _transform_value

    for mode, value in (("upper", "straße"), ("lower", "İstanbul")):
        step = type("S", (), {"op": "case", "options": {"mode": mode}})()
        assert _transform_value(step, value) == apply_transform(value, mode)[0]
