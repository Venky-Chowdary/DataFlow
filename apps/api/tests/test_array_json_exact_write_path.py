"""ARRAY JSON numbers use json_loads_exact, not stdlib float().

json.loads('[0.1]') invented a binary64 0.1 before element fit.
Long fractions and dest-canonical 0.1 stay Decimal identity.
Integers and IEEE-exact 1.5 stay native.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.array_wire import parse_array_wire_elements  # noqa: E402
from connectors.writer_common import array_element_unfit_reason  # noqa: E402


def test_plain_integer_json_array_still_binds():
    elements, err = parse_array_wire_elements("[1,2,3]")
    assert err is None
    assert elements == [1, 2, 3]
    assert parse_array_wire_elements("[]") == ([], None)


def test_ieee_exact_fraction_stays_float():
    elements, err = parse_array_wire_elements("[1.5, 2.0]")
    assert err is None
    assert elements == [1.5, 2.0]


def test_long_json_fraction_stays_decimal_identity():
    import json

    raw = "[1.234567890123456789]"
    collapsed = json.loads(raw)[0]
    elements, err = parse_array_wire_elements(raw)
    assert err is None
    assert elements is not None
    assert elements[0] == Decimal("1.234567890123456789")
    assert collapsed != elements[0]
    assert array_element_unfit_reason(elements[0], "DECIMAL(38,18)") is None


def test_coerce_array_wire_keeps_long_fraction():
    from connectors.sql_bind import coerce_array_wire

    got = coerce_array_wire("[1.234567890123456789]", engine="postgresql")
    assert got == [Decimal("1.234567890123456789")]
    assert coerce_array_wire("[1,2,3]", engine="postgresql") == [1, 2, 3]


def test_locale_and_auto_string_elements_unchanged():
    elements, err = parse_array_wire_elements('["$1.50", "1.234"]')
    assert err is None
    assert elements == ["$1.50", "1.234"]
    assert array_element_unfit_reason("$1.50", "DECIMAL(10,2)") is None
    assert array_element_unfit_reason("1.234", "DECIMAL(10,3)")
