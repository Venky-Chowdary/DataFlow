"""Decimal scale survives the delimited/excel wire.

C26's ``to_json_value`` demote turns ``Decimal("10.50")`` into ``10.5`` — right
for JSON (binary64 has no scale), wrong for a delimited file where the file is
the deliverable and the stated digits are the value. ``to_delimited_value``
already kept textual numbers; native Decimal cells get the same guard.
"""

from __future__ import annotations

from decimal import Decimal

from connectors.writer_common import to_delimited_value, to_json_value
from services.value_serializer import cell_to_string


def test_delimited_keeps_decimal_stated_scale():
    out = to_delimited_value(Decimal("10.50"), "amount", {"amount": "decimal"})
    assert cell_to_string(out) == "10.50"


def test_delimited_keeps_decimal_inexact_fraction():
    out = to_delimited_value(Decimal("0.33333334"), "r", {"r": "float"})
    assert cell_to_string(out) == "0.33333334"


def test_delimited_keeps_textual_number_digits():
    out = to_delimited_value("10.50", "amount", {"amount": "decimal"})
    assert out == "10.50"


def test_json_wire_still_emits_number_for_exact_decimal():
    # The demote stays correct where it belongs: JSON/kafka payloads carry a
    # JSON number, not a quoted decimal.
    out = to_json_value(Decimal("10.50"), "amount", {"amount": "decimal"})
    assert out == 10.5 and isinstance(out, float)
