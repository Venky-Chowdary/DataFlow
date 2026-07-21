"""Unit tests for exact-value serialization in value_serializer."""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.value_serializer import (  # noqa: E402
    cell_to_string,
    json_default,
    sanitize_json_value,
)


def test_decimal_json_default_is_exact_string():
    """Decimal values must not be silently rounded to float."""
    assert json_default(Decimal("123456789012345678901.23")) == "123456789012345678901.23"
    assert json_default(Decimal("0.10")) == "0.10"
    # Scientific notation is expanded to fixed-point so no precision is lost.
    assert json_default(Decimal("1E-20")) == "0.00000000000000000001"


def test_extreme_exponent_stays_scientific_not_overflow():
    """Never expand 1e+1000000 into a million-char string (hangs / Overflow mid-job)."""
    from services.value_serializer import safe_decimal_text

    huge = Decimal("1E+1000000")
    text = safe_decimal_text(huge)
    assert text is not None
    assert "e" in text.lower()
    assert len(text) < 32
    assert json_default(huge) == text
    assert cell_to_string(huge) == text


def test_decimal_overflow_exception_serializes_safely():
    from decimal import Overflow

    from services.error_handling import format_exception_message, humanize_transfer_failure

    msg = format_exception_message(Overflow())
    assert "decimal.Overflow" in msg
    assert "[<class" not in msg
    human = humanize_transfer_failure(Overflow())
    assert human["code"] == "decimal_overflow"
    assert "[<class" not in human["message"]


def test_parse_decimal_extreme_scientific_is_bounded():
    from services.transform_engine import _parse_decimal, apply_transform

    parsed = _parse_decimal("1e1000000")
    assert parsed is not None
    assert len(parsed) < 32
    assert "e" in parsed.lower()
    val, err = apply_transform("1e1000000", "decimal")
    assert err is None
    assert val is not None
    assert len(str(val)) < 32


def test_parse_integer_rejects_unbounded_magnitude():
    from services.transform_engine import _parse_integer

    assert _parse_integer("1e100000") is None
    assert _parse_integer("1e100") is None
    assert _parse_integer("42") == 42
    assert _parse_integer("1000000") == 1000000


def test_decimal_sanitize_json_value_is_exact_string():
    assert sanitize_json_value(Decimal("123456789012345678901.23")) == "123456789012345678901.23"
    assert sanitize_json_value(Decimal("0.10")) == "0.10"


def test_cell_to_string_preserves_decimal_in_nested_json():
    payload = {"amount": Decimal("1000.00"), "precision": Decimal("0.0000000000001")}
    text = cell_to_string(payload)
    parsed = json.loads(text)
    assert parsed["amount"] == "1000.00"
    assert parsed["precision"] == "0.0000000000001"


def test_cell_to_string_decimal_scalar_is_text():
    assert cell_to_string(Decimal("1000.00")) == "1000.00"
    assert cell_to_string(Decimal("-999999999999999999.999999")) == "-999999999999999999.999999"
