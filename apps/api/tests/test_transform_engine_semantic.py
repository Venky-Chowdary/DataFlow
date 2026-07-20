"""Tests for semantic transforms in the transform engine."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from decimal import Decimal

from services.transform_engine import apply_transform, infer_transform_for_mapping


def test_apply_transform_phone():
    # For generic string targets the phone transform preserves formatting.
    assert apply_transform("+1 (555) 123-4567", "phone") == ("+1 (555) 123-4567", None)
    assert apply_transform("555-123-4567", "phone") == ("555-123-4567", None)


def test_apply_transform_email():
    assert apply_transform("  User@Example.COM  ", "email") == ("user@example.com", None)


def test_apply_transform_url():
    value, err = apply_transform("https://Example.COM/Path", "url")
    assert err is None
    assert value.lower() == "https://example.com/path"


def test_apply_transform_iban():
    assert apply_transform("GB82 WEST 1234 5698 7654 32", "iban") == ("GB82WEST12345698765432", None)


def test_apply_transform_currency():
    assert apply_transform("$1,234.56", "currency") == ("1234.56", None)
    assert apply_transform("€100", "currency") == ("100", None)
    value, err = apply_transform("free", "currency")
    assert err is not None


def test_apply_transform_percentage():
    assert apply_transform("12.5%", "percentage") == ("12.5", None)


def test_apply_transform_postal():
    assert apply_transform("sw1a 1aa", "postal") == ("SW1A1AA", None)


def test_infer_transform_chooses_phone_for_string_target():
    assert infer_transform_for_mapping("phone_number", "phone_number", "VARCHAR", "VARCHAR") == "phone"


def test_infer_transform_preserves_currency_for_string_target():
    assert infer_transform_for_mapping("price", "price", "VARCHAR", "VARCHAR") == "none"


def test_infer_transform_parses_currency_for_decimal_target():
    assert infer_transform_for_mapping("price", "price", "VARCHAR", "DECIMAL") == "currency"


def test_infer_transform_parses_percentage_for_decimal_target():
    assert infer_transform_for_mapping("tax_rate", "tax_rate", "VARCHAR", "DECIMAL") == "percentage"
