"""Locale-aware numeric parsing tests for currency, decimal, and integer transforms."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transform_engine import apply_transform, infer_date_locale  # noqa: E402


@pytest.mark.parametrize(
    "raw,transform,expected",
    [
        # US and EU standard formats
        ("1,234.50", "decimal", "1234.50"),
        ("1.234,56", "decimal", "1234.56"),
        ("1,234,567.89", "decimal", "1234567.89"),
        ("1.234.567,89", "decimal", "1234567.89"),
        ("1,234,567", "decimal", "1234567"),
        ("1.234.567", "decimal", "1234567"),
        ("1,234,567,89", "decimal", "1234567.89"),
        ("1.234.567.89", "decimal", "1234567.89"),
        ("1,234,567,890", "decimal", "1234567890"),
        ("1.234.567.890", "decimal", "1234567890"),
        ("1 000 000.89", "decimal", "1000000.89"),
        ("1 000 000,89", "decimal", "1000000.89"),
        ("1 000 000", "decimal", "1000000"),
        ("12,34", "decimal", "12.34"),
        ("12.34", "decimal", "12.34"),
        # Lone 3-digit group is US-thousands XOR EU-decimal — Auto refuses.
        ("1.234", "decimal", None),
        ("1,234", "decimal", None),
        ("$1,000.00", "decimal", "1000.00"),
        ("€1.000,00", "decimal", "1000.00"),
        ("USD 1 000 000.89", "decimal", "1000000.89"),
        ("1 000 000 USD", "decimal", "1000000"),
        # Integers
        ("1,000,000", "integer", 1000000),
        ("1.000.000", "integer", 1000000),
        ("1 000 000", "integer", 1000000),
        ("1,234,567", "integer", 1234567),
        ("1.234.567", "integer", 1234567),
        ("1 234 567", "integer", 1234567),
        ("1,000.00", "integer", 1000),
        ("1.000,00", "integer", 1000),
        # Ambiguous mixed separators and other invalid values
        ("1,234,56", "decimal", "1234.56"),
        ("1.234.56", "decimal", "1234.56"),
        ("12,34,56", "decimal", None),
        ("12.34.56", "decimal", None),
        ("1,234,567.89", "integer", None),
        ("1.234.567,89", "integer", None),
        ("not_a_number", "decimal", None),
    ],
)
def test_locale_numeric_parsing(raw, transform, expected):
    value, err = apply_transform(raw, transform)
    if expected is None:
        assert err is not None, f"Expected {raw!r} to fail for {transform}"
    else:
        assert err is None, f"Expected {raw!r} to parse for {transform}: {err}"
        if transform == "integer":
            assert value == expected
        else:
            assert str(value) == str(expected)


def test_infer_date_locale_detects_dmy_from_unambiguous_day():
    rows = [{"d": "31/12/2024"}, {"d": "5/8/1967"}, {"d": "7/9/1982"}]
    assert infer_date_locale(rows, ["d"]) == "DMY"


def test_infer_date_locale_detects_mdy_from_unambiguous_month():
    rows = [{"d": "12/31/2024"}, {"d": "5/8/1967"}, {"d": "7/9/1982"}]
    assert infer_date_locale(rows, ["d"]) == "MDY"


def test_infer_date_locale_respects_explicit_locale():
    rows = [{"d": "12/31/2024"}]
    assert infer_date_locale(rows, ["d"], existing_locale="DMY") == "DMY"


def test_infer_date_locale_returns_empty_when_all_ambiguous():
    rows = [{"d": "5/8/1967"}, {"d": "7/9/1982"}, {"d": "11/2/1980"}]
    assert infer_date_locale(rows, ["d"]) == ""
