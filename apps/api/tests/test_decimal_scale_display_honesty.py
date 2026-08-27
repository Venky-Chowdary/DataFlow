"""Trailing zeros after the decimal are display scale — not a bigger time."""

from __future__ import annotations

from services.decimal_observe import (
    dest_scale_padding_honesty,
    fractional_trailing_zeros_same_value,
)


def test_flights_dep_time_dest_padding_is_the_same_number():
    """Snowsight JURTY: 9.083333000000 vs source 9.083333."""
    assert fractional_trailing_zeros_same_value("9.083333", "9.083333000000")
    assert fractional_trailing_zeros_same_value("12.483334", "12.483334000000")
    assert fractional_trailing_zeros_same_value("8.95", "8.950000000000")
    assert fractional_trailing_zeros_same_value("12", "12.000000000000")


def test_zeros_before_the_decimal_would_change_the_value():
    assert not fractional_trailing_zeros_same_value("9.083333", "908333.3")
    assert not fractional_trailing_zeros_same_value("9.083333", "908333300000")


def test_honesty_copy_names_the_equal_pair():
    text = dest_scale_padding_honesty()
    assert "display scale" in text.lower()
    assert "did not increase" in text.lower()
    assert "9.083333" in text
