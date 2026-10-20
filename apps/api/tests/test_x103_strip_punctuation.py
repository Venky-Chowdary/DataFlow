"""X-103: strip_characters 'punctuation' must also remove '_' (Unicode
connector punctuation Pc). Symbols/digits behavior unchanged.
"""

from __future__ import annotations

from services.shape_expr import _fn_strip_characters


def test_underscore_is_punctuation():
    assert _fn_strip_characters("snake_case_name!", "punctuation") == "snakecasename"


def test_digits_letters_space_kept():
    assert _fn_strip_characters("abc 123 _-x!", "punctuation") == "abc 123 x"


def test_symbols_still_removed():
    assert _fn_strip_characters("a$€+b", "punctuation") == "ab"
