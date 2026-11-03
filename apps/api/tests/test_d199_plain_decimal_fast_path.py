"""D199: plain ASCII decimals skip the locale resolver and parse identically.

The 800k cancel/resume run spent most of its CPU in ``_parse_decimal``'s
currency / NFKC / separator resolution for cells like ``12.50`` and ``400000``,
called per row by cursor compares, row filters and the verification ladder.
"""
from __future__ import annotations

import random
from decimal import Decimal

import pytest

import services.transform_engine as te
from services.value_serializer import safe_decimal_text


def _resolver(text: str) -> str | None:
    implied = te._implied_number_locale_from_currency(text)
    norm = te._normalize_locale_separators(te._normalize_numeric_text(text), implied)
    if not norm:
        return None
    return safe_decimal_text(Decimal(norm))


@pytest.mark.parametrize("text", ["0", "-0", "+5", "007", "400000", "12.50", "-1.5", "1.2345", "0.0001", "9" * 60])
def test_plain_decimal_does_not_reach_the_locale_resolver(monkeypatch, text):
    def boom(*_a, **_k):
        raise AssertionError("plain decimal went through the locale resolver")

    monkeypatch.setattr(te, "_normalize_locale_separators", boom)
    monkeypatch.setattr(te, "_implied_number_locale_from_currency", boom)
    assert te.decimal_wire_value(text) == Decimal(text)


@pytest.mark.parametrize("text", ["1.234", "-12.345", "1,234", "$1,000.00", "1.234,50", "12.5x"])
def test_ambiguous_or_marked_text_keeps_the_resolver(text):
    assert te._parse_decimal(text) == _resolver(text) if text != "12.5x" else te._parse_decimal(text) is None


def test_fast_path_matches_the_resolver_on_a_random_corpus():
    rng = random.Random(199)
    checked = 0
    for _ in range(20000):
        s = rng.choice(["", "-", "+"]) + str(rng.randint(0, 10 ** rng.randint(1, 40))).zfill(rng.randint(1, 3))
        if rng.random() < 0.6:
            s += "." + str(rng.randint(0, 10 ** rng.randint(1, 10))).zfill(rng.randint(1, 7))
        if not te._PLAIN_ASCII_DECIMAL_RE.match(s):
            continue
        checked += 1
        assert te._parse_decimal(s) == _resolver(s), s
    assert checked > 15000
