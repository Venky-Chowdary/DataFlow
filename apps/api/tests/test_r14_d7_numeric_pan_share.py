"""QA MySQL D7 review: real card numbers in numeric columns are still PCI.

04f5423fd stopped integer ids (one in ten passes Luhn) reading as cards by
switching numeric columns to the name rule alone, which silently missed real
PANs stored as BIGINT / DECIMAL under neutral names. A numeric column is now
judged on its share of card numbers (13-19 digits, Luhn, a network IIN):
>= NUMERIC_PAN_SHARE (0.8) flags it; the name rule stays an extra trigger.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.compliance_guard import NUMERIC_PAN_SHARE, detect_pii_fields

# The networks' published test PANs: Visa, Mastercard, Amex, Discover, JCB, UnionPay.
_PANS = [4111111111111111, 5555555555554444, 378282246310005, 6011111111111117,
         3530111333300000, 6200000000000005, 2221000000000009]
_IDS = [100000000007 * k for k in range(1, 201)]


def _risk(col, values, declared):
    return detect_pii_fields([col], [{col: v} for v in values],
                             column_types={col: declared}).get("field_risk", {})


@pytest.mark.parametrize("declared,cast", [
    ("BIGINT", int), ("DECIMAL(19,0)", Decimal), ("VARCHAR(20)", str), ("NUMERIC(19,2)", None),
])
@pytest.mark.parametrize("col", ["value", "ref", "account_ref", "amount_key"])
def test_real_pan_columns_are_flagged_under_neutral_names(col, declared, cast):
    values = ([Decimal(p).quantize(Decimal("0.01")) for p in _PANS] if cast is None
              else [cast(p) for p in _PANS]) * 3
    assert "pci_pan" in _risk(col, values, declared).get(col, []), (col, declared)


@pytest.mark.parametrize("col", ["cc_number", "payment_card", "card_pan", "pan_value", "card"])
def test_card_named_bigint_columns_are_flagged(col):
    assert "pci_pan" in _risk(col, _PANS * 2, "BIGINT").get(col, [])


def test_card_name_alone_flags_even_without_card_values():
    for col in ("cc_number", "payment_card", "card_pan", "pan_value"):
        assert "pci_pan" in _risk(col, [1, 2, 3], "BIGINT").get(col, []), col


@pytest.mark.parametrize("declared", ["BIGINT", "DECIMAL(20,0)"])
def test_qa_integer_ids_are_not_cards(declared):
    luhn_share = sum(1 for v in _IDS if len(str(v)) >= 13 and _luhn(str(v))) / len(_IDS)
    assert luhn_share < NUMERIC_PAN_SHARE
    assert _risk("id", _IDS[:100], declared) == {}
    assert _risk("id", [Decimal(v) for v in _IDS[100:]], declared) == {}


def test_a_mixed_column_below_the_threshold_is_not_flagged():
    values = _PANS * 2 + _IDS[:10]  # 14 / 24 = 0.58
    assert len(_PANS * 2) / len(values) < NUMERIC_PAN_SHARE
    assert _risk("ref", values, "BIGINT") == {}
    at_threshold = _PANS * 4 + _IDS[:7]  # 28 / 35 = 0.8
    assert "pci_pan" in _risk("ref", at_threshold, "BIGINT")["ref"]


def test_neutral_words_containing_pan_are_not_card_names():
    risk = detect_pii_fields(["span", "company", "panel", "japan", "expand"],
                             [{"span": 1, "company": "x", "panel": 2, "japan": 3, "expand": 4}])
    assert risk.get("field_risk", {}) == {}


def _luhn(d: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(d)):
        n = int(ch) * (2 if i % 2 else 1)
        total += n - 9 if n > 9 else n
    return total % 10 == 0
