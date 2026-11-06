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


# --- second review: every network that issues Luhn PANs ----------------------

import random as _random  # noqa: E402

_NETWORKS = {
    "diners_36": ("36", 14), "diners_300": ("300", 14), "diners_305": ("305", 14),
    "diners_38": ("38", 14), "diners_39": ("39", 14),
    "maestro_6759": ("6759", 16), "maestro_50": ("50", 16), "maestro_56": ("56", 16),
    "maestro_58": ("58", 16), "maestro_63": ("63", 16), "maestro_67": ("67", 19),
    "mir_2200": ("2200", 16), "mir_2204": ("2204", 16),
    "rupay_60": ("60", 16), "rupay_6521": ("6521", 16), "rupay_508": ("508", 16),
    "unionpay_81": ("81", 16),
}


def _pan(rng, prefix: str, length: int) -> int:
    body = prefix + "".join(str(rng.randint(0, 9)) for _ in range(length - len(prefix) - 1))
    for check in "0123456789":
        if _luhn(body + check):
            return int(body + check)
    raise AssertionError(body)


@pytest.mark.parametrize("network", sorted(_NETWORKS))
def test_twenty_pans_of_every_network_flag_a_neutral_bigint_column(network):
    rng = _random.Random(network)
    prefix, length = _NETWORKS[network]
    values = [_pan(rng, prefix, length) for _ in range(20)]
    assert "pci_pan" in _risk("value", values, "BIGINT").get("value", []), network


def test_random_sixteen_digit_values_stay_below_the_threshold():
    from services.compliance_guard import _card_iin

    rng = _random.Random(16)
    values = [rng.randint(10**15, 10**16 - 1) for _ in range(2000)]
    share = sum(1 for v in values if _luhn(str(v)) and _card_iin(str(v))) / len(values)
    assert share < NUMERIC_PAN_SHARE / 4, share
    assert _risk("value", values[:200], "BIGINT") == {}


# --- second review: a minimum sample before the share rule applies ----------


@pytest.mark.parametrize("values", [
    [4111111111111111],                      # one row
    [4111111111111111, None, None],          # one hit among blanks
    [5555555555554444, 4111111111111111, None, 6011111111111117],  # 3 hits
])
def test_too_few_card_hits_are_judged_by_the_name_alone(values):
    from services.compliance_guard import NUMERIC_PAN_MIN_HITS

    hits = [v for v in values if v is not None]
    assert len(hits) < NUMERIC_PAN_MIN_HITS
    assert _risk("id", values, "BIGINT") == {}
    assert "pci_pan" in _risk("card_number", values, "BIGINT")["card_number"]


def test_a_one_message_kafka_sample_with_an_int_key_is_not_a_card():
    # A Kafka sample of one message: the key arrives as a native int cell and
    # no declared type; one Luhn-valid key is not a card column.
    risk = detect_pii_fields(["order_key", "amount"],
                             [{"order_key": 4111111111111111, "amount": 12}], column_types={})
    assert risk.get("field_risk", {}) == {}


def test_five_card_hits_meet_the_minimum():
    assert "pci_pan" in _risk("value", _PANS[:5], "BIGINT")["value"]


# --- review 3: masked / encrypted / tokenized PAN names (their only guard) ----

_CARD_NAMES = ["masked_pan", "encrypted_pan", "pan_encrypted", "customer_pan", "stored_pan",
               "tokenized_pan", "pan_number_enc", "pan_values", "pan", "card_pan", "pan_value",
               "pan_masked", "pan_number", "customer_card_pan", "pan_hash"]
_NOT_CARD_NAMES = ["pan_id", "pan_ids", "pan_african", "pan_region", "pan_india", "pan_tilt",
                   "pan_x", "pan_tilt_speed", "japan", "span", "company", "panel", "expand",
                   "panama"]


@pytest.mark.parametrize("col", _CARD_NAMES)
def test_a_pan_token_with_any_prefix_or_suffix_is_a_card_column(col):
    # A masked / encrypted value is not Luhn-valid: only the name can flag it.
    risk = detect_pii_fields([col], [{col: "enc:9f2c…"}]).get("field_risk", {})
    assert "pci_pan" in risk.get(col, []), col


@pytest.mark.parametrize("col", _NOT_CARD_NAMES)
def test_known_non_card_pan_words_are_not_card_columns(col):
    risk = detect_pii_fields([col], [{col: "x"}]).get("field_risk", {})
    assert "pci_pan" not in risk.get(col, []), col


@pytest.mark.parametrize("prefix,length", [("9792", 16), ("1", 15)])
def test_troy_and_uatp_pans_flag_a_neutral_bigint_column(prefix, length):
    rng = _random.Random(prefix)
    values = [_pan(rng, prefix, length) for _ in range(20)]
    assert "pci_pan" in _risk("value", values, "BIGINT").get("value", []), prefix


def test_a_sixteen_digit_number_starting_with_1_is_not_uatp():
    from services.compliance_guard import _card_iin

    assert _card_iin("1" + "0" * 14) is True        # 15 digits: UATP
    assert _card_iin("1" + "0" * 15) is False       # 16 digits: not a network
