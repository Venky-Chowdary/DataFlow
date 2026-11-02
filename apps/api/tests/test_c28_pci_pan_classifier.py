"""C28 (D94/D8): PCI PAN is its own class — Luhn-valid 13-19 digits.

Before the fix the classifiers had no PCI class: a bare ``\\d{8,19}`` run was
called "account" (so every numeric ID false-fired), dashed/spaced real card
numbers were missed entirely, and there was no Luhn gate.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.compliance_guard import detect_pii_fields, score_compliance_risk
from services.pii_guard import luhn_valid, mask, pii_findings

# Well-known test PANs (public PCI test numbers, not real cards).
VISA = "4111111111111111"
VISA_DASHED = "4111-1111-1111-1111"
VISA_SPACED = "4111 1111 1111 1111"
AMEX_15 = "378282246310005"
MASTERCARD = "5555555555554444"
NOT_LUHN_16 = "4111111111111112"
ORDER_ID_12 = "123456789012"
IBAN = "CZ6508000000192000145399"


def _luhn_make(base: str) -> str:
    """Append the Luhn check digit to a digit base."""
    total = 0
    for i, ch in enumerate(reversed(base)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return base + str((10 - total % 10) % 10)


def test_luhn_helper_accepts_real_pans_and_rejects_lookalikes() -> None:
    assert luhn_valid(VISA)
    assert luhn_valid(VISA_DASHED)
    assert luhn_valid(AMEX_15)
    assert luhn_valid(_luhn_make("630490017740292441"))  # 19 digits
    assert not luhn_valid(NOT_LUHN_16)
    assert not luhn_valid(ORDER_ID_12)  # too short
    assert not luhn_valid("12345678901234567890123456789")  # too long


def test_pii_findings_labels_pan_as_pci_pan() -> None:
    assert pii_findings(VISA)["pci_pan"] == 1
    assert pii_findings(VISA_DASHED)["pci_pan"] == 1
    assert pii_findings(VISA_SPACED)["pci_pan"] == 1
    assert pii_findings(AMEX_15)["pci_pan"] == 1
    # A PAN is not additionally a phone — PCI owns the precedence.
    assert "phone" not in pii_findings(VISA)
    # Non-Luhn digit runs are not PCI.
    assert "pci_pan" not in pii_findings(NOT_LUHN_16)
    assert "pci_pan" not in pii_findings(ORDER_ID_12)


def test_pii_findings_embedded_pan() -> None:
    assert pii_findings(f"pay with {VISA_DASHED} now")["pci_pan"] == 1


def test_mask_redacts_pan_but_other_digits_are_not_pii() -> None:
    masked = mask(f"card {VISA_DASHED} end")
    assert VISA_DASHED not in masked
    # 12 digits is outside the PAN range — phone-shaped, but never PCI.
    from services.pii_guard import detect_pii

    assert "pci_pan" not in detect_pii("order 123456789012")["findings"]


def test_compliance_value_hit_is_pci_pan_own_class() -> None:
    report = detect_pii_fields(
        ["note", "ref"],
        [{"note": VISA_SPACED, "ref": "7"}, {"note": AMEX_15, "ref": "8"}],
    )
    assert "note" in report["sensitive_fields"]
    assert "pci_pan" in report["field_risk"]["note"]
    assert "account" not in report["field_risk"]["note"]
    assert "phone" not in report["field_risk"]["note"]
    assert "note" in report["high_risk_fields"]


def test_compliance_name_hit_is_pci_pan() -> None:
    report = detect_pii_fields(["order_ref", "card_number"], [])
    assert "pci_pan" in report["field_risk"]["card_number"]
    assert "card_number" in report["high_risk_fields"]


def test_bare_digits_on_neutral_column_are_not_account() -> None:
    report = detect_pii_fields(["order_seq"], [{"order_seq": ORDER_ID_12}])
    risk = report["field_risk"].get("order_seq", [])
    assert "account" not in risk
    assert "pci_pan" not in risk


def test_iban_still_account_class() -> None:
    report = detect_pii_fields(["wire"], [{"wire": IBAN}])
    assert "account" in report["field_risk"]["wire"]


def test_score_tags_pci_dss_on_pan() -> None:
    report = score_compliance_risk(["charge_ref"], [{"charge_ref": VISA}])
    assert "PCI-DSS" in report["compliance_tags"]
    assert report["risk_level"] == "high"


def test_luhn_valid_rejects_non_card_charset() -> None:
    """X-MG1: hex/UUID/ObjectId values contain 13-19 extractable digits but are
    not card numbers — PAN detection must require the digit+separator shape."""
    from services.pii_guard import luhn_valid

    assert not luhn_valid("68f1c2a4b3d5e6f7a8b9c0d1")   # ObjectId-shaped hex
    assert not luhn_valid("507f1f77bcf86cd799439011")   # ObjectId-shaped hex
    assert not luhn_valid("550e8400-e29b-41d4-a716-446655440000")  # UUID
    assert not luhn_valid("deadbeefcafe411111111111")   # hex with digits
    assert luhn_valid(VISA)                              # real card still flags
    assert luhn_valid(VISA.replace(" ", "-"))            # dash separators ok
    assert not luhn_valid("") and not luhn_valid(None)


def test_objectid_values_do_not_flag_pii() -> None:
    report = detect_pii_fields(
        ["_id"],
        [{"_id": v} for v in [
            "68f1c2a4b3d5e6f7a8b9c0d1",
            "6523ab91cd82ef04bb23aa77",
            "507f1f77bcf86cd799439011",
        ]],
    )
    assert "pci_pan" not in report["field_risk"].get("_id", [])
    assert "_id" not in report["high_risk_fields"]
