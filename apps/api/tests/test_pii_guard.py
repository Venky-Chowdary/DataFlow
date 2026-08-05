"""PII/PHI detection, masking, and hash transforms."""
from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.pii_guard import (  # noqa: E402
    classify_columns,
    detect_pii,
    hash_token,
    is_sensitive_name,
    mask,
    mask_record,
    redact_reconciliation,
)
from services.transform_engine import apply_transform  # noqa: E402


def test_is_sensitive_name():
    assert is_sensitive_name("email")
    assert is_sensitive_name("patient_ssn")
    assert is_sensitive_name("credit_card")
    assert not is_sensitive_name("quantity")


def test_detect_pii_email():
    result = detect_pii("contact me at alice@example.com today")
    assert result["has_pii"]
    assert "email" in result["findings"]


def test_detect_pii_ssn():
    result = detect_pii("SSN 123-45-6789")
    assert result["has_pii"]
    assert "ssn" in result["findings"]


def test_detect_pii_phone():
    result = detect_pii("call +1-555-123-4567")
    assert result["has_pii"]
    assert "phone" in result["findings"]


def test_detect_pii_credit_card():
    result = detect_pii("card 4111-1111-1111-1111")
    assert result["has_pii"]
    assert "credit_card" in result["findings"]


def test_mask_short_and_long():
    assert mask("1234") == "****"
    masked = mask("alice@example.com")
    assert masked.startswith("a")
    assert "*" in masked
    assert "@" in masked
    # Org domain must not leak — only the TLD may remain for shape.
    assert "example" not in masked
    assert "alice" not in masked


def test_mask_preserves_json_array_with_embedded_email():
    """Regression: notifications/referrals previews must not collapse to one email."""
    import json

    raw = json.dumps(
        [
            {
                "_id": "69584f2a",
                "data": {
                    "referredEmail": "gayathriprasadkadiyala@gmail.com",
                    "referredUserId": "69584f27",
                },
                "type": "REFERRAL_INVITE_USED_INVITER",
            }
        ]
    )
    masked = mask(raw)
    assert masked.lstrip().startswith("[")
    assert "referredEmail" in masked
    assert "REFERRAL_INVITE_USED_INVITER" in masked
    assert "gayathriprasadkadiyala" not in masked
    assert "@" in masked
    # Still parseable JSON after in-place redaction.
    parsed = json.loads(masked)
    assert isinstance(parsed, list) and parsed[0]["type"] == "REFERRAL_INVITE_USED_INVITER"


def test_mask_embedded_email_keeps_surrounding_prose():
    masked = mask("contact me at alice@example.com today")
    assert masked.startswith("contact me at ")
    assert masked.endswith(" today")
    assert "alice" not in masked
    assert "example" not in masked


def test_mask_record():
    record = {"email": "a@b.com", "amount": "100"}
    masked = mask_record(record, {"email"})
    assert masked["email"].startswith("a")
    assert "*" in masked["email"]
    assert masked["amount"] == "100"


def test_hash_token_deterministic():
    assert hash_token("secret") == hash_token("secret")
    assert hash_token("secret") != hash_token("other")


def test_classify_columns():
    assert classify_columns(["email", "quantity"]) == {"email": "sensitive", "quantity": "low"}


def test_apply_transform_mask_pii():
    val, err = apply_transform("alice@example.com", "mask_pii")
    assert err is None
    assert "*" in val
    assert "example.com" not in val
    assert "alice" not in val


def test_apply_transform_hash_pii(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "unit-test-pii-key")
    val, err = apply_transform("alice@example.com", "hash_pii")
    assert err is None
    assert len(val) == 32
    assert val == apply_transform("alice@example.com", "hash_pii")[0]


def test_apply_transform_hash_pii_refuses_insecure_default(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PII_HASH_KEY", raising=False)
    monkeypatch.delenv("DATAFLOW_SECRET", raising=False)
    val, err = apply_transform("alice@example.com", "hash_pii")
    assert val is None or val == ""
    assert err and "DATAFLOW_PII_HASH_KEY" in err


def test_redact_reconciliation_masks_mismatch_values():
    recon = {
        "passed": False,
        "mismatches": [
            {
                "row": "1",
                "source": "email",
                "target": "email",
                "source_value": "alice@example.com",
                "target_value": "alice@example.com",
            },
            {
                "row": "2",
                "source": "amount",
                "target": "amount",
                "source_value": "100.50",
                "target_value": "100.5",
            },
        ],
    }
    redacted = redact_reconciliation(recon, [])
    src = redacted["mismatches"][0]["source_value"]
    assert src.startswith("a")
    assert "*" in src
    assert "alice" not in src
    assert "example" not in src
    assert redacted["mismatches"][1]["source_value"] == "100.50"


def test_redact_reconciliation_masks_sample_compare_mismatches():
    recon = {
        "passed": False,
        "sample_compare": {
            "passed": False,
            "compared": 1,
            "mismatches": [
                {
                    "source": "email",
                    "target": "email",
                    "source_value": "bob@example.com",
                    "target_value": "bob@example.com",
                }
            ],
        },
    }
    redacted = redact_reconciliation(recon, [])
    src = redacted["sample_compare"]["mismatches"][0]["source_value"]
    tgt = redacted["sample_compare"]["mismatches"][0]["target_value"]
    assert "*" in src and "*" in tgt
    assert "bob" not in src and "example" not in src
