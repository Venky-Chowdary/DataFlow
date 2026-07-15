"""PII/PHI detection, masking, and hash transforms."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

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
    assert mask("alice@example.com") == "al***om"


def test_mask_record():
    record = {"email": "a@b.com", "amount": "100"}
    masked = mask_record(record, {"email"})
    assert masked["email"].startswith("a")
    assert "***" in masked["email"]
    assert masked["amount"] == "100"


def test_hash_token_deterministic():
    assert hash_token("secret") == hash_token("secret")
    assert hash_token("secret") != hash_token("other")


def test_classify_columns():
    assert classify_columns(["email", "quantity"]) == {"email": "sensitive", "quantity": "low"}


def test_apply_transform_mask_pii():
    val, err = apply_transform("alice@example.com", "mask_pii")
    assert err is None
    assert "***" in val
    assert "example.com" not in val


def test_apply_transform_hash_pii():
    val, err = apply_transform("alice@example.com", "hash_pii")
    assert err is None
    assert len(val) == 32
    assert val == apply_transform("alice@example.com", "hash_pii")[0]
