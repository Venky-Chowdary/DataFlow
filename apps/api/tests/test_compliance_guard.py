"""Tests for deterministic PII and compliance safety scoring."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.compliance_guard import (  # noqa: E402
    detect_pii_fields,
    score_compliance_risk,
)


def test_detects_pii_columns_from_names_and_values() -> None:
    columns = ["email", "phone_number", "customer_name", "amount"]
    rows = [
        {
            "email": "alice@example.com",
            "phone_number": "555-123-4567",
            "customer_name": "Alice Johnson",
            "amount": "100.00",
        }
    ]

    report = detect_pii_fields(columns, rows)
    assert "email" in report["sensitive_fields"]
    assert "phone_number" in report["sensitive_fields"]
    assert "customer_name" in report["sensitive_fields"]
    assert report["risk_level"] in {"medium", "high"}


def test_compliance_score_flags_high_risk_for_identity_data() -> None:
    columns = ["email", "ssn", "dob", "amount"]
    rows = [
        {
            "email": "alice@example.com",
            "ssn": "123-45-6789",
            "dob": "1990-01-01",
            "amount": "10.00",
        }
    ]

    report = score_compliance_risk(columns, rows)
    assert report["risk_score"] >= 0.5
    assert report["requires_review"] is True
    assert report["high_risk_fields"]


def test_account_locked_flag_is_not_pci_high_risk() -> None:
    """Boolean CRM flags like accountLocked must not hard-block as PCI account PII."""
    columns = [
        "email",
        "accountLocked",
        "dailyJobDigest",
        "countryAutoDetected",
        "notifications",
    ]
    rows = [
        {
            "email": "alice@example.com",
            "accountLocked": False,
            "dailyJobDigest": True,
            "countryAutoDetected": True,
            "notifications": True,
        }
    ]
    report = score_compliance_risk(columns, rows)
    assert "accountLocked" not in report["high_risk_fields"]
    assert "accountLocked" not in report.get("field_risk", {}).get("accountLocked", [])
    # Medium PII (email) may still surface, but must not force requires_review via false PCI.
    assert report["risk_score"] < 0.45
    assert report["requires_review"] is False


def test_bank_account_number_still_high_risk() -> None:
    report = score_compliance_risk(
        ["customer_id", "bank_account_number"],
        [{"customer_id": "1", "bank_account_number": "123456789012"}],
    )
    assert report["requires_review"] is True
    assert "bank_account_number" in report["high_risk_fields"]
