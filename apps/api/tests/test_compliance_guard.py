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


def test_slash_dates_on_event_date_are_not_dob_pii() -> None:
    """01/02/2024 is a calendar string, not date-of-birth, unless the name says so."""
    report = score_compliance_risk(
        ["id", "event_date"],
        [
            {"id": "1", "event_date": "01/02/2024"},
            {"id": "2", "event_date": "03/04/2024"},
        ],
    )
    assert "event_date" not in report["sensitive_fields"]
    assert "event_date" not in report["high_risk_fields"]
    assert "dob" not in report.get("field_risk", {}).get("event_date", [])
    assert "HIPAA" not in report["compliance_tags"]
    assert report["requires_review"] is False


def test_iso_dates_on_created_at_are_not_dob_pii() -> None:
    report = score_compliance_risk(
        ["created_at"],
        [{"created_at": "2024-01-02"}, {"created_at": "2024-03-04"}],
    )
    assert report["high_risk_fields"] == []
    assert report["requires_review"] is False


def test_named_dob_column_still_high_risk() -> None:
    report = score_compliance_risk(
        ["patient_id", "date_of_birth"],
        [{"patient_id": "1", "date_of_birth": "1990-01-01"}],
    )
    assert "date_of_birth" in report["high_risk_fields"]
    assert report["requires_review"] is True
    assert "HIPAA" in report["compliance_tags"]


def test_birth_date_name_gates_slash_samples() -> None:
    report = score_compliance_risk(
        ["birth_date"],
        [{"birth_date": "01/02/1990"}],
    )
    assert "birth_date" in report["high_risk_fields"]
    assert report["requires_review"] is True


def test_typed_uuid_bytea_interval_are_not_pci_review() -> None:
    """Technical UUID / binary / interval columns are not regulated identifiers."""
    report = score_compliance_risk(
        ["id", "payload", "uid", "blob", "span"],
        [
            {
                "id": 1,
                "payload": '{"k": 1}',
                "uid": "11111111-1111-4111-8111-111111111111",
                "blob": "deadbeef",
                "span": "1 day 02:00:00",
            }
        ],
    )
    assert report["requires_review"] is False
    assert "uid" not in report["high_risk_fields"]
    assert "span" not in report["sensitive_fields"]
    assert "PCI-DSS" not in report["compliance_tags"]


def test_span_interval_is_not_card_pan() -> None:
    """``span`` must not match the card PAN token (word-boundary ``pan``)."""
    report = score_compliance_risk(["span"], [{"span": "1 day 02:00:00"}])
    assert report["requires_review"] is False
    assert "span" not in report["sensitive_fields"]
