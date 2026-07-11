"""Domain profile detection for enterprise verticals."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.domain_profiles import detect_data_domain, domain_type_hints  # noqa: E402


def test_detect_logistics_domain():
    cols = ["order_id", "tracking_no", "carrier", "origin_zip", "dest_zip", "freight_class", "weight"]
    profile = detect_data_domain(cols)
    assert profile["domain"] == "logistics"
    assert profile["confidence"] >= 0.35
    assert "order_id" in profile["signals"] or "tracking_no" in profile["signals"]


def test_detect_healthcare_domain():
    cols = ["patient_id", "mrn", "diagnosis_code", "encounter_id", "provider_npi"]
    profile = detect_data_domain(cols)
    assert profile["domain"] == "healthcare"
    assert "HIPAA" in profile.get("compliance", [])


def test_domain_type_override():
    assert domain_type_hints("finance", "transaction_amount", "VARCHAR") == "DECIMAL"
    assert domain_type_hints("general", "name", "VARCHAR") == "VARCHAR"
