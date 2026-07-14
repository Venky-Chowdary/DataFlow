"""Tests for semantic type detection and normalization."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from decimal import Decimal

from services.semantic_types import SemanticType, detect_semantic_type, normalize_value


def test_detect_by_name():
    assert detect_semantic_type("phone_number") == SemanticType.PHONE
    assert detect_semantic_type("email_address") == SemanticType.EMAIL
    assert detect_semantic_type("website_url") == SemanticType.URL
    assert detect_semantic_type("iban_code") == SemanticType.IBAN
    assert detect_semantic_type("price") == SemanticType.CURRENCY
    assert detect_semantic_type("discount_rate") == SemanticType.PERCENTAGE
    assert detect_semantic_type("uuid") == SemanticType.UUID
    assert detect_semantic_type("postal_code") == SemanticType.POSTAL
    assert detect_semantic_type("created_at") == SemanticType.UNKNOWN


def test_detect_by_sample():
    assert detect_semantic_type("col", ["user@example.com"]) == SemanticType.EMAIL
    assert detect_semantic_type("col", ["https://example.com"]) == SemanticType.URL
    assert detect_semantic_type("col", ["GB82WEST12345698765432"]) == SemanticType.IBAN
    assert detect_semantic_type("col", ["$1,234.56"]) == SemanticType.CURRENCY
    assert detect_semantic_type("col", ["12.5%"]) == SemanticType.PERCENTAGE
    assert detect_semantic_type("col", ["2024-01-15 10:30:00"]) == SemanticType.TIMESTAMP


def test_phone_normalization():
    assert normalize_value("+1 (555) 123-4567", SemanticType.PHONE) == "+15551234567"
    assert normalize_value("(555) 123-4567", SemanticType.PHONE) == "5551234567"
    assert normalize_value("N/A", SemanticType.PHONE) == "N/A"


def test_email_normalization():
    assert normalize_value("  John.Doe@Example.COM  ", SemanticType.EMAIL) == "john.doe@example.com"


def test_url_normalization():
    assert normalize_value("HTTPS://Example.COM/Path", SemanticType.URL).lower() == "https://example.com/path"


def test_iban_normalization():
    assert normalize_value("GB82 WEST 1234 5698 7654 32", SemanticType.IBAN) == "GB82WEST12345698765432"


def test_currency_normalization():
    assert normalize_value("$1,234.56", SemanticType.CURRENCY, target_string=False) == Decimal("1234.56")
    assert normalize_value("€100", SemanticType.CURRENCY, target_string=False) == Decimal("100")
    assert normalize_value("free", SemanticType.CURRENCY, target_string=False) == "free"


def test_percentage_normalization():
    assert normalize_value("12.5%", SemanticType.PERCENTAGE, target_string=False) == Decimal("12.5")
    assert normalize_value("100%", SemanticType.PERCENTAGE, target_string=False) == Decimal("100")


def test_postal_normalization():
    assert normalize_value("sw1a 1aa", SemanticType.POSTAL) == "SW1A1AA"
