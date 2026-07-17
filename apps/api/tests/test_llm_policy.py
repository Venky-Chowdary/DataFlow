"""Tests for the LLM policy gate and PII masking."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.llm_policy import (
    is_llm_enabled,
    is_pii_masking_enabled,
    mask_pii_samples,
    mask_pii_value,
)


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    monkeypatch.delenv("DATAFLOW_LLM_ENABLED", raising=False)
    monkeypatch.delenv("DATAFLOW_PII_MASKING", raising=False)


def test_llm_enabled_by_default():
    assert is_llm_enabled() is True


def test_llm_disabled_via_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    assert is_llm_enabled() is False


def test_pii_masking_enabled_by_default():
    assert is_pii_masking_enabled() is True


def test_pii_masking_disabled_via_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_MASKING", "false")
    assert is_pii_masking_enabled() is False


def test_mask_pii_email():
    assert mask_pii_value("user@example.com") == "<redacted>"


def test_mask_pii_credit_card():
    assert mask_pii_value("1234-5678-9012-3456") == "<redacted>"


def test_mask_pii_phone():
    assert mask_pii_value("555-123-4567") == "<redacted>"


def test_mask_pii_aws_key():
    assert mask_pii_value("AKIAIOSFODNN7EXAMPLE") == "<redacted>"


def test_mask_pii_samples():
    assert mask_pii_samples({"email": ["a@b.com", "ok"], "amount": ["100.00"]}) == {
        "email": ["<redacted>", "ok"],
        "amount": ["100.00"],
    }
