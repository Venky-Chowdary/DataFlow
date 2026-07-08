"""Regression tests for value-aware semantic role inference."""

from __future__ import annotations

from services.semantic_analyzer import analyze_column


def test_generic_numeric_samples_do_not_become_payment_amount() -> None:
    analyzed = analyze_column("col_7", "DECIMAL", ["10.5", "11.25", "12.0"])
    assert analyzed["semantic_role"] == "numeric_value"
    assert analyzed["semantic_role"] != "payment_amount"


def test_generic_identifier_samples_do_not_become_customer_id() -> None:
    analyzed = analyze_column("source_key", "VARCHAR", ["ABCD123456", "WXYZ987654"])
    assert analyzed["semantic_role"] == "identifier"
    assert analyzed["semantic_role"] != "customer_id"


def test_created_at_is_not_a_payment_date() -> None:
    analyzed = analyze_column("created_at", "TIMESTAMP", ["2024-01-01T10:00:00Z"])
    assert analyzed["semantic_role"] == "created_timestamp"


def test_business_headers_still_win_over_generic_samples() -> None:
    analyzed = analyze_column("payment_amount", "DECIMAL", ["10.5", "11.25"])
    assert analyzed["semantic_role"] == "payment_amount"
