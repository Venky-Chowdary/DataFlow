"""BSON decimal128 is a specified carrier, not an unresolved bare DECIMAL.

A bare SQL ``DECIMAL`` is a promise the engine fills in with a platform default
that is usually narrower than the source (MySQL 10,0 · SQL Server 18,0 ·
Snowflake 38,0), so an unresolved one is correctly treated as narrowing. BSON
``decimal`` is not that: it is IEEE 754-2008 decimal128, holding 34 significant
digits at any in-range scale. Reading it as unresolved refused PostgreSQL →
MongoDB routes for a collapse that never happens, while a genuinely wider source
must still be caught.
"""

from __future__ import annotations

import pytest

from services.conversion_contract import classify_conversion
from services.type_system import (
    DECIMAL128_SIGNIFICANT_DIGITS,
    decimal_params_would_narrow,
    dest_decimal_is_decimal128,
)


def test_only_decimal128_dialects_claim_the_carrier():
    assert dest_decimal_is_decimal128(dest_db="mongodb")
    for other in ("mysql", "snowflake", "sqlserver", "postgresql", "bigquery", ""):
        assert not dest_decimal_is_decimal128(dest_db=other), other


@pytest.mark.parametrize("source", ["DECIMAL(12,2)", "NUMERIC(12,2)", "DECIMAL(34,0)", "DECIMAL(20,14)"])
def test_sources_within_capacity_are_lossless(source):
    assert not decimal_params_would_narrow(source, "decimal", dest_db="mongodb")
    verdict = classify_conversion(source, "decimal", dest_db="mongodb")
    assert verdict["lossy"] is False, verdict
    assert verdict["invents_capacity"] is False, verdict
    assert verdict["requires_risk_contract"] is False, verdict


@pytest.mark.parametrize("source", ["DECIMAL(38,15)", "DECIMAL(40,2)", "DECIMAL(76,38)"])
def test_sources_beyond_capacity_still_block(source):
    """More significant digits than decimal128 holds is a real collapse."""
    assert decimal_params_would_narrow(source, "decimal", dest_db="mongodb")
    verdict = classify_conversion(source, "decimal", dest_db="mongodb")
    assert verdict["lossy"] is True, verdict
    assert verdict["requires_risk_contract"] is True, verdict


def test_capacity_boundary_is_exactly_thirty_four_digits():
    at_capacity = f"DECIMAL({DECIMAL128_SIGNIFICANT_DIGITS},4)"
    past_capacity = f"DECIMAL({DECIMAL128_SIGNIFICANT_DIGITS + 1},4)"
    assert not decimal_params_would_narrow(at_capacity, "decimal", dest_db="mongodb")
    assert decimal_params_would_narrow(past_capacity, "decimal", dest_db="mongodb")


@pytest.mark.parametrize(
    "dest_db,expect_narrow",
    [("mysql", True), ("snowflake", True), ("sqlserver", True), ("postgresql", False)],
)
def test_sql_dialect_bare_decimal_rules_are_unchanged(dest_db, expect_narrow):
    """The decimal128 carve-out must not leak into engines with real defaults."""
    assert decimal_params_would_narrow("DECIMAL(12,2)", "DECIMAL", dest_db=dest_db) is expect_narrow
