"""Exact decimal identity is unscaled integer × 10^(−scale), not a float.

Airbyte JSON number and Python float() collapse 1.2300 into 1.23 and round
integers past 2**53. PostgreSQL NUMERIC(p,s) rounds excess scale. SQLite
DECIMAL affinity is IEEE REAL. These tests pin the identity, the storage
classifier, and the fidelity aspect so the certificate cannot say carried
for a FLOAT dest or a narrower scale.
"""

from __future__ import annotations

from decimal import Decimal

from services.decimal_identity import (
    IEEE754_SAFE_INT,
    classify_storage,
    decide_decimal_identity,
    extract_decimal_identity,
    identities_match,
    identities_same_magnitude,
)
from services.json_polarity import IEEE754_SAFE_INT as JSON_IEEE
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity

MONEY = "1.2300"
TIE = "1.225"


def test_trailing_zeros_are_the_money_scale_contract():
    a = extract_decimal_identity(MONEY)
    b = extract_decimal_identity("1.23")
    assert a is not None and b is not None
    assert a.scale == 4 and a.digits == "12300"
    assert b.scale == 2 and b.digits == "123"
    assert Decimal(a.to_canonical_text()) == Decimal(b.to_canonical_text())
    assert not identities_match(a, b)
    assert identities_same_magnitude(a, b)


def test_identity_never_goes_through_float():
    ident = extract_decimal_identity(str(IEEE754_SAFE_INT + 1))
    assert ident is not None
    assert ident.beyond_ieee is True
    assert ident.unscaled == IEEE754_SAFE_INT + 1
    assert JSON_IEEE == IEEE754_SAFE_INT
    leaked = extract_decimal_identity(float(1.5))
    assert leaked is not None
    assert leaked.approximate is True


def test_pg_numeric_unconstrained_is_exact():
    cap = classify_storage("postgresql", "NUMERIC")
    assert cap.kind == "exact"
    assert cap.unconstrained is True
    typed = classify_storage("postgresql", "NUMERIC(10,2)")
    assert typed.kind == "exact"
    assert typed.scale == 2
    assert typed.unconstrained is False


def test_mysql_decimal_is_exact_float_is_not():
    assert classify_storage("mysql", "DECIMAL(12,4)").kind == "exact"
    assert classify_storage("mysql", "DOUBLE").kind == "approximate"
    assert classify_storage("postgresql", "DOUBLE PRECISION").kind == "approximate"


def test_sqlite_decimal_affinity_is_ieee_create_new_is_text():
    assert classify_storage("sqlite", "DECIMAL(38,18)").kind == "sqlite_affinity"
    assert classify_storage("sqlite", "DECIMAL(38,18)", create_new=True).kind == "text_digits"
    assert classify_storage("sqlite", "TEXT").kind == "text_digits"


def test_decimal_to_float_is_unsupported_not_carried():
    decision = decide_decimal_identity(
        source_engine="postgresql",
        source_type="NUMERIC(12,4)",
        dest_engine="mysql",
        dest_type="DOUBLE",
        source_column="amt",
        dest_column="amt",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert "IEEE" in decision.reason or "binary" in decision.reason.lower()


def test_narrower_dest_scale_is_unsupported():
    decision = decide_decimal_identity(
        source_engine="postgresql",
        source_type="NUMERIC(10,4)",
        dest_engine="mysql",
        dest_type="DECIMAL(10,2)",
        source_column="amt",
        dest_column="amt",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert "scale" in decision.reason.lower()


def test_equal_or_wider_dest_scale_is_carried():
    decision = decide_decimal_identity(
        source_engine="mysql",
        source_type="DECIMAL(10,2)",
        dest_engine="postgresql",
        dest_type="NUMERIC(12,4)",
        source_column="amt",
        dest_column="amt",
    )
    assert decision is not None
    assert decision.status == "carried"


def test_float_source_is_skipped_never_had_identity():
    decision = decide_decimal_identity(
        source_engine="postgresql",
        source_type="DOUBLE PRECISION",
        dest_engine="mysql",
        dest_type="DECIMAL(12,4)",
        source_column="amt",
        dest_column="amt",
    )
    assert decision is not None
    assert decision.status == "skipped"


def test_sqlite_existing_affinity_unsupported_create_new_text_carried():
    existing = decide_decimal_identity(
        source_engine="postgresql",
        source_type="NUMERIC(12,4)",
        dest_engine="sqlite",
        dest_type="DECIMAL(12,4)",
        source_column="amt",
        dest_column="amt",
    )
    assert existing is not None
    assert existing.status == "unsupported"
    created = decide_decimal_identity(
        source_engine="postgresql",
        source_type="NUMERIC(12,4)",
        dest_engine="sqlite",
        dest_type="DECIMAL(12,4)",
        source_column="amt",
        dest_column="amt",
        create_new=True,
    )
    assert created is not None
    assert created.status == "carried"


def test_create_new_pg_numeric_to_mysql_decimal_is_carried():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "amt"],
        column_types={"id": "BIGINT", "amt": "NUMERIC(12,4)"},
        primary_key=["id"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "amt"],
        target_types=["BIGINT", "DECIMAL(12,4)"],
        source_to_target={"id": "id", "amt": "amt"},
    )
    items = [i for i in plan.report.items if i.aspect == "decimal"]
    assert items
    assert any(i.status == "carried" for i in items)


def test_create_new_decimal_to_float_is_unsupported():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "amt"],
        column_types={"id": "BIGINT", "amt": "NUMERIC(12,4)"},
        primary_key=["id"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "amt"],
        target_types=["BIGINT", "DOUBLE"],
        source_to_target={"id": "id", "amt": "amt"},
    )
    items = [i for i in plan.report.items if i.aspect == "decimal"]
    assert items
    assert any(i.status == "unsupported" for i in items)


def test_integer_column_has_no_decimal_aspect_decision():
    decision = decide_decimal_identity(
        source_engine="postgresql",
        source_type="BIGINT",
        dest_engine="mysql",
        dest_type="BIGINT",
        source_column="id",
        dest_column="id",
    )
    assert decision is None
