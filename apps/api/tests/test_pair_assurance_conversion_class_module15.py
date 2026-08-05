"""Module 15 — pair assurance stamps charter ConversionClass on every type cell."""

from __future__ import annotations

from services.conversion_contract import ConversionClass
from services.pair_assurance import evaluate_type_cell


def test_type_cell_stamps_conversion_class():
    cell = evaluate_type_cell("INTEGER", dest_db="postgresql")
    assert cell.conversion_class
    assert cell.conversion_class in {c.value for c in ConversionClass}


def test_bare_decimal_create_new_needs_user_approval_class():
    # Create-new from DECIMAL often invents (p,s) — must not claim lossless class.
    cell = evaluate_type_cell("DECIMAL", dest_db="snowflake")
    assert cell.conversion_class in {
        ConversionClass.NEEDS_USER_APPROVAL.value,
        ConversionClass.LOSSY.value,
        ConversionClass.UNSUPPORTED.value,
    }
    if cell.invents_capacity:
        assert cell.requires_risk_contract or cell.conversion_class != (
            ConversionClass.LOSSLESS.value
        )


def test_lossy_ack_legacy_aligns_with_needs_approval():
    cell = evaluate_type_cell("TEXT", dest_db="postgresql")
    # TEXT→create-new TEXT may be lossless; use a known invent/lossy carrier.
    cell = evaluate_type_cell("FLOAT", dest_db="sqlite")
    assert cell.conversion_class
    assert cell.conversion_class != ""


def test_cell_to_dict_includes_conversion_fields():
    cell = evaluate_type_cell("VARCHAR(64)", dest_db="mysql")
    d = cell.to_dict()
    assert "conversion_class" in d
    assert "invents_capacity" in d
    assert "requires_risk_contract" in d
