"""Regression tests for value-aware semantic role inference."""

from __future__ import annotations

from services.semantic_analyzer import analyze_column, role_match_boost


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


def test_contact_roles_never_boost_into_person_names() -> None:
    """QA M02 — ``phone_number -> full_name`` scored 0.87 and auto-pinned
    phone digits into a name column. Contact channels boost within their own
    family only; a name is not a contact channel."""
    assert role_match_boost("phone_number", "full_name") is None
    assert role_match_boost("email_address", "full_name") is None
    assert role_match_boost("full_name", "phone_number") is None
    # Within-family still aligns.
    assert role_match_boost("phone_number", "mobile_number") is not None
    assert role_match_boost("phone_number", "email_address") is not None
    assert role_match_boost("first_name", "full_name") is not None


def test_map_never_pins_phone_onto_full_name() -> None:
    """Mapper-level M02 proof: Phone must not bind to full_name."""
    from services.semantic_mapper import map_columns

    out = map_columns(
        ["Phone"],
        ["full_name"],
        source_schemas=[
            {"name": "Phone", "inferred_type": "VARCHAR", "samples": ["555-1234"]}
        ],
        target_schemas=[{"name": "full_name", "inferred_type": "VARCHAR"}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    assert len(out) == 1
    assert out[0]["target"] != "full_name"
    assert out[0]["confidence"] < 0.85
