"""Tests for cross-field mapping quality analysis and honesty caps."""

from services.mapping_quality import (
    analyze_column_profile,
    detect_cross_field_issues,
    refine_mappings_with_quality,
    score_mapping_pair,
)
from services.semantic_mapper import map_columns


def test_email_profile_detection():
    profile = analyze_column_profile(
        "contact_email",
        ["alice@example.com", "bob@corp.io", "bad", "carol@test.org"],
    )
    assert profile["likely_email"] is True


def test_uuid_profile_detection():
    profile = analyze_column_profile(
        "row_id",
        [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        ],
    )
    assert profile["likely_uuid"] is True
    assert profile["likely_identifier"] is True


def test_quality_boost_for_email_alignment():
    mapping = {"source": "e_mail", "target": "customer_email", "confidence": 0.78, "reasoning": "test"}
    profile = analyze_column_profile("e_mail", ["a@b.com", "c@d.com"])
    delta, notes = score_mapping_pair(mapping, source_profile=profile)
    assert delta > 0
    assert any("email" in n for n in notes)


def test_quality_penalty_misaligned_email():
    mapping = {"source": "e_mail", "target": "amount", "confidence": 0.8, "reasoning": "test"}
    profile = analyze_column_profile("e_mail", ["a@b.com", "c@d.com"])
    delta, _ = score_mapping_pair(mapping, source_profile=profile)
    assert delta < 0


def test_email_to_varchar_snowflake_is_pii_note_not_type_defect():
    mapping = {
        "source": "contact_email",
        "target": "user_notes",
        "confidence": 0.92,
        "target_type": "VARCHAR",
        "reasoning": "identity",
    }
    profile = analyze_column_profile("contact_email", ["a@b.com", "b@c.com"])
    delta, notes = score_mapping_pair(mapping, source_profile=profile)
    assert delta >= 0
    assert any("pii" in n.lower() or "mask" in n.lower() for n in notes)
    assert not any("non-string" in n for n in notes)


def test_timestamp_to_timestamp_no_non_temporal_warning():
    mapping = {
        "source": "last_login",
        "target": "last_login",
        "confidence": 0.88,
        "source_type": "TIMESTAMP",
        "target_type": "TIMESTAMP",
        "reasoning": "exact",
    }
    profile = analyze_column_profile(
        "last_login",
        ["2024-01-01 12:00:00", "2024-02-01 08:30:00"],
    )
    _, notes = score_mapping_pair(mapping, source_profile=profile)
    assert not any("non-temporal" in n for n in notes)
    assert any("temporal" in n for n in notes)


def test_identity_passthrough_skips_name_match_boost():
    mapping = {
        "source": "id",
        "target": "id",
        "confidence": 0.92,
        "assignment_strategy": "identity_passthrough",
        "create_new": True,
        "reasoning": "new table",
    }
    profile = analyze_column_profile("id", ["1", "2", "3"])
    delta, _ = score_mapping_pair(mapping, source_profile=profile)
    assert delta < 0.1


def test_refine_create_new_confidence_capped():
    schemas = [{"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]}]
    mappings = [{
        "source": "id",
        "target": "id",
        "confidence": 0.92,
        "reasoning": "New destination table — identity mapping",
        "assignment_strategy": "identity_passthrough",
        "create_new": True,
        "source_type": "INTEGER",
        "target_type": "NUMBER(38,0)",
    }]
    refined = refine_mappings_with_quality(mappings, source_schemas=schemas)
    assert refined[0]["confidence"] <= 0.93


def test_create_new_identity_why_contains_new_table_language():
    mappings = map_columns(
        ["id", "email"],
        [],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["a@b.com"]},
        ],
        destination_db_type="snowflake",
        destination_table_exists=False,
    )
    assert all(m["assignment_strategy"] == "identity_passthrough" for m in mappings)
    assert all("New destination table" in m["reasoning"] for m in mappings)
    assert all("CREATE on first write" in m["reasoning"] for m in mappings)
    assert mappings[0]["target_type"] == "NUMBER(38,0)"
    assert mappings[0]["confidence"] <= 0.95


def test_existing_table_empty_columns_never_invents_create_new():
    """Shared SQL/warehouse failure mode: table exists, columns []. Must not claim CREATE."""
    for exists in (True, None):
        mappings = map_columns(
            ["id", "title"],
            [],
            destination_db_type="postgresql",
            destination_table_exists=exists,
        )
        assert mappings
        assert all(m.get("create_new") is False for m in mappings)
        assert all(m.get("assignment_strategy") == "pending_dest_schema" for m in mappings)
        assert all("New destination table" not in m["reasoning"] for m in mappings)
        assert all(m.get("requires_review") is True for m in mappings)


def test_refine_mappings_with_quality():
    schemas = [{"name": "email", "inferred_type": "VARCHAR", "samples": ["x@y.com", "a@b.com"]}]
    mappings = [{"source": "email", "target": "customer_email", "confidence": 0.8, "reasoning": "lexical"}]
    refined = refine_mappings_with_quality(mappings, source_schemas=schemas)
    assert refined[0]["confidence"] >= 0.8
    assert "column_profile" in refined[0]


def test_detect_duplicate_identifier_targets():
    schemas = [
        {"name": "id_a", "samples": ["1", "2", "3", "4"]},
        {"name": "id_b", "samples": ["5", "6", "7", "8"]},
    ]
    mappings = [
        {"source": "id_a", "target": "primary_key"},
        {"source": "id_b", "target": "primary_key"},
    ]
    issues = detect_cross_field_issues(mappings, source_schemas=schemas)
    assert any("identifier-like" in i for i in issues)
