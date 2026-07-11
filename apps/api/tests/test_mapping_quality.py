"""Tests for cross-field mapping quality analysis."""

from services.mapping_quality import (
    analyze_column_profile,
    detect_cross_field_issues,
    refine_mappings_with_quality,
    score_mapping_pair,
)


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
