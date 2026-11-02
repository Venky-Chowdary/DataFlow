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


def test_auto_ambiguous_grouped_numbers_are_not_numeric_like():
    """1,234 is US thousands or EU decimal — name must not invent likely_numeric."""
    profile = analyze_column_profile("amount", ["1,234", "5,678"])
    assert profile["likely_numeric"] is False
    _, notes = score_mapping_pair(
        {
            "source": "amount",
            "target": "amount",
            "target_type": "DECIMAL",
            "confidence": 0.9,
        },
        source_profile=profile,
    )
    assert not any("numeric samples" in n for n in notes)


def test_bindable_currency_still_scores_as_numeric_like():
    us = analyze_column_profile("amount", ["$1,000.00", "$2,500.50"])
    assert us["likely_numeric"] is True
    euro = analyze_column_profile("price", ["€1.000,89", "€2.500,00"])
    assert euro["likely_numeric"] is True
    _, notes = score_mapping_pair(
        {
            "source": "amount",
            "target": "amount",
            "target_type": "DECIMAL",
            "confidence": 0.9,
        },
        source_profile=us,
    )
    assert any("numeric samples on numeric target" in n for n in notes)


def test_auto_ambiguous_slash_dates_are_not_date_like():
    """01/02/2024 is Jan 2 or Feb 1 — name must not invent likely_date."""
    profile = analyze_column_profile("event_date", ["01/02/2024", "03/04/2024"])
    assert profile["likely_date"] is False
    delta, notes = score_mapping_pair(
        {
            "source": "event_date",
            "target": "event_date",
            "target_type": "DATE",
            "confidence": 0.9,
        },
        source_profile=profile,
    )
    assert not any("date-like" in n for n in notes)
    _, varchar_notes = score_mapping_pair(
        {
            "source": "event_date",
            "target": "event_date",
            "target_type": "VARCHAR",
            "confidence": 0.9,
        },
        source_profile=profile,
    )
    assert not any("non-temporal" in n for n in varchar_notes)


def test_name_alone_does_not_invent_email_phone_or_uuid():
    """contact_email / phone / uuid with plain-text samples must not invent a type."""
    email = analyze_column_profile("contact_email", ["alice", "bob"])
    assert email["likely_email"] is False
    phone = analyze_column_profile("phone_number", ["alice", "bob"])
    assert phone["likely_phone"] is False
    uuid = analyze_column_profile("user_uuid", ["alice", "bob"])
    assert uuid["likely_uuid"] is False
    identifier = analyze_column_profile("identifier", ["alice", "bob"])
    assert identifier["likely_uuid"] is False
    _, notes = score_mapping_pair(
        {
            "source": "contact_email",
            "target": "amount",
            "target_type": "DECIMAL",
            "confidence": 0.8,
        },
        source_profile=email,
    )
    assert not any("email-like" in n for n in notes)


def test_bindable_email_phone_uuid_still_score():
    email = analyze_column_profile(
        "notes",
        ["alice@example.com", "bob@corp.io", "carol@test.org", "dave@x.io"],
    )
    assert email["likely_email"] is True
    phone = analyze_column_profile("notes", ["555-123-4567", "+1 555 000 1212"])
    assert phone["likely_phone"] is True
    mid = analyze_column_profile("contact_email", ["a@b.com", "not-an-email"])
    assert mid["likely_email"] is True


def test_unambiguous_and_iso_dates_still_score_as_date_like():
    dmy = analyze_column_profile("event_date", ["31/12/2024", "30/11/2024"])
    assert dmy["likely_date"] is True
    iso = analyze_column_profile("event_date", ["2024-03-05", "2024-03-06"])
    assert iso["likely_date"] is True
    _, notes = score_mapping_pair(
        {
            "source": "event_date",
            "target": "event_date",
            "target_type": "DATE",
            "confidence": 0.9,
        },
        source_profile=iso,
    )
    assert any("date-like source aligned to temporal target" in n for n in notes)


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
    # Snowflake create-new may stamp INTEGER (logical) — materialize_dest_ddl owns
    # NUMBER(38,0) wire. Never require inventing (38,0) on the Map stamp itself.
    from services.type_system import materialize_dest_ddl

    stamp = str(mappings[0]["target_type"] or "")
    assert stamp.upper() in {"INTEGER", "NUMBER(38,0)", "NUMBER(38, 0)"}
    wire = materialize_dest_ddl("snowflake", stamp).upper().replace(" ", "")
    assert "NUMBER" in wire or wire == "INTEGER"
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


def test_column_profile_stamps_observed_decimal_for_map_strip():
    from services.mapping_quality import analyze_column_profile, refine_mappings_with_quality

    samples = ["85.5", "92.0", "78.25", "100", "66.75"]
    profile = analyze_column_profile("credit_score", samples)
    assert profile.get("numeric_kind") == "fixed_decimal"
    assert profile.get("observed_precision") is not None
    assert profile.get("min") is not None
    assert profile.get("max") is not None

    schemas = [
        {
            "name": "credit_score",
            "inferred_type": "DECIMAL",
            "samples": samples,
            "null_rate": 0.0,
            "statistics": {
                "observed_precision": profile["observed_precision"],
                "observed_scale": profile["observed_scale"],
                "numeric_kind": "fixed_decimal",
                "min": profile["min"],
                "max": profile["max"],
            },
        }
    ]
    refined = refine_mappings_with_quality(
        [{"source": "credit_score", "target": "score", "confidence": 0.9, "reasoning": "identity"}],
        source_schemas=schemas,
    )
    cp = refined[0]["column_profile"]
    assert cp.get("null_rate") == 0.0
    assert cp.get("observed_precision") is not None
    assert cp.get("observed_scale") is not None
    assert cp.get("numeric_kind") == "fixed_decimal"
    assert "min" in cp and "max" in cp


def test_pipeline_attaches_profiler_stats_to_schemas():
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["amount", "note"],
        target_columns=[],
        source_samples={
            "amount": ["12.50", "99.99", "0.01", "", "1500.00"],
            "note": ["a", "b", "c", "d", "e"],
        },
        destination_db_type="postgresql",
        destination_table_exists=False,
        use_llm=False,
    )
    mappings = result.get("mappings") or []
    amt = next(m for m in mappings if m["source"] == "amount")
    cp = amt.get("column_profile") or {}
    assert "null_rate" in cp
    assert cp.get("null_rate") == 0.2 or cp.get("null_rate") is not None
    assert cp.get("observed_precision") is not None or cp.get("numeric_kind") in {
        "fixed_decimal",
        "integer",
        "ieee_float",
    }


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
