"""Regression: status enums must not become BOOLEAN on new tables."""

from __future__ import annotations

from services.mapping_quality import analyze_column_profile, detect_cross_field_issues, score_mapping_pair
from services.schema_inference import infer_type
from services.transform_engine import _parse_boolean, apply_transform, dry_run_sample


def test_active_inactive_are_not_boolean_literals():
    assert _parse_boolean("active") is None
    assert _parse_boolean("inactive") is None
    assert _parse_boolean("enabled") is None
    assert _parse_boolean("disabled") is None
    assert _parse_boolean("invalidated") is None
    assert _parse_boolean("true") is True
    assert _parse_boolean("false") is False
    assert _parse_boolean("1") is True
    assert _parse_boolean("0") is False


def test_status_samples_infer_varchar_not_boolean():
    assert infer_type(["active"], field_name="status") == "VARCHAR"
    assert infer_type(["active", "inactive"], field_name="status") == "VARCHAR"
    assert infer_type(["active", "invalidated"], field_name="status") == "VARCHAR"
    # Real boolean flags still work
    assert infer_type(["true", "false"], field_name="deviceVerified") == "BOOLEAN"
    assert infer_type(["0", "1"], field_name="is_active") == "BOOLEAN"


def test_status_boolean_transform_fails_explicitly():
    value, err = apply_transform("invalidated", "boolean")
    assert value is None
    assert err and "Invalid boolean" in err


def test_dry_run_status_as_varchar_passes():
    ok, errors = dry_run_sample(
        headers=["status"],
        sample_rows=[["active"], ["invalidated"]],
        mappings=[{"source": "status", "target": "status", "target_type": "VARCHAR", "transform": "trim"}],
        column_types={"status": "VARCHAR"},
    )
    assert ok is True
    assert errors == []


def test_dry_run_status_as_boolean_blocks():
    ok, errors = dry_run_sample(
        headers=["status"],
        sample_rows=[["active"], ["invalidated"]],
        mappings=[{"source": "status", "target": "status", "target_type": "BOOLEAN", "transform": "boolean"}],
        column_types={"status": "VARCHAR"},
    )
    assert ok is False
    assert any("Invalid boolean" in e for e in errors)


def test_mapping_quality_flags_enum_to_boolean():
    profile = analyze_column_profile("status", ["active", "invalidated", "pending"])
    assert profile["likely_boolean"] is False
    delta, notes = score_mapping_pair(
        {"source": "status", "target": "status", "target_type": "BOOLEAN", "confidence": 0.9},
        source_profile=profile,
    )
    assert delta < 0
    assert any("string enum" in n for n in notes)

    issues = detect_cross_field_issues(
        [{"source": "status", "target": "status", "target_type": "BOOLEAN"}],
        source_schemas=[{"name": "status", "samples": ["active", "invalidated"]}],
    )
    assert any("string enum" in i for i in issues)
