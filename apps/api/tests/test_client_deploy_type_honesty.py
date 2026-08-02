"""Client-deploy honesty: no silent datetime invent, Accept-risk honored, no false *_id blocks."""

from __future__ import annotations

from services.ddl_compatibility import evaluate_ddl_compatibility
from services.transform_engine import apply_transform, infer_transform_for_mapping


def test_ambiguous_datetime_with_time_fails_closed():
    """US vs EU calendars must not invent — with or without time-of-day."""
    for raw in (
        "06/05/2024 14:30:00",
        "06/05/2024 2:30:00 PM",
        "06/05/2024",
    ):
        val, err = apply_transform(raw, "datetime")
        assert val is None, raw
        assert err is not None, raw


def test_ambiguous_datetime_parses_when_locale_set():
    from services.transform_engine import reset_active_date_locale, set_active_date_locale

    token = set_active_date_locale("DMY")
    try:
        val, err = apply_transform("06/05/2024 14:30:00", "datetime")
        assert err is None
        assert val is not None
        assert "2024-05-06" in str(val)
    finally:
        reset_active_date_locale(token)


def test_infer_transform_does_not_force_date_on_status_text():
    xf = infer_transform_for_mapping(
        "status",
        "posted_date_estimated",
        "VARCHAR",
        "DATE",
        source_samples=["active", "inactive", "draft"],
    )
    assert xf == "none"


def test_infer_transform_uses_date_when_samples_are_temporal():
    xf = infer_transform_for_mapping(
        "posted_at",
        "posted_date",
        "VARCHAR",
        "DATE",
        source_samples=["2024-06-05", "2024-07-01", "2024-08-15"],
    )
    assert xf == "date"


def test_g6_does_not_require_optional_unmapped_fk_ids():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_schema={"id": "INTEGER"},
        target_schema={
            "id": "INTEGER",
            "org_id": "INTEGER",
            "customer_id": "INTEGER",
            "created_by_id": "INTEGER",
        },
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
        destination_pk_columns=["id"],
    )
    assert ok is True
    assert not any("unmapped" in i.lower() for i in issues)


def test_mongo_majority_emits_type_mix_warning():
    from services.schema_introspect import _finalize_mongodb_type_with_note

    chosen, note = _finalize_mongodb_type_with_note({"INTEGER": 49, "TEXT": 1})
    assert chosen == "INTEGER"
    assert note and "TEXT sentinel" in note


def test_g9_surfaces_mongo_type_mix_as_warning_not_block():
    from services.data_integrity import run_integrity_audit

    report = run_integrity_audit(
        source_columns=["age"],
        mappings=[{"source": "age", "target": "age", "confidence": 0.99}],
        source_schemas=[{
            "name": "age",
            "inferred_type": "INTEGER",
            "type_mix_warning": "1 TEXT sentinel(s) among 50 samples — majority INTEGER",
        }],
        sample_rows=[{"age": "30"}],
        validation_mode="strict",
    )
    mix = next((c for c in report["checks"] if c["check"] == "mongo_type_mix"), None)
    assert mix is not None
    assert mix["blocks_transfer"] is False
    assert mix["warnings"]


def test_g6_blocks_unmapped_composite_pk_id():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_schema={"id": "INTEGER"},
        target_schema={"id": "INTEGER", "tenant_id": "INTEGER"},
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
        destination_pk_columns=["id", "tenant_id"],
    )
    assert ok is False
    assert any("primary-key" in i.lower() and "tenant_id" in i for i in issues)


def test_bq_timestamp_naive_fails_closed():
    from connectors.warehouse_temporal import format_bigquery_bind
    from services.type_system import ddl_type
    import pytest

    # Create-new bare datetime → DATETIME (wall-clock), not TIMESTAMP invent.
    assert ddl_type("bigquery", "datetime") == "DATETIME"
    assert ddl_type("bigquery", "TIMESTAMPTZ") == "TIMESTAMP"

    with pytest.raises(ValueError, match="refuses naive"):
        format_bigquery_bind("2024-01-05T10:30:00", "TIMESTAMP")


def test_sf_ntz_preserves_offset_wall_clock():
    from connectors.warehouse_temporal import format_snowflake_bind

    assert format_snowflake_bind("2024-01-05T10:30:00+05:30", "TIMESTAMP_NTZ") == (
        "2024-01-05 10:30:00"
    )


def test_json_scalar_wrap_is_warn_not_silent_ok():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"payload": "plain-text"}],
        mappings=[{"source": "payload", "target": "payload"}],
        source_types={"payload": "TEXT"},
        dest_types={"payload": "VARIANT"},
        dest_db_type="snowflake",
    )
    col = report["by_source"]["payload"]
    assert col["failed"] == 0
    assert col["json_scalar_wraps"] >= 1
    assert col["severity"] == "warn"
    assert "Accept risk" in (col.get("suggested_fix") or "")
