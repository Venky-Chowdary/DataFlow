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


def test_timetz_preserves_offset_and_refuses_naive_utc_invent():
    from datetime import time, timezone, timedelta
    import pytest
    from connectors.sql_temporal import coerce_sql_temporal
    from services.transform_engine import apply_transform

    val, err = apply_transform("15:30:00+05:30", "time")
    assert err is None
    assert val is not None
    assert "+05:30" in val or "+0530" in val.replace(":", "")

    from connectors.sql_temporal import sql_base_type

    assert sql_base_type("TIMETZ") == "TIMETZ"
    tm = coerce_sql_temporal("15:30:00+05:30", "TIMETZ")
    assert isinstance(tm, time), tm
    assert tm.tzinfo is not None
    assert tm.hour == 15 and tm.minute == 30
    # Must not become 15:30 UTC (offset strip + invent).
    assert tm.utcoffset() == timedelta(hours=5, minutes=30)

    with pytest.raises(ValueError, match="refuses naive"):
        coerce_sql_temporal("15:30:00", "TIMETZ")


def test_boolean_refuses_informal_yes_invent():
    from services.transform_engine import apply_transform
    from services.type_system import boolean_value_fits, is_lossy_coercion
    from connectors.sql_bind import coerce_boolean_wire

    val, err = apply_transform("yes", "boolean")
    assert val is None and err is not None
    assert boolean_value_fits("yes") is False
    # Bind must not invent TRUE from yes (pass-through for quarantine).
    assert coerce_boolean_wire("yes") == "yes"
    assert coerce_boolean_wire("true") is True


def test_binary_to_text_is_lossy_not_preserve():
    from services.mapping_proof import mapping_fidelity
    from services.type_system import is_lossy_coercion

    assert is_lossy_coercion("BYTEA", "TEXT") is True
    verdict = mapping_fidelity(
        {"source": "blob", "target": "blob_text", "transform": "none"},
        declared_source_type="BYTEA",
        declared_target_type="TEXT",
    )
    assert verdict["verdict"] == "lossy_cast"


def test_bare_datetime_create_new_is_wall_clock_not_tz_invent():
    from services.type_system import ddl_type

    assert ddl_type("postgresql", "datetime") == "TIMESTAMP"
    assert ddl_type("redshift", "datetime") == "TIMESTAMP"
    assert ddl_type("snowflake", "datetime") == "TIMESTAMP_NTZ"
    assert ddl_type("oracle", "datetime") == "TIMESTAMP"
    # Explicit aware carriers still land on TZ-aware DDL.
    assert ddl_type("postgresql", "TIMESTAMPTZ") == "TIMESTAMPTZ"


def test_array_to_json_is_document_collapse_not_preserve():
    from services.type_system import (
        is_lossy_coercion,
        is_nested_document_collapse,
        normalize_logical_type,
        ddl_type,
        parse_array_element,
    )

    assert is_nested_document_collapse("ARRAY<INTEGER>", "JSONB") is True
    assert is_lossy_coercion("ARRAY<INTEGER>", "JSONB") is True
    assert normalize_logical_type("INTEGER[]") == "array"
    assert parse_array_element("INTEGER[]") == "INTEGER"
    assert ddl_type("postgresql", "ARRAY<INTEGER>") == "BIGINT[]"
    assert ddl_type("postgresql", "INTEGER[]") == "BIGINT[]"


def test_ntz_to_tz_is_polarity_loss_and_bind_refuses_naive():
    import pytest
    from connectors.sql_temporal import coerce_sql_temporal
    from services.type_system import is_lossy_coercion, is_timezone_polarity_loss

    assert is_timezone_polarity_loss("TIMESTAMP_NTZ", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("TIMESTAMP_NTZ", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("DATETIME2", "DATETIMEOFFSET") is True
    with pytest.raises(ValueError, match="refuses naive"):
        coerce_sql_temporal("2024-01-05 10:30:00", "TIMESTAMPTZ")
    # Offset wire still binds.
    got = coerce_sql_temporal("2024-01-05T10:30:00Z", "TIMESTAMPTZ")
    assert got is not None


def test_bitstring_bytea_and_year_polarity():
    from services.type_system import is_lossy_coercion

    assert is_lossy_coercion("BIT(8)", "BYTEA") is True
    assert is_lossy_coercion("BYTEA", "BIT(8)") is True
    # Intentional create-new 0/1 digit text sink.
    assert is_lossy_coercion("BIT(8)", "VARCHAR(8)") is False
    assert is_lossy_coercion("YEAR", "SMALLINT") is True
    assert is_lossy_coercion("SET('a','b')", "TEXT[]") is False


def test_specialty_to_text_is_lossy_not_preserve():
    from services.mapping_proof import mapping_fidelity
    from services.type_system import ddl_type, is_lossy_coercion

    assert is_lossy_coercion("INTERVAL", "VARCHAR") is True
    assert is_lossy_coercion("GEOGRAPHY", "STRING") is True
    assert is_lossy_coercion("VECTOR", "VARCHAR") is True
    assert mapping_fidelity(
        {"source": "iv", "target": "iv", "transform": "none"},
        declared_source_type="INTERVAL",
        declared_target_type="VARCHAR",
    )["verdict"] == "lossy_cast"

    # BIGINT UNSIGNED must not invent fractional DECIMAL scale.
    assert ddl_type("snowflake", "BIGINT UNSIGNED") == "NUMBER(38,0)"
    assert ddl_type("mysql", "BIGINT UNSIGNED").endswith(",0)")
    assert ddl_type("postgresql", "BIGINT UNSIGNED") == "NUMERIC(20,0)"

    assert is_lossy_coercion("DECIMAL(19,4)", "MONEY") is True
