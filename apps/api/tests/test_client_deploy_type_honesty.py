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
    # Open text → VARIANT invents document domain — Accept risk (not silent ok).
    assert col["severity"] == "block"
    assert col.get("fidelity_collapse") is True
    assert "Accept risk" in (col.get("suggested_fix") or "") or col.get("framing")


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
    assert ddl_type("postgresql", "ARRAY<INTEGER>") == "INTEGER[]"
    assert ddl_type("postgresql", "INTEGER[]") == "INTEGER[]"


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


def test_schema_qualified_decimal_invent_oracle_wave18():
    from services.type_system import (
        bitstring_pad_polarity_loss,
        create_new_mapping_target_type,
        decimal_params_would_narrow,
        identity_domain_would_invent,
        interval_precision_would_narrow,
        is_lossy_coercion,
        oracle_char_byte_polarity_loss,
        oracle_long_numeric_invent,
        specialty_carrier_base,
        specialty_carrier_would_collapse,
    )

    assert specialty_carrier_base("SYS.XMLTYPE") == "XMLTYPE"
    assert specialty_carrier_would_collapse("SYS.XMLTYPE", "VARCHAR") is True
    assert is_lossy_coercion("SYS.XMLTYPE", "VARCHAR") is True
    assert specialty_carrier_base("PG_CATALOG.INET") == "INET"
    assert is_lossy_coercion("PG_CATALOG.INET", "TEXT") is True

    assert decimal_params_would_narrow("DECIMAL", "DECIMAL(10,2)") is True
    assert is_lossy_coercion("NUMERIC", "NUMERIC(5,2)") is True

    assert oracle_char_byte_polarity_loss(
        "VARCHAR2(100 CHAR)", "VARCHAR2(100 BYTE)"
    ) is True
    assert is_lossy_coercion("VARCHAR2(100 CHAR)", "VARCHAR2(100 BYTE)") is True

    assert bitstring_pad_polarity_loss("BIT VARYING(8)", "BIT(8)") is True
    assert is_lossy_coercion("VARBIT(8)", "BIT(8)") is True

    assert identity_domain_would_invent("INTEGER", "SERIAL") is True
    assert is_lossy_coercion("INTEGER", "SERIAL") is True
    assert is_lossy_coercion("BIGINT", "BIGSERIAL") is True

    assert interval_precision_would_narrow(
        "INTERVAL DAY TO SECOND(6)", "INTERVAL DAY TO SECOND(0)"
    ) is True
    assert is_lossy_coercion(
        "INTERVAL DAY(9) TO SECOND(6)", "INTERVAL DAY(2) TO SECOND(6)"
    ) is True

    assert oracle_long_numeric_invent("LONG", "NUMBER(38,0)") is True
    assert is_lossy_coercion("LONG", "NUMBER(38,0)") is True
    assert create_new_mapping_target_type("LONG", "oracle") == "CLOB"
    # Off-Oracle: Spark/Hive INT64 synonym — BIGINT invent (gated Accept risk).
    assert create_new_mapping_target_type("LONG", "postgresql") == "BIGINT"
    assert is_lossy_coercion("LONG", "BIGINT") is True
    assert create_new_mapping_target_type("LONG", "databricks") == "BIGINT"
    assert is_lossy_coercion("LONG", "BIGINT") is True

    assert is_lossy_coercion("ANYDATA", "VARCHAR") is True
    assert is_lossy_coercion("HLLSKETCH", "VARCHAR") is True


def test_datetime2_half_collation_ws_wave19():
    from services.type_system import (
        create_new_mapping_target_type,
        float_mantissa_bits,
        is_lossy_coercion,
        kana_fold_polarity_invent,
        normalize_logical_type,
        temporal_precision_would_narrow,
        width_fold_polarity_invent,
    )

    assert temporal_precision_would_narrow("DATETIME2", "DATETIME") is True
    assert is_lossy_coercion("DATETIME2", "DATETIME") is True

    assert normalize_logical_type("HALF") == "float"
    assert float_mantissa_bits("HALF") == 11
    assert float_mantissa_bits("FLOAT16") == 11
    assert float_mantissa_bits("halffloat") == 11
    assert is_lossy_coercion("DOUBLE", "HALF") is True
    assert is_lossy_coercion("REAL", "FLOAT16") is True
    assert create_new_mapping_target_type("FLOAT16", "postgresql") == "REAL"
    assert create_new_mapping_target_type("HALF", "postgresql") == "REAL"

    assert width_fold_polarity_invent(
        "NVARCHAR COLLATE Latin1_General_CS_AS_WS",
        "NVARCHAR COLLATE Latin1_General_CS_AS",
    ) is True
    assert is_lossy_coercion(
        "NVARCHAR COLLATE Latin1_General_CS_AS_WS",
        "NVARCHAR COLLATE Latin1_General_CS_AS",
    ) is True
    assert kana_fold_polarity_invent(
        "NVARCHAR COLLATE Japanese_XJIS_140_CS_AS_KS",
        "NVARCHAR COLLATE Japanese_XJIS_140_CS_AS",
    ) is True
    assert is_lossy_coercion(
        "NVARCHAR COLLATE Japanese_XJIS_140_CS_AS_KS",
        "NVARCHAR COLLATE Japanese_XJIS_140_CS_AS",
    ) is True


def test_float_int_udt_invent_width_wave20():
    """Create-new must not soft-pass invent-widen or opaque UDT→TEXT."""
    from services.type_system import (
        create_new_mapping_target_type,
        is_lossy_coercion,
        specialty_carrier_base,
    )

    assert create_new_mapping_target_type("BINARY_FLOAT", "postgresql") == "REAL"
    assert create_new_mapping_target_type("REAL", "postgresql") == "REAL"
    assert create_new_mapping_target_type("FLOAT32", "oracle") == "BINARY_FLOAT"
    assert create_new_mapping_target_type("FLOAT", "postgresql") == "DOUBLE PRECISION"
    assert is_lossy_coercion("REAL", "DOUBLE") is False

    assert create_new_mapping_target_type("TINYINT", "postgresql") == "SMALLINT"
    assert create_new_mapping_target_type("INTEGER", "postgresql") == "INTEGER"
    assert create_new_mapping_target_type("TINYINT", "mysql") == "TINYINT"
    assert create_new_mapping_target_type("TINYINT UNSIGNED", "mysql") == "TINYINT UNSIGNED"
    assert create_new_mapping_target_type("MEDIUMINT", "mysql") == "MEDIUMINT"
    assert create_new_mapping_target_type("INTEGER", "oracle") == "NUMBER(10,0)"
    assert create_new_mapping_target_type("BIGINT", "oracle") == "NUMBER(38,0)"
    assert is_lossy_coercion("INTEGER", "BIGINT") is False

    assert specialty_carrier_base("USER-DEFINED") == "USER-DEFINED"
    assert create_new_mapping_target_type("USER-DEFINED", "postgresql") == "TEXT"
    assert is_lossy_coercion("USER-DEFINED", "TEXT") is True

    assert create_new_mapping_target_type("VARCHAR(MAX)", "oracle") == "CLOB"
    assert create_new_mapping_target_type("NVARCHAR(MAX)", "oracle") == "NCLOB"


def test_dest_alias_tz_longraw_uuid_wave21():
    """PG-family aliases, TZ→TEXT, LONG RAW, UNIQUEIDENTIFIER physical stamp."""
    from services.type_system import (
        create_new_mapping_target_type,
        is_fixed_width_binary_carrier,
        is_lossy_coercion,
        is_unlimited_binary_carrier,
        long_raw_locator_would_collapse,
        timezone_aware_would_collapse_to_string,
    )

    # Dest alias miss used to invent TEXT with soft-pass.
    assert create_new_mapping_target_type("NUMBER", "postgres") == "NUMERIC"
    assert create_new_mapping_target_type("DATE", "cockroachdb") == "DATE"
    assert create_new_mapping_target_type("BOOLEAN", "supabase") == "BOOLEAN"
    assert create_new_mapping_target_type("INTEGER", "alloydb") == "INTEGER"

    assert timezone_aware_would_collapse_to_string("TIMESTAMPTZ", "TEXT") is True
    assert is_lossy_coercion("TIMESTAMPTZ", "TEXT") is True
    assert is_lossy_coercion("DATETIMEOFFSET", "STRING") is True
    assert is_lossy_coercion("TIMETZ", "STRING") is True
    assert create_new_mapping_target_type("TIMETZ", "databricks") == "STRING"

    assert is_fixed_width_binary_carrier("LONG RAW") is False
    assert is_unlimited_binary_carrier("LONG RAW") is True
    assert long_raw_locator_would_collapse("LONG RAW", "BINARY") is True
    assert is_lossy_coercion("LONG RAW", "BINARY") is True
    assert create_new_mapping_target_type("LONG RAW", "postgresql") == "BYTEA"
    assert create_new_mapping_target_type("LONG RAW", "oracle") == "BLOB"

    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "sqlserver") == "UNIQUEIDENTIFIER"
    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "postgresql") == "UUID"

    assert create_new_mapping_target_type("INTERVAL", "oracle") == "VARCHAR2(64)"
    assert is_lossy_coercion("INTERVAL", "VARCHAR2(64)") is True
    assert create_new_mapping_target_type("INTERVAL DAY TO SECOND", "oracle") == (
        "INTERVAL DAY TO SECOND"
    )


def test_decfloat_alias_sdo_array_wave22():
    """DECFLOAT invent, mongo/db2 aliases, SDO polarity, nested INT width, SMALLDATETIME."""
    from services.type_system import (
        create_new_mapping_target_type,
        float_mantissa_bits,
        is_lossy_coercion,
        spatial_polarity,
    )

    assert create_new_mapping_target_type("DECFLOAT(34)", "oracle") == "BINARY_DOUBLE"
    assert is_lossy_coercion("DECFLOAT(34)", "BINARY_DOUBLE") is True
    assert is_lossy_coercion("DECFLOAT(34)", "NUMBER(34,0)") is True
    assert create_new_mapping_target_type("DECFLOAT", "postgresql") == "NUMERIC"
    assert is_lossy_coercion("DECFLOAT", "NUMERIC") is True

    assert create_new_mapping_target_type("INTEGER", "mongo") == "long"
    assert create_new_mapping_target_type("INTEGER", "documentdb") == "long"
    assert create_new_mapping_target_type("INTEGER", "neon") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "db2") == "BIGINT"
    assert create_new_mapping_target_type("INTEGER", "fabric") == "INT"

    assert spatial_polarity("SDO_GEOMETRY") == "sdo"
    assert is_lossy_coercion("GEOGRAPHY", "SDO_GEOMETRY") is True
    assert is_lossy_coercion("SDO_GEOMETRY", "GEOMETRY") is True

    assert float_mantissa_bits("REAL UNSIGNED") == 24
    assert create_new_mapping_target_type("REAL UNSIGNED", "postgresql") == "REAL"
    assert create_new_mapping_target_type("ARRAY<INT>", "postgresql") == "INTEGER[]"

    assert is_lossy_coercion("NUMBER", "BIGNUMERIC") is True
    assert is_lossy_coercion("SMALLDATETIME", "TIMESTAMP(0)") is True
    assert create_new_mapping_target_type("SMALLDATETIME", "postgresql") == "TIMESTAMP(0)"


def test_long_int8_varbyte_national_wave23():
    """Spark long→BIGINT, ClickHouse Int8 width, VARBYTE/binData, national invent."""
    from services.type_system import (
        create_new_mapping_target_type,
        integer_bit_width,
        is_lossy_coercion,
        is_national_string_carrier,
        national_charset_would_invent,
        normalize_logical_type,
    )

    assert create_new_mapping_target_type("long", "postgresql") == "BIGINT"
    assert is_lossy_coercion("LONG", "BIGINT") is True
    assert create_new_mapping_target_type("LONG", "oracle") == "CLOB"

    assert integer_bit_width("Int8") == 8
    assert integer_bit_width("INT8") == 64
    assert create_new_mapping_target_type("Int8", "postgresql") == "SMALLINT"

    assert normalize_logical_type("VARBYTE") == "binary"
    assert create_new_mapping_target_type("VARBYTE", "postgresql") == "BYTEA"
    assert create_new_mapping_target_type("binData", "postgresql") == "BYTEA"
    assert normalize_logical_type("decimal128") == "decimal"
    assert create_new_mapping_target_type("decimal128", "postgresql") == "NUMERIC"

    assert is_national_string_carrier("NATIONAL CHARACTER VARYING(50)") is True
    assert is_lossy_coercion("NATIONAL CHARACTER VARYING(50)", "VARCHAR(50)") is True
    assert create_new_mapping_target_type("CHAR(10)", "sqlserver") == "CHAR(10)"
    assert national_charset_would_invent("CHAR(10)", "NCHAR(10)") is True
    assert is_lossy_coercion("CHAR(10)", "NCHAR(10)") is True

    assert create_new_mapping_target_type("SYSNAME", "sqlserver") == "NVARCHAR(128)"
    assert normalize_logical_type("Nullable(Int32)") == "integer"
    assert create_new_mapping_target_type("Nullable(Int32)", "postgresql") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "azure-sql") == "INT"


def test_aurora_clickhouse_unsigned_wave24():
    """Aurora/pgbouncer aliases, ClickHouse Enum/Nothing, UInt signed invent."""
    from services.type_system import (
        create_new_mapping_target_type,
        is_lossy_coercion,
        specialty_carrier_base,
        unsigned_signed_polarity_invent,
    )

    assert create_new_mapping_target_type("INTEGER", "aurora_postgres") == "INTEGER"
    assert create_new_mapping_target_type("DATE", "pgbouncer") == "DATE"
    assert create_new_mapping_target_type("INTEGER", "aurora_mysql") == "INT"
    assert create_new_mapping_target_type("INTEGER", "singlestore") == "INT"
    assert create_new_mapping_target_type("BOOLEAN", "memsql") == "BOOLEAN"

    assert specialty_carrier_base("Enum8('a'=1)") == "ENUM8"
    assert specialty_carrier_base("Nothing") == "NOTHING"
    assert specialty_carrier_base("Dynamic") == "DYNAMIC"
    assert create_new_mapping_target_type("Enum8('a'=1)", "postgresql") == "TEXT"
    assert is_lossy_coercion("Enum8('a'=1)", "TEXT") is True
    assert is_lossy_coercion("Nothing", "TEXT") is True
    assert is_lossy_coercion("Dynamic", "TEXT") is True

    assert unsigned_signed_polarity_invent("UInt8", "SMALLINT") is True
    assert is_lossy_coercion("UInt8", "SMALLINT") is True
    assert create_new_mapping_target_type("UInt8", "postgresql") == "SMALLINT"


def test_cloud_aliases_uuid_agg_wave25():
    """Cloud SQL/Athena/Hive aliases, UUID physical stamp, AggregateFunction collapse."""
    from services.type_system import (
        create_new_mapping_target_type,
        is_lossy_coercion,
        specialty_carrier_base,
    )

    assert create_new_mapping_target_type("INTEGER", "athena") == "integer"
    assert create_new_mapping_target_type("INTEGER", "hive") == "INT"
    assert create_new_mapping_target_type("INTEGER", "redshift_serverless") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "snowflake_aws") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "cloudsql_postgres") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "cloudsql_mysql") == "INT"
    assert create_new_mapping_target_type("INTEGER", "rds_mysql") == "INT"
    assert create_new_mapping_target_type("INTEGER", "percona") == "INT"
    assert create_new_mapping_target_type("INTEGER", "spanner") == "INT64"
    assert create_new_mapping_target_type("INTEGER", "cassandra") == "BIGINT"

    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "mysql") == "CHAR(36)"
    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "oracle") == "VARCHAR2(36)"
    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "postgresql") == "UUID"
    assert create_new_mapping_target_type("UNIQUEIDENTIFIER", "sqlserver") == "UNIQUEIDENTIFIER"

    assert specialty_carrier_base("AggregateFunction(sum, UInt64)") == "AGGREGATEFUNCTION"
    assert specialty_carrier_base("SimpleAggregateFunction(sum, UInt64)") == (
        "SIMPLEAGGREGATEFUNCTION"
    )
    assert is_lossy_coercion("AggregateFunction(sum, UInt64)", "TEXT") is True
    assert create_new_mapping_target_type("Bool", "clickhouse") == "Bool"


def test_connector_aliases_identity_ring_decimal_wave26():
    """Wave 26: dest aliases, IDENTITY invent, Ring geometry, Decimal→TEXT honesty."""
    from services.type_system import (
        create_new_mapping_target_type,
        decimal_fixed_point_would_collapse_to_text,
        is_identity_column,
        is_lossy_coercion,
        normalize_logical_type,
        specialty_carrier_base,
    )

    assert create_new_mapping_target_type("INTEGER", "doris") == "INT"
    assert create_new_mapping_target_type("INTEGER", "starrocks") == "INT"
    assert create_new_mapping_target_type("INTEGER", "tidb_cloud") == "INT"
    assert create_new_mapping_target_type("INTEGER", "oceanbase") == "INT"
    assert create_new_mapping_target_type("INTEGER", "polardb") == "INT"
    assert create_new_mapping_target_type("INTEGER", "gaussdb") == "INT"
    assert create_new_mapping_target_type("INTEGER", "openGauss") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "kingbase") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "hologres") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "greenplum_cloud") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "supabase_db") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "neon_serverless") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "motherduck") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "libsql") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "turso") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "bytehouse") == "Int32"
    assert create_new_mapping_target_type("INTEGER", "teradata") == "BIGINT"
    assert create_new_mapping_target_type("INTEGER", "vertica") == "BIGINT"
    assert create_new_mapping_target_type("INTEGER", "materialize") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "risingwave") == "INTEGER"
    assert create_new_mapping_target_type("INTEGER", "dremio") == "integer"
    assert create_new_mapping_target_type("INTEGER", "maxcompute") == "INT"

    assert is_identity_column("IDENTITY(1,1)") is True
    assert normalize_logical_type("IDENTITY(1,1)") == "integer"
    assert create_new_mapping_target_type("IDENTITY(1,1)", "postgresql") == (
        "INTEGER GENERATED BY DEFAULT AS IDENTITY"
    )
    assert is_lossy_coercion(
        "IDENTITY(1,1)", "INTEGER GENERATED BY DEFAULT AS IDENTITY"
    ) is False
    assert is_lossy_coercion("IDENTITY(1,1)", "INTEGER") is True
    assert create_new_mapping_target_type("IDENTITY(1,1)", "mysql") == "INT AUTO_INCREMENT"
    assert create_new_mapping_target_type("IDENTITY(1,1)", "sqlserver") == (
        "INT IDENTITY(1,1)"
    )

    assert specialty_carrier_base("Ring") == "RING"
    assert create_new_mapping_target_type("Ring", "postgresql") == "GEOMETRY"
    assert is_lossy_coercion("Ring", "GEOMETRY") is True
    assert create_new_mapping_target_type("LineString", "postgresql") == "GEOMETRY"

    assert create_new_mapping_target_type("Decimal256(30)", "mysql") == "TEXT"
    assert decimal_fixed_point_would_collapse_to_text("Decimal256(30)", "TEXT") is True
    assert is_lossy_coercion("Decimal256(30)", "TEXT") is True


def test_struct_int64_interval_identity_document_wave17():
    from services.type_system import (
        bfile_locator_would_collapse,
        create_new_mapping_target_type,
        document_domain_would_invent,
        identity_polarity_would_collapse,
        integer_bit_width,
        integer_width_would_narrow,
        interval_family_would_collapse,
        is_lossy_coercion,
        is_nested_shape_collapse,
        normalize_logical_type,
        resolve_mapping_target_type,
    )

    assert is_nested_shape_collapse("STRUCT<a:INET>", "STRUCT<a:TEXT>") is True
    assert is_lossy_coercion("STRUCT<a:INET>", "STRUCT<a:TEXT>") is True

    assert integer_bit_width("INT64") == 64
    assert integer_bit_width("LONG") == 64
    assert integer_width_would_narrow("INT64", "INT") is True
    assert is_lossy_coercion("INT64", "INTEGER") is True
    assert is_lossy_coercion("LONG", "INT") is True

    assert normalize_logical_type("INTERVAL DAY") == "interval"
    assert interval_family_would_collapse("INTERVAL DAY", "INTERVAL YEAR") is True
    assert is_lossy_coercion("INTERVAL DAY", "INTERVAL YEAR") is True

    assert resolve_mapping_target_type(
        {"create_new": True, "target": "u", "target_type": ""},
        source_type="UUID",
        dest_db_type="bigquery",
    ) == create_new_mapping_target_type("UUID", "bigquery")

    assert identity_polarity_would_collapse(
        "INT GENERATED ALWAYS AS IDENTITY", "BIGINT"
    ) is True
    assert is_lossy_coercion("INT GENERATED ALWAYS AS IDENTITY", "BIGINT") is True

    assert document_domain_would_invent("STRING", "JSON") is True
    assert is_lossy_coercion("TEXT", "VARIANT") is True
    assert is_lossy_coercion("STRING", "JSON") is True

    assert bfile_locator_would_collapse("BFILE", "BLOB") is True
    assert is_lossy_coercion("BFILE", "BLOB") is True


def test_document_nested_decimal_bare_wave16():
    from services.type_system import (
        decimal_params_would_narrow,
        document_domain_would_collapse,
        is_lossy_coercion,
        is_nested_shape_collapse,
        is_precision_collapse_coercion,
    )

    assert document_domain_would_collapse("JSON", "STRING") is True
    assert document_domain_would_collapse("VARIANT", "TEXT") is True
    assert is_lossy_coercion("SUPER", "VARCHAR") is True
    assert is_lossy_coercion("JSON", "JSON") is False

    assert is_nested_shape_collapse("ARRAY", "ARRAY<STRING>") is True
    assert is_lossy_coercion("ARRAY<INT>", "ARRAY") is True
    assert is_lossy_coercion("MAP", "MAP<STRING,INT>") is True
    assert is_lossy_coercion("MAP<STRING,INT>", "MAP") is True

    # Unknown dest: bare DECIMAL stays fail-closed collapse.
    assert decimal_params_would_narrow("DECIMAL(38,10)", "DECIMAL") is True
    # MySQL bare DECIMAL invents (10,0) — proven wide (p,s) narrows.
    assert decimal_params_would_narrow(
        "DECIMAL(38,10)", "DECIMAL", dest_db="mysql"
    ) is True
    assert is_lossy_coercion("DECIMAL(10,2)", "NUMERIC") is True
    # PostgreSQL bare NUMERIC/DECIMAL is unconstrained — proven (p,s) widens.
    assert decimal_params_would_narrow(
        "DECIMAL(38,15)", "DECIMAL", dest_db="postgresql"
    ) is False
    assert is_lossy_coercion(
        "DECIMAL(38,15)", "DECIMAL", dest_db="postgresql"
    ) is False
    assert is_precision_collapse_coercion(
        "DECIMAL(38,15)", "NUMERIC", dest_db="postgresql"
    ) is False
    # Snowflake bare NUMBER → (38,0): equal capacity widens; fractional scale narrows.
    assert decimal_params_would_narrow(
        "DECIMAL(38,0)", "NUMBER", dest_db="snowflake"
    ) is False
    assert decimal_params_would_narrow(
        "DECIMAL(10,2)", "NUMBER", dest_db="snowflake"
    ) is True
    # SQL Server bare DECIMAL → (18,0): DECIMAL(10,0) widens; DECIMAL(20,0) narrows.
    assert decimal_params_would_narrow(
        "DECIMAL(10,0)", "DECIMAL", dest_db="sqlserver"
    ) is False
    assert decimal_params_would_narrow(
        "DECIMAL(20,0)", "DECIMAL", dest_db="mssql"
    ) is True
    # BigQuery bare NUMERIC → (38,9): DECIMAL(10,2) widens; DECIMAL(38,15) scale-narrows.
    assert decimal_params_would_narrow(
        "DECIMAL(10,2)", "NUMERIC", dest_db="bigquery"
    ) is False
    assert is_precision_collapse_coercion(
        "DECIMAL(10,2)", "NUMERIC", dest_db="bigquery"
    ) is False
    assert decimal_params_would_narrow(
        "DECIMAL(38,15)", "NUMERIC", dest_db="bigquery"
    ) is True
    # Oracle bare NUMBER is unconstrained — proven (p,s) widens (PG-class).
    assert decimal_params_would_narrow(
        "DECIMAL(38,15)", "NUMBER", dest_db="oracle"
    ) is False
    assert is_precision_collapse_coercion(
        "DECIMAL(38,15)", "NUMBER", dest_db="oracle"
    ) is False


def test_year_invent_geometry_pad_nested_wave15():
    from services.type_coercion_validator import validate_mapping_coercions
    from services.type_system import (
        ddl_type,
        fixed_width_pad_polarity_loss,
        geography_contract_would_collapse,
        is_lossy_coercion,
        is_nested_shape_collapse,
        specialty_carrier_base,
        specialty_carrier_would_collapse,
        year_domain_would_collapse,
    )

    assert year_domain_would_collapse("INTEGER", "YEAR") is True
    assert is_lossy_coercion("SMALLINT", "YEAR") is True
    assert is_lossy_coercion("YEAR", "SMALLINT") is True

    assert geography_contract_would_collapse(
        "GEOMETRY(Polygon,4326)", "GEOMETRY(Point,4326)"
    ) is True
    assert is_lossy_coercion("POINT", "GEOMETRY") is True
    assert is_lossy_coercion("GEOMETRY", "POINT") is True

    assert fixed_width_pad_polarity_loss("CHAR(5)", "VARCHAR(5)") is True
    assert is_lossy_coercion("VARCHAR(5)", "CHAR(5)") is True
    assert is_lossy_coercion("NCHAR(10)", "NVARCHAR(10)") is True

    assert is_nested_shape_collapse("STRUCT<a:INT>", "STRUCT<a:STRING>") is True
    assert is_lossy_coercion("ARRAY<INTEGER>", "ARRAY<STRING>") is True
    assert is_nested_shape_collapse(
        "MAP<STRING,INT>", "MAP<STRING,STRING>"
    ) is True

    assert ddl_type("mysql", "NCHAR(10)") == "NCHAR(10)"
    assert ddl_type("mysql", "NVARCHAR(20)").startswith("NVARCHAR")

    # Dialect range twins are one specialty base — not a collapse.
    assert specialty_carrier_base("DATERANGE") == specialty_carrier_base("RANGE<DATE>")
    assert specialty_carrier_would_collapse("DATERANGE", "RANGE<DATE>") is False

    # BOOLEAN→SMALLINT is allow-listed widen — Map/G3 must not invent warn drift.
    assert is_lossy_coercion("BOOLEAN", "SMALLINT") is False
    issues = validate_mapping_coercions(
        [{"source": "b", "target": "b"}],
        source_types={"b": "BOOLEAN"},
        target_types={"b": "SMALLINT"},
    )
    assert issues == []


def test_set_enum_collation_halfvec_struct_wave14():
    from services.type_coercion_validator import validate_mapping_coercions
    from services.type_system import (
        create_new_mapping_target_type,
        ddl_type,
        is_lossy_coercion,
        nested_struct_fields_incompatible,
    )

    assert is_lossy_coercion("SET('a','b')", "ENUM('a','b')") is True
    assert is_lossy_coercion("ENUM('a')", "SET('a')") is True
    assert is_lossy_coercion(
        "VARCHAR(5) COLLATE utf8_general_ci",
        "VARCHAR(5) COLLATE utf8_bin",
    ) is True
    assert is_lossy_coercion("CITEXT", "TEXT") is True
    assert is_lossy_coercion("MONEY", "SMALLMONEY") is True

    assert nested_struct_fields_incompatible("RECORD", "STRUCT<a:INT>") is True
    assert is_lossy_coercion("RECORD", "STRUCT<a:INT>") is True

    assert ddl_type("postgresql", "HALFVEC(3)") == "halfvec(3)"
    assert create_new_mapping_target_type("HALFVEC(3)", "postgresql") == "halfvec(3)"
    assert ddl_type("postgresql", "SPARSEVEC(8)") == "sparsevec(8)"

    # UUID exact wire — Map/G3 SSOT (not false bounded truncate).
    assert is_lossy_coercion("UUID", "CHAR(36)") is False
    assert is_lossy_coercion("UNIQUEIDENTIFIER", "CHAR(36)") is False
    issues = validate_mapping_coercions(
        mappings=[{"source": "c", "target": "c", "confidence": 0.99}],
        source_types={"c": "UNIQUEIDENTIFIER"},
        target_types={"c": "CHAR(36)"},
        validation_mode="strict",
    )
    assert issues == []


def test_bounded_sink_lob_tier_national_srid_wave13():
    from services.type_system import (
        bounded_string_sink_would_truncate,
        geography_contract_would_collapse,
        is_lossy_coercion,
        national_charset_would_collapse,
        specialty_carrier_would_collapse,
        string_width_would_narrow,
    )

    assert bounded_string_sink_would_truncate("INTEGER", "VARCHAR(1)") is True
    assert is_lossy_coercion("INTEGER", "VARCHAR(1)") is True
    assert is_lossy_coercion("BOOLEAN", "CHAR(1)") is True
    assert is_lossy_coercion("JSON", "VARCHAR(10)") is True
    assert is_lossy_coercion("INTEGER", "TEXT") is False

    assert string_width_would_narrow("LONGTEXT", "TINYTEXT") is True
    assert is_lossy_coercion("LONGTEXT", "MEDIUMTEXT") is True
    assert is_lossy_coercion("LONGBLOB", "TINYBLOB") is True

    assert is_lossy_coercion("JSON", "ARRAY") is True
    assert is_lossy_coercion("VARIANT", "ARRAY<STRING>") is True

    assert national_charset_would_collapse("NVARCHAR(50)", "VARCHAR(50)") is True
    assert is_lossy_coercion("NVARCHAR(50)", "VARCHAR(50)") is True
    # NCHAR↔NVARCHAR changes blank-pad polarity (same as CHAR↔VARCHAR).
    assert is_lossy_coercion("NCHAR(10)", "NVARCHAR(10)") is True

    assert geography_contract_would_collapse(
        "GEOMETRY(POINT,4326)", "GEOMETRY"
    ) is True
    assert is_lossy_coercion("GEOMETRY(POINT,4326)", "GEOMETRY") is True

    assert specialty_carrier_would_collapse("REGCLASS", "TEXT") is True
    assert is_lossy_coercion("NAME", "VARCHAR(32)") is True


def test_g3_same_logical_and_specialty_invent_wave12():
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )
    from services.type_system import (
        interval_family_would_collapse,
        is_lossy_coercion,
        specialty_domain_would_invent,
        vector_dim_mismatch,
        vector_encoding_would_collapse,
    )

    # Map fidelity lossy must also surface in G3 coercion validator.
    # MONEY→DECIMAL(19,4) is the intentional create-new money-scale wire (not lossy).
    for src, tgt in (
        ("YEAR", "SMALLINT"),
        ("BIT(8)", "BYTEA"),
        ("OID", "INTEGER"),
        ("TEXT", "INET"),
        ("TEXT", "TSVECTOR"),
        ("VECTOR", "VECTOR(1536)"),
        ("HALFVEC(3)", "VECTOR(3)"),
        ("INTERVAL", "INTERVAL YEAR TO MONTH"),
    ):
        assert is_lossy_coercion(src, tgt) is True, (src, tgt)
        issues = validate_mapping_coercions(
            mappings=[{"source": "c", "target": "c", "confidence": 0.99}],
            source_types={"c": src},
            target_types={"c": tgt},
            validation_mode="strict",
        )
        assert issues and issues[0]["lossy"] is True, (src, tgt, issues)
    assert is_lossy_coercion("MONEY", "DECIMAL(19,4)", dest_db="postgresql") is False
    assert is_lossy_coercion("MONEY", "SMALLINT") is True

    assert specialty_domain_would_invent("TEXT", "INET") is True
    assert specialty_domain_would_invent("JSON", "HSTORE") is True
    assert vector_dim_mismatch("VECTOR", "VECTOR(1536)") is True
    assert vector_encoding_would_collapse("HALFVEC(3)", "VECTOR(3)") is True
    assert interval_family_would_collapse("INTERVAL", "INTERVAL DAY TO SECOND") is True
    from services.type_system import normalize_logical_type

    assert normalize_logical_type("FLOAT UNSIGNED") == "float"
    assert normalize_logical_type("DECIMAL(10,2) UNSIGNED") == "decimal"


def test_integer_float_specialty_vector_honesty_wave11():
    from services.type_system import (
        case_fold_polarity_invent,
        date_to_tz_aware_invent,
        float_mantissa_would_narrow,
        integer_width_would_narrow,
        is_lossy_coercion,
        specialty_polarity_mismatch,
        vector_dim_mismatch,
    )

    assert integer_width_would_narrow("BIGINT", "INTEGER") is True
    assert is_lossy_coercion("BIGINT", "INTEGER") is True
    assert is_lossy_coercion("INTEGER", "SMALLINT") is True
    assert is_lossy_coercion("INTEGER", "BIGINT") is False

    assert float_mantissa_would_narrow("DOUBLE", "REAL") is True
    assert is_lossy_coercion("DOUBLE PRECISION", "FLOAT") is True
    assert is_lossy_coercion("REAL", "DOUBLE") is False

    assert specialty_polarity_mismatch("INET", "CIDR") is True
    assert is_lossy_coercion("INET", "CIDR") is True
    assert is_lossy_coercion("MACADDR", "MACADDR8") is True

    assert vector_dim_mismatch("VECTOR(1536)", "VECTOR(768)") is True
    assert is_lossy_coercion("VECTOR(1536)", "VECTOR(768)") is True
    assert is_lossy_coercion("VECTOR(1536)", "FLOAT[]") is True

    assert case_fold_polarity_invent("TEXT", "CITEXT") is True
    assert is_lossy_coercion("TEXT", "CITEXT") is True

    assert date_to_tz_aware_invent("DATE", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("DATE", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("DATE", "TIMESTAMP") is False


def test_bare_datetime_and_time_tz_invent_are_lossy():
    from services.type_system import (
        create_new_mapping_target_type,
        is_lossy_coercion,
        is_nested_document_collapse,
        is_timezone_polarity_loss,
        temporal_precision_would_narrow,
        time_timezone_polarity_loss,
    )

    assert is_timezone_polarity_loss("DATETIME", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("DATETIME", "TIMESTAMPTZ") is True
    assert is_lossy_coercion("TIMESTAMP", "DATETIMEOFFSET") is True
    assert time_timezone_polarity_loss("TIME", "TIMETZ") is True
    assert is_lossy_coercion("TIME", "TIMETZ") is True

    assert is_nested_document_collapse("STRUCT<a:INT>", "VARCHAR") is True
    assert is_lossy_coercion("STRUCT<a:INT>", "TEXT") is True
    assert is_lossy_coercion("ARRAY<INTEGER>", "VARCHAR") is True

    # Open string → closed ENUM needs Accept risk (not write-only quarantine).
    assert is_lossy_coercion("VARCHAR", "ENUM('a','b')") is True

    # Create-new specialty stamps physical off-engine sink.
    stamped = create_new_mapping_target_type("INET", "snowflake")
    assert stamped.upper() in {"VARCHAR", "TEXT", "STRING"} or "VARCHAR" in stamped.upper()
    assert stamped.upper() != "INET"

    assert temporal_precision_would_narrow("TIME(6)", "TIME") is True
