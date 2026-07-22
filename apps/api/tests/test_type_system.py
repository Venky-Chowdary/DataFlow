"""Universal type-system tests."""

from services.type_system import ddl_type, normalize_logical_type


def test_normalize_common_source_types():
    assert normalize_logical_type("VARCHAR") == "string"
    assert normalize_logical_type("VARCHAR(255)") == "string"
    assert normalize_logical_type("TIMESTAMP_TZ") == "datetime"
    assert normalize_logical_type("object") == "json"
    assert normalize_logical_type("BYTEA") == "binary"


def test_type_system_redshift_ddl():
    assert ddl_type("redshift", "integer") == "BIGINT"
    assert ddl_type("redshift", "json") == "SUPER"
    assert ddl_type("postgresql", "JSON") == "JSONB"
    assert ddl_type("snowflake", "ARRAY") == "VARIANT"
    assert ddl_type("mysql", "UUID") == "CHAR(36)"
    assert ddl_type("bigquery", "BINARY") == "BYTES"
    assert ddl_type("unknown", "DECIMAL") == "TEXT"


def test_type_system_lakehouse_ddl():
    assert ddl_type("databricks", "integer") == "BIGINT"
    assert ddl_type("databricks", "json") == "STRING"
    assert ddl_type("delta", "TIMESTAMP") == "TIMESTAMP"
    assert ddl_type("iceberg", "integer") == "long"
    assert ddl_type("apache_iceberg", "json") == "string"
    assert ddl_type("iceberg", "UUID") == "uuid"
    assert ddl_type("unity_catalog", "DECIMAL") == "DECIMAL(38,10)"


def test_decimal_precision_propagated_not_truncated():
    """Oracle NUMBER(38,18) → MySQL must keep scale 18 (was hardcoded DECIMAL(38,15))."""
    from services.type_system import decimal_scale_would_truncate

    assert ddl_type("mysql", "NUMBER(38,18)") == "DECIMAL(38,18)"
    assert ddl_type("snowflake", "DECIMAL(28,12)") == "NUMBER(28,12)"
    assert ddl_type("sqlserver", "NUMERIC(20,8)") == "DECIMAL(20,8)"
    # Scale beyond MySQL cap (30) → lossless TEXT, never silent truncate
    assert ddl_type("mysql", "NUMBER(38,31)") == "TEXT"
    assert decimal_scale_would_truncate("NUMBER(38,31)", "mysql") is True
    assert decimal_scale_would_truncate("NUMBER(38,18)", "mysql") is False


def test_vector_dimension_propagated_never_invented():
    """VECTOR dims flow like DECIMAL(p,s); bare VECTOR must not invent 1536."""
    from services.type_system import (
        normalize_logical_type,
        parse_vector_dimension,
        vector_dim_mismatch,
        vector_dim_unknown_for_native,
    )

    assert normalize_logical_type("VECTOR(FLOAT, 768)") == "vector"
    assert parse_vector_dimension("VECTOR(1536)") == 1536
    assert parse_vector_dimension("VECTOR(FLOAT, 768)") == 768
    assert parse_vector_dimension("HALFVEC(384)") == 384
    assert parse_vector_dimension("VECTOR") is None

    assert ddl_type("postgresql", "VECTOR(1536)") == "vector(1536)"
    assert ddl_type("snowflake", "HALFVEC(768)") == "VECTOR(FLOAT, 768)"
    # No invented default dimension on either native engine.
    assert ddl_type("postgresql", "VECTOR") == "TEXT"
    assert ddl_type("snowflake", "VECTOR") == "VARCHAR"
    assert "1536" not in ddl_type("snowflake", "VECTOR")

    assert vector_dim_mismatch("VECTOR(768)", "VECTOR(FLOAT, 1536)") is True
    assert vector_dim_mismatch("VECTOR(768)", "VECTOR(FLOAT, 768)") is False
    assert vector_dim_unknown_for_native("VECTOR", "postgresql") is True
    assert vector_dim_unknown_for_native("VECTOR(768)", "postgresql") is False
    assert vector_dim_unknown_for_native("VECTOR", "mysql") is False
