"""Universal type-system tests."""

from services.type_system import ddl_type, normalize_logical_type


def test_normalize_common_source_types():
    assert normalize_logical_type("VARCHAR") == "string"
    assert normalize_logical_type("VARCHAR(255)") == "string"
    assert normalize_logical_type("TIMESTAMP_TZ") == "datetime"
    assert normalize_logical_type("object") == "json"
    assert normalize_logical_type("BYTEA") == "binary"


def test_oracle_sdo_geometry_normalizes_to_geography():
    """DDL carriers must stay geography — never collapse to string (Map/SA bind)."""
    from services.type_system import is_lossy_coercion

    for raw in (
        "SDO_GEOMETRY",
        "MDSYS.SDO_GEOMETRY",
        "sdo_geometry",
        "ST_GEOMETRY",
    ):
        assert normalize_logical_type(raw) == "geography", raw
    assert ddl_type("oracle", "geography") == "SDO_GEOMETRY"
    # GEOGRAPHY → SDO invents Oracle opaque polarity — Accept risk (not silent identity).
    assert is_lossy_coercion("geography", "SDO_GEOMETRY") is True
    assert is_lossy_coercion("geography", "VARCHAR2") is True


def test_type_system_redshift_ddl():
    # Bare / ambiguous integer invents 64-bit (DDL_TYPES / never-narrower).
    # Unambiguous INT4 stays width-preserving.
    assert ddl_type("redshift", "integer") == "BIGINT"
    assert ddl_type("redshift", "INTEGER") == "BIGINT"
    assert ddl_type("redshift", "INT4") == "INTEGER"
    assert ddl_type("redshift", "json") == "SUPER"
    assert ddl_type("postgresql", "JSON") == "JSONB"
    assert ddl_type("snowflake", "ARRAY") == "VARIANT"
    assert ddl_type("mysql", "UUID") == "CHAR(36)"
    assert ddl_type("bigquery", "BINARY") == "BYTES"
    assert ddl_type("unknown", "DECIMAL") == "TEXT"


def test_type_system_lakehouse_ddl():
    # Bare / ambiguous → 64-bit; unambiguous INT4 stays INT/int.
    assert ddl_type("databricks", "integer") == "BIGINT"
    assert ddl_type("databricks", "INTEGER") == "BIGINT"
    assert ddl_type("databricks", "INT4") == "INT"
    assert ddl_type("databricks", "json") == "STRING"
    assert ddl_type("delta", "TIMESTAMP") == "TIMESTAMP_NTZ"
    assert ddl_type("iceberg", "integer") == "long"
    assert ddl_type("iceberg", "INTEGER") == "long"
    assert ddl_type("iceberg", "INT4") == "int"
    assert ddl_type("apache_iceberg", "json") == "string"
    assert ddl_type("iceberg", "UUID") == "uuid"
    assert ddl_type("unity_catalog", "DECIMAL") == "DECIMAL(38,10)"


def test_decimal_precision_propagated_not_truncated():
    """Oracle NUMBER(38,18) → MySQL must keep scale 18 (was hardcoded DECIMAL(38,15))."""
    from services.type_system import decimal_scale_would_truncate

    assert ddl_type("mysql", "NUMBER(38,18)") == "DECIMAL(38,18)"
    assert ddl_type("snowflake", "DECIMAL(28,12)") == "NUMBER(28,12)"
    assert ddl_type("sqlserver", "NUMERIC(20,8)") == "DECIMAL(20,8)"
    # PostgreSQL / BigQuery must keep source (p,s) — never bare NUMERIC / BIGNUMERIC.
    assert ddl_type("postgresql", "DECIMAL(12,4)") == "NUMERIC(12,4)"
    assert ddl_type("postgresql", "NUMERIC(18,2)") == "NUMERIC(18,2)"
    assert ddl_type("bigquery", "DECIMAL(20,6)") == "BIGNUMERIC(20,6)"
    from services.type_system import ddl_carrier_type

    assert ddl_carrier_type("DECIMAL(12,4)") == "DECIMAL(12,4)"
    assert ddl_carrier_type("numeric(12,4)") == "DECIMAL(12,4)"
    # UNSIGNED / specialty must not collapse before create-new risk stamping.
    assert ddl_carrier_type("INT UNSIGNED") == "INT UNSIGNED"
    assert ddl_carrier_type("BIGINT UNSIGNED") == "BIGINT UNSIGNED"
    assert ddl_carrier_type("INET") == "INET"
    assert ddl_carrier_type("OBJECTID") == "OBJECTID"
    assert ddl_carrier_type("UInt8") == "UInt8"
    assert ddl_carrier_type("HALFVEC(3)") == "HALFVEC(3)"
    assert ddl_carrier_type("SPARSEVEC(16)") == "SPARSEVEC(16)"
    assert ddl_carrier_type("VECTOR(768)") == "VECTOR(768)"
    # Scale beyond MySQL cap (30) → lossless TEXT, never silent truncate
    assert ddl_type("mysql", "NUMBER(38,31)") == "TEXT"
    assert decimal_scale_would_truncate("NUMBER(38,31)", "mysql") is True
    assert decimal_scale_would_truncate("NUMBER(38,18)", "mysql") is False


def test_studio_file_export_formats_keep_the_source_class():
    """``json`` / ``csv`` are Studio dest ids, not unknown dialects that invent TEXT."""
    from services.type_system import destination_is_file_export

    for dest in ("json", "csv", "parquet", "file_export", "jsonl", "xlsx", "s3"):
        assert destination_is_file_export(dest) is True, dest
        # Ambiguous INTEGER invents 64-bit on every dest — file export included.
        assert ddl_type(dest, "INTEGER") == "BIGINT", dest
        assert ddl_type(dest, "BIGINT") == "BIGINT", dest
        assert ddl_type(dest, "DATE") == "DATE", dest
        assert ddl_type(dest, "DECIMAL(10,2)") == "DECIMAL(38,2)", dest
        assert ddl_type(dest, "VARCHAR(50)") == "VARCHAR", dest
    assert destination_is_file_export("mysql") is False
    assert destination_is_file_export("postgresql") is False


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


def test_ddl_float_is_not_rewritten_to_fixed_point():
    from services.type_system import is_lossy_coercion

    assert normalize_logical_type("FLOAT") == "float"
    assert normalize_logical_type("DOUBLE PRECISION") == "float"
    assert ddl_type("postgresql", "FLOAT") == "DOUBLE PRECISION"
    assert ddl_type("snowflake", "FLOAT") == "FLOAT"
    assert ddl_type("bigquery", "DOUBLE") == "FLOAT64"
    assert "NUMBER(38,10)" not in ddl_type("snowflake", "FLOAT")
    assert is_lossy_coercion("float", "integer") is True
    # Large ints lose precision in IEEE float mantissa — Accept risk required.
    assert is_lossy_coercion("integer", "float") is True
    assert is_lossy_coercion("float", "decimal") is True
    assert is_lossy_coercion("float", "string") is False
