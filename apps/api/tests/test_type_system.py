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
