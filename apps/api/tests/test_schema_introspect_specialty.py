"""Enterprise introspect fidelity for specialty + parametric types.

Proves INTERVAL / GEOGRAPHY / VECTOR / DECIMAL(p,s) survive logical mapping
from PostgreSQL ``format_type``, MySQL ``column_type``, and Snowflake DDL —
never silently collapsed to TEXT when the engine declared a richer type.
"""

from __future__ import annotations

from services.schema_introspect import _mysql_to_logical, _pg_to_logical, _sf_to_logical


def test_pg_preserves_interval_geography_vector_dims():
    assert _pg_to_logical("interval") == "INTERVAL"
    assert _pg_to_logical("interval day to second") == "INTERVAL"
    assert _pg_to_logical("geometry") == "GEOGRAPHY"
    assert _pg_to_logical("geography(Point,4326)") == "GEOGRAPHY"
    assert _pg_to_logical("vector(1536)") == "VECTOR(1536)"
    assert _pg_to_logical("halfvec(768)") == "VECTOR(768)"
    assert _pg_to_logical("vector") == "VECTOR"


def test_pg_redshift_super_varbyte_not_collapsed_to_text():
    assert _pg_to_logical("super") == "JSON"
    assert _pg_to_logical("varbyte") == "BINARY"
    assert _pg_to_logical("varbyte(65535)") == "BINARY"


def test_pg_preserves_decimal_precision_scale():
    assert _pg_to_logical("numeric(38,18)") == "DECIMAL(38,18)"
    assert _pg_to_logical("decimal(20,4)") == "DECIMAL(20,4)"
    assert _pg_to_logical("numeric") == "DECIMAL"


def test_pg_mysql_bq_sf_preserve_ieee_float():
    from services.schema_introspect import _bq_to_logical, _sample_logical_type

    assert _pg_to_logical("double precision") == "FLOAT"
    assert _pg_to_logical("real") == "FLOAT"
    assert _pg_to_logical("float8") == "FLOAT"
    assert _pg_to_logical("numeric") == "DECIMAL"
    assert _mysql_to_logical("double") == "FLOAT"
    assert _mysql_to_logical("float") == "FLOAT"
    assert _mysql_to_logical("decimal(10,2)") == "DECIMAL(10,2)"
    assert _bq_to_logical("FLOAT64") == "FLOAT"
    assert _bq_to_logical("BIGNUMERIC") == "DECIMAL"
    assert _sf_to_logical("FLOAT") == "FLOAT"
    assert _sf_to_logical("FLOAT8") == "FLOAT"
    assert _sf_to_logical("NUMBER(38,10)") == "DECIMAL(38,10)"
    assert _sample_logical_type(1.5) == "FLOAT"
    assert _sample_logical_type(3) == "INTEGER"


def test_pg_does_not_classify_interval_as_text():
    assert _pg_to_logical("interval") != "TEXT"
    assert _pg_to_logical("user-defined") == "TEXT"  # unknown UDT stays text


def test_mysql_preserves_decimal_and_geometry():
    assert _mysql_to_logical("decimal(28,12)") == "DECIMAL(28,12)"
    assert _mysql_to_logical("numeric(10,2)") == "DECIMAL(10,2)"
    assert _mysql_to_logical("geometry") == "GEOGRAPHY"
    assert _mysql_to_logical("point") == "GEOGRAPHY"
    assert _mysql_to_logical("tinyint(1)") == "BOOLEAN"


def test_snowflake_preserves_vector_and_number():
    assert _sf_to_logical("VECTOR(FLOAT, 768)") == "VECTOR(FLOAT, 768)"
    assert _sf_to_logical("GEOGRAPHY") == "GEOGRAPHY"
    # Wide zero-scale stays DECIMAL — never signed BIGINT overflow.
    assert _sf_to_logical("NUMBER(38,0)") == "DECIMAL(38,0)"
    assert _sf_to_logical("NUMBER(38,18)") == "DECIMAL(38,18)"


def test_oracle_preserves_number_scale_and_ieee_float():
    from services.schema_introspect import _oracle_to_logical

    assert _oracle_to_logical("NUMBER(12,4)") == "DECIMAL(12,4)"
    assert _oracle_to_logical("NUMBER(38,0)") == "DECIMAL(38,0)"
    assert _oracle_to_logical("NUMBER(10,0)") == "INTEGER"
    assert _oracle_to_logical("BINARY_DOUBLE") == "FLOAT"
    assert _oracle_to_logical("BINARY_FLOAT") == "FLOAT"
    assert _oracle_to_logical("FLOAT") == "FLOAT"
    assert _oracle_to_logical("DATE") == "TIMESTAMP"  # Oracle DATE is datetime
    assert _oracle_to_logical("INTERVAL DAY TO SECOND") == "INTERVAL"
    assert _oracle_to_logical("SDO_GEOMETRY") == "GEOGRAPHY"
    assert _oracle_to_logical("VARCHAR2") == "TEXT"


def test_sqlserver_preserves_decimal_and_float():
    from services.schema_introspect import _sqlserver_to_logical

    assert _sqlserver_to_logical("decimal(18,4)") == "DECIMAL(18,4)"
    assert _sqlserver_to_logical("numeric(10,0)") == "INTEGER"
    assert _sqlserver_to_logical("float") == "FLOAT"
    assert _sqlserver_to_logical("real") == "FLOAT"
    assert _sqlserver_to_logical("money") == "DECIMAL(19,4)"
    assert _sqlserver_to_logical("bit") == "BOOLEAN"
    assert _sqlserver_to_logical("uniqueidentifier") == "UUID"
    assert _sqlserver_to_logical("datetime2") == "TIMESTAMP_NTZ"
    assert _sqlserver_to_logical("geography") == "GEOGRAPHY"
    assert _sqlserver_to_logical("varbinary") == "BINARY"
    assert _sqlserver_to_logical("json") == "JSON"


def test_mysql_bigint_unsigned_not_signed_integer():
    """UNSIGNED BIGINT exceeds signed 64-bit — DECIMAL carrier (Airbyte-class fidelity)."""
    from services.type_system import ddl_type, normalize_logical_type

    for raw in ("bigint unsigned", "bigint(20) unsigned", "BIGINT UNSIGNED"):
        logical = _mysql_to_logical(raw)
        assert logical == "BIGINT UNSIGNED", raw
        assert normalize_logical_type(logical) == "decimal"
        pg_ddl = ddl_type("postgresql", logical).upper()
        assert "NUMERIC" in pg_ddl or "DECIMAL" in pg_ddl
        assert ddl_type("snowflake", logical).upper().startswith("NUMBER")


def test_bq_sf_specialty_carriers_not_text():
    from services.schema_introspect import _bq_to_logical

    assert _bq_to_logical("BYTES") == "BINARY"
    assert _bq_to_logical("JSON") == "JSON"
    assert _bq_to_logical("GEOGRAPHY") == "GEOGRAPHY"
    assert _bq_to_logical("TIME") == "TIME"
    assert _bq_to_logical("DATETIME") == "TIMESTAMP_NTZ"
    assert _bq_to_logical("TIMESTAMP") == "TIMESTAMPTZ"
    assert _bq_to_logical("RECORD") == "JSON"
    assert _bq_to_logical("NUMERIC", precision=38, scale=9) == "DECIMAL(38,9)"
    assert _sf_to_logical("VARIANT") == "JSON"
    assert _sf_to_logical("OBJECT") == "JSON"
    assert _sf_to_logical("ARRAY") == "ARRAY"
    assert _sf_to_logical("BINARY") == "BINARY"
    assert _sf_to_logical("TIME") == "TIME"
    assert _sf_to_logical("TIMESTAMP_TZ") == "TIMESTAMPTZ"
    assert _sf_to_logical("TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"


def test_tz_polarity_introspect_and_ddl():
    """TIMESTAMPTZ vs TIMESTAMP must survive introspect → create-new DDL."""
    from services.schema_introspect import (
        _mysql_to_logical,
        _oracle_to_logical,
        _pg_to_logical,
        _sqlserver_to_logical,
    )
    from services.type_system import ddl_type

    assert _pg_to_logical("timestamp with time zone") == "TIMESTAMPTZ"
    assert _pg_to_logical("timestamp without time zone") == "TIMESTAMP_NTZ"
    assert _pg_to_logical("timestamptz") == "TIMESTAMPTZ"
    assert _mysql_to_logical("timestamp") == "TIMESTAMPTZ"
    assert _mysql_to_logical("datetime") == "TIMESTAMP_NTZ"
    assert _mysql_to_logical("datetime(6)") == "TIMESTAMP_NTZ"
    assert _sqlserver_to_logical("datetimeoffset") == "TIMESTAMPTZ"
    assert _sqlserver_to_logical("datetime2") == "TIMESTAMP_NTZ"
    assert _oracle_to_logical("TIMESTAMP WITH TIME ZONE") == "TIMESTAMPTZ"
    assert _oracle_to_logical("TIMESTAMP(6)") == "TIMESTAMP_NTZ"

    assert ddl_type("postgresql", "TIMESTAMPTZ") == "TIMESTAMPTZ"
    assert ddl_type("postgresql", "TIMESTAMP_NTZ") == "TIMESTAMP"
    # Ambiguous bare TIMESTAMP keeps PG platform default (TIMESTAMPTZ).
    assert ddl_type("postgresql", "TIMESTAMP") == "TIMESTAMPTZ"
    assert ddl_type("snowflake", "TIMESTAMPTZ") == "TIMESTAMP_TZ"
    assert ddl_type("snowflake", "TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"
    assert ddl_type("mysql", "TIMESTAMPTZ").upper().startswith("TIMESTAMP")
    assert ddl_type("mysql", "TIMESTAMP_NTZ").upper().startswith("DATETIME")
    assert ddl_type("redshift", "TIMESTAMPTZ") == "TIMESTAMPTZ"
    assert ddl_type("redshift", "TIMESTAMP_NTZ") == "TIMESTAMP"
    assert ddl_type("bigquery", "TIMESTAMPTZ") == "TIMESTAMP"
    assert ddl_type("bigquery", "TIMESTAMP_NTZ") == "DATETIME"
    assert ddl_type("sqlserver", "TIMESTAMPTZ") == "DATETIMEOFFSET"
    assert ddl_type("sqlserver", "TIMESTAMP_NTZ") == "DATETIME2"


def test_pg_hstore_point_not_text():
    assert _pg_to_logical("hstore") == "JSON"
    assert _pg_to_logical("point") == "GEOGRAPHY"


def test_elasticsearch_decimal_ddl_is_honest_keyword():
    """scaled_float without scaling_factor is invalid — keyword preserves decimal strings."""
    from services.type_system import ddl_type

    assert ddl_type("elasticsearch", "DECIMAL") == "keyword"
    assert ddl_type("elasticsearch", "FLOAT") == "double"


def test_sqlalchemy_float_not_collapsed_to_decimal():
    """generic_sql reflection must not rewrite IEEE float as decimal."""
    import sqlalchemy as sa

    from connectors.generic_sql import _logical_type_from_sa

    assert _logical_type_from_sa(sa.Float()) == "float"
    assert _logical_type_from_sa(sa.Double()) == "float"
    assert _logical_type_from_sa(sa.Numeric(12, 4)) == "DECIMAL(12,4)"
    assert _logical_type_from_sa(sa.Numeric(10, 0)) == "integer"