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
    assert _sf_to_logical("NUMBER(38,0)") == "INTEGER"
    assert _sf_to_logical("NUMBER(38,18)") == "DECIMAL(38,18)"


def test_oracle_preserves_number_scale_and_ieee_float():
    from services.schema_introspect import _oracle_to_logical

    assert _oracle_to_logical("NUMBER(12,4)") == "DECIMAL(12,4)"
    assert _oracle_to_logical("NUMBER(38,0)") == "INTEGER"
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
    assert _sqlserver_to_logical("datetime2") == "TIMESTAMP"
    assert _sqlserver_to_logical("geography") == "GEOGRAPHY"
    assert _sqlserver_to_logical("varbinary") == "BINARY"


def test_sqlalchemy_float_not_collapsed_to_decimal():
    """generic_sql reflection must not rewrite IEEE float as decimal."""
    import sqlalchemy as sa

    from connectors.generic_sql import _logical_type_from_sa

    assert _logical_type_from_sa(sa.Float()) == "float"
    assert _logical_type_from_sa(sa.Double()) == "float"
    assert _logical_type_from_sa(sa.Numeric(12, 4)) == "DECIMAL(12,4)"
    assert _logical_type_from_sa(sa.Numeric(10, 0)) == "integer"