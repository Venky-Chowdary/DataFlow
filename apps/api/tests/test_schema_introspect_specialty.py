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


def test_pg_preserves_decimal_precision_scale():
    assert _pg_to_logical("numeric(38,18)") == "DECIMAL(38,18)"
    assert _pg_to_logical("decimal(20,4)") == "DECIMAL(20,4)"
    assert _pg_to_logical("numeric") == "DECIMAL"


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
