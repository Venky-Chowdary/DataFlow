"""Map≡CREATE — BigQuery writer must use materialize_dest_ddl SSOT.

Headline invent: Map TIMESTAMP → SchemaField DATETIME via blind ddl_type
(wall-clock instant polarity invent). Foreign REAL/DATETIME2 already legalize
via materialize; writer must not re-invent.
"""

from __future__ import annotations

from types import SimpleNamespace


from connectors.bigquery_writer import (
    bq_schema_field,
    bq_type,
    resolve_bigquery_decimal_target_types,
)
from services.type_system import materialize_dest_ddl


def test_bq_type_honors_map_timestamp_not_datetime_invent():
    """Approved Map TIMESTAMP must CREATE as TIMESTAMP — never DATETIME."""
    assert materialize_dest_ddl("bigquery", "TIMESTAMP") == "TIMESTAMP"
    assert bq_type("TIMESTAMP") == "TIMESTAMP"


def test_bq_type_foreign_temporals_match_materialize_field_type():
    assert bq_type("DATETIME2") == "DATETIME"
    assert bq_type("TIMESTAMP_NTZ") == "DATETIME"
    assert bq_type("TIMESTAMPTZ") == "TIMESTAMP"
    assert bq_type("DATETIME") == "DATETIME"


def test_bq_type_foreign_floats_match_materialize_field_type():
    assert bq_type("REAL") == "FLOAT64"
    assert bq_type("FLOAT4") == "FLOAT64"
    assert bq_type("DOUBLE") == "FLOAT64"


def test_bq_type_boolean_and_bit_match_materialize():
    assert bq_type("BIT") == "BOOL"
    assert bq_type("BOOLEAN") == "BOOL"


def test_bq_schema_field_timestamp_stamp():
    bq = SimpleNamespace(
        SchemaField=lambda name, field_type, **kwargs: SimpleNamespace(
            args=(name, field_type), kwargs=kwargs
        )
    )
    field = bq_schema_field(bq, "ts", "TIMESTAMP")
    assert field.args == ("ts", "TIMESTAMP")


def test_resolve_bigquery_types_honors_timestamp_stamp():
    types = resolve_bigquery_decimal_target_types(["ts"], ["TIMESTAMP"])
    assert types == ["TIMESTAMP"]


def test_resolve_bigquery_types_foreign_datetime2():
    types = resolve_bigquery_decimal_target_types(["ts"], ["DATETIME2"])
    assert types == ["DATETIME"]


def test_bq_keeps_explicit_numeric_typmod_map_authority():
    """NUMERIC(10,2) is valid BQ Map stamp — writer must not invent bare family."""
    assert bq_type("NUMERIC(10,2)") == "NUMERIC"
    # materialize must not rewrite parameterized NUMERIC to BIGNUMERIC family.
    assert materialize_dest_ddl("bigquery", "NUMERIC(10,2)").upper().startswith("NUMERIC")


def test_bq_type_varchar_rematerializes_to_string():
    assert bq_type("VARCHAR(32)") == "STRING"
    assert bq_type("INTEGER") == "INT64"
