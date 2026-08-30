"""Mongo numeric carriers come from BSON, not from a stringified sample.

A schemaless sample bounds only the values it saw. Profiling the first 100
documents of a collection whose ``amount`` starts near zero stamped
``DECIMAL(3,2)`` as the *source* type, create-new built the PostgreSQL column
from it, and every later row at ``10.00`` quarantined: a live 100k
Mongo→PostgreSQL CDC snapshot landed 999 of 100,000 rows while the run reported
success. The stored BSON type is the declared domain, so it owns the carrier.
"""

from services.schema_introspect import (
    mongodb_bson_column_types,
    prefer_bson_numeric_carrier,
)


def test_bson_double_beats_sample_sized_decimal():
    docs = [{"id": i, "amount": i / 100.0} for i in range(1, 101)]
    bson_types = mongodb_bson_column_types(docs)
    assert bson_types["amount"] == "FLOAT"
    sampled = {"id": "INTEGER", "amount": "DECIMAL(3,2)"}
    merged = prefer_bson_numeric_carrier(sampled, bson_types)
    assert merged["amount"] == "FLOAT"


def test_decimal128_keeps_the_observed_size():
    from bson.decimal128 import Decimal128

    docs = [{"amount": Decimal128("0.01")}, {"amount": Decimal128("12345.6789")}]
    bson_types = mongodb_bson_column_types(docs)
    assert bson_types["amount"] == "DECIMAL"
    # Decimal128 cells really are decimal, so observed digits refine the right
    # family — replacing DECIMAL(10,2) with unsized DECIMAL would only widen the
    # carrier and trip the destination fidelity gate.
    merged = prefer_bson_numeric_carrier({"amount": "DECIMAL(10,2)"}, bson_types)
    assert merged["amount"] == "DECIMAL(10,2)"


def test_non_numeric_and_unknown_stamps_are_left_alone():
    docs = [{"note": "hello", "amount": 1.5}]
    bson_types = mongodb_bson_column_types(docs)
    sampled = {"note": "VARCHAR(5)", "updated_at": "TIMESTAMPTZ"}
    merged = prefer_bson_numeric_carrier(sampled, bson_types)
    # A text carrier keeps the sample's measured width, and a column the sample
    # never typed is not invented from BSON.
    assert merged == {"note": "VARCHAR(5)", "updated_at": "TIMESTAMPTZ"}


def test_profiling_never_invents_fixed_point_off_a_declared_float():
    from services.data_profiler import merge_profiler_schema

    merged = merge_profiler_schema(
        {"amt_float": "FLOAT", "note": "TEXT"},
        {"amt_float": "DECIMAL(7,3)", "note": "VARCHAR"},
        authoritative_existing=False,
    )
    assert merged["amt_float"] == "FLOAT"


def test_profiling_still_upgrades_an_untyped_carrier_to_decimal():
    from services.data_profiler import merge_profiler_schema

    merged = merge_profiler_schema(
        {"amount": "VARCHAR"},
        {"amount": "DECIMAL(7,3)"},
        authoritative_existing=False,
    )
    assert merged["amount"] == "DECIMAL(7,3)"


def test_text_sentinel_among_doubles_keeps_typed_majority():
    docs = [{"amount": float(i)} for i in range(50)] + [{"amount": "N/A"}]
    assert mongodb_bson_column_types(docs)["amount"] == "FLOAT"
