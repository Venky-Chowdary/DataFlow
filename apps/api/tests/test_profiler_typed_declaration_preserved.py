"""Profiling stringified samples must never erase a typed declaration.

The profiler reads ``cell_to_string`` output, so a BSON ``ObjectId`` reaches it
as ``"6991173f8d64fcf16f3a0805"`` and a ``Decimal128`` as ``"12.34"``. Letting
that inference win demoted ``OBJECTID`` to ``VARCHAR``, which made create-new
invent a ``TEXT`` column and then report its own invent as an ``OBJECTID → TEXT``
fidelity collapse at write — Validate green, Execute blocked.
"""

from __future__ import annotations

from services.data_profiler import merge_profiler_schema


def test_objectid_declaration_survives_string_inference():
    merged = merge_profiler_schema({"_id": "OBJECTID"}, {"_id": "VARCHAR"})
    assert merged["_id"] == "OBJECTID"


def test_json_and_binary_declarations_survive_string_inference():
    merged = merge_profiler_schema(
        {"payload": "JSON", "blob": "BINARY", "uid": "UUID"},
        {"payload": "VARCHAR", "blob": "TEXT", "uid": "STRING"},
    )
    assert merged == {"payload": "JSON", "blob": "BINARY", "uid": "UUID"}


def test_decimal_declaration_survives_string_inference():
    merged = merge_profiler_schema({"amount": "DECIMAL(18,4)"}, {"amount": "VARCHAR"})
    assert merged["amount"] == "DECIMAL(18,4)"


def test_bare_decimal_survives_integer_sample_inference():
    """DynamoDB N / Decimal128 must not collapse to INTEGER from samples ``1``, ``2``."""
    merged = merge_profiler_schema(
        {"id": "DECIMAL", "amount": "DECIMAL"},
        {"id": "INTEGER", "amount": "BIGINT"},
        authoritative_existing=False,
    )
    assert merged["id"] == "DECIMAL"
    assert merged["amount"] == "DECIMAL"


def test_untyped_carrier_still_upgrades_from_inference():
    """CSV declares VARCHAR for every column — profiling it is evidence, not loss."""
    merged = merge_profiler_schema(
        {"amount": "VARCHAR", "when": "VARCHAR", "flag": "TEXT"},
        {"amount": "DECIMAL(12,2)", "when": "TIMESTAMP", "flag": "BOOLEAN"},
    )
    assert merged == {
        "amount": "DECIMAL(12,2)",
        "when": "TIMESTAMP",
        "flag": "BOOLEAN",
    }


def test_same_family_declaration_keeps_its_parameters():
    merged = merge_profiler_schema({"code": "VARCHAR(24)"}, {"code": "VARCHAR"})
    assert merged["code"] == "VARCHAR(24)"


def test_authoritative_source_declaration_always_wins():
    merged = merge_profiler_schema(
        {"amount": "NUMERIC(10,2)"},
        {"amount": "DECIMAL(38,9)"},
        authoritative_existing=True,
    )
    assert merged["amount"] == "NUMERIC(10,2)"
