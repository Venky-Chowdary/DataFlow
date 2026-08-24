"""JSON scalar polarity: the string \"1\" and the number 1 are different values.

Airbyte-class pipelines json.loads a cell and str() it back, so JSON \"1\"
and JSON 1 collapse, and integers past 2**53 round through binary64.
DataFlow classifies polarity and carries engine JSON text (col::text).
"""

from __future__ import annotations

from decimal import Decimal

from connectors.sql_bind import coerce_json_wire
from services.json_polarity import (
    IEEE754_SAFE_INT,
    classify_json_text,
    classify_json_value,
    is_json_catalog_type,
    json_document_wire,
    json_object_path_kind,
    polarities_match,
)
from services.value_serializer import SQL_NULL_SENTINEL


def test_json_string_one_is_not_the_number_one():
    assert classify_json_text('"1"').kind == "string"
    assert classify_json_text("1").kind == "number"
    assert classify_json_text("1").integer is True
    assert not polarities_match("string", "number")
    assert polarities_match("INTEGER", "number")
    assert polarities_match("DOUBLE", "number")


def test_json_true_is_not_the_string_true():
    assert classify_json_text("true").kind == "boolean"
    assert classify_json_text('"true"').kind == "string"


def test_json_null_is_not_sql_null_on_the_wire():
    assert classify_json_text("null").kind == "null"
    assert json_document_wire(None) == SQL_NULL_SENTINEL


def test_integer_past_ieee_mantissa_is_flagged_not_stringified():
    n = IEEE754_SAFE_INT + 1
    pol = classify_json_value(n)
    assert pol.kind == "number"
    assert pol.integer is True
    assert pol.beyond_ieee is True
    text = json_document_wire(n)
    assert text == str(n)
    assert classify_json_text(text).beyond_ieee is True


def test_object_member_polarities():
    doc = '{"n":1,"s":"1","b":true,"z":null}'
    assert json_object_path_kind(doc, "n").kind == "number"
    assert json_object_path_kind(doc, "s").kind == "string"
    assert json_object_path_kind(doc, "b").kind == "boolean"
    assert json_object_path_kind(doc, "z").kind == "null"


def test_catalog_type_detects_json_and_jsonb_not_json_array():
    assert is_json_catalog_type("json")
    assert is_json_catalog_type("jsonb", "jsonb")
    assert is_json_catalog_type("USER-DEFINED", "jsonb")
    assert not is_json_catalog_type("text")
    assert not is_json_catalog_type("ARRAY", "_json")


def test_coerce_json_wire_keeps_string_one_quoted():
    out = coerce_json_wire('"1"', as_text=True)
    assert out == '"1"'
    assert classify_json_text(out).kind == "string"


def test_coerce_json_wire_keeps_number_one_unquoted():
    out = coerce_json_wire("1", as_text=True)
    assert out == "1"
    assert classify_json_text(out).kind == "number"


def test_decoded_python_str_that_is_not_json_is_quoted():
    # A VARCHAR-looking cell mapped onto JSON becomes a JSON string, not a
    # re-parsed number.
    out = json_document_wire("hello")
    assert out == '"hello"'
    assert classify_json_text(out).kind == "string"


def test_high_precision_decimal_does_not_round_through_float():
    text = "12345678901234567890.123456789"
    pol = classify_json_value(Decimal(text))
    assert pol.kind == "number"
    wire = json_document_wire(Decimal(text))
    assert "12345678901234567890" in wire
