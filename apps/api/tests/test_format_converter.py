"""Tests for unified file format conversion."""

import pytest

from services.format_converter import can_convert, convert_rows, conversion_matrix


def test_can_convert_same_format():
    assert can_convert("csv", "csv") is True
    assert can_convert("json", "json") is True


def test_can_convert_csv_to_json():
    assert can_convert("csv", "json") is True
    assert can_convert("csv", "jsonl") is True
    assert can_convert("csv", "tsv") is True


def test_cannot_convert_unsupported():
    assert can_convert("parquet", "csv") is False


def test_convert_csv_to_json():
    headers = ["id", "name"]
    rows = [["1", "Alice"], ["2", "Bob"]]
    content, mime = convert_rows(headers, rows, source_format="csv", target_format="json")
    assert mime == "application/json"
    text = content.decode("utf-8")
    assert "Alice" in text
    assert "Bob" in text


def test_convert_csv_to_jsonl():
    headers = ["id"]
    rows = [["1"], ["2"]]
    content, mime = convert_rows(headers, rows, source_format="csv", target_format="jsonl")
    assert mime == "application/x-ndjson"
    lines = content.decode("utf-8").strip().split("\n")
    assert len(lines) == 2


def test_convert_jsonl_to_csv():
    headers = ["a", "b"]
    rows = [["x", "y"]]
    content, mime = convert_rows(headers, rows, source_format="jsonl", target_format="csv")
    assert mime == "text/csv"
    assert b"a,b" in content


def test_conversion_matrix_structure():
    matrix = conversion_matrix()
    assert "csv" in matrix["formats"]
    assert "json" in matrix["matrix"]["csv"]


def test_convert_unsupported_raises():
    with pytest.raises(ValueError, match="not supported"):
        convert_rows(["a"], [["1"]], source_format="parquet", target_format="csv")
