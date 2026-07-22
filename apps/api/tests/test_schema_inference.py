"""Unit tests for schema type inference accuracy."""

from __future__ import annotations

from pathlib import Path

import pytest

from services.file_parser import store_upload
from services.schema_inference import infer_columns_from_rows, infer_type

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class TestInferType:
    @pytest.mark.parametrize(
        "samples,expected",
        [
            (["1", "2", "100"], "INTEGER"),
            (["1.5", "2.0", "100.99"], "DECIMAL"),
            (["true", "false"], "BOOLEAN"),
            (["0", "1"], "INTEGER"),
            (["2024-01-15"], "DATE"),
            (["20240115"], "DATE"),
            (["01/15/2024"], "DATE"),
            (["2024-01-15 10:00:00"], "TIMESTAMP"),
            (["2024-01-15T10:00:00Z"], "TIMESTAMPTZ"),
            (["1705312200000"], "TIMESTAMP"),
            (["550e8400-e29b-41d4-a716-446655440000"], "UUID"),
            (['{"k":"v"}', '{"a":1}'], "JSON"),
            (["[]", "[1,2]"], "ARRAY"),
            (["SGVsbG8gV29ybGQ="], "BINARY"),
            (["a" * 300], "TEXT"),
            (["hello", "world"], "VARCHAR"),
            (["user@test.com"], "VARCHAR"),
            (["POINT(30 10)", "POLYGON((0 0,1 0,1 1,0 0))"], "GEOGRAPHY"),
            (['{"type":"Point","coordinates":[30,10]}'], "GEOGRAPHY"),
            (["P1D", "PT15M", "1 day, 0:00:01"], "INTERVAL"),
            (["2024-01-15T10:00:00Z", "2024-06-01T12:30:00+00:00"], "TIMESTAMPTZ"),
        ],
    )
    def test_single_type_columns(self, samples: list[str], expected: str) -> None:
        assert infer_type(samples) == expected

    def test_mixed_numeric_defaults_decimal(self) -> None:
        assert infer_type(["1", "2.5", "3"]) == "DECIMAL"

    def test_empty_samples_varchar(self) -> None:
        assert infer_type(["", "  "]) == "VARCHAR"

    def test_zero_one_with_boolean_field_name(self) -> None:
        assert infer_type(["0", "1"], field_name="is_active") == "BOOLEAN"

    def test_zero_one_without_boolean_field_name_is_integer(self) -> None:
        assert infer_type(["0", "1"], field_name="row_id") == "INTEGER"

    def test_vector_from_homogeneous_float_arrays(self) -> None:
        vec8 = "[" + ",".join(str(float(i)) for i in range(8)) + "]"
        assert infer_type([vec8, vec8]) == "VECTOR(8)"

    def test_vector_named_field_allows_shorter_dims(self) -> None:
        assert infer_type(["[0.1,0.2,0.3]", "[0.4,0.5,0.6]"], field_name="embedding") == "VECTOR(3)"

    def test_small_array_without_vector_name_stays_array(self) -> None:
        assert infer_type(["[]", "[1,2]"]) == "ARRAY"
        assert infer_type(["[1.0,2.0,3.0]"], field_name="scores") == "ARRAY"

    def test_vector_disagreeing_dims_stays_array(self) -> None:
        a = "[" + ",".join(["0.1"] * 8) + "]"
        b = "[" + ",".join(["0.2"] * 9) + "]"
        assert infer_type([a, b]) == "ARRAY"

    def test_never_invents_vector_1536(self) -> None:
        # Sparse / short sample must not invent a warehouse default dim.
        assert "1536" not in infer_type(["[0.1,0.2]"], field_name="embedding")


class TestSchemaTypesFixture:
    def test_all_columns_detected(self) -> None:
        path = FIXTURES / "sample_schema_types.csv"
        record = store_upload("sample_schema_types.csv", path.read_bytes())
        types = {c["name"]: c["inferred_type"] for c in record["columns"]}
        assert types["row_id"] == "INTEGER"
        assert types["amount"] == "DECIMAL"
        assert types["is_active"] == "BOOLEAN"
        assert types["created_at"] == "TIMESTAMPTZ"
        assert types["birth_date"] == "DATE"
        assert types["txn_yyyymmdd"] == "DATE"
        assert types["record_uuid"] == "UUID"
        assert types["metadata_json"] == "JSON"
        assert types["narrative_body"] == "TEXT"
        assert types["payload_b64"] == "BINARY"
        assert types["customer_email"] == "VARCHAR"
        assert types["updated_epoch_ms"] == "TIMESTAMP"

    def test_samples_populated(self) -> None:
        path = FIXTURES / "sample_schema_types.csv"
        record = store_upload("sample_schema_types.csv", path.read_bytes())
        for col in record["columns"]:
            assert col["name"]
            assert col["inferred_type"]


class TestInferColumnsFromRows:
    def test_nullable_detection(self) -> None:
        cols = infer_columns_from_rows(
            ["a", "b"],
            [["1", ""], ["2", "x"]],
        )
        assert cols[1]["nullable"] is True
        assert cols[0]["nullable"] is False
