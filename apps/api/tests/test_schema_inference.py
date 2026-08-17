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
            # Sample-aware DECIMAL(p,s) — bare DECIMAL invents (38,15) floors.
            (["1.5", "2.0", "100.99"], "DECIMAL(8,4)"),
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
            # Short base64-looking tokens stay VARCHAR without binary field evidence.
            (["SGVsbG8gV29ybGQ="], "VARCHAR"),
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
        assert infer_type(["1", "2.5", "3"]).startswith("DECIMAL")

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

    def test_epoch_shaped_digits_mixed_with_short_ints_stay_numeric(self) -> None:
        # 10-digit values classify per-value as TIMESTAMP; mixed with ordinary
        # integers that produced {INTEGER, TIMESTAMP} and fell through to
        # VARCHAR, landing an integer key column as text on Mongo/CSV → SQL.
        assert infer_type(["1234567890", "5"], field_name="order_id") == "INTEGER"
        assert infer_type(["1705312200000", "5"], field_name="seq") == "INTEGER"
        assert infer_type(["1234567890", "5"]) == "INTEGER"

    def test_epoch_recovery_widens_rather_than_narrowing(self) -> None:
        # Beyond int64 the carrier widens; it must never narrow or go to text.
        assert infer_type(
            ["1234567890", "9223372036854775807"], field_name="big_id"
        ).startswith("DECIMAL")

    def test_temporal_named_epoch_column_is_still_a_timestamp(self) -> None:
        assert infer_type(["1705312200000"], field_name="updated_epoch_ms") == "TIMESTAMP"
        assert infer_type(["1705312200000"], field_name="created_at") == "TIMESTAMP"

    def test_non_digit_samples_are_not_recovered_as_numeric(self) -> None:
        assert infer_type(["1234567890", "abc"], field_name="code") == "VARCHAR"

    def test_never_invents_vector_1536(self) -> None:
        # Sparse / short sample must not invent a warehouse default dim.
        assert "1536" not in infer_type(["[0.1,0.2]"], field_name="embedding")


class TestSchemaTypesFixture:
    def test_all_columns_detected(self) -> None:
        path = FIXTURES / "sample_schema_types.csv"
        record = store_upload("sample_schema_types.csv", path.read_bytes())
        types = {c["name"]: c["inferred_type"] for c in record["columns"]}
        assert types["row_id"] == "INTEGER"
        assert str(types["amount"]).startswith("DECIMAL")
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
