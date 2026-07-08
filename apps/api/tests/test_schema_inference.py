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
            (["0", "1"], "BOOLEAN"),
            (["2024-01-15"], "DATE"),
            (["20240115"], "DATE"),
            (["01/15/2024"], "DATE"),
            (["2024-01-15 10:00:00"], "TIMESTAMP"),
            (["2024-01-15T10:00:00Z"], "TIMESTAMP"),
            (["1705312200000"], "TIMESTAMP"),
            (["550e8400-e29b-41d4-a716-446655440000"], "UUID"),
            (['{"k":"v"}', '{"a":1}'], "JSON"),
            (["[]", "[1,2]"], "JSON"),
            (["SGVsbG8gV29ybGQ="], "BINARY"),
            (["a" * 300], "TEXT"),
            (["hello", "world"], "VARCHAR"),
            (["user@test.com"], "VARCHAR"),
        ],
    )
    def test_single_type_columns(self, samples: list[str], expected: str) -> None:
        assert infer_type(samples) == expected

    def test_mixed_numeric_defaults_decimal(self) -> None:
        assert infer_type(["1", "2.5", "3"]) == "DECIMAL"

    def test_empty_samples_varchar(self) -> None:
        assert infer_type(["", "  "]) == "VARCHAR"


class TestSchemaTypesFixture:
    def test_all_columns_detected(self) -> None:
        path = FIXTURES / "sample_schema_types.csv"
        record = store_upload("sample_schema_types.csv", path.read_bytes())
        types = {c["name"]: c["inferred_type"] for c in record["columns"]}
        assert types["row_id"] == "INTEGER"
        assert types["amount"] == "DECIMAL"
        assert types["is_active"] == "BOOLEAN"
        assert types["created_at"] == "TIMESTAMP"
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
