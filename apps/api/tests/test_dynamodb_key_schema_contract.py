"""DynamoDB table-key contracts, schemaless attributes, and key wire fidelity."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import moto
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.dynamodb_writer import _coerce_dynamo_cell, write_mapped_rows  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
)


def _client():
    return boto3.client("dynamodb", region_name="us-east-1")


def _create_table(
    client: Any,
    table: str,
    keys: list[tuple[str, str, str]],
    *,
    gsi: tuple[str, str] | None = None,
) -> None:
    definitions = {
        name: scalar
        for name, _role, scalar in keys
    }
    indexes = []
    if gsi:
        index_name, scalar = gsi
        definitions[index_name] = scalar
        indexes.append(
            {
                "IndexName": "by_gsi_attr",
                "KeySchema": [{"AttributeName": index_name, "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        )
    request = {
        "TableName": table,
        "AttributeDefinitions": [
            {"AttributeName": name, "AttributeType": scalar}
            for name, scalar in definitions.items()
        ],
        "KeySchema": [
            {"AttributeName": name, "KeyType": role}
            for name, role, _scalar in keys
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    if indexes:
        request["GlobalSecondaryIndexes"] = indexes
    client.create_table(**request)


def _write(
    table: str,
    *,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    headers: list[str],
    rows: list[list[Any]],
    live_types: dict[str, str] | None = None,
    conflict_columns: list[str] | None = None,
):
    return write_mapped_rows(
        host="",
        port=0,
        database="test",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        column_types=column_types,
        create_table=False,
        conflict_columns=conflict_columns,
        destination_column_types=live_types,
    )


def test_hash_only_existing_table_accepts_unconstrained_mapped_attributes():
    with moto.mock_aws():
        client = _client()
        _create_table(client, "hash_only", [("id", "HASH", "N")])

        result = _write(
            "hash_only",
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "INTEGER", "amount": "DECIMAL", "name": "TEXT"},
            headers=["id", "amount", "name"],
            rows=[["7", "12.50", "Ada"]],
            live_types={"id": "DECIMAL"},
        )

        assert result.ok, result.error
        assert result.rows_written == 1
        item = client.get_item(
            TableName="hash_only", Key={"id": {"N": "7"}}, ConsistentRead=True
        )["Item"]
        assert item["id"] == {"N": "7"}
        assert item["amount"] == {"N": "12.50"}
        assert item["name"] == {"S": "Ada"}


def test_hash_and_range_keys_keep_their_scalar_types_with_nonkey_attributes():
    with moto.mock_aws():
        client = _client()
        _create_table(
            client,
            "hash_range",
            [("pk", "HASH", "S"), ("sk", "RANGE", "N")],
        )

        result = _write(
            "hash_range",
            mappings=[
                {"source": "pk", "target": "pk"},
                {"source": "sk", "target": "sk"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"pk": "TEXT", "sk": "INTEGER", "amount": "DECIMAL"},
            headers=["pk", "sk", "amount"],
            rows=[["eu", "9", "12.5"]],
            live_types={"pk": "VARCHAR", "sk": "DECIMAL"},
        )

        assert result.ok, result.error
        item = client.get_item(
            TableName="hash_range",
            Key={"pk": {"S": "eu"}, "sk": {"N": "9"}},
            ConsistentRead=True,
        )["Item"]
        assert item["pk"] == {"S": "eu"}
        assert item["sk"] == {"N": "9"}
        assert item["amount"] == {"N": "12.5"}


def test_unmapped_sparse_gsi_attribute_is_not_required_by_the_map():
    with moto.mock_aws():
        client = _client()
        _create_table(
            client,
            "sparse_gsi",
            [("id", "HASH", "N")],
            gsi=("gsi_attr", "S"),
        )

        result = _write(
            "sparse_gsi",
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "INTEGER", "amount": "DECIMAL"},
            headers=["id", "amount"],
            rows=[["3", "4.25"]],
            live_types={"id": "DECIMAL", "gsi_attr": "VARCHAR"},
        )

        assert result.ok, result.error
        item = client.get_item(
            TableName="sparse_gsi", Key={"id": {"N": "3"}}, ConsistentRead=True
        )["Item"]
        assert item["amount"] == {"N": "4.25"}
        assert "gsi_attr" not in item


def test_mapped_gsi_attribute_with_incompatible_map_type_is_refused_before_write():
    with moto.mock_aws():
        client = _client()
        _create_table(
            client,
            "typed_gsi",
            [("id", "HASH", "N")],
            gsi=("gsi_attr", "S"),
        )

        result = _write(
            "typed_gsi",
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "gsi_attr", "target": "gsi_attr", "target_type": "N"},
            ],
            column_types={"id": "INTEGER", "gsi_attr": "TEXT"},
            headers=["id", "gsi_attr"],
            rows=[["3", "group-a"]],
            live_types={"id": "DECIMAL", "gsi_attr": "VARCHAR"},
        )

        assert not result.ok
        assert "gsi_attr" in (result.error or "")
        assert client.scan(TableName="typed_gsi")["Items"] == []


@pytest.mark.parametrize(
    ("keys", "mappings", "column_types", "headers", "rows", "expected"),
    [
        (
            [("id", "HASH", "N")],
            [{"source": "amount", "target": "amount"}],
            {"amount": "DECIMAL"},
            ["amount"],
            [["1.5"]],
            "'id'",
        ),
        (
            [("pk", "HASH", "S"), ("sk", "RANGE", "N")],
            [{"source": "pk", "target": "pk"}, {"source": "amount", "target": "amount"}],
            {"pk": "TEXT", "amount": "DECIMAL"},
            ["pk", "amount"],
            [["eu", "1.5"]],
            "'sk'",
        ),
    ],
    ids=["missing-hash", "missing-range"],
)
def test_missing_table_key_is_refused_before_any_item_write(
    keys, mappings, column_types, headers, rows, expected
):
    with moto.mock_aws():
        client = _client()
        _create_table(client, "missing_key", keys)

        result = _write(
            "missing_key",
            mappings=mappings,
            column_types=column_types,
            headers=headers,
            rows=rows,
        )

        assert not result.ok
        assert expected in (result.error or "")
        assert client.scan(TableName="missing_key")["Items"] == []


@pytest.mark.parametrize(
    ("key_scalar", "source_type", "target_type"),
    [
        ("N", "BOOLEAN", None),
        ("N", "INTEGER", "S"),
        ("N", "INTEGER", "BOOLEAN"),
        ("B", "INTEGER", None),
    ],
    ids=["boolean-to-number-key", "string-stamp-on-number-key", "boolean-stamp", "number-to-binary-key"],
)
def test_incompatible_key_scalar_is_refused_before_any_item_write(
    key_scalar, source_type, target_type
):
    with moto.mock_aws():
        client = _client()
        _create_table(client, "wrong_key_type", [("id", "HASH", key_scalar)])

        mapping = {"source": "id", "target": "id"}
        if target_type:
            mapping["target_type"] = target_type
        result = _write(
            "wrong_key_type",
            mappings=[mapping],
            column_types={"id": source_type},
            headers=["id"],
            rows=[["1"]],
        )

        assert not result.ok
        assert "id" in (result.error or "")
        assert client.scan(TableName="wrong_key_type")["Items"] == []


def test_case_mismatched_key_target_is_refused_before_any_item_write():
    with moto.mock_aws():
        client = _client()
        _create_table(client, "case_key", [("id", "HASH", "N")])

        result = _write(
            "case_key",
            mappings=[{"source": "id", "target": "ID"}],
            column_types={"id": "INTEGER"},
            headers=["id"],
            rows=[["1"]],
        )

        assert not result.ok
        assert "'id'" in (result.error or "")
        assert client.scan(TableName="case_key")["Items"] == []


def test_conflict_columns_must_match_the_live_table_key_schema():
    with moto.mock_aws():
        client = _client()
        _create_table(client, "identity_mismatch", [("id", "HASH", "N")])

        result = _write(
            "identity_mismatch",
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "other", "target": "other"},
            ],
            column_types={"id": "INTEGER", "other": "INTEGER"},
            headers=["id", "other"],
            rows=[["1", "2"]],
            conflict_columns=["other"],
        )

        assert not result.ok
        assert "KeySchema" in (result.error or "")
        assert client.scan(TableName="identity_mismatch")["Items"] == []


@pytest.mark.parametrize(
    "description",
    [
        {"KeySchema": [], "AttributeDefinitions": []},
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [],
        },
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "BOOL"}],
        },
        {
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "other", "KeyType": "HASH"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "N"},
                {"AttributeName": "other", "AttributeType": "S"},
            ],
        },
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "by_gsi",
                    "KeySchema": [{"AttributeName": "gsi", "KeyType": "HASH"}],
                }
            ],
        },
    ],
    ids=["no-key-schema", "missing-key-definition", "invalid-key-scalar", "two-hash-keys", "missing-index-definition"],
)
def test_parse_table_description_fails_closed_for_malformed_key_contracts(description):
    from connectors.dynamodb_schema import parse_table_description

    with pytest.raises(ValueError):
        parse_table_description(description)


_INVALID_SHARED_KEY_VALUES = [
    pytest.param(None, id="none"),
    pytest.param(SQL_NULL_SENTINEL, id="sql-null"),
    pytest.param(DF_MISSING_SENTINEL, id="missing"),
    pytest.param(float("nan"), id="float-nan"),
    pytest.param(float("inf"), id="float-positive-infinity"),
    pytest.param(float("-inf"), id="float-negative-infinity"),
    pytest.param(Decimal("NaN"), id="decimal-nan"),
    pytest.param(Decimal("sNaN"), id="decimal-snan"),
    pytest.param(Decimal("Infinity"), id="decimal-positive-infinity"),
    pytest.param(Decimal("-Infinity"), id="decimal-negative-infinity"),
    pytest.param({}, id="dict"),
    pytest.param([], id="list"),
    pytest.param((), id="tuple"),
    pytest.param(set(), id="set"),
    pytest.param(frozenset(), id="frozenset"),
    pytest.param({"_df_ddb_set": "NS", "v": ["1"]}, id="ddb-set-envelope"),
]


@pytest.mark.parametrize("key_type", ["S", "N", "B"])
@pytest.mark.parametrize("value", _INVALID_SHARED_KEY_VALUES)
def test_all_dynamo_key_scalars_refuse_null_nonfinite_and_container_values(
    key_type, value
):
    with pytest.raises(ValueError, match="refused"):
        _coerce_dynamo_cell(
            value,
            col="key",
            logical_type=key_type,
            key_types={"key": key_type},
        )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
        pytest.param(b"x", id="bytes"),
        pytest.param(bytearray(b"x"), id="bytearray"),
        pytest.param(memoryview(b"x"), id="memoryview"),
    ],
)
def test_string_keys_refuse_blank_or_binary_values(value):
    with pytest.raises(ValueError, match="refused"):
        _coerce_dynamo_cell(
            value, col="key", logical_type="TEXT", key_types={"key": "S"}
        )


@pytest.mark.parametrize("value", [b"", ""])
def test_binary_keys_refuse_empty_values(value):
    with pytest.raises(ValueError, match="refused"):
        _coerce_dynamo_cell(
            value, col="key", logical_type="BINARY", key_types={"key": "B"}
        )


@pytest.mark.parametrize("value", [True, "NaN", "Infinity", "-Infinity", "abc"])
def test_number_keys_refuse_boolean_nonfinite_and_unparseable_values(value):
    with pytest.raises(ValueError, match="refused"):
        _coerce_dynamo_cell(
            value, col="key", logical_type="DECIMAL", key_types={"key": "N"}
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("abc", "abc"),
        (7, "7"),
        (True, "true"),
        (Decimal("1E+2"), "100"),
        ("NaN", "NaN"),
    ],
)
def test_string_key_scalar_round_trips(value, expected):
    assert (
        _coerce_dynamo_cell(
            value, col="key", logical_type="TEXT", key_types={"key": "S"}
        )
        == expected
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (5, Decimal("5")),
        ("5.50", Decimal("5.50")),
        (Decimal("1E+2"), Decimal("1E+2")),
    ],
)
def test_number_key_scalar_round_trips(value, expected):
    assert (
        _coerce_dynamo_cell(
            value, col="key", logical_type="DECIMAL", key_types={"key": "N"}
        )
        == expected
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [(b"\x00\x01", b"\x00\x01"), ("AAE=", b"\x00\x01")],
)
def test_binary_key_scalar_round_trips(value, expected):
    assert (
        _coerce_dynamo_cell(
            value, col="key", logical_type="BINARY", key_types={"key": "B"}
        )
        == expected
    )


def _dynamo_preflight(mappings):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id", "amount"],
        column_types={"id": "INTEGER", "amount": "DECIMAL"},
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        destination_table_exists=True,
        destination_can_create=True,
        destination_db_type="dynamodb",
        destination_column_types={"id": "DECIMAL"},
        destination_pk_columns=["id"],
        destination_dynamo_key_schema=[
            {"name": "id", "key_type": "HASH", "attr_type": "DECIMAL", "scalar": "N"}
        ],
        destination_dynamo_index_attributes={},
        source_kind="file",
        sample_rows=[{"id": "1", "amount": "2.5"}],
    )


def _g6(result):
    return next(gate for gate in result["gates"] if gate["id"] == "g6_target_ddl")


def test_validate_blocks_existing_dynamodb_table_when_hash_key_is_not_mapped():
    result = _dynamo_preflight([{"source": "amount", "target": "amount"}])
    gate = _g6(result)

    assert gate["status"] == "block"
    assert gate["details"]["rule_id"] == "g6_target_ddl.dynamo_key_contract"


def test_validate_accepts_nonkey_attribute_when_table_keys_are_mapped():
    result = _dynamo_preflight(
        [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ]
    )
    gate = _g6(result)

    assert gate["details"].get("rule_id") != "g6_target_ddl.dynamo_key_contract"
