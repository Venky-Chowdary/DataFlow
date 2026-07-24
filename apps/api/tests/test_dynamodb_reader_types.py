"""DynamoDB reader honesty — sparse attrs, BS, NULL vs missing, nested M/L."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

try:
    import moto  # noqa: E402
except ImportError as exc:
    pytest.skip(f"requires moto: {exc}", allow_module_level=True)

import boto3  # noqa: E402

from connectors.dynamodb_reader import (  # noqa: E402
    DDB_NULL_SENTINEL,
    read_all_paginated,
    read_table_batch,
)
from services.transform_engine import apply_transform  # noqa: E402


CFG = {
    "host": "us-east-1",
    "port": 443,
    "username": "",
    "password": "",
    "connection_string": "",
}


def _create_table(name: str = "df_ddb_types") -> None:
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def test_sparse_attrs_union_across_items():
    with moto.mock_aws():
        _create_table()
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.put_item(TableName="df_ddb_types", Item={"pk": {"S": "a"}, "only_a": {"S": "x"}})
        client.put_item(TableName="df_ddb_types", Item={"pk": {"S": "b"}, "only_b": {"N": "42"}})
        batch = read_all_paginated(CFG, "df_ddb_types", limit=100)
        assert "pk" in batch.headers
        assert "only_a" in batch.headers
        assert "only_b" in batch.headers
        # Missing attr is empty string — not dropped, not equated with explicit NULL.
        for row in batch.rows:
            assert len(row) == len(batch.headers)


def test_explicit_null_vs_missing_and_transform():
    with moto.mock_aws():
        _create_table()
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.put_item(
            TableName="df_ddb_types",
            Item={"pk": {"S": "n"}, "maybe": {"NULL": True}, "present": {"S": "ok"}},
        )
        client.put_item(
            TableName="df_ddb_types",
            Item={"pk": {"S": "m"}, "present": {"S": "ok"}},
        )
        batch, _ = read_table_batch(cfg=CFG, table="df_ddb_types", limit=50)
        maybe_i = batch.headers.index("maybe")
        null_row = next(r for r in batch.rows if r[batch.headers.index("pk")] == "n")
        missing_row = next(r for r in batch.rows if r[batch.headers.index("pk")] == "m")
        assert null_row[maybe_i] == DDB_NULL_SENTINEL
        assert missing_row[maybe_i] == ""
        val, err = apply_transform(DDB_NULL_SENTINEL, "decimal")
        assert err is None and val is None
        val2, err2 = apply_transform(DDB_NULL_SENTINEL, "none")
        assert err2 is None and val2 is None


def test_binary_set_does_not_crash():
    with moto.mock_aws():
        _create_table()
        client = boto3.client("dynamodb", region_name="us-east-1")
        blob = base64.b64encode(b"hello").decode("ascii")
        client.put_item(
            TableName="df_ddb_types",
            Item={
                "pk": {"S": "bs"},
                "bins": {"BS": [b"hello", b"world"]},
            },
        )
        batch, _ = read_table_batch(cfg=CFG, table="df_ddb_types", limit=10)
        assert "bins" in batch.headers
        cell = batch.rows[0][batch.headers.index("bins")]
        # Serialized set envelope / JSON — never silent drop.
        assert cell
        assert blob in cell or "hello" in cell or "_df_ddb_set" in cell


def test_nested_map_list_preserved_as_json():
    with moto.mock_aws():
        _create_table()
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.put_item(
            TableName="df_ddb_types",
            Item={
                "pk": {"S": "nest"},
                "addr": {"M": {"city": {"S": "Austin"}, "zip": {"N": "78701"}}},
                "tags": {"L": [{"S": "a"}, {"S": "b"}]},
            },
        )
        batch, _ = read_table_batch(cfg=CFG, table="df_ddb_types", limit=10, expand_nested=False)
        assert "addr" in batch.headers
        assert "tags" in batch.headers
        addr = batch.rows[0][batch.headers.index("addr")]
        tags = batch.rows[0][batch.headers.index("tags")]
        assert "Austin" in addr
        assert "a" in tags
