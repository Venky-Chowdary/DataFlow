"""Dynamo/Redis rematerialize when live carriers differ from Map stamps."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock


def test_dynamo_rematerialize_when_physical_decimal_vs_map_varchar():
    from connectors.dynamodb_writer import _dynamo_rematerialize_if_physical_differs

    batch = _dynamo_rematerialize_if_physical_differs(
        physical={"amount": "DECIMAL", "AMOUNT": "DECIMAL"},
        dest_types={"amount": "VARCHAR"},
        target_cols=["amount"],
        headers=["amount"],
        data_rows=[["12.50"], ["not-a-number"]],
        mappings=[{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        column_types={"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "DECIMAL" in str(live.get("amount") or "").upper()
    assert len(mapped_rows) + len(rejected) >= 1


def test_dynamo_no_rematerialize_when_carriers_match():
    from connectors.dynamodb_writer import _dynamo_rematerialize_if_physical_differs

    assert (
        _dynamo_rematerialize_if_physical_differs(
            physical={"amount": "DECIMAL"},
            dest_types={"amount": "DECIMAL"},
            target_cols=["amount"],
            headers=["amount"],
            data_rows=[["1"]],
            mappings=[
                {"source": "amount", "target": "amount", "target_type": "DECIMAL"}
            ],
            column_types={"amount": "DECIMAL"},
            logical_types=["DECIMAL"],
            policy="quarantine",
        )
        is None
    )


def test_fetch_dynamo_physical_types_attrdefs_and_sample():
    from connectors.dynamodb_writer import _fetch_dynamo_physical_types

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            "ItemCount": 2,
        }
    }
    client.scan.return_value = {
        "Items": [
            {
                "id": {"S": "1"},
                "sk": {"N": "1"},
                "amount": {"N": "10.5"},
                "flag": {"BOOL": True},
            },
            {
                "id": {"S": "2"},
                "sk": {"N": "2"},
                "amount": {"N": "20"},
                "flag": {"BOOL": False},
            },
        ]
    }
    physical, sample_ok = _fetch_dynamo_physical_types(
        client, "orders", ["id", "sk", "amount", "flag"]
    )
    assert sample_ok is True
    assert physical.get("id") == "VARCHAR"
    assert physical.get("sk") == "DECIMAL"
    assert physical.get("amount") in {"DECIMAL", "INTEGER", "FLOAT"}
    assert "BOOL" in str(physical.get("flag") or "").upper()


def test_redis_rematerialize_when_physical_int_vs_map_varchar():
    from connectors.redis_writer import _redis_rematerialize_if_physical_differs

    batch = _redis_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER", "QTY": "INTEGER"},
        dest_types={"qty": "VARCHAR"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["7"], ["x"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "INT" in str(live.get("qty") or "").upper()
    assert len(mapped_rows) + len(rejected) >= 1


def test_fetch_redis_physical_types_majority_vote():
    from connectors.redis_writer import _fetch_redis_physical_types

    client = MagicMock()
    client.scan.side_effect = [
        (0, ["orders:1", "orders:2"]),
    ]
    client.get.side_effect = [
        '{"qty": 1, "flag": true}',
        '{"qty": 2, "flag": false}',
    ]
    physical, sampled = _fetch_redis_physical_types(client, "orders", ["qty", "flag"])
    assert sampled == 2
    assert physical.get("qty") == "INTEGER"
    assert "BOOL" in str(physical.get("flag") or "").upper()


def test_redis_prefix_hint_loops_until_keys_or_wrap():
    from connectors.redis_writer import _redis_prefix_key_count_hint

    client = MagicMock()
    # First SCAN: non-zero cursor, empty batch (false empty cliff).
    client.scan.side_effect = [(7, []), (0, ["orders:1"])]
    assert _redis_prefix_key_count_hint(client, "orders") == 1


def test_fetch_dynamo_sample_decimal_carrier():
    """Dynamo N → Decimal must not demote to TEXT via generic sample."""
    from connectors.dynamodb_writer import _fetch_dynamo_physical_types

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {"AttributeDefinitions": [], "ItemCount": 1}
    }
    client.scan.return_value = {
        "Items": [{"amount": {"N": "99.99"}}],
    }
    physical, sample_ok = _fetch_dynamo_physical_types(client, "t", ["amount"])
    assert sample_ok is True
    assert physical.get("amount") == "DECIMAL"
    assert isinstance(Decimal("99.99"), Decimal)
