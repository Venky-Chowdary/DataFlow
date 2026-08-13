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


def test_dynamo_rematerialize_refuses_map_varchar_gap_fill():
    """Incomplete AttrDef/sample must not soft-invent Map VARCHAR for gaps."""
    from connectors.dynamodb_writer import _dynamo_rematerialize_if_physical_differs

    batch = _dynamo_rematerialize_if_physical_differs(
        physical={"id": "VARCHAR"},
        dest_types={"id": "VARCHAR", "amount": "VARCHAR"},
        target_cols=["id", "amount"],
        headers=["id", "amount"],
        data_rows=[["1", "9.99"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "amount": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is None


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


def test_dynamo_force_remap_when_carriers_match_partial_studio():
    from connectors.dynamodb_writer import _dynamo_rematerialize_if_physical_differs

    batch = _dynamo_rematerialize_if_physical_differs(
        physical={"amount": "DECIMAL"},
        dest_types={"amount": "DECIMAL"},
        target_cols=["amount"],
        headers=["amount"],
        data_rows=[["12.50"]],
        mappings=[{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        column_types={"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        force_remap=True,
    )
    assert batch is not None
    _rows, _errs, _rej, live = batch
    assert "DECIMAL" in str(live.get("amount") or "").upper()


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
    physical, sample_ok, _items = _fetch_dynamo_physical_types(
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


def test_redis_rematerialize_refuses_map_varchar_gap_fill():
    from connectors.redis_writer import _redis_rematerialize_if_physical_differs

    batch = _redis_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER"},
        dest_types={"qty": "VARCHAR", "flag": "VARCHAR"},
        target_cols=["qty", "flag"],
        headers=["qty", "flag"],
        data_rows=[["7", "true"]],
        mappings=[
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            {"source": "flag", "target": "flag", "target_type": "VARCHAR"},
        ],
        column_types={"qty": "VARCHAR", "flag": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is None


def test_redis_force_remap_when_carriers_match_partial_studio():
    from connectors.redis_writer import _redis_rematerialize_if_physical_differs

    batch = _redis_rematerialize_if_physical_differs(
        physical={"qty": "INTEGER"},
        dest_types={"qty": "INTEGER"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["7"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        force_remap=True,
    )
    assert batch is not None
    _rows, _errs, _rej, live = batch
    assert "INT" in str(live.get("qty") or "").upper()


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


def test_redis_writer_refuses_partial_json_sample_coverage():
    """Existing keys + sample missing a mapped field → refuse Map invent."""
    from unittest.mock import patch

    from connectors.redis_writer import write_mapped_rows

    client = MagicMock()
    with (
        patch("connectors.redis_writer._redis_client", return_value=client),
        patch("connectors.redis_writer._redis_prefix_key_count_hint", return_value=2),
        patch(
            "connectors.redis_writer._fetch_redis_physical_types",
            return_value=({"qty": "INTEGER"}, 2),
        ),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["qty", "flag"],
            data_rows=[["1", "true"]],
            mappings=[
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
                {"source": "flag", "target": "flag", "target_type": "VARCHAR"},
            ],
            column_types={"qty": "VARCHAR", "flag": "VARCHAR"},
            create_table=False,
        )
    assert result.ok is False
    assert "flag" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()


def test_redis_writer_probe_failure_refuses_even_with_studio():
    """key_hint=-1 must not fall through to empty-prefix Map path."""
    from unittest.mock import patch

    from connectors.redis_writer import write_mapped_rows

    client = MagicMock()
    with (
        patch("connectors.redis_writer._redis_client", return_value=client),
        patch("connectors.redis_writer._redis_prefix_key_count_hint", return_value=-1),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["qty"],
            data_rows=[["1"]],
            mappings=[{"source": "qty", "target": "qty", "target_type": "VARCHAR"}],
            column_types={"qty": "VARCHAR"},
            create_table=False,
            destination_column_types={"qty": "INTEGER"},
        )
    assert result.ok is False
    assert "probe failed" in (result.error or "").lower()


def test_redis_empty_prefix_refuses_partial_studio():
    """Empty keyspace + partial Studio — refuse Map VARCHAR invent."""
    from unittest.mock import patch

    from connectors.redis_writer import write_mapped_rows

    client = MagicMock()
    with (
        patch("connectors.redis_writer._redis_client", return_value=client),
        patch("connectors.redis_writer._redis_prefix_key_count_hint", return_value=0),
    ):
        result = write_mapped_rows(
            host="localhost",
            port=6379,
            database="0",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "qty"],
            data_rows=[["1", "7"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "qty": "VARCHAR"},
            create_table=True,
            destination_column_types={"id": "INTEGER"},
        )
    assert result.ok is False
    assert "qty" in (result.error or "").lower()


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
    physical, sample_ok, _items = _fetch_dynamo_physical_types(client, "t", ["amount"])
    assert sample_ok is True
    assert physical.get("amount") == "DECIMAL"
    assert isinstance(Decimal("99.99"), Decimal)


def test_dynamo_writer_refuses_partial_physical_coverage():
    """Populated table + only key AttrDefs typed → refuse Map invent on amount."""
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "ItemCount": 5,
        }
    }
    # Sample misses amount → partial physical (id only from AttrDefs path).
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types",
            return_value={"id": "S"},
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            # A live table whose scan saw an item: emptiness is not proven, so
            # the coverage guard must run.
            return_value=({"id": "VARCHAR"}, True, 1),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "9.99"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "VARCHAR"},
                {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            create_table=False,
        )
    assert result.ok is False
    assert "amount" in (result.error or "").lower()
    assert "refuse" in (result.error or "").lower()
