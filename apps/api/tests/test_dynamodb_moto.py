"""End-to-end DynamoDB write using moto (no real AWS credentials needed)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

moto = pytest.importorskip("moto")  # noqa: E402

from connectors.dynamodb_writer import write_mapped_rows  # noqa: E402


def test_dynamodb_writer_creates_table_and_writes_items():
    with moto.mock_aws():
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ]
        column_types = {"id": "INTEGER", "amount": "DECIMAL"}
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
            data_rows=[["1", "1000.00"], ["2", "2000.50"]],
            mappings=mappings,
            column_types=column_types,
            create_table=True,
        )
        assert result.ok, result.error
        assert result.rows_written == 2

        import boto3

        client = boto3.client("dynamodb", region_name="us-east-1")
        items = client.scan(TableName="payments")["Items"]
        assert len(items) == 2
        ids = {int(item["id"]["N"]) for item in items}
        assert ids == {1, 2}


def test_dynamodb_writer_overwrites_existing_items():
    """DynamoDB PutRequest should overwrite, so repeated transfers are idempotent."""
    with moto.mock_aws():
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ]
        column_types = {"id": "INTEGER", "amount": "DECIMAL"}
        write_mapped_rows(
            host="", port=0, database="test", username="", password="",
            schema="", connection_string="", ssl=False, table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "1000.00"], ["2", "2000.50"]],
            mappings=mappings, column_types=column_types, create_table=True,
        )
        result = write_mapped_rows(
            host="", port=0, database="test", username="", password="",
            schema="", connection_string="", ssl=False, table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "1111.00"], ["3", "3000.00"]],
            mappings=mappings, column_types=column_types, create_table=False,
        )
        assert result.ok, result.error
        assert result.rows_written == 2

        import boto3

        client = boto3.client("dynamodb", region_name="us-east-1")
        items = client.scan(TableName="payments")["Items"]
        assert len(items) == 3
        by_id = {int(item["id"]["N"]): item["amount"]["N"] for item in items}
        assert by_id[1] == "1111.00"
        assert by_id[2] == "2000.50"
        assert by_id[3] == "3000.00"
