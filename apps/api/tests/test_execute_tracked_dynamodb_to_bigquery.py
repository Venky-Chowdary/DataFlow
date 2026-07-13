"""DynamoDB → BigQuery end-to-end streaming."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_dynamodb_to_bigquery():
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import boto3

    src_table = "ddb_to_bq_src_" + uuid.uuid4().hex[:8]
    dst_table = "ddb_to_bq_" + uuid.uuid4().hex[:8]

    db = boto3.resource(
        "dynamodb", endpoint_url="http://localhost:8000",
        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
    )
    db.create_table(
        TableName=src_table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    ).wait_until_exists()
    table = db.Table(src_table)
    table.put_item(Item={"id": "1", "name": "alice"})
    table.put_item(Item={"id": "2", "name": "bob"})

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="dynamodb",
            host="localhost", port=8000,
            database="us-east-1", username="test", password="test",
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="bigquery",
            host="localhost", port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test", schema="dataflow", table=dst_table,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
