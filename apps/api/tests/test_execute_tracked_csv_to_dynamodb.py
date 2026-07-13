"""End-to-end CSV → DynamoDB Local upsert."""

from __future__ import annotations

import csv
import io
import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_dynamodb_upsert():
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            pass
    except OSError:
        pytest.skip("DynamoDB Local not reachable")

    import boto3

    dst_table = f"csv_to_ddb_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=EndpointConfig(
            kind="database",
            format="dynamodb",
            host="localhost",
            port=8000,
            database="test",
            table=dst_table,
        ),
        sync_mode="upsert",
        stream_contracts=[{
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    ddb = boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        region_name="us-east-1",
    )
    items = ddb.scan(TableName=dst_table).get("Items", [])
    assert len(items) == 2
    records = {int(it["id"]["N"]): Decimal(it["amount"]["N"]) for it in items}
    assert records == {1: Decimal("1000.00"), 2: Decimal("2000.50")}
