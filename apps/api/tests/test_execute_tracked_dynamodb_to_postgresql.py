"""DynamoDB → PostgreSQL streaming migration."""

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


def test_dynamodb_to_postgresql():
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Dependency not reachable: {exc}")

    import boto3

    src_table = f"ddb_src_{uuid.uuid4().hex[:8]}"
    pg_table = f"pg_from_ddb_{uuid.uuid4().hex[:8]}"

    db = boto3.resource(
        "dynamodb", endpoint_url="http://localhost:8000",
        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
    )
    try:
        db.create_table(
            TableName=src_table,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        ).wait_until_exists()
    except Exception:
        pass

    table = db.Table(src_table)
    table.put_item(Item={"id": "1", "name": "alice"})
    table.put_item(Item={"id": "2", "name": "bob"})

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="dynamodb", host="localhost", port=8000,
            database="us-east-1", username="test", password="test", table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host="localhost", port=5432,
            database="dataflow", username="dataflow", password="dataflow",
            schema="public", table=pg_table,
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

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{pg_table}"')
            assert cur.fetchone()[0] == 2
    finally:
        conn.close()
