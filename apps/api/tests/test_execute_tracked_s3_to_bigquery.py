"""S3 (MinIO) → BigQuery end-to-end via local emulators."""

from __future__ import annotations

import csv
import io
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


def test_s3_minio_to_bigquery():
    try:
        with socket.create_connection(("localhost", 9000), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import boto3

    bucket_name = "s3-to-bq-" + uuid.uuid4().hex[:8]
    table_name = "s3_to_bq_" + uuid.uuid4().hex[:8]

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=bucket_name)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name"])
    writer.writeheader()
    writer.writerows([{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}])
    s3.put_object(Bucket=bucket_name, Key="users.csv", Body=buf.getvalue().encode("utf-8"))

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="s3",
            host="localhost", port=9000,
            connection_string="http://localhost:9000",
            username="dataflow", password="dataflowsecret",
            database=bucket_name, table="users.csv",
        ),
        destination=EndpointConfig(
            kind="database", format="bigquery",
            host="localhost", port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test", schema="dataflow", table=table_name,
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
