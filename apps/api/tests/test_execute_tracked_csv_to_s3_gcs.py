"""End-to-end CSV → S3/MinIO and CSV → GCS object exports."""

from __future__ import annotations

import csv
import io
import json
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


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_s3_minio_export():
    try:
        with socket.create_connection(("localhost", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO not reachable")

    import boto3

    key = f"export_csv_to_s3_{uuid.uuid4().hex[:8]}.json"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=EndpointConfig(
            kind="database",
            format="s3",
            host="localhost",
            port=9000,
            database="dataflow",
            username="dataflow",
            password="dataflowsecret",
            table=key,
            endpoint_url="http://localhost:9000",
            path_style=True,
            region="us-east-1",
        ),
        sync_mode="upsert",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )
    obj = s3.get_object(Bucket="dataflow", Key=key)
    data = json.loads(obj["Body"].read())
    assert data == [{"id": 1, "amount": 1000.0}, {"id": 2, "amount": 2000.5}]


def test_csv_to_gcs_export():
    try:
        with socket.create_connection(("localhost", 4443), timeout=1):
            pass
    except OSError:
        pytest.skip("GCS fake server not reachable")

    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    key = f"export_csv_to_gcs_{uuid.uuid4().hex[:8]}.json"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=EndpointConfig(
            kind="database",
            format="gcs",
            host="localhost",
            port=4443,
            database="dataflow-test",
            table=key,
        ),
        sync_mode="upsert",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    client = storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:4443"),
    )
    blob = client.bucket("dataflow-test").blob(key)
    data = json.loads(blob.download_as_bytes())
    assert data == [{"id": 1, "amount": 1000.0}, {"id": 2, "amount": 2000.5}]
