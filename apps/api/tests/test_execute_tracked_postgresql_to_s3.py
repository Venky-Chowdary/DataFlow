"""End-to-end PostgreSQL → S3/MinIO object export."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import boto3
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_postgresql_to_s3_minio():
    try:
        with socket.create_connection(("localhost", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO not reachable")

    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    table_name = "pg_to_s3_e2e_" + uuid.uuid4().hex[:8]
    bucket_name = "pg-to-s3-" + uuid.uuid4().hex[:8]
    key = f"exports/{table_name}/export.json"

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=bucket_name)

    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    cur = conn.cursor()
    src_table = "pg_s3_src_" + uuid.uuid4().hex[:8]
    cur.execute(f"CREATE TABLE IF NOT EXISTS {src_table} (id int PRIMARY KEY, amount decimal)")
    cur.execute(f"INSERT INTO {src_table} VALUES (1, 1000.50), (2, 2000.75)")
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow", schema="public",
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="s3",
            host="localhost", port=9000,
            database=bucket_name, table=key,
            username="dataflow", password="dataflowsecret",
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "pg_s3",
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

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = obj["Body"].read()
    assert data
