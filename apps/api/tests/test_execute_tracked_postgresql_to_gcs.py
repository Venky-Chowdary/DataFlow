"""End-to-end PostgreSQL → GCS object export."""

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


def test_postgresql_to_gcs():
    try:
        with socket.create_connection(("localhost", 4443), timeout=1):
            pass
    except OSError:
        pytest.skip("GCS emulator not reachable")

    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    from google.auth.credentials import AnonymousCredentials
    from google.api_core.client_options import ClientOptions
    from google.cloud import storage

    bucket_name = "pg-to-gcs-" + uuid.uuid4().hex[:8]
    key = "export.json"

    client = storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:4443"),
    )
    client.create_bucket(bucket_name)

    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    cur = conn.cursor()
    src_table = "pg_gcs_src_" + uuid.uuid4().hex[:8]
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
            kind="database", format="gcs",
            host="localhost", port=4443,
            connection_string="http://localhost:4443",
            database=bucket_name, table=key,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "pg_gcs",
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

    blob = client.bucket(bucket_name).blob(key)
    data = blob.download_as_bytes()
    assert data
