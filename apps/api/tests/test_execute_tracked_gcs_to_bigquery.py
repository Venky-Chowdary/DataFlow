"""GCS → BigQuery end-to-end via fake-gcs-server and bigquery-emulator."""

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


def test_gcs_to_bigquery():
    try:
        with socket.create_connection(("localhost", 4443), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from google.auth.credentials import AnonymousCredentials
    from google.api_core.client_options import ClientOptions
    from google.cloud import storage

    bucket_name = "gcs_to_bq_" + uuid.uuid4().hex[:8]
    table_name = "gcs_to_bq_" + uuid.uuid4().hex[:8]

    client = storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:4443"),
    )
    bucket = client.create_bucket(bucket_name)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name"])
    writer.writeheader()
    writer.writerows([{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}])
    bucket.blob("users.csv").upload_from_string(
        buf.getvalue().encode("utf-8"), content_type="text/csv"
    )

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="gcs",
            host="localhost", port=4443,
            connection_string="http://localhost:4443",
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
