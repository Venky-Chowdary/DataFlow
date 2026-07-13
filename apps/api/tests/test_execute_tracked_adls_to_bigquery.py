"""ADLS (Azurite) → BigQuery end-to-end via local emulators."""

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


def test_adls_to_bigquery():
    try:
        with socket.create_connection(("localhost", 10000), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from azure.storage.blob import BlobServiceClient

    conn = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )
    container = "adlstobq" + uuid.uuid4().hex[:8]
    table_name = "adls_to_bq_" + uuid.uuid4().hex[:8]

    client = BlobServiceClient.from_connection_string(conn)
    client.create_container(container)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name"])
    writer.writeheader()
    writer.writerows([{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}])
    client.get_blob_client(container, "users.csv").upload_blob(
        buf.getvalue().encode("utf-8"), overwrite=True
    )

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="adls",
            connection_string=conn, database=container, table="users.csv",
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
