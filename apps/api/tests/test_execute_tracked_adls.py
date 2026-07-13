"""End-to-end ADLS / Azure Blob (Azurite) transfers."""

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

_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_adls_export():
    try:
        with socket.create_connection(("localhost", 10000), timeout=1):
            pass
    except OSError:
        pytest.skip("Azurite not reachable on localhost:10000")

    key = f"export_csv_to_adls_{uuid.uuid4().hex[:8]}.json"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=EndpointConfig(
            kind="database",
            format="adls",
            host="localhost",
            port=10000,
            database="test",
            username="devstoreaccount1",
            password=_AZURITE_KEY,
            table=key,
        ),
        sync_mode="upsert",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient(
        account_url="http://localhost:10000/devstoreaccount1",
        credential=_AZURITE_KEY,
    )
    blob = client.get_blob_client("test", key)
    data = json.loads(blob.download_blob().readall())
    assert data == [{"id": 1, "amount": 1000.0}, {"id": 2, "amount": 2000.5}]


def test_adls_to_postgresql_upsert():
    try:
        with socket.create_connection(("localhost", 10000), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("Azurite or PostgreSQL emulator not reachable")

    from azure.storage.blob import BlobServiceClient

    client = BlobServiceClient(
        account_url="http://localhost:10000/devstoreaccount1",
        credential=_AZURITE_KEY,
    )
    container = client.get_container_client("test")
    key = f"payments_adls_to_pg_{uuid.uuid4().hex[:8]}.json"
    container.upload_blob(key, json.dumps([
        {"id": 1, "amount": "1000.00"},
        {"id": 2, "amount": "2000.50"},
    ]))

    dst_table = f"dst_adls_pg_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="adls",
            host="localhost",
            port=10000,
            database="test",
            username="devstoreaccount1",
            password=_AZURITE_KEY,
            table=key,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
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

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
        assert cur.fetchone()[0] == 2
        cur.execute(f'SELECT id, amount FROM public."{dst_table}" ORDER BY id')
        rows = cur.fetchall()
    conn.close()
    assert rows == [(1, 1000.0), (2, 2000.5)]
