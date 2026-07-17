"""BigQuery → PostgreSQL end-to-end streaming."""

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


def test_bigquery_to_postgresql():
    try:
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    src_table = "bq_to_pg_src_" + uuid.uuid4().hex[:8]
    dst_table = "bq_to_pg_" + uuid.uuid4().hex[:8]

    client = bigquery.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:9050"),
    )
    dataset_id = "dataflow"
    if dataset_id not in {ds.dataset_id for ds in client.list_datasets()}:
        client.create_dataset(bigquery.Dataset(f"dataflow-test.{dataset_id}"))
    table_ref = f"dataflow-test.{dataset_id}.{src_table}"
    client.create_table(
        bigquery.Table(table_ref, schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
        ]),
        exists_ok=True,
    )
    client.insert_rows_json(table_ref, [
        {"id": "1", "name": "alice"},
        {"id": "2", "name": "bob"},
    ])

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="bigquery",
            host="localhost", port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test", schema="dataflow", table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql",
            host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow",
            schema="public", table=dst_table,
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
