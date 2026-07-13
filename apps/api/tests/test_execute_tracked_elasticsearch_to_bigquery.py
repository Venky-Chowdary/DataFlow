"""Elasticsearch → BigQuery end-to-end streaming."""

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


def test_elasticsearch_to_bigquery():
    try:
        with socket.create_connection(("localhost", 9200), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from elasticsearch import Elasticsearch

    index = "es_to_bq_" + uuid.uuid4().hex[:8]
    table_name = "es_to_bq_" + uuid.uuid4().hex[:8]

    es = Elasticsearch(hosts=["http://localhost:9200"])
    for i in range(1, 3):
        es.index(index=index, id=str(i), body={
            "id": str(i),
            "name": "alice" if i == 1 else "bob",
        })
    es.indices.refresh(index=index)

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="elasticsearch",
            host="localhost", port=9200,
            database=index, table="",
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
