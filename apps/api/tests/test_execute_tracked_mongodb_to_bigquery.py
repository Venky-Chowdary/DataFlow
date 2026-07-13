"""MongoDB → BigQuery end-to-end streaming."""

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


def test_mongodb_to_bigquery():
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from pymongo import MongoClient

    src_collection = "mdb_to_bq_src_" + uuid.uuid4().hex[:8]
    dst_table = "mdb_to_bq_" + uuid.uuid4().hex[:8]

    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    db = client["dataflow"]
    db[src_collection].insert_many([
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ])
    client.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mongodb",
            host="localhost", port=27017,
            database="dataflow", table=src_collection,
        ),
        destination=EndpointConfig(
            kind="database", format="bigquery",
            host="localhost", port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test", schema="dataflow", table=dst_table,
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
