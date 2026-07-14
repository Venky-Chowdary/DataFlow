"""JSON file → MongoDB end-to-end."""

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


def test_json_to_mongodb():
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"MongoDB not reachable: {exc}")

    collection = "json_to_mongo_" + uuid.uuid4().hex[:8]
    json_content = b'[{"id": 1, "name": "alice", "active": true}, {"id": 2, "name": "bob", "active": false}]'

    request = TransferRequest(
        source=EndpointConfig(
            kind="file", format="json",
        ),
        destination=EndpointConfig(
            kind="database", format="mongodb",
            host="localhost", port=27017, database="dataflow",
            table=collection,
        ),
        source_filename="users.json",
        source_content=json_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
