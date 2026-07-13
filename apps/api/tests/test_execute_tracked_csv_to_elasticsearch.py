"""CSV → Elasticsearch end-to-end streaming test."""

from __future__ import annotations

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


def test_csv_to_elasticsearch():
    try:
        with socket.create_connection(("localhost", 9200), timeout=1):
            pass
    except OSError:
        pytest.skip("Elasticsearch not reachable on localhost:9200")

    index = f"csv_to_es_{uuid.uuid4().hex[:8]}"

    def csv_bytes(rows):
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["id", "name"])
        w.writeheader()
        w.writerows(rows)
        return buf.getvalue().encode("utf-8")

    rows = [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]

    import csv
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="users.csv",
        source_content=csv_bytes(rows),
        destination=EndpointConfig(
            kind="database", format="elasticsearch", host="localhost", port=9200,
            database="dataflow", table=index,
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

    from elasticsearch import Elasticsearch
    es = Elasticsearch("http://localhost:9200")
    es.indices.refresh(index=index)
    docs = es.search(index=index, size=10)
    assert docs["hits"]["total"]["value"] == 2
    es.close()
