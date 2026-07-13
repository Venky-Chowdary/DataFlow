"""CSV → Redis end-to-end streaming test."""

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


def test_csv_to_redis():
    try:
        with socket.create_connection(("localhost", 6379), timeout=1):
            pass
    except OSError:
        pytest.skip("Redis not reachable on localhost:6379")

    prefix = f"csv_to_redis_{uuid.uuid4().hex[:8]}"

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
            kind="database", format="redis", host="localhost", port=6379,
            database="0", table=prefix,
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

    import redis
    client = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)
    keys = [k.decode() for k in client.keys(f"{prefix}:*")]
    assert len(keys) == 2
    assert all(k.startswith(f"{prefix}:") for k in keys)
    client.close()
