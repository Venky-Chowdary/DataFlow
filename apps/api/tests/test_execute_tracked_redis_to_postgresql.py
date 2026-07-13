"""Redis → PostgreSQL streaming migration."""

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


def test_redis_to_postgresql():
    try:
        with socket.create_connection(("localhost", 6379), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Dependency not reachable: {exc}")

    import redis

    prefix = f"redis_src_{uuid.uuid4().hex[:8]}"
    pg_table = f"pg_from_redis_{uuid.uuid4().hex[:8]}"

    client = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)
    client.set(f"{prefix}:1", '{"id":"1","name":"alice"}')
    client.set(f"{prefix}:2", '{"id":"2","name":"bob"}')
    client.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="redis", host="localhost", port=6379,
            database="0", table=prefix,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host="localhost", port=5432,
            database="dataflow", username="dataflow", password="dataflow",
            schema="public", table=pg_table,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "redis_key",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{pg_table}"')
            assert cur.fetchone()[0] == 2
    finally:
        conn.close()
