"""PostgreSQL → BigQuery end-to-end streaming."""

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


def test_postgresql_to_bigquery():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import psycopg2

    src_table = "pg_to_bq_src_" + uuid.uuid4().hex[:8]
    dst_table = "pg_to_bq_" + uuid.uuid4().hex[:8]

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {src_table} VALUES (1,'alice'),(2,'bob')")
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow",
            schema="public", table=src_table,
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
