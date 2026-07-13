"""SQLite → BigQuery end-to-end streaming."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_sqlite_to_bigquery():
    try:
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("BigQuery emulator not reachable on localhost:9050")

    path = f"/tmp/sqlite_to_bq_{uuid.uuid4().hex[:8]}.db"
    table_name = "sqlite_to_bq_" + uuid.uuid4().hex[:8]

    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO users VALUES (?,?)", [(1, "alice"), (2, "bob")])
    conn.commit()
    conn.close()

    try:
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite",
                connection_string=path, database=path, table="users",
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
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
