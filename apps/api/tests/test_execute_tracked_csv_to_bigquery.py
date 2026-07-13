"""CSV → BigQuery end-to-end via bigquery-emulator."""

from __future__ import annotations

import csv
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


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_bigquery():
    try:
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("BigQuery emulator not reachable on localhost:9050")

    table_name = "bq_csv_e2e_" + uuid.uuid4().hex[:8]

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="users.csv",
        source_content=_csv_bytes([
            {"id": "1", "name": "alice"},
            {"id": "2", "name": "bob"},
        ]),
        destination=EndpointConfig(
            kind="database",
            format="bigquery",
            host="localhost",
            port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test",
            schema="dataflow",
            table=table_name,
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
