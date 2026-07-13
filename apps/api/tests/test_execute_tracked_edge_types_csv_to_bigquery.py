"""CSV → BigQuery end-to-end with JSON, BINARY, and TIMESTAMP edge types."""

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


def _csv_bytes(rows: list[dict], fieldnames: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_bigquery_edge_types():
    try:
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("BigQuery emulator not reachable on localhost:9050")

    table_name = "bq_edge_types_" + uuid.uuid4().hex[:8]
    fieldnames = ["id", "json_col", "binary_col", "timestamp_col"]
    rows = [
        {"id": "1", "json_col": '{"key":"value"}', "binary_col": "hello", "timestamp_col": "2024-01-15T10:30:00Z"},
        {"id": "2", "json_col": '[1,2,3]', "binary_col": "world", "timestamp_col": "2024-06-01 14:00:00+00:00"},
    ]

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="edge_types.csv",
        source_content=_csv_bytes(rows, fieldnames),
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
            "name": "edge_types",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == len(rows)
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
