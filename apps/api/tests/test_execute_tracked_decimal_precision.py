"""Data-integrity: long decimals and scientific notation survive transfer to PostgreSQL."""

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
    fieldnames = ["id", "amount"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_postgresql_preserves_decimal_precision():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    table_name = "decimal_precision_" + uuid.uuid4().hex[:8]
    rows = [
        {"id": "1", "amount": "12345678901234567890.1234567890"},
        {"id": "2", "amount": "0.00000000000000000001"},
        {"id": "3", "amount": "-999999999999999999.999999"},
        {"id": "4", "amount": "1.23E-10"},
        {"id": "5", "amount": "9.87E+20"},
    ]

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="decimals.csv",
        source_content=_csv_bytes(rows),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table_name,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "decimals",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, _new_job_id())
    assert result.success, result.error
    assert result.records_transferred == len(rows)
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")


def _new_job_id() -> str:
    return uuid.uuid4().hex[:24]
