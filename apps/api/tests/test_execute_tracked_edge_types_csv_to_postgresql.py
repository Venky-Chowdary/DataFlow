"""Data integrity: edge CSV types — UUID, timestamps, scientific, percent, binary, unicode."""

from __future__ import annotations

import csv
import io
import socket
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "id", "user_id", "measured_at", "sensor_value", "tax_rate",
        "payload", "note", "flag",
    ])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_edge_types_csv_to_postgresql():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    table_name = "edge_types_test_" + uuid.uuid4().hex[:8]
    rows = [
        {
            "id": "1",
            "user_id": "550e8400-e29b-41d4-a716-446655440000",
            "measured_at": "2024-12-31T23:59:59+00:00",
            "sensor_value": "1.5e3",
            "tax_rate": "12.5%",
            "payload": "aGVsbG8=",
            "note": "Café Münich ",
            "flag": "1",
        },
        {
            "id": "2",
            "user_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "measured_at": "1735689600",
            "sensor_value": "2.5E-2",
            "tax_rate": "0%",
            "payload": "d29ybGQ=",
            "note": "",
            "flag": "0",
        },
    ]

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="edge.csv",
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
        sync_mode="upsert",
        stream_contracts=[{
            "name": "measurements",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT id, user_id, measured_at, sensor_value, tax_rate, payload, note, flag '
            f'FROM public."{table_name}" ORDER BY id'
        )
        rows = cur.fetchall()
    conn.close()

    assert rows[0][0] == 1
    assert rows[0][1] == "550e8400-e29b-41d4-a716-446655440000"
    assert rows[0][2] == datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    assert rows[0][3] == Decimal("1500")
    assert rows[0][4] == Decimal("12.5")
    assert bytes(rows[0][5]) == b"hello"
    assert rows[0][6] == "Café Münich"
    assert rows[0][7] is True

    assert rows[1][0] == 2
    assert rows[1][2] == datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert rows[1][3] == Decimal("0.025")
    assert rows[1][4] == Decimal("0")
    assert bytes(rows[1][5]) == b"world"
    assert rows[1][6] is None
    assert rows[1][7] is False
