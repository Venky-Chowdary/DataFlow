"""End-to-end execute_tracked CSV→PostgreSQL upsert with reconciliation."""

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
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _new_job_id() -> str:
    return uuid.uuid4().hex[:24]


def test_csv_to_postgresql_upsert_updates_and_appends():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    destination = EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table="payments_upsert_test_e2e",
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=destination,
        sync_mode="upsert",
        stream_contracts=[{
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, _new_job_id())
    assert result1.success, result1.error
    assert result1.records_transferred == 2
    assert result1.reconciliation.get("passed") is True

    # Update row 1 and append row 3.
    request.source_content = _csv_bytes([
        {"id": "1", "amount": "1111.00"},
        {"id": "3", "amount": "3000.00"},
    ])
    result2 = engine.execute_tracked(request, _new_job_id())
    assert result2.success, result2.error
    assert result2.records_transferred == 2
    assert result2.reconciliation.get("passed") is True
    # Table should now hold 3 rows total.
    assert result2.reconciliation.get("target_rows") == 3

    # Verify actual target state.
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    cur = conn.cursor()
    cur.execute('SELECT id, amount FROM public."payments_upsert_test_e2e" ORDER BY id')
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 3
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[1] == pytest.approx(1111.00)
    assert by_id[2] == pytest.approx(2000.50)
    assert by_id[3] == pytest.approx(3000.00)
