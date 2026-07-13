"""End-to-end CSV → PostgreSQL schema drift with backfill_new_fields."""

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
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_to_postgresql_backfill_new_fields():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    table_name = "payments_backfill_test_" + uuid.uuid4().hex[:8]
    destination = EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table_name,
    )

    def make_request(rows: list[dict]) -> TransferRequest:
        return TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="payments.csv",
            source_content=_csv_bytes(rows),
            destination=destination,
            sync_mode="upsert",
            stream_contracts=[{
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }],
            backfill_new_fields=True,
            skip_preflight=True,
        )

    engine = UniversalTransferEngine()

    result1 = engine.execute_tracked(make_request([
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.00"},
    ]), uuid.uuid4().hex[:24])
    assert result1.success, result1.error
    assert result1.records_transferred == 2

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
        cols = [r[0] for r in cur.fetchall()]
    assert "currency" not in cols, cols

    result2 = engine.execute_tracked(make_request([
        {"id": "1", "amount": "1000.00", "currency": "USD"},
        {"id": "2", "amount": "2000.00", "currency": "USD"},
        {"id": "3", "amount": "3000.00", "currency": "EUR"},
    ]), uuid.uuid4().hex[:24])
    assert result2.success, result2.error
    assert result2.records_transferred == 3

    with conn.cursor() as cur:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
        cols = [r[0] for r in cur.fetchall()]
        cur.execute(f'SELECT id, amount, currency FROM public."{table_name}" ORDER BY id')
        rows = cur.fetchall()
    conn.close()

    assert "currency" in cols, cols
    assert rows == [(1, 1000.00, "USD"), (2, 2000.00, "USD"), (3, 3000.00, "EUR")]
