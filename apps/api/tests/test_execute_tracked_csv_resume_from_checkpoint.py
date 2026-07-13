"""End-to-end resume: file streaming picks up from a persisted checkpoint."""

from __future__ import annotations

import csv
import io
import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer import file_stream
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


@pytest.fixture(autouse=True)
def _small_chunk_size(monkeypatch):
    old = file_stream.CHUNK_SIZE
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 2)
    yield
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", old)


def test_csv_to_postgresql_resume_from_checkpoint():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    table_name = "payments_resume_test_" + uuid.uuid4().hex[:8]
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
            skip_preflight=True,
        )

    engine = UniversalTransferEngine()
    job_id = uuid.uuid4().hex[:24]

    first = engine.execute_tracked(make_request([
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.00"},
    ]), job_id)
    assert first.success, first.error
    assert first.records_transferred == 2
    assert first.reconciliation.get("target_rows") == 2

    full = make_request([
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.00"},
        {"id": "3", "amount": "3000.00"},
        {"id": "4", "amount": "4000.00"},
        {"id": "5", "amount": "5000.00"},
    ])
    result = engine.execute_tracked(full, job_id, resume=True)
    assert result.success, result.error
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("target_rows") == 5

    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, amount FROM public."{table_name}" ORDER BY id')
        rows = cur.fetchall()
    conn.close()

    assert len(rows) == 5
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[1] == Decimal("1000.00")
    assert by_id[2] == Decimal("2000.00")
    assert by_id[5] == Decimal("5000.00")
