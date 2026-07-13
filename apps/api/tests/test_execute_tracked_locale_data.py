"""End-to-end data integrity: CSV with mixed locale/currency formats into PostgreSQL."""

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


def test_csv_to_postgresql_preserves_locale_and_currency_values():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    table_name = "payments_locale_test_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "$1,000.00"},
            {"id": "2", "amount": "€1.000,00"},
            {"id": "3", "amount": "1 000 000.89"},
            {"id": "4", "amount": "USD 500"},
            {"id": "5", "amount": "1,234,567"},
        ]),
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
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, _new_job_id())
    assert result.success, result.error
    assert result.records_transferred == 5
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
    assert by_id[2] == Decimal("1000.00")
    assert by_id[3] == Decimal("1000000.89")
    assert by_id[4] == Decimal("500")
    assert by_id[5] == Decimal("1234567")
