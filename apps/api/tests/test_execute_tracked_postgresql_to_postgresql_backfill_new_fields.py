"""PostgreSQL → PostgreSQL schema drift: backfill_new_fields adds a new column."""

from __future__ import annotations

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


def test_postgresql_to_postgresql_backfill_new_fields():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    src_table = f"pg_src_backfill_{uuid.uuid4().hex[:8]}"
    dst_table = f"pg_dst_backfill_{uuid.uuid4().hex[:8]}"

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.\"{src_table}\"")
        cur.execute(f"DROP TABLE IF EXISTS public.\"{dst_table}\"")
        cur.execute(f'CREATE TABLE public."{src_table}" (id INT PRIMARY KEY, amount NUMERIC)')
        cur.execute(f'CREATE TABLE public."{dst_table}" (id INT PRIMARY KEY, amount NUMERIC)')
        cur.execute(f'INSERT INTO public."{src_table}" (id, amount) VALUES (1, 1000.00), (2, 2000.50)')
        cur.execute(f'INSERT INTO public."{dst_table}" (id, amount) VALUES (1, 1000.00)')
        conn.commit()
    conn.close()

    # First run: only id and amount.
    request1 = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host="localhost", port=5432,
            database="dataflow", username="dataflow", password="dataflow",
            schema="public", table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host="localhost", port=5432,
            database="dataflow", username="dataflow", password="dataflow",
            schema="public", table=dst_table,
        ),
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
    result1 = engine.execute_tracked(request1, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    # Add currency to source table.
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE public."{src_table}" ADD COLUMN currency VARCHAR(10)')
        cur.execute(f'UPDATE public."{src_table}" SET currency = %s WHERE id = %s', ("USD", 1))
        cur.execute(f'UPDATE public."{src_table}" SET currency = %s WHERE id = %s', ("EUR", 2))
        conn.commit()
    conn.close()

    # Second run: destination should get the new currency column and data.
    result2 = engine.execute_tracked(request1, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, amount, currency FROM public."{dst_table}" ORDER BY id')
            rows = cur.fetchall()
            assert rows == [(1, Decimal("1000.00"), "USD"), (2, Decimal("2000.50"), "EUR")]
    finally:
        conn.close()
