"""PostgreSQL → PostgreSQL schema drift: backfill widens an existing column."""

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

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_postgresql_to_postgresql_backfill_widens_varchar_and_numeric():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    src_table = f"pg_src_widen_{uuid.uuid4().hex[:8]}"
    dst_table = f"pg_dst_widen_{uuid.uuid4().hex[:8]}"

    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        cur.execute(
            f'CREATE TABLE public."{src_table}" '
            f"(id INT PRIMARY KEY, note VARCHAR(5), amount NUMERIC(8,2))"
        )
        cur.execute(
            f'CREATE TABLE public."{dst_table}" '
            f"(id INT PRIMARY KEY, note VARCHAR(5), amount NUMERIC(8,2))"
        )
        cur.execute(
            f'INSERT INTO public."{src_table}" '
            f"(id, note, amount) VALUES (1, %s, %s), (2, %s, %s)",
            ("hello", Decimal("1234.56"), "world", Decimal("7890.12")),
        )
        cur.execute(
            f'INSERT INTO public."{dst_table}" '
            f"(id, note, amount) VALUES (1, %s, %s)",
            ("hi", Decimal("10.00")),
        )
        conn.commit()
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=dst_table,
        ),
        sync_mode="upsert",
        stream_contracts=[
            {
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }
        ],
        backfill_new_fields=True,
        skip_preflight=True,
    )

    # First run: table types match.
    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    # Widen source columns.
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(
            f'ALTER TABLE public."{src_table}" '
            f"ALTER COLUMN note TYPE VARCHAR(50)"
        )
        cur.execute(
            f'ALTER TABLE public."{src_table}" '
            f"ALTER COLUMN amount TYPE NUMERIC(12,2)"
        )
        cur.execute(
            f'UPDATE public."{src_table}" '
            f"SET note = %s, amount = %s WHERE id = %s",
            ("a-much-longer-value-that-exceeds-five", Decimal("1234567890.12"), 1),
        )
        conn.commit()
    conn.close()

    # Second run: destination should be widened and not truncate the larger values.
    result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, note, amount '
                f'FROM public."{dst_table}" ORDER BY id'
            )
            rows = cur.fetchall()
            assert rows[0][0] == 1
            assert rows[0][1] == "a-much-longer-value-that-exceeds-five"
            assert rows[0][2] == Decimal("1234567890.12")
            assert rows[1][0] == 2
            assert rows[1][1] == "world"
            assert rows[1][2] == Decimal("7890.12")
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        conn.close()
