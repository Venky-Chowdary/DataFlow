"""End-to-end PostgreSQL → PostgreSQL incremental streaming with cursor."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def test_postgresql_to_postgresql_incremental_deduped():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL emulator not reachable on localhost:5432")

    import psycopg2

    src_table = f"orders_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"orders_dst_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{src_table}, public.{dst_table}")
        cur.execute(f"CREATE TABLE public.{src_table} (order_id BIGSERIAL PRIMARY KEY, amount NUMERIC)")
        for i in range(1, 6):
            cur.execute(f"INSERT INTO public.{src_table} (order_id, amount) VALUES (%s, %s)", (i, f"{i}00.50"))
    conn.commit()

    try:
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
            sync_mode="incremental_deduped",
            stream_contracts=[{
                "name": "orders",
                "sync_mode": "incremental_deduped",
                "primary_key": "order_id",
                "cursor_field": "order_id",
                "selected": True,
            }],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "0" * 24)
        assert result.success is True, result.error
        assert result.records_transferred == 5

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
            assert cur.fetchone()[0] == 5

        with conn.cursor() as cur:
            for i in range(6, 8):
                cur.execute(f"INSERT INTO public.{src_table} (order_id, amount) VALUES (%s, %s)", (i, f"{i}00.50"))
        conn.commit()

        result2 = engine.execute_tracked(request, "1" * 24)
        assert result2.success is True, result2.error
        assert result2.records_transferred == 2

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
            assert cur.fetchone()[0] == 7
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{src_table}, public.{dst_table}")
        conn.commit()
        conn.close()
