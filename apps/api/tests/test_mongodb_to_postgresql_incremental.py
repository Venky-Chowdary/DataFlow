"""End-to-end MongoDB → PostgreSQL incremental streaming with typed cursor."""

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

pymongo = pytest.importorskip("pymongo")  # noqa: E402
from bson.decimal128 import Decimal128  # noqa: E402
from pymongo import MongoClient  # noqa: E402

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


def _mongo_client():
    return MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)


def test_mongodb_to_postgresql_incremental_deduped():
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB or PostgreSQL emulator not reachable")

    src_db = f"test_mongo_src_{uuid.uuid4().hex}"
    dst_table = f"orders_mongo_pg_{uuid.uuid4().hex[:8]}"

    client = _mongo_client()
    try:
        src = client[src_db]["orders"]
        for i in range(1, 6):
            src.insert_one({
                "order_id": i,
                "amount": Decimal128(Decimal(f"{i}00.50")),
            })

        request = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="mongodb",
                database=src_db,
                table="orders",
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

        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432, database="dataflow",
            user="dataflow", password="dataflow",
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
            assert cur.fetchone()[0] == 5

        for i in range(6, 8):
            src.insert_one({
                "order_id": i,
                "amount": Decimal128(Decimal(f"{i}00.50")),
            })

        result2 = engine.execute_tracked(request, "1" * 24)
        assert result2.success is True, result2.error
        assert result2.records_transferred == 2

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
            assert cur.fetchone()[0] == 7
        conn.close()
    finally:
        client.drop_database(src_db)
