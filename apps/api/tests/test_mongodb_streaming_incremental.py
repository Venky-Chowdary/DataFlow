"""End-to-end MongoDB → MongoDB incremental streaming with cursor type casting."""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

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


def _client():
    return MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)


def test_mongodb_to_mongodb_incremental_deduped():
    src_db = f"test_mongo_src_{uuid.uuid4().hex}"
    dst_db = f"test_mongo_dst_{uuid.uuid4().hex}"
    client = _client()
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
                format="mongodb",
                database=dst_db,
                table="orders",
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

        dst = client[dst_db]["orders"]
        assert dst.count_documents({}) == 5

        # Add new rows and run a second incremental sync
        for i in range(6, 8):
            src.insert_one({
                "order_id": i,
                "amount": Decimal128(Decimal(f"{i}00.50")),
            })

        result2 = engine.execute_tracked(request, "1" * 24)
        assert result2.success is True, result2.error
        assert result2.records_transferred == 2
        assert dst.count_documents({}) == 7

        ids = {d["order_id"] for d in dst.find()}
        assert ids == {1, 2, 3, 4, 5, 6, 7}
    finally:
        client.drop_database(src_db)
        client.drop_database(dst_db)
