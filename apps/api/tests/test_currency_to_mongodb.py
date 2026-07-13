"""Verify currency / locale values survive a real MongoDB transfer."""

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


def test_currency_csv_to_mongodb_preserves_decimal():
    db_name = f"test_currency_{uuid.uuid4().hex}"
    collection_name = "payments"
    client = None
    try:
        content = (
            'id,amount,note\n'
            '1,"$1,000.00",US payment\n'
            '2,"€2.000,50",EU payment\n'
            '3,"USD 1000000.89",Rich customer\n'
        ).encode("utf-8")
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database",
                format="mongodb",
                database=db_name,
                table=collection_name,
            ),
            source_content=content,
            source_filename="payments.csv",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "0" * 24)
        assert result.success is True, result.error
        assert result.records_transferred == 3

        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        coll = client[db_name][collection_name]
        docs = list(coll.find().sort("id"))
        assert len(docs) == 3
        assert docs[0]["amount"] == Decimal128(Decimal("1000.00"))
        assert docs[1]["amount"] == Decimal128(Decimal("2000.50"))
        assert docs[2]["amount"] == Decimal128(Decimal("1000000.89"))
    finally:
        if client:
            client.drop_database(db_name)
