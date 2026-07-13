"""End-to-end execute_tracked CSV→MongoDB upsert with reconciliation."""

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


def test_csv_to_mongodb_upsert_updates_and_appends():
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB emulator not reachable on localhost:27017")

    collection_name = "payments_mongodb_upsert_e2e_" + uuid.uuid4().hex[:8]
    destination = EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="dataflow",
        table=collection_name,
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
    assert result1.reconciliation.get("target_rows") == 2

    request.source_content = _csv_bytes([
        {"id": "1", "amount": "1111.00"},
        {"id": "3", "amount": "3000.00"},
    ])
    result2 = engine.execute_tracked(request, _new_job_id())
    assert result2.success, result2.error
    assert result2.records_transferred == 2
    assert result2.reconciliation.get("passed") is True
    assert result2.reconciliation.get("target_rows") == 3

    import pymongo
    from bson.decimal128 import Decimal128

    client = pymongo.MongoClient("localhost", 27017)
    try:
        coll = client["dataflow"][collection_name]
        docs = {d["id"]: d["amount"] for d in coll.find({}, {"_id": 0})}
    finally:
        client.close()
    assert len(docs) == 3
    assert docs[1] == Decimal128("1111.00")
    assert docs[2] == Decimal128("2000.50")
    assert docs[3] == Decimal128("3000.00")
