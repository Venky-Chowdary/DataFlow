"""A job document may carry Decimal. BSON must not refuse to start the transfer.

DEF-01: confirming start_transfer raised
``InvalidDocument: cannot encode object: Decimal('1')`` and no job was created.
The control-plane database codec is the carrier. Digits come back as Decimal.
"""

from __future__ import annotations

import socket
from decimal import Decimal

import pytest


def _mongo_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=0.4):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="MongoDB not reachable on :27017")


def test_create_transfer_job_stores_decimal_one_and_reads_it_back() -> None:
    from bson import ObjectId
    from bson.decimal128 import Decimal128
    from pymongo import MongoClient

    from services.mongodb_service import MongoDBService

    svc = MongoDBService("mongodb://127.0.0.1:27017")
    job_oid = ObjectId()
    job_id = str(job_oid)
    try:
        created = svc.create_transfer_job(
            {
                "_id": job_oid,
                "name": "def01-decimal-probe",
                "transfer_request": {
                    "mappings": [
                        {
                            "source": "qty",
                            "target": "qty",
                            "statistics": {"min": Decimal("1"), "mean": Decimal("10.50")},
                        }
                    ]
                },
            }
        )
        assert created == job_id

        # A client without the product codec sees the on-disk carrier.
        raw = MongoClient(
            "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=1500
        )["datatransfer"]["transfer_jobs"].find_one({"_id": job_oid})
        assert raw is not None
        stored = raw["transfer_request"]["mappings"][0]["statistics"]["min"]
        assert isinstance(stored, Decimal128)
        assert stored.to_decimal() == Decimal("1")

        loaded = svc.get_job(job_id)
        assert loaded is not None
        stats = loaded["transfer_request"]["mappings"][0]["statistics"]
        assert stats["min"] == Decimal("1")
        assert stats["mean"] == Decimal("10.50")
    finally:
        try:
            svc.get_database()["transfer_jobs"].delete_one({"_id": job_oid})
        except Exception:
            pass


def test_confirm_transfer_and_schedule_acks_encode_decimal_one() -> None:
    """confirm_action writes the ack, then the job or the schedule.

    Both documents carry the mapping statistic that used to raise
    InvalidDocument on Decimal('1').
    """
    from decimal import Decimal

    from bson.decimal128 import Decimal128
    from pymongo import MongoClient

    from services.mongodb_service import MongoDBService
    from services.schedule_store import PipelineSchedule, _save_mongo
    from src.ai.copilot.ack_ledger import MongoAckLedger

    svc = MongoDBService("mongodb://127.0.0.1:27017")
    db = svc.get_database()
    ledger = MongoAckLedger(collection=db["pilot_acks"])
    statistic = {"min": Decimal("1"), "mean": Decimal("10.50")}
    transfer_id = ledger.put(
        kind="start_transfer",
        payload={"mappings": [{"source": "qty", "statistics": statistic}]},
    )
    schedule_id = ledger.put(
        kind="create_schedule",
        payload={"mappings": [{"source": "qty", "statistics": statistic}]},
    )
    schedule = PipelineSchedule(
        id="def01-schedule-decimal",
        name="def01 schedule",
        source_connector_id="src",
        source_table="orders",
        dest_connector_id="dst",
        dest_table="orders",
        interval="daily",
        mappings=[{"source": "qty", "target": "qty", "statistics": dict(statistic)}],
    )
    try:
        for aid in (transfer_id, schedule_id):
            payload, err = ledger.get_pending_payload(aid)
            assert err == ""
            assert payload is not None
            stored = payload["mappings"][0]["statistics"]["min"]
            assert stored == Decimal("1")

        _save_mongo(svc, [schedule])
        raw = MongoClient(
            "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=1500
        )["datatransfer"]["pipeline_schedules"].find_one({"_id": schedule.id})
        assert raw is not None
        on_disk = raw["mappings"][0]["statistics"]["min"]
        assert isinstance(on_disk, Decimal128)
        assert on_disk.to_decimal() == Decimal("1")
    finally:
        db["pilot_acks"].delete_many({"_id": {"$in": [transfer_id, schedule_id]}})
        db["pipeline_schedules"].delete_one({"_id": schedule.id})
