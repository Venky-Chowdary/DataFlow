"""Real (non-mocked) MongoDB change-stream CDC integration test.

Runs against a live MongoDB replica set (change streams require an oplog — see
docker-compose ``mongodb`` + ``mongo-init`` services). It snapshots a
collection, captures a real resume token, applies real inserts/updates/deletes,
then tails the change stream and asserts they are captured.

Skips cleanly when MongoDB is a standalone (no change streams) or unreachable,
so laptops with a single-node mongod stay green while docker-compose / CI
exercise the real oplog tail.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mongodb_change_stream import MongodbChangeStreamCdc  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 27017,
    "database": "dataflow_cdc",
    # directConnection avoids RS hostname redirect issues for single-node sets.
    "connection_string": "mongodb://localhost:27017/dataflow_cdc?directConnection=true",
    "ssl": False,
}


def _change_streams_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError:
        return False
    try:
        cdc = MongodbChangeStreamCdc(CFG, collection="probe", primary_key="_id")
        return cdc.is_available()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _change_streams_ready(),
    reason="MongoDB replica set (change streams) not reachable on localhost:27017",
)


def test_mongodb_change_stream_snapshot_then_poll_captures_real_cdc():
    """Business-key pipeline with pre-images: insert, update and delete all land.

    A delete event carries ``documentKey._id`` only, so a pipeline keyed on
    ``id`` needs ``changeStreamPreAndPostImages`` (Mongo 6+) to address the
    destination row — this is the documented remedy, exercised for real here.
    """
    collection = "cdc_orders_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(
        CFG, collection=collection, primary_key="id", max_wait_seconds=8.0
    )
    coll = cdc.coll
    try:
        coll.insert_many([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "20.00"}])
        cdc.client[cdc.db_name].command(
            "collMod", collection, changeStreamPreAndPostImages={"enabled": True}
        )

        # Snapshot backfills existing docs and captures a resume token.
        batches = list(cdc.snapshot())
        snap_inserts = [r for b in batches for r in b.inserts]
        assert len(snap_inserts) == 2, snap_inserts
        resume = batches[-1].resume_token
        assert resume is not None, "expected a change-stream resume token"

        # Apply real changes after the captured token.
        coll.insert_one({"id": 3, "amount": "30.00"})
        coll.update_one({"id": 1}, {"$set": {"amount": "99.00"}})
        coll.delete_one({"id": 2})

        cdc_resume = MongodbChangeStreamCdc(
            CFG,
            collection=collection,
            primary_key="id",
            resume_token=resume,
            max_wait_seconds=8.0,
        )
        changes = list(cdc_resume.poll())
        inserts = [r for b in changes for r in b.inserts]
        updates = [r for b in changes for r in b.updates]

        assert any(str(r.get("id")) == "3" for r in inserts), f"insert not captured: {inserts}"
        assert any(
            str(r.get("id")) == "1" and str(r.get("amount")).startswith("99") for r in updates
        ), f"update not captured: {updates}"
        deletes = [r for b in changes for r in b.deletes]
        assert "2" in [str(k) for k in deletes], f"delete not captured: {deletes}"
    finally:
        try:
            coll.drop()
        except Exception:
            pass
        try:
            cdc.client.close()
        except Exception:
            pass


def test_mongodb_business_key_delete_without_pre_images_fails_closed():
    """Without pre-images a business-key delete cannot be addressed: refuse.

    Applying nothing would leave the deleted row at the destination forever, so
    the poll raises with the ``collMod`` remedy instead of advancing.
    """
    from services.cdc_capability import LogCaptureUnavailable

    collection = "cdc_orders_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(
        CFG, collection=collection, primary_key="id", max_wait_seconds=8.0
    )
    coll = cdc.coll
    try:
        coll.insert_many([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "20.00"}])
        batches = list(cdc.snapshot())
        resume = batches[-1].resume_token
        assert resume is not None

        coll.delete_one({"id": 2})
        cdc_resume = MongodbChangeStreamCdc(
            CFG,
            collection=collection,
            primary_key="id",
            resume_token=resume,
            max_wait_seconds=8.0,
        )
        with pytest.raises(LogCaptureUnavailable, match="collMod|pre-image"):
            list(cdc_resume.poll())
    finally:
        coll.drop()
        cdc.client.close()
