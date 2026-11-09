"""A bounded Mongo CDC poll ends once the stream has passed the oplog head it
saw at start, instead of waiting out ``max_wait_seconds`` on every idle run."""

import time
import uuid

import pytest

from connectors.mongodb_change_stream import MongodbChangeStreamCdc, _caught_up, _token_optime

URI = "mongodb://localhost:27017/?directConnection=true"


def _tok(seconds: int, inc: int) -> dict:
    return {"_data": f"82{seconds:08X}{inc:08X}2B0229296E04"}


def test_token_optime_reads_seconds_and_increment() -> None:
    assert _token_optime(_tok(1_700_000_000, 7)) == (1_700_000_000, 7)
    assert _token_optime({"_data": "zz"}) is None
    assert _token_optime(None) is None


@pytest.mark.parametrize(
    ("token", "head", "expected"),
    [
        (_tok(100, 5), (100, 5), True),
        (_tok(101, 0), (100, 5), True),
        (_tok(100, 4), (100, 5), False),
        (_tok(99, 9), (100, 5), False),
        (None, (100, 5), False),
        (_tok(200, 0), None, False),
    ],
)
def test_caught_up_only_at_or_past_start_head(token, head, expected) -> None:
    assert _caught_up(token, head) is expected


def _ready() -> bool:
    try:
        from pymongo import MongoClient

        c = MongoClient(URI, serverSelectionTimeoutMS=1500)
        ok = bool(c.admin.command("hello").get("setName"))
        c.close()
        return ok
    except Exception:
        return False


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
def test_idle_poll_returns_well_before_max_wait_and_keeps_pending_events() -> None:
    name = "cdcidle_" + uuid.uuid4().hex[:8]
    cfg = {"connection_string": URI, "database": "dataflow_cdc_idle"}
    cdc = MongodbChangeStreamCdc(cfg, collection=name, primary_key="_id", max_wait_seconds=30.0)
    try:
        cdc.coll.insert_one({"_id": 0})
        list(cdc.poll())
        cdc.coll.insert_many([{"_id": i, "v": i} for i in range(1, 4)])
        t0 = time.monotonic()
        batches = list(cdc.poll())
        elapsed = time.monotonic() - t0
        got = sorted(int(pk) for b in batches for pk in (r.get("_id") for r in b.inserts))
        assert got == [1, 2, 3]
        assert elapsed < 15, elapsed
        t0 = time.monotonic()
        idle = list(cdc.poll())
        assert time.monotonic() - t0 < 15
        assert not any(b.inserts or b.updates or b.deletes for b in idle)
    finally:
        cdc.coll.drop()
        cdc.client.close()
