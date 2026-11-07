"""Multi-replica scheduler failover on the real MongoDB claim path.

A Railway replica that dies mid-run leaves running=True on the schedule doc.
These tests drive _claim_running_mongo against a LIVE MongoDB (skipped when
Mongo is unreachable) and prove:

* a second replica cannot claim while the first's job is live,
* a stale claim is reclaimed ONLY by the exact-claim CAS (same start stamp and
  job id), and two beats that both judged it stale cannot both win,
* a reclaimed claim keeps one writer: the loser's insert is refused.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _live_backend():
    """A real MongoDB connection — the suite pins DATAFLOW_JOB_STORE=memory, so
    get_mongodb_service() cannot answer this; connect directly like the other
    live-mongo tests do."""
    import pymongo

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception:
        return None

    class _Backend:
        def __init__(self, c):
            self.client = c

        def get_database(self):
            return self.client["datatransfer"]

    return _Backend(client)


@pytest.fixture()
def mongo_coll(monkeypatch):
    import services.schedule_store as store

    svc = _live_backend()
    if svc is None:
        pytest.skip("live MongoDB required for the CAS failover matrix")
    monkeypatch.setattr(store, "_mongo_backend", lambda: svc)
    coll = svc.get_database()["pipeline_schedules"]
    created: list[str] = []
    yield store, coll, created
    for sid in created:
        coll.delete_one({"_id": sid})


def _seed_schedule(store, coll, created, sid: str, **fields):
    now = datetime.now(timezone.utc)
    doc = {
        "_id": sid,
        "id": sid,
        "name": f"failover-{sid}",
        "source_connector_id": "a",
        "source_table": "s",
        "dest_connector_id": "b",
        "dest_table": "d",
        "interval": "hourly",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
        "created_at": now.isoformat(),
        "next_run_at": (now + timedelta(hours=1)).isoformat(),
        "running": False,
        "version": 0,
        **fields,
    }
    coll.insert_one(doc)
    created.append(sid)


def _stale_view(store, coll, sid: str):
    doc = coll.find_one({"_id": sid})
    return store._schedule_from_doc(doc)


def test_live_claim_blocks_a_second_replica(mongo_coll):
    store, coll, created = mongo_coll
    sid = "failover-live-claim"
    _seed_schedule(store, coll, created, sid)
    # Replica A claims the slot.
    a = store._claim_running_mongo(sid, "replica-a", datetime.now(timezone.utc).isoformat(), job_id="job-a")
    assert a is not None and a.running_instance == "replica-a"
    # Replica B (fresh doc read, still running=True) must lose.
    b = store._claim_running_mongo(sid, "replica-b", datetime.now(timezone.utc).isoformat(), job_id="job-b")
    assert b is None
    doc = coll.find_one({"_id": sid})
    assert doc["running_instance"] == "replica-a"
    assert doc["running_job_id"] == "job-a"


def test_stale_claim_reclaimed_by_exact_match_only(mongo_coll):
    store, coll, created = mongo_coll
    sid = "failover-stale"
    dead_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    _seed_schedule(
        store, coll, created, sid,
        running=True, running_instance="replica-a",
        running_started_at=dead_at, running_job_id="job-dead",
    )
    stale = _stale_view(store, coll, sid)
    # Replica B reclaims: the doc still carries exactly the claim B judged stale.
    b = store._claim_running_mongo(
        sid, "replica-b", datetime.now(timezone.utc).isoformat(),
        stale=stale, job_id="job-b",
    )
    assert b is not None
    assert b.running_instance == "replica-b"
    assert b.running_job_id == "job-b"


def test_two_beats_judging_stale_cannot_both_win(mongo_coll):
    store, coll, created = mongo_coll
    sid = "failover-race"
    dead_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    _seed_schedule(
        store, coll, created, sid,
        running=True, running_instance="replica-a",
        running_started_at=dead_at, running_job_id="job-dead",
    )
    # Both replicas read the SAME stale document before either writes.
    stale_b = _stale_view(store, coll, sid)
    stale_c = _stale_view(store, coll, sid)
    winner = store._claim_running_mongo(
        sid, "replica-b", datetime.now(timezone.utc).isoformat(),
        stale=stale_b, job_id="job-b",
    )
    loser = store._claim_running_mongo(
        sid, "replica-c", datetime.now(timezone.utc).isoformat(),
        stale=stale_c, job_id="job-c",
    )
    assert winner is not None
    # The loser's CAS no longer matches: doc carries replica-b's fresh claim.
    assert loser is None
    doc = coll.find_one({"_id": sid})
    assert doc["running_instance"] == "replica-b"


def test_fresh_claim_overwrites_a_stale_judged_older_claim(mongo_coll):
    """A claim re-taken meanwhile is not stolen by a stale observer."""
    store, coll, created = mongo_coll
    sid = "failover-not-stolen"
    dead_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    _seed_schedule(
        store, coll, created, sid,
        running=True, running_instance="replica-a",
        running_started_at=dead_at, running_job_id="job-dead",
    )
    stale = _stale_view(store, coll, sid)
    # Meanwhile replica-a's lease recovered and re-claimed legitimately.
    fresh_at = datetime.now(timezone.utc).isoformat()
    coll.update_one(
        {"_id": sid},
        {"$set": {"running_started_at": fresh_at, "running_job_id": "job-new"}},
    )
    # The stale observer's CAS matches neither the free pattern nor its exact
    # stale claim — the fresh claim stands, no second writer.
    c = store._claim_running_mongo(
        sid, "replica-c", datetime.now(timezone.utc).isoformat(),
        stale=stale, job_id="job-c",
    )
    assert c is None
    doc = coll.find_one({"_id": sid})
    assert doc["running_job_id"] == "job-new"


def test_catch_up_counter_survives_failover(mongo_coll):
    """A catch-up in flight is durable state: the reclaiming replica keeps
    draining the same counter — windows can't replay twice or be dropped."""
    store, coll, created = mongo_coll
    sid = "failover-catchup"
    _seed_schedule(store, coll, created, sid, catch_up_windows_left=2, claimed_catch_up=False)
    # Replica A dies mid-catch-up (its claim is stale).
    dead_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    coll.update_one(
        {"_id": sid},
        {"$set": {"running": True, "running_instance": "replica-a",
                  "running_started_at": dead_at, "running_job_id": "job-dead"}},
    )
    stale = _stale_view(store, coll, sid)
    b = store._claim_running_mongo(
        sid, "replica-b", datetime.now(timezone.utc).isoformat(),
        stale=stale, job_id="job-b",
        catch_up_windows_left=1, claimed_catch_up=True,
    )
    assert b is not None
    doc = coll.find_one({"_id": sid})
    assert doc["catch_up_windows_left"] == 1
    assert doc["claimed_catch_up"] is True
