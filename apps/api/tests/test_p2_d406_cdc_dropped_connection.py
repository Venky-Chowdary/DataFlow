"""D406: a destination connection that drops during an exactly-once CDC apply
is rolled back and retried, and never surfaces SQLAlchemy's session text.

Reproduced on a live PostgreSQL destination by terminating the apply's own
backend (``pg_terminate_backend(pg_backend_pid())``) at the watermark lock.
Root cause: ``cdc_eos_sa._lock_watermark`` caught *every* SQLAlchemyError
from its SELECT (meant for a watermark table without the newer columns) and
ran a fallback SELECT on the same connection. After a disconnect that
connection is invalid inside its transaction, so the fallback raised
``PendingRollbackError: Can't reconnect until invalid transaction is rolled
back`` -- the QA text -- and the batch was lost to the run.

Now a disconnect is not a missing column: it propagates, the transaction is
discarded, and the batch is retried once on a fresh connection. Retrying is
safe for this route: the destination-owned watermark is read under lock in
the retried transaction, so a commit that did land is recognised as already
committed (exactly-once), not applied twice.

Live MongoDB replica set (source) + PostgreSQL (destination).
"""

from __future__ import annotations

import uuid

import pytest

from tests.r13_mapping_harness import drop, endpoint, isolated, run_sql  # noqa: F401
from tests.test_d315_mongo_cdc_missing_nested_field_pg_e2e import CFG, _doc, _ready, _run

pytestmark = pytest.mark.skipif(
    not _ready(), reason="MongoDB replica set (change streams) not reachable"
)

KILL = "SELECT pg_terminate_backend(pg_backend_pid())"


def _kill_lock(monkeypatch, times: int) -> dict:
    import connectors.cdc_eos_sa as eos

    real = eos._lock_watermark_sql
    state = {"left": times, "fired": 0}

    def killing(dialect, *, with_seq=True):
        if with_seq and state["left"] > 0:
            state["left"] -= 1
            state["fired"] += 1
            return KILL
        return real(dialect, with_seq=with_seq)

    monkeypatch.setattr(eos, "_lock_watermark_sql", killing)
    return state


@pytest.fixture
def collection():
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    endpoint("postgresql", "probe")
    name = "p2d406_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    cdc.coll.insert_many([_doc(i) for i in range(1, 11)])
    try:
        yield cdc.coll, name
    finally:
        cdc.coll.drop()
        cdc.client.close()
        drop("postgresql", name)
        drop("postgresql", f"{name}_df_quarantine")


def _names(table: str) -> dict:
    return dict(run_sql("postgresql", f'SELECT "_id", name FROM "{table}"'))  # nosec B608


def test_a_dropped_connection_is_rolled_back_and_the_batch_retried(isolated, monkeypatch, collection):  # noqa: F811
    coll, name = collection
    first, _ = _run(isolated, name, name, "cdc_position")
    assert first.success, first.error
    state = _kill_lock(monkeypatch, times=1)
    coll.update_one({"_id": 5}, {"$set": {"name": "renamed"}})
    coll.delete_one({"_id": 6})
    result, job_id = _run(isolated, name, name, "cdc_position")
    assert state["fired"] == 1
    assert result.success, result.error
    names = _names(name)
    assert names[5] == "renamed" and 6 not in names and len(names) == 9
    message = str(isolated.get_job(job_id).get("message") or "")
    assert "invalid transaction" not in message.lower(), message


def test_a_connection_that_keeps_dropping_fails_with_the_real_cause(isolated, monkeypatch, collection):  # noqa: F811
    coll, name = collection
    first, _ = _run(isolated, name, name, "cdc_position")
    assert first.success, first.error
    _kill_lock(monkeypatch, times=50)
    coll.update_one({"_id": 5}, {"$set": {"name": "renamed"}})
    result, job_id = _run(isolated, name, name, "cdc_position")
    assert not result.success
    job = isolated.get_job(job_id)
    text = f"{result.error} {job.get('message')} {job.get('error')}".lower()
    assert "invalid transaction" not in text and "can't reconnect" not in text, text
    assert "connection closed" in text, text
    assert _names(name)[5] == "n5"  # nothing half-applied
    monkeypatch.undo()
    again, _ = _run(isolated, name, name, "cdc_position")
    assert again.success, again.error
    assert _names(name)[5] == "renamed"
