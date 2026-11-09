"""D-2 — false 409 ``duplicate_transfer`` with ``existing_job_id=""``.

QA2: on PG→MySQL, posting the next run right after the previous one finished
was refused as a duplicate in 2/3 attempts, naming no holder; a retry worked.

The previous run releases its claim (``delete_one``) in the engine's
``finally``, after the job is already marked completed. The next submit's
``insert_one`` could fail on the still-present claim, then the release landed
before the read: the read found nothing, the takeover matched nothing, and the
claim store answered ``(False, "", "")``. A vanished claim is a free slot, so
the store now contends again instead of reporting a duplicate with no holder.
"""

from __future__ import annotations

import os
import threading
import uuid
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

from services.mongodb_service import MongoDBService


class _Claims:
    """Just enough of a pymongo collection for ``claim_job_idempotency``.

    ``after_duplicate`` / ``before_update`` run inside the two windows where a
    concurrent release can land.
    """

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}
        self.after_duplicate = None
        self.before_update = None

    def create_index(self, *_a, **_k):
        return None

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            hook, self.after_duplicate = self.after_duplicate, None
            if hook:
                hook()
            raise DuplicateKeyError("E11000 duplicate key", 11000)
        self.docs[doc["_id"]] = dict(doc)

    def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return dict(doc) if doc else None

    def update_one(self, query, update):
        hook, self.before_update = self.before_update, None
        if hook:
            hook()
        doc = self.docs.get(query["_id"])
        if not doc or doc.get("job_id") != query.get("job_id"):
            return SimpleNamespace(matched_count=0)
        doc.update(update["$set"])
        return SimpleNamespace(matched_count=1)

    def delete_one(self, query):
        doc = self.docs.get(query["_id"])
        if doc and (not query.get("job_id") or doc.get("job_id") == query["job_id"]):
            del self.docs[query["_id"]]
            return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


@pytest.fixture
def store():
    claims = _Claims()
    statuses: dict[str, str] = {}
    svc = MongoDBService(connection_string="mongodb://unused.invalid:1/")
    svc.get_database = lambda *_a, **_k: {"job_claims": claims}
    svc._job_status = lambda jid: statuses.get(jid, "")
    svc._claim_indexes_ready = True
    key = f"d2-{uuid.uuid4().hex}"
    yield svc, claims, statuses, key
    MongoDBService._LOCAL_CLAIMS.pop(key, None)


def _finished_holder(svc, statuses, key) -> str:
    prev = uuid.uuid4().hex[:24]
    assert svc.claim_job_idempotency(key=key, job_id=prev) == (True, "", "")
    MongoDBService._LOCAL_CLAIMS.pop(key, None)
    statuses[prev] = "completed"
    return prev


def test_release_between_failed_insert_and_read_is_a_free_slot(store):
    svc, claims, statuses, key = store
    prev = _finished_holder(svc, statuses, key)
    claims.after_duplicate = lambda: svc.release_job_idempotency(key, prev)

    assert svc.claim_job_idempotency(key=key, job_id="__pending__") == (True, "", "")
    assert claims.docs[key]["job_id"] == "__pending__"


def test_release_between_read_and_takeover_is_a_free_slot(store):
    svc, claims, statuses, key = store
    prev = _finished_holder(svc, statuses, key)
    claims.before_update = lambda: svc.release_job_idempotency(key, prev)

    assert svc.claim_job_idempotency(key=key, job_id="__pending__") == (True, "", "")
    assert claims.docs[key]["job_id"] == "__pending__"


def test_finished_holder_without_a_race_is_taken_over(store):
    svc, claims, statuses, key = store
    prev = _finished_holder(svc, statuses, key)

    assert svc.claim_job_idempotency(key=key, job_id="__pending__") == (True, "", "")
    assert claims.docs[key]["superseded_job_id"] == prev


def test_running_holder_still_refuses_and_names_itself(store):
    svc, claims, statuses, key = store
    live = uuid.uuid4().hex[:24]
    assert svc.claim_job_idempotency(key=key, job_id=live) == (True, "", "")
    statuses[live] = "running"

    assert svc.claim_job_idempotency(key=key, job_id="__pending__") == (
        False,
        live,
        "running",
    )


def test_another_submit_winning_the_takeover_is_reported_by_name(store):
    svc, claims, statuses, key = store
    _finished_holder(svc, statuses, key)
    winner = uuid.uuid4().hex[:24]

    def other_submit_wins():
        claims.docs[key]["job_id"] = winner
        statuses[winner] = "running"

    claims.before_update = other_submit_wins
    assert svc.claim_job_idempotency(key=key, job_id="__pending__") == (
        False,
        winner,
        "running",
    )


def _live_mongo() -> MongoDBService | None:
    uri = os.environ.get(
        "MONGODB_URI", "mongodb://127.0.0.1:27017/?directConnection=true"
    )
    svc = MongoDBService(connection_string=uri)
    try:
        svc.get_database()["job_claims"].estimated_document_count()
    except (ConnectionError, PyMongoError):
        return None
    return svc


def test_live_mongo_release_racing_the_next_submit_never_reports_empty_holder():
    svc = _live_mongo()
    if svc is None:
        pytest.skip("MongoDB not reachable (docker compose up -d mongodb)")
    claims = svc.get_database()["job_claims"]
    statuses: dict[str, str] = {}
    svc._job_status = lambda jid: statuses.get(jid, "")
    false_duplicates = 0
    for _ in range(100):
        key = f"d2-live-{uuid.uuid4().hex}"
        prev = uuid.uuid4().hex[:24]
        assert svc.claim_job_idempotency(key=key, job_id=prev)[0]
        MongoDBService._LOCAL_CLAIMS.pop(key, None)
        statuses[prev] = "completed"
        out: dict = {}
        gate = threading.Barrier(2)

        def release(key=key, prev=prev):
            gate.wait()
            svc.release_job_idempotency(key, prev)

        def submit(key=key):
            gate.wait()
            out["r"] = svc.claim_job_idempotency(key=key, job_id="__pending__")

        threads = [threading.Thread(target=release), threading.Thread(target=submit)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        acquired, holder, _ = out["r"]
        if not acquired and not holder:
            false_duplicates += 1
        claims.delete_one({"_id": key})
        MongoDBService._LOCAL_CLAIMS.pop(key, None)
    assert false_duplicates == 0
