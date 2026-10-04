"""Worker fleet enqueue / claim wiring (Phase F5) — no live Mongo required."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock

from services.scheduler_mode import api_claim_loop_enabled, claim_queue_enabled, scheduler_mode
from services.worker_fleet import (
    claim_next_job,
    enqueue_job,
    fleet_enabled,
    reclaim_stale_claims,
    start_api_claim_loop,
    stop_api_claim_loop,
)
from services.worker_leases import WorkerLeaseStore


def _force_local(monkeypatch) -> None:
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "0")
    monkeypatch.setenv("DATAFLOW_SCHEDULER_MODE", "local")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)
    monkeypatch.delenv("DATAWRAP_SCHEDULER_MODE", raising=False)


def _force_claim(monkeypatch) -> None:
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)


def test_fleet_disabled_when_forced_local(monkeypatch):
    _force_local(monkeypatch)
    assert fleet_enabled() is False
    assert scheduler_mode() == "local"
    assert claim_queue_enabled() is False
    assert api_claim_loop_enabled() is False


def test_fleet_enabled_flag(monkeypatch):
    _force_claim(monkeypatch)
    assert fleet_enabled() is True
    assert scheduler_mode() == "claim"


def test_scheduler_mode_auto_follows_distributed_backend(monkeypatch):
    monkeypatch.delenv("DATAFLOW_WORKER_FLEET", raising=False)
    monkeypatch.delenv("DATAWRAP_WORKER_FLEET", raising=False)
    monkeypatch.setenv("DATAFLOW_SCHEDULER_MODE", "auto")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    assert scheduler_mode() == "local"

    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.delenv("DATAFLOW_JOB_STORE", raising=False)
    assert scheduler_mode() == "claim"


def test_api_claim_loop_respects_disable(monkeypatch):
    _force_claim(monkeypatch)
    monkeypatch.setenv("DATAFLOW_API_CLAIM_LOOP", "0")
    assert api_claim_loop_enabled() is False
    assert start_api_claim_loop() is False


def test_enqueue_returns_false_without_mongo(monkeypatch):
    _force_claim(monkeypatch)
    with mock.patch("services.worker_fleet._queue_coll", return_value=None):
        assert enqueue_job("job_x") is False


def test_run_transfer_async_enqueues_when_fleet_on(monkeypatch):
    _force_claim(monkeypatch)
    from src.transfer.background import run_transfer_async
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    with mock.patch("services.worker_fleet.enqueue_job", return_value=True) as enq:
        fut = run_transfer_async("job_fleet_1", req)
        assert fut.result() is None
        enq.assert_called_once()
        assert enq.call_args[0][0] == "job_fleet_1"


def _doc_matches(doc: dict, filt: dict) -> bool:
    """Equality plus the operators the claim filter and ack ledger use."""
    for key, expected in filt.items():
        if key == "$or":
            if not any(_doc_matches(doc, sub) for sub in expected):
                return False
            continue
        if isinstance(expected, dict) and any(
            op in expected for op in ("$ne", "$gt", "$lt", "$gte", "$lte", "$exists")
        ):
            actual = doc.get(key)
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$gt" in expected and not (actual is not None and actual > expected["$gt"]):
                return False
            if "$lt" in expected and not (actual is not None and actual < expected["$lt"]):
                return False
            if "$gte" in expected and not (actual is not None and actual >= expected["$gte"]):
                return False
            if "$lte" in expected and not (actual is not None and actual <= expected["$lte"]):
                return False
            if "$exists" in expected:
                present = key in doc and doc.get(key) is not None
                if bool(expected["$exists"]) != present:
                    return False
            continue
        if doc.get(key) != expected:
            return False
    return True


class _FakeQueueColl:
    """Minimal Mongo-like collection for claim/reclaim unit tests."""

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    def update_one(self, filt, update, upsert=False):  # noqa: ANN001
        _id = filt.get("_id")
        if _id is None:
            for doc in list(self.docs.values()):
                if _doc_matches(doc, filt):
                    _id = doc["_id"]
                    break
        if _id is None:
            return
        doc = self.docs.get(_id)
        if doc is None:
            if not upsert:
                return
            doc = {"_id": _id}
            self.docs[_id] = doc
        if "$set" in update:
            doc.update(update["$set"])
        if "$unset" in update:
            for key in update["$unset"]:
                doc.pop(key, None)
        if "$setOnInsert" in update and doc.get("_created_via_insert") is None:
            for k, v in update["$setOnInsert"].items():
                doc.setdefault(k, v)
            doc["_created_via_insert"] = True

    def find_one_and_update(self, filt, update, sort=None, return_document=None):  # noqa: ANN001
        candidates = [d for d in self.docs.values() if _doc_matches(d, filt)]
        if sort:
            key, direction = sort[0]
            candidates.sort(key=lambda d: d.get(key) or datetime.min.replace(tzinfo=timezone.utc), reverse=direction < 0)
        if not candidates:
            return None
        doc = candidates[0]
        if "$set" in update:
            doc.update(update["$set"])
        if "$unset" in update:
            for key in update["$unset"]:
                doc.pop(key, None)
        return dict(doc)

    def find(self, filt):  # noqa: ANN001
        matched = [d for d in self.docs.values() if _doc_matches(d, filt)]

        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def limit(self, n):
                return self._rows[:n]

        return _Cursor(matched)


def test_claim_next_job_acquires_lease_and_requeues_on_lease_fail(monkeypatch):
    _force_claim(monkeypatch)
    coll = _FakeQueueColl()
    now = datetime.now(timezone.utc)
    coll.docs["j1"] = {
        "_id": "j1",
        "job_id": "j1",
        "status": "queued",
        "created_at": now,
    }
    store = WorkerLeaseStore("test-worker")

    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        with mock.patch.object(store, "acquire", return_value=False):
            with mock.patch("services.worker_fleet._mark_transfer_job_claimed"):
                assert claim_next_job(store) is None
                assert coll.docs["j1"]["status"] == "queued"

        with mock.patch.object(store, "acquire", return_value=True):
            with mock.patch("services.worker_fleet._mark_transfer_job_claimed") as mark:
                with mock.patch.object(store, "get_fence", return_value=1):
                    assert claim_next_job(store) == "j1"
                    assert coll.docs["j1"]["status"] == "claimed"
                    mark.assert_called_once()


def test_reclaim_stale_claims(monkeypatch):
    _force_claim(monkeypatch)
    coll = _FakeQueueColl()
    old = datetime.now(timezone.utc) - timedelta(seconds=600)
    coll.docs["stale"] = {
        "_id": "stale",
        "job_id": "stale",
        "status": "claimed",
        "claimed_at": old,
        "worker": "dead",
    }
    coll.docs["fresh"] = {
        "_id": "fresh",
        "job_id": "fresh",
        "status": "claimed",
        "claimed_at": datetime.now(timezone.utc),
        "worker": "alive",
    }
    class _NotHeld:
        def is_held(self, job_id: str) -> bool:
            return False

    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        n = reclaim_stale_claims(older_than_seconds=120, lease_store=_NotHeld())  # type: ignore[arg-type]
    assert n == 1
    assert coll.docs["stale"]["status"] == "queued"
    assert coll.docs["fresh"]["status"] == "claimed"


def test_stop_api_claim_loop_is_idempotent(monkeypatch):
    _force_local(monkeypatch)
    stop_api_claim_loop()
    stop_api_claim_loop()


def _queued(job_id: str, workload: str, created_at: datetime) -> dict:
    return {
        "_id": job_id,
        "job_id": job_id,
        "status": "queued",
        "workload": workload,
        "created_at": created_at,
    }


def test_batch_worker_skips_cdc_and_claims_batch(monkeypatch):
    _force_claim(monkeypatch)
    monkeypatch.setenv("DATAFLOW_WORKER_MODE", "batch")
    monkeypatch.delenv("DATAWRAP_WORKER_MODE", raising=False)
    coll = _FakeQueueColl()
    now = datetime.now(timezone.utc)
    coll.docs["cdc-old"] = _queued("cdc-old", "cdc", now)
    coll.docs["batch-new"] = _queued("batch-new", "batch", now + timedelta(seconds=5))
    store = WorkerLeaseStore("batch-worker")
    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        with mock.patch.object(store, "acquire", return_value=True):
            with mock.patch("services.worker_fleet._mark_transfer_job_claimed"):
                assert claim_next_job(store) == "batch-new"
    assert coll.docs["cdc-old"]["status"] == "queued"


def test_cdc_worker_claims_only_cdc(monkeypatch):
    _force_claim(monkeypatch)
    monkeypatch.setenv("DATAFLOW_WORKER_MODE", "cdc")
    monkeypatch.delenv("DATAWRAP_WORKER_MODE", raising=False)
    coll = _FakeQueueColl()
    now = datetime.now(timezone.utc)
    coll.docs["batch-old"] = _queued("batch-old", "batch", now)
    coll.docs["cdc-new"] = _queued("cdc-new", "cdc", now + timedelta(seconds=5))
    store = WorkerLeaseStore("cdc-worker")
    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        with mock.patch.object(store, "acquire", return_value=True):
            with mock.patch("services.worker_fleet._mark_transfer_job_claimed"):
                assert claim_next_job(store) == "cdc-new"
    assert coll.docs["batch-old"]["status"] == "queued"


def test_unfiltered_worker_still_claims_oldest(monkeypatch):
    _force_claim(monkeypatch)
    monkeypatch.delenv("DATAFLOW_WORKER_MODE", raising=False)
    monkeypatch.delenv("DATAWRAP_WORKER_MODE", raising=False)
    coll = _FakeQueueColl()
    now = datetime.now(timezone.utc)
    coll.docs["cdc-old"] = _queued("cdc-old", "cdc", now)
    coll.docs["batch-new"] = _queued("batch-new", "batch", now + timedelta(seconds=5))
    store = WorkerLeaseStore("both-worker")
    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        with mock.patch.object(store, "acquire", return_value=True):
            with mock.patch("services.worker_fleet._mark_transfer_job_claimed"):
                assert claim_next_job(store) == "cdc-old"


def test_reclaim_leaves_a_held_cdc_lease(monkeypatch):
    """A batch-capable worker must not requeue CDC while the lease is live."""
    _force_claim(monkeypatch)
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAWRAP_JOB_STORE", raising=False)
    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    monkeypatch.setenv("DATAFLOW_WORKER_MODE", "cdc,batch")
    monkeypatch.delenv("DATAWRAP_WORKER_MODE", raising=False)
    monkeypatch.setattr(WorkerLeaseStore, "_mongo_collection", lambda self: None)

    coll = _FakeQueueColl()
    job_id = "cdc-held-lease-topology"
    old = datetime.now(timezone.utc) - timedelta(seconds=600)
    coll.docs[job_id] = {
        "_id": job_id,
        "job_id": job_id,
        "status": "claimed",
        "workload": "cdc",
        "claimed_at": old,
        "worker": "cdc-owner",
    }
    owner = WorkerLeaseStore("cdc-owner")
    assert owner.acquire(job_id, ttl_seconds=600)
    try:
        with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
            assert reclaim_stale_claims(older_than_seconds=120) == 0
        assert coll.docs[job_id]["status"] == "claimed"
    finally:
        owner.release(job_id)


def test_batch_reclaim_does_not_touch_cdc_rows(monkeypatch):
    _force_claim(monkeypatch)
    monkeypatch.setenv("DATAFLOW_WORKER_MODE", "batch")
    monkeypatch.delenv("DATAWRAP_WORKER_MODE", raising=False)
    coll = _FakeQueueColl()
    old = datetime.now(timezone.utc) - timedelta(seconds=600)
    coll.docs["cdc-stale"] = {
        "_id": "cdc-stale",
        "job_id": "cdc-stale",
        "status": "claimed",
        "workload": "cdc",
        "claimed_at": old,
        "worker": "gone",
    }
    with mock.patch("services.worker_fleet._queue_coll", return_value=coll):
        assert reclaim_stale_claims(older_than_seconds=120) == 0
    assert coll.docs["cdc-stale"]["status"] == "claimed"
