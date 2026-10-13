"""QA MX3-24 — schedules skipped forever with "a run is already in progress".

Root causes proven here, each against a Mongo fake that implements the update
semantics the store relies on ($set merges, $inc, $or/$in/$exists filters):

1. Whole-snapshot saves clobbered concurrent claim/release writes, and their
   ``upsert`` re-inserted schedules another request had deleted.
2. A claim whose job was never bound counted as "unknown" for four hours.
3. A schedule blocked by a peer stayed due and was retried every beat with no
   backoff and no name for the holder.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from services import schedule_store as store


def _match(doc: dict[str, Any] | None, filt: dict[str, Any]) -> bool:
    if doc is None:
        return False
    for key, want in filt.items():
        if key == "$or":
            if not any(_match(doc, sub) for sub in want):
                return False
            continue
        present = key in doc
        have = doc.get(key)
        if isinstance(want, dict):
            if "$exists" in want and present != bool(want["$exists"]):
                return False
            if "$in" in want and have not in want["$in"]:
                return False
            continue
        if have != want:
            return False
    return True


class _Coll:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}

    def find(self, *_a: Any, **_k: Any) -> list[dict[str, Any]]:
        return [copy.deepcopy(d) for d in self.docs.values()]

    def find_one(self, filt: dict[str, Any], *_a: Any, **_k: Any) -> dict[str, Any] | None:
        doc = self.docs.get(filt.get("_id"))
        return copy.deepcopy(doc) if doc is not None and _match(doc, filt) else None

    def _apply(self, doc: dict[str, Any], update: dict[str, Any]) -> None:
        doc.update(copy.deepcopy(update.get("$set") or {}))
        for k, n in (update.get("$inc") or {}).items():
            doc[k] = int(doc.get(k) or 0) + int(n)

    def find_one_and_update(
        self,
        filt: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
        return_document: bool = True,
    ) -> dict[str, Any] | None:
        oid = filt["_id"]
        doc = self.docs.get(oid)
        if doc is not None and _match(doc, filt):
            self._apply(doc, update)
            return copy.deepcopy(doc)
        if doc is None and upsert:
            doc = {"_id": oid}
            self._apply(doc, update)
            self.docs[oid] = doc
            return copy.deepcopy(doc)
        return None

    def update_one(self, filt: dict[str, Any], update: dict[str, Any], upsert: bool = False) -> None:
        doc = self.docs.get(filt.get("_id"))
        if doc is not None and _match(doc, filt):
            self._apply(doc, update)
        elif doc is None and upsert:
            doc = {"_id": filt.get("_id")}
            self._apply(doc, update)
            self.docs[doc["_id"]] = doc

    def replace_one(self, filt: dict[str, Any], doc: dict[str, Any], upsert: bool = False) -> None:
        self.docs[filt["_id"]] = copy.deepcopy(doc)

    def delete_many(self, filt: dict[str, Any]) -> None:
        for key in list(self.docs):
            if key in set((filt.get("_id") or {}).get("$in") or []):
                del self.docs[key]


class _DB:
    def __init__(self) -> None:
        self.colls: dict[str, _Coll] = {}

    def __getitem__(self, name: str) -> _Coll:
        return self.colls.setdefault(name, _Coll())


class _Mongo:
    client = object()

    def __init__(self) -> None:
        self.db = _DB()

    def get_database(self) -> _DB:
        return self.db


@pytest.fixture()
def mongo(monkeypatch: pytest.MonkeyPatch) -> _Mongo:
    svc = _Mongo()
    monkeypatch.setattr(store, "_mongo_backend", lambda: svc)
    getattr(store, "_BASE_DOCS", {}).clear()
    return svc


def _docs(mongo: _Mongo) -> dict[str, dict[str, Any]]:
    return mongo.db["pipeline_schedules"].docs


def _make(name: str, *, dest_table: str = "orders_wh", src: str = "src") -> store.PipelineSchedule:
    return store.create_schedule({
        "name": name,
        "source_connector_id": src,
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": dest_table,
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })


def _jobs(monkeypatch: pytest.MonkeyPatch, statuses: dict[str, str]) -> None:
    class _Svc:
        def get_job(self, job_id: str) -> dict[str, Any] | None:
            st = statuses.get(job_id)
            return {"_id": job_id, "status": st} if st else None

    import services.mongodb_service as ms

    monkeypatch.setattr(ms, "get_mongodb_service", lambda: _Svc())


# ── 1. Stale snapshots must not clobber or resurrect ────────────────────────


def test_stale_snapshot_cannot_reassert_a_released_claim(mongo, monkeypatch):
    _jobs(monkeypatch, {"job-1": "completed"})
    sched = _make("nightly")
    assert store.mark_schedule_running(sched.id, "beat-a") is not None
    store.set_running_job(sched.id, "job-1")

    stale = store._load_all()  # e.g. a list/history writer read while running
    store.mark_schedule_run(sched.id, "job-1", status="completed")
    assert _docs(mongo)[sched.id]["running"] is False

    # The stale writer changes an unrelated field and saves its whole snapshot.
    stale[0].notify_on_success = True
    store._save_all(stale)

    doc = _docs(mongo)[sched.id]
    assert doc["running"] is False, "stale copy re-asserted running=True (MX3-24)"
    assert doc["running_job_id"] == ""
    assert doc["notify_on_success"] is True
    assert doc["last_job_id"] == "job-1"


def test_stale_snapshot_cannot_resurrect_a_deleted_schedule(mongo):
    keep = _make("keep", dest_table="a")
    gone = _make("gone", dest_table="b")
    stale = store._load_all()

    assert store.delete_schedule(gone.id) is True
    assert gone.id not in _docs(mongo)

    for s in stale:
        if s.id == keep.id:
            s.notify_on_success = True
    store._save_all(stale)

    assert gone.id not in _docs(mongo), "delete reported success but schedule came back"
    assert _docs(mongo)[keep.id]["notify_on_success"] is True


def test_untouched_schedules_are_not_rewritten(mongo):
    a = _make("a", dest_table="a")
    b = _make("b", dest_table="b")
    before = _docs(mongo)[b.id]["version"]
    loaded = store._load_all()
    for s in loaded:
        if s.id == a.id:
            s.notify_on_success = True
    store._save_all(loaded)
    assert _docs(mongo)[b.id]["version"] == before


def test_every_write_bumps_the_revision(mongo, monkeypatch):
    _jobs(monkeypatch, {})
    sched = _make("v")
    v0 = _docs(mongo)[sched.id]["version"]
    store.mark_schedule_running(sched.id, "beat")
    v1 = _docs(mongo)[sched.id]["version"]
    store.set_running_job(sched.id, "job-x")
    v2 = _docs(mongo)[sched.id]["version"]
    assert v0 < v1 < v2


# ── 2. Claim liveness ───────────────────────────────────────────────────────


def _age_claim(mongo: _Mongo, sid: str, minutes: int, job_id: str = "") -> None:
    doc = _docs(mongo)[sid]
    doc.update({
        "running": True,
        "running_instance": "dead-instance",
        "running_started_at": (
            datetime.now(timezone.utc) - timedelta(minutes=minutes)
        ).isoformat(),
        "running_job_id": job_id,
    })
    doc["version"] = int(doc.get("version") or 0) + 1


def test_unbound_claim_past_grace_is_reclaimable_not_four_hours(mongo, monkeypatch):
    _jobs(monkeypatch, {})
    sched = _make("unbound")
    _age_claim(mongo, sched.id, minutes=20)
    assert store._is_running_stale(store.get_schedule(sched.id)) is True
    assert store.mark_schedule_running(sched.id, "beat-b") is not None


def test_unbound_claim_held_while_this_process_dispatches(mongo, monkeypatch):
    _jobs(monkeypatch, {})
    sched = _make("dispatching")
    _age_claim(mongo, sched.id, minutes=20)
    with store.dispatch_in_flight(sched.id):
        assert store._is_running_stale(store.get_schedule(sched.id)) is False
        assert store.mark_schedule_running(sched.id, "beat-b") is None


def test_unbound_claim_within_grace_is_held(mongo, monkeypatch):
    _jobs(monkeypatch, {})
    sched = _make("fresh")
    _age_claim(mongo, sched.id, minutes=2)
    assert store.mark_schedule_running(sched.id, "beat-b") is None
    assert store.last_claim_refusal(sched.id)["kind"] == "this_schedule"


def test_finished_bound_job_releases_immediately(mongo, monkeypatch):
    _jobs(monkeypatch, {"job-done": "completed"})
    sched = _make("done")
    _age_claim(mongo, sched.id, minutes=1, job_id="job-done")
    assert store.mark_schedule_running(sched.id, "beat-b") is not None


def test_two_beats_cannot_both_reclaim_one_stale_claim(mongo, monkeypatch):
    _jobs(monkeypatch, {"job-done": "completed"})
    sched = _make("race")
    _age_claim(mongo, sched.id, minutes=1, job_id="job-done")
    judged_stale = store.get_schedule(sched.id)

    first = store._claim_running_mongo(
        sched.id, "beat-a", store._now(), stale=judged_stale
    )
    second = store._claim_running_mongo(
        sched.id, "beat-b", store._now(), stale=judged_stale
    )
    assert first is not None
    assert second is None, "both reclaimers won — two writers on one destination"
    assert _docs(mongo)[sched.id]["running_instance"] == "beat-a"


def test_lost_cas_does_not_fall_back_to_a_blind_write(mongo, monkeypatch):
    _jobs(monkeypatch, {})
    sched = _make("lost")
    monkeypatch.setattr(store, "_claim_running_mongo", lambda *a, **k: None)
    assert store.mark_schedule_running(sched.id, "beat") is None
    assert _docs(mongo)[sched.id].get("running") in (False, None)
    assert store.last_claim_refusal(sched.id)["kind"] == "claim_lost"


# ── 3. The operator is told what holds it ───────────────────────────────────


def test_peer_holder_is_named(mongo, monkeypatch):
    _jobs(monkeypatch, {"job-peer": "running"})
    holder = _make("peer writer", dest_table="shared", src="src-a")
    blocked = _make("blocked", dest_table="shared", src="src-b")
    _age_claim(mongo, holder.id, minutes=1, job_id="job-peer")
    _docs(mongo)[holder.id]["running_started_at"] = datetime.now(timezone.utc).isoformat()

    assert store.mark_schedule_running(blocked.id, "beat") is None
    refusal = store.last_claim_refusal(blocked.id)
    assert refusal["kind"] == "same_destination"
    assert refusal["schedule_id"] == holder.id
    assert refusal["job_id"] == "job-peer"
    text = store.describe_claim_holder(refusal)
    assert "peer writer" in text and "job-peer" in text and "shared" in text


def test_peer_with_finished_job_does_not_block(mongo, monkeypatch):
    _jobs(monkeypatch, {"job-peer": "failed"})
    holder = _make("peer", dest_table="shared", src="src-a")
    blocked = _make("blocked", dest_table="shared", src="src-b")
    _age_claim(mongo, holder.id, minutes=1, job_id="job-peer")
    assert store.mark_schedule_running(blocked.id, "beat") is not None


def test_blocked_schedule_backs_off_instead_of_retrying_every_beat(monkeypatch):
    from services import schedule_runner as runner

    clock = {"t": 1000.0}
    import time as _time

    monkeypatch.setattr(_time, "monotonic", lambda: clock["t"])
    runner._clear_claim_backoff("s1")
    holder = {"kind": "same_destination", "schedule_id": "p", "job_id": "j"}
    runner._note_claim_blocked("s1", holder, "held")
    assert runner._claim_backed_off("s1") is True
    clock["t"] += runner._CLAIM_BACKOFF_MIN_S + 0.1
    assert runner._claim_backed_off("s1") is False
    runner._note_claim_blocked("s1", holder, "held")
    delay = runner._claim_backoff["s1"][1]
    assert delay == runner._CLAIM_BACKOFF_MIN_S * 2
    for _ in range(10):
        runner._note_claim_blocked("s1", holder, "held")
    assert runner._claim_backoff["s1"][1] == runner._CLAIM_BACKOFF_MAX_S
    runner._clear_claim_backoff("s1")
    assert runner._claim_backed_off("s1") is False
