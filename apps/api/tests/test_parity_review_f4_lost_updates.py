"""Review finding 4: parity decisions must survive concurrent writers.

The API runs as several replicas (values-production.yaml), so a per-process
``threading.Lock`` serializes nothing. Before the fix:

* every GET wrote the whole ``parity`` object back from the snapshot it read
  (``remember_report``), so a GET that started before an approval erased it;
* ``save_state`` overwrote unconditionally, so two replicas approving at once
  kept only the last write.

Now each write is a compare-and-swap on ``parity.version`` in the job store
(MongoDB ``update_one`` with the version in the filter; the memory store checks
under its lock), a read only caches ``parity.summary`` under the same guard,
and a client may pin ``expected_version`` (409 on mismatch).
"""

from __future__ import annotations

import threading
import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService


def _recon() -> dict:
    return {
        "passed": False, "source_rows": 100, "target_rows": 99,
        "sample_compare": {"rows_compared": 100, "mismatches": [
            {"key": "5", "source": "amount", "target": "amount", "source_value": "10.1049", "target_value": "10.10"},
        ]},
        "culprit_keys": {"missing_in_dest": ["42"]},
    }


def _job_doc(job_id: str) -> dict:
    return {
        "_id": job_id, "name": "payments", "workspace_id": "ws-cas", "status": "completed",
        "reconciliation": _recon(),
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    }


def _live_mongo():
    import os

    import pymongo

    from services.mongodb_service import MongoDBService

    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"
    try:
        pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500).admin.command("ping")
    except Exception:  # noqa: BLE001 - no server: the live variant is skipped
        pytest.skip("MongoDB not reachable")
    svc = MongoDBService(uri)
    svc.db_name = "parity_cas_" + uuid.uuid4().hex[:8]
    svc.connect()
    return svc


@pytest.fixture(params=["memory", "mongo"])
def store(request, monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService() if request.param == "memory" else _live_mongo()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    # No process lock exists to mask the race (the old ``job_lock`` is gone): the
    # threads below race exactly as replicas do, on the store's compare-and-swap.
    yield svc
    if request.param == "mongo":
        svc.client.drop_database(svc.db_name)


def _new_job(svc) -> str:
    job_id = "cas" + uuid.uuid4().hex[:12]
    created = svc.create_transfer_job(_job_doc(job_id))
    svc.update_job_status(created, "completed")
    return created


def test_store_compare_and_swap_writes_only_on_a_match(store):
    job_id = _new_job(store)
    assert store.update_job_fields(job_id, {"parity": {"version": 1}}, touch_updated_at=False,
                                   expected={"parity.version": (0, None)})
    # A second writer that read version 0 loses: nothing is written.
    assert not store.update_job_fields(job_id, {"parity": {"version": 1, "x": "lost"}},
                                       touch_updated_at=False, expected={"parity.version": (0, None)})
    assert store.get_job(job_id)["parity"] == {"version": 1}
    # A dotted path sets one field and leaves the rest of the object alone.
    assert store.update_job_fields(job_id, {"parity.summary": {"verdict": "proven"}},
                                   touch_updated_at=False, expected={"parity.version": 1})
    assert store.get_job(job_id)["parity"] == {"version": 1, "summary": {"verdict": "proven"}}


def test_store_compare_and_swap_two_interleaved_writers(store):
    job_id = _new_job(store)
    gate = threading.Barrier(2, timeout=10)
    results: list[bool] = []

    def writer(tag: str) -> None:
        gate.wait()  # both read version 0, then both write
        results.append(store.update_job_fields(
            job_id, {"parity": {"version": 1, "by": tag}}, touch_updated_at=False,
            expected={"parity.version": (0, None)},
        ))

    threads = [threading.Thread(target=writer, args=(t,)) for t in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [False, True]


def test_a_stale_read_never_erases_a_decision(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.parity_summary import remember_report

    job_id = _new_job(store)
    snapshot = store.get_job(job_id)  # a GET starts here...
    report = build_parity_report(snapshot, reread=False, job_id=job_id)
    accept = next(f for f in report["fixes"] if f["action"] == "accept")
    approve_fix(job_id, accept["id"], actor="ops@example.com", note="known rounding")  # ...a decision lands...
    remember_report(job_id, snapshot, report)  # ...and the GET caches what it computed
    state = store.get_job(job_id)["parity"]
    assert state["fixes"][accept["id"]]["status"] == "applied"
    assert state["acceptances"]


def test_two_replicas_approving_at_once_lose_nothing(store, monkeypatch):
    from services import parity_fixes, parity_report
    from services.parity_report import build_parity_report

    job_id = _new_job(store)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    accepts = [f for f in report["fixes"] if f["action"] == "accept"]
    assert len(accepts) == 2
    gate = threading.Barrier(2, timeout=10)
    real_build = parity_report.build_parity_report
    calls = {"n": 0}
    lock = threading.Lock()

    def interleaved(job, **kw):
        out = real_build(job, **kw)
        with lock:
            calls["n"] += 1
            first_two = calls["n"] <= 2
        if first_two:
            gate.wait()  # both replicas have read the same state before either writes
        return out

    monkeypatch.setattr(parity_report, "build_parity_report", interleaved)
    outcomes: dict[str, str] = {}

    def replica(fix_id: str) -> None:
        try:
            parity_fixes.approve_fix(job_id, fix_id, actor=f"{fix_id}@example.com", note="ok")
            outcomes[fix_id] = "applied"
        except parity_fixes.ParityConflict as exc:
            outcomes[fix_id] = f"409: {exc}"

    threads = [threading.Thread(target=replica, args=(f["id"],)) for f in accepts]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stored = store.get_job(job_id)["parity"]["fixes"]
    applied = {fid for fid, o in outcomes.items() if o == "applied"}
    # Every approval that answered 200 is stored; the other one was told (409), not silently lost.
    assert applied and applied == {fid for fid, f in stored.items() if f["status"] == "applied"}, (outcomes, stored)
    assert len(applied) == 1 and any(o.startswith("409") for o in outcomes.values()), outcomes


def test_expected_version_mismatch_is_a_409(store):
    from services.parity_fixes import ParityConflict, approve_fix, dismiss_fix
    from services.parity_report import build_parity_report

    job_id = _new_job(store)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    first, second = [f for f in report["fixes"] if f["action"] == "accept"]
    approve_fix(job_id, first["id"], actor="a@example.com", note="ok", expected_version=report["version"])
    with pytest.raises(ParityConflict, match="changed since it was read"):
        dismiss_fix(job_id, second["id"], actor="b@example.com", expected_version=report["version"])
