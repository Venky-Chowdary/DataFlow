"""Two real processes finalize one scheduled job against one Mongo store.

The job's own terminal write and the scheduler beat both finalize a job, and
on a multi-replica deployment they run in different processes: no in-process
lock orders them, only the store's version CAS does. Each child here is a
fresh interpreter (``spawn``) with its own store module, wired to the same
throwaway database. A barrier makes the two overlap at a chosen point:

- ``save``: both have loaded the schedule inside ``mark_schedule_run`` and
  decided the run's outcome, and write together (the CAS loser must find the
  winner's record and stand down);
- ``entry``: both are past ``_finalize_run``'s unlocked "already recorded"
  checks, and race for the store from there.

Either way the run is recorded once and announced once.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

REFUSED = {
    "status": "failed",
    "phase": "validate",
    "error": "Transfer refused by validation gate g22: value does not fit",
    "records_processed": 0,
}
COMPLETED = {"status": "completed", "phase": "completed", "records_processed": 3}
URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://127.0.0.1:27017")
BARRIER_SECONDS = 30


def _wire(dbname: str, store_path: str, jobs: dict[str, Any]):
    """Point this process's store at ``dbname``; capture alerts and events."""
    import pymongo

    import services.schedule_store as store
    from services import schedule_runner as runner

    client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=2000)

    class _Svc:
        def __init__(self) -> None:
            self.client = client

        def get_database(self):
            return client[dbname]

    svc = _Svc()
    store.STORE_PATH = Path(store_path)
    store._mongo_backend = lambda: svc
    seen: dict[str, list] = {"notified": [], "events": []}
    runner._job_doc = lambda jid: jobs.get(jid)
    runner._observe_parallel_run = lambda *a, **k: None
    runner._notify_schedule = lambda sched, job_id, status, doc: seen["notified"].append(
        [job_id, status]
    )

    class _Events(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            event = getattr(record, "event", None)
            if event:
                seen["events"].append(event)

    logging.getLogger().addHandler(_Events())
    logging.getLogger().setLevel(logging.INFO)
    return store, runner, seen, client


def _replica(
    dbname: str, store_path: str, tasks: Any, results: Any, barrier: Any
) -> None:
    """One replica (a spawned interpreter): finalize ``job-1`` once per task."""
    jobs: dict[str, Any] = {}
    store, runner, seen, client = _wire(dbname, store_path, jobs)
    real_save, real_entry = store._save_all, runner._run_entry
    try:
        while True:
            task = tasks.get()
            if task is None:
                return
            jobs["job-1"] = task["job"]
            seen.clear()
            seen.update({"notified": [], "events": []})
            waited: list[bool] = []

            def overlap() -> None:
                if waited:
                    return
                waited.append(True)
                try:
                    barrier.wait(BARRIER_SECONDS)
                    seen["overlapped"] = True
                except Exception as exc:  # noqa: BLE001 - reported to the parent
                    seen["barrier"] = repr(exc)

            def save(schedules, **kwargs):
                guard = kwargs.get("guard")
                if guard is not None and guard.reload_if_moved:
                    overlap()
                return real_save(schedules, **kwargs)

            def entry(*args):
                overlap()
                return real_entry(*args)

            store._save_all = save if task["where"] == "save" else real_save
            runner._run_entry = entry if task["where"] == "entry" else real_entry
            try:
                runner._finalize_run(task["schedule_id"], "job-1", 0, datetime.now(timezone.utc))
            except BaseException as exc:  # noqa: BLE001 - reported to the parent
                seen["error"] = repr(exc)
            results.put(json.dumps(seen))
    finally:
        client.close()


class _Replicas:
    """Two spawned replica processes, started once for the module."""

    def __init__(self, dbname: str, store_path: str) -> None:
        ctx = multiprocessing.get_context("spawn")
        self.barrier = ctx.Barrier(2)
        self.results = ctx.Queue()
        self.tasks = [ctx.Queue(), ctx.Queue()]
        self.procs = [
            ctx.Process(target=_replica,
                        args=(dbname, store_path, q, self.results, self.barrier), daemon=True)
            for q in self.tasks
        ]
        for proc in self.procs:
            proc.start()

    def finalize(self, schedule_id: str, jobs: tuple, where: str) -> list[dict[str, Any]]:
        """Both replicas finalize ``job-1`` of ``schedule_id``; what each said."""
        self.barrier.reset()
        for q, job in zip(self.tasks, jobs):
            q.put({"schedule_id": schedule_id, "job": job, "where": where})
        return [json.loads(self.results.get(timeout=120)) for _ in self.procs]

    def close(self) -> None:
        for q in self.tasks:
            q.put(None)
        for proc in self.procs:
            proc.join(30)
            if proc.is_alive():
                proc.kill()


@pytest.fixture(scope="module")
def replicas(tmp_path_factory):
    """Two replica processes on a throwaway database (skipped without MongoDB)."""
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        client.close()
        pytest.skip(f"MongoDB not reachable: {exc}")
    dbname = "finalize_two_proc_" + uuid.uuid4().hex[:8]
    store_path = str(tmp_path_factory.mktemp("two_proc") / "schedules.json")
    pair = _Replicas(dbname, store_path)
    try:
        yield pair, client, dbname, store_path
    finally:
        pair.close()
        client.drop_database(dbname)
        client.close()


@pytest.fixture
def shared_store(replicas, monkeypatch):
    """This process's store on the replicas' database."""
    import services.schedule_store as store

    pair, client, dbname, store_path = replicas

    class _Svc:
        def __init__(self) -> None:
            self.client = client

        def get_database(self):
            return client[dbname]

    svc = _Svc()
    monkeypatch.setattr(store, "STORE_PATH", Path(store_path))
    monkeypatch.setattr(store, "_mongo_backend", lambda: svc)
    return store, pair


@pytest.mark.parametrize(
    "where, jobs",
    [
        ("save", (REFUSED, REFUSED)),
        ("entry", (REFUSED, REFUSED)),
        ("save", (COMPLETED, COMPLETED)),
        ("save", (REFUSED, COMPLETED)),
    ],
    ids=["refused-at-save", "refused-at-entry", "success-at-save", "disagree-at-save"],
)
def test_two_processes_finalizing_one_job_record_and_announce_it_once(
    shared_store, request, where, jobs
):
    from services import schedule_run_lifecycle as life

    store, pair = shared_store
    name = request.node.callspec.id
    sched = store.create_schedule({
        "name": f"two-proc {name}", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": f"orders_{name}", "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}], "max_retries": 0,
    })
    if COMPLETED in jobs:
        store.update_schedule(sched.id, {
            "approval_request": {"id": "apr_old", "status": "open", "code": "RUN_REFUSED",
                                 "job_id": "job-0", "occurrences": 1, "finding": "old"},
            "last_status": "needs_approval",
        })
    run_id = life.begin_run(sched.id, trigger=life.TRIGGER_CADENCE)
    assert store.mark_schedule_running(sched.id, "beat-a", "job-1") is not None
    life.mark_running(sched.id, run_id, "job-1")

    seen = pair.finalize(sched.id, jobs, where)

    assert [s.get("error") for s in seen] == [None, None], seen
    assert [s.get("overlapped") for s in seen] == [True, True], (
        "the finalizers never overlapped", [s.get("barrier") for s in seen])
    final = store.get_schedule(sched.id)
    notified = [tuple(n) for s in seen for n in s["notified"]]
    events = [e for s in seen for e in s["events"]]
    assert final.run_count == 1, final.run_count
    assert final.running is False and final.running_job_id == ""
    assert [r.get("job_id") for r in final.run_history] == ["job-1"]
    if jobs == (REFUSED, REFUSED):
        assert notified == [("job-1", "failed")]
        assert events.count("schedule.run.give_up") == 1
        assert events.count("approval.requested") == 1
        assert final.last_status == "needs_approval"
        assert final.approval_request["occurrences"] == 1
    elif jobs == (COMPLETED, COMPLETED):
        assert notified == [("job-1", "completed")]
        assert final.last_status == "completed"
        assert final.approval_request["resolved_by"] == "system:run-succeeded"
    else:
        # The two replicas read different verdicts for one job: the run is
        # recorded once, and each verdict is announced at most once.
        assert len(notified) == len(set(notified)) and notified
        assert events.count("approval.requested") <= 1
        assert events.count("schedule.run.give_up") <= 1
