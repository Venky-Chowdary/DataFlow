"""The file-backed schedule store never loses or hides a schedule under concurrency.

CI run 41106911750 failed ``test_r5_schedule_run_lifecycle_e2e`` with
``get_schedule(...)`` returning None mid-test. The cause was the store, not the
test: ``_save_all`` rewrote ``schedules.json`` with ``write_text`` (truncate,
then write), so a reader in that window parsed an empty/partial file and saw no
schedules at all. A run finishing on the job thread while the scheduler (or an
operator's GET) read the store made the schedule vanish, and a run result
recorded in that window was dropped as "no such schedule".

Second, the store's writers took two different locks (name writers vs claim
writers) and some none, so two whole-file read-modify-writes interleaved and
the later save dropped the other's change: a schedule created while a run was
being recorded was lost.
"""

from __future__ import annotations

import threading

import pytest

import services.schedule_store as store


@pytest.fixture
def file_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    return tmp_path / "schedules.json"


def _sched(sid: str, **extra) -> store.PipelineSchedule:
    return store.PipelineSchedule.from_dict({
        "id": sid, "name": sid, "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": f"t_{sid}", "interval": "hourly",
        "enabled": True,
        # A realistic history makes each save a multi-block write.
        "run_history": [
            {"job_id": f"j{i}", "status": "completed", "error": "x" * 200} for i in range(40)
        ],
        **extra,
    })


def test_reader_never_sees_the_store_empty_while_a_run_is_saved(file_store):
    store._save_all([_sched("a")])
    stop = threading.Event()
    errors: list[BaseException] = []

    def _writer() -> None:
        try:
            while not stop.is_set():
                store.clear_schedule_running("a")
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    t = threading.Thread(target=_writer, daemon=True)
    t.start()
    try:
        missing = sum(1 for _ in range(3000) if store.get_schedule("a") is None)
    finally:
        stop.set()
        t.join(timeout=30)
    assert not t.is_alive()
    assert not errors, errors
    assert missing == 0, f"get_schedule returned None {missing}/3000 times during saves"


def test_concurrent_edit_and_run_record_both_survive(file_store):
    store._save_all([_sched("a"), _sched("b")])
    rounds = 150
    errors: list[BaseException] = []

    def _edits() -> None:  # operator edits schedule a (name-lock writer)
        try:
            for i in range(rounds):
                store.update_schedule("a", {"name": f"a edit {i}"})
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def _runs() -> None:  # job threads finishing runs of schedule b (claim writer)
        try:
            for i in range(rounds):
                store.mark_schedule_run("b", f"job-{i}", status="completed")
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def _creates() -> None:  # new pipelines saved meanwhile
        try:
            for i in range(20):
                with store._NAME_LOCK:
                    rows = store._load_all()
                    rows.append(_sched(f"new{i}"))
                    store._save_all(rows)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=f, daemon=True) for f in (_edits, _runs, _creates)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not any(t.is_alive() for t in threads)
    assert not errors, errors
    rows = {s.id: s for s in store._load_all()}
    assert {f"new{i}" for i in range(20)} <= set(rows), sorted(rows)
    assert rows["a"].name == f"a edit {rounds - 1}", rows["a"].name
    assert rows["b"].last_job_id == f"job-{rounds - 1}", rows["b"].last_job_id
