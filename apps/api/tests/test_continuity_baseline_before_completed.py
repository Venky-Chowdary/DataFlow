"""The append-continuity baseline is recorded before a job reads ``completed``.

Found while root-causing a CI failure on PR 395 (test_d206): the engine stored
the success status and only then, still on the job's worker, read the whole
destination to record the baseline the next append is compared with. An
append started in that window (a schedule, an operator, the next tool call)
compared the destination, already holding this run's rows, with the previous
run's baseline and was refused as "changed outside the pipeline". On a large
table the window is the whole fingerprint read.

Real SQLite -> SQLite runs through the engine. The job store hook stands in
for "the next append starts the moment this one reads completed".
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.sync_cursor as sc  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from services import append_continuity as ac  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402

SUCCESS = {"completed", "completed_with_quarantine"}


class _WatchingStore(_FakeMongo):
    """Runs ``on_completed(job_id)`` as the success status is stored."""

    def __init__(self) -> None:
        super().__init__()
        self.on_completed: Any = None

    def update_job_status(self, job_id, status, *args, **kwargs):  # type: ignore[override]
        if status in SUCCESS and self.on_completed is not None:
            self.on_completed(job_id)
        return super().update_job_status(job_id, status, *args, **kwargs)


@pytest.fixture
def store(monkeypatch, tmp_path):
    fake = _WatchingStore()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(ac, "_collection", lambda: None)
    monkeypatch.setattr(ac, "store_path", lambda: tmp_path / "append_continuity.json")
    return fake


def _rows(path: Path, lo: int, hi: int) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i, f"v{i}") for i in range(lo, hi)])
    conn.close()


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": "t", "sync_mode": "incremental_append", "selected": True,
            "cursor_field": "seq", "primary_key": ["id"],
        }],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(store: _WatchingStore, src: Path, dst: Path):
    job_id = "cbl-" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return job_id, UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)


@pytest.fixture
def route(tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    conn.close()
    _rows(src, 1, 11)
    return src, dst


def test_the_baseline_names_the_run_when_it_reads_completed(store, route):
    src, dst = route
    key = ac.resolve_target(_request(src, dst).destination).key
    seen: dict[str, Any] = {}

    def at_completed(job_id: str) -> None:
        seen[job_id] = (ac.load_baseline(key) or {}).get("job_id")

    store.on_completed = at_completed
    first_id, first = _run(store, src, dst)
    assert first.success, first.error
    _rows(src, 11, 16)
    second_id, second = _run(store, src, dst)
    assert second.success, second.error
    assert seen == {first_id: first_id, second_id: second_id}


def test_an_append_started_as_the_previous_one_completes_is_not_refused(store, route):
    src, dst = route
    first_id, first = _run(store, src, dst)
    assert first.success, first.error
    _rows(src, 11, 16)
    verdicts: list[Any] = []
    store.on_completed = lambda _job: verdicts.append(ac.check_before_write(_request(src, dst)))
    _second_id, second = _run(store, src, dst)
    assert second.success, second.error
    assert verdicts == [None], [getattr(v, "message", v) for v in verdicts]
    # ...and the next append really runs green.
    store.on_completed = None
    _rows(src, 16, 18)
    _third_id, third = _run(store, src, dst)
    assert third.success, third.error


def test_a_failed_run_records_no_baseline(store, route, monkeypatch):
    src, dst = route
    key = ac.resolve_target(_request(src, dst).destination).key
    first_id, first = _run(store, src, dst)
    assert first.success, first.error
    _rows(src, 11, 16)

    def boom(*_a, **_k):
        raise RuntimeError("write failed")

    monkeypatch.setattr(engine_mod.UniversalTransferEngine, "_execute_tracked_inner", boom)
    with pytest.raises(RuntimeError):
        _run(store, src, dst)
    assert (ac.load_baseline(key) or {}).get("job_id") == first_id
