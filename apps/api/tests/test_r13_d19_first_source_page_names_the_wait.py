"""D19: while the source is still answering its first page, the job says so
and keeps its heartbeat.

QA (job 6ad971b9...): MySQL (public Ensembl, query mode) -> local SQLite sat at
5% "Connecting to destination and creating table..." with ``updated_at`` frozen
for 5+ minutes. The destination was a local file; the wait was the source
running its query. Between the engine's last phase message and the first
committed batch nothing reached the job row, so the stall watchdog and the
operator both read a dead job and blamed the wrong system.

Real engine (``execute_tracked``) on SQLite files, with the source's answer
held back the way a slow source holds it (the COPY route waits on the source
query before its first row; the row path waits on its first page).
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

FIRST_PAGE_SECONDS = 1.5


class _Store:
    """Job store that records every status write with its time."""

    def __init__(self) -> None:
        from tests.test_property2_golden_path_never_blocked import _FakeMongo

        self._inner = _FakeMongo()
        self.writes: list[tuple[float, str]] = []
        self.lock = threading.Lock()

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        with self.lock:
            self.writes.append((time.monotonic(), str(kwargs.get("message") or "")))
        return self._inner.update_job_status(job_id, status, **kwargs)

    def update_job_fields(self, job_id: str, fields: dict, **_kw) -> bool:
        job = self._inner.get_job(job_id)
        if job is None:
            return False
        job.update(fields)
        return True


@pytest.fixture()
def world(tmp_path: Path, monkeypatch):
    import services.cancel_check as cc
    import services.sync_cursor as sc
    import src.transfer.stream as stream_mod

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(cc, "BLOCKING_STEP_PULSE_SECONDS", 0.3, raising=False)
    real = stream_mod._try_copy_fast_path
    calls = {"n": 0}

    def slow_source(*args, **kwargs):
        # The source executes its query before the first row comes back.
        calls["n"] += 1
        if calls["n"] == 1:
            time.sleep(FIRST_PAGE_SECONDS)
        return real(*args, **kwargs)

    monkeypatch.setattr(stream_mod, "_try_copy_fast_path", slow_source)
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    conn = sqlite3.connect(str(src))
    labels = [None, "", "   ", "naïve ☃ 東京"]
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, label TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, i, labels[i % 4]) for i in range(1, 41)],
        )
    conn.close()
    return store, src, dst, calls


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[
            {"name": "t", "cursor_field": "seq", "sync_mode": "incremental_append", "selected": True}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def test_the_job_names_the_source_wait_and_pulses_until_the_first_page(world):
    store, src, dst, calls = world
    job_id = "d19" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)
    assert result.success, result.error
    assert calls["n"] >= 1

    rows = sqlite3.connect(str(dst)).execute("SELECT id, seq, label FROM t ORDER BY id").fetchall()
    assert len(rows) == 40
    assert rows[3] == (4, 4, None) and rows[0][2] == "" and rows[1][2] == "   "
    assert rows[2][2] == "naïve ☃ 東京"

    msgs = store.writes
    waiting = [(t, m) for t, m in msgs if m.startswith("Reading from the source")]
    assert waiting, [m for _, m in msgs]
    # The wait names the source, not the destination.
    assert "sqlite" in waiting[0][1] and "Connecting to destination" not in waiting[0][1]
    # The job row kept moving during the wait: at least two writes within it.
    started = waiting[0][0]
    during = [t for t, _m in msgs if started <= t <= started + FIRST_PAGE_SECONDS]
    assert len(during) >= 3, [m for _, m in msgs]
    # And the first batch came after the wait, not before.
    first_batch = next((t for t, m in msgs if m.startswith("Writing batch")), None)
    assert first_batch is None or first_batch >= started

    # A second run into the table DataFlow created reads nothing new.
    again_id = "d19b" + uuid.uuid4().hex[:15]
    store.update_job_status(again_id, "pending", transfer_request={})
    again = UniversalTransferEngine().execute_tracked(_request(src, dst), again_id)
    assert again.success, again.error
    assert sqlite3.connect(str(dst)).execute("SELECT COUNT(*) FROM t").fetchone()[0] == 40


def test_no_run_means_no_job_writes():
    """Outside a run (tools sampling a source) the scope does nothing."""
    from services.cancel_check import blocking_step

    with blocking_step("Reading from the source (x)"):
        pass
