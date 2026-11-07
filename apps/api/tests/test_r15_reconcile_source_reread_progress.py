"""QA 2026-11-06 (job 6aee763d586f58f1b23b772a): reconcile showed no progress.

Reporting Postgres -> Inventory MySQL, incremental deduped on ``id``, 1,000,000
rows. The writer acknowledged batch 200/200 at 8:19:42 PM. The job then sat at
99% "Reconciling data / Verifying fidelity..." with no new message until the
operator cancelled at 8:24:32 PM.

After the last batch the stream re-reads the whole run window from the source
and fingerprints every row (Gate-8's independent source read, warehouse to
warehouse). That loop ran inside ``stream_database_transfer``, before the
reconcile heartbeat starts. It posted no progress and never polled for a
cancel, so nothing said it was working and a cancel did not stop it.

Real SQLite -> SQLite runs through the engine. Both engines are in the re-read
set, so the run takes the same path as the reported route.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
import src.transfer.stream as stream_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ROWS = 600
PAGE = 50
REREAD_PREFIX = "Verifying, step 1 of 2"
GATE8_ENTER = "All rows written — reconciling destination"


class _RecordingMongo:
    """The test job store, recording every message the run posts."""

    def __init__(self) -> None:
        from tests.test_property2_golden_path_never_blocked import _FakeMongo

        self._inner = _FakeMongo()
        self.jobs = self._inner.jobs
        self.messages: list[str] = []
        #: Messages written through update_job_status: each is an event-log line.
        self.status_messages: list[str] = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def _note(self, fields: dict) -> None:
        msg = str((fields or {}).get("message") or "")
        if msg:
            self.messages.append(msg)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self._note(kwargs)
        if kwargs.get("message"):
            self.status_messages.append(str(kwargs["message"]))
        return self._inner.update_job_status(job_id, status, **kwargs)

    def update_job_fields(self, job_id: str, fields: dict, **_kw) -> bool:
        self._note(fields)
        return self._inner.update_job_fields(job_id, fields)


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


def _count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])
    finally:
        conn.close()


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "id",
                "primary_key": ["id"],
                "sync_mode": "incremental_deduped",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.fixture()
def world(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc

    store = _RecordingMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    # Several pages, so the re-read has progress to report and a page boundary
    # to stop at.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", PAGE)
    # Every page reports: the pulse thread is not what this test times.
    monkeypatch.setattr(stream_mod, "SOURCE_REREAD_PULSE_SECONDS", 0.01, raising=False)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC, note TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)",
          [(i, f"{i}.25", f"row {i} ✓") for i in range(1, ROWS + 1)])
    return store, src, dst


def _run(store, src: Path, dst: Path):
    job_id = "r15rr" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request={})
    return job_id, UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)


def test_the_source_reread_reports_progress_before_the_destination_checksum(world):
    store, src, dst = world
    _job, result = _run(store, src, dst)
    assert result.success, result.error
    assert _count(dst) == ROWS

    reread = [m for m in store.messages if m.startswith(REREAD_PREFIX)]
    assert reread, store.messages[-15:]
    # It names the population and moves: the last report covers every row.
    assert any(f"{ROWS:,} of {ROWS:,} rows" in m for m in reread), reread[-3:]
    # It comes after the writer's last batch and before Gate-8's own phase.
    first_reread = store.messages.index(reread[0])
    gate8 = [i for i, m in enumerate(store.messages) if m.startswith(GATE8_ENTER)]
    assert gate8 and first_reread < gate8[0], (first_reread, gate8)
    # The Jobs log gets about one line per tenth, not one per pulse: twelve
    # pages here each moved the count.
    logged = [m for m in store.status_messages if m.startswith(REREAD_PREFIX)]
    assert len(logged) <= 12, len(logged)
    assert f"{ROWS:,} of {ROWS:,} rows" in logged[-1], logged[-1]
    summary = result.destination_summary or {}
    assert summary.get("source_independently_reread") is not False, summary.get("checksum_mode")


def test_a_cancel_during_the_source_reread_stops_it_at_the_next_page(world, monkeypatch):
    store, src, dst = world
    pages = {"n": 0}
    real_map = stream_mod.map_rows_for_fingerprint

    def _map_and_cancel(*args, **kwargs):
        # The operator presses Cancel while the re-read is on its first page.
        pages["n"] += 1
        if pages["n"] == 1:
            for job in store.jobs.values():
                job["cancel_requested"] = True
        return real_map(*args, **kwargs)

    monkeypatch.setattr(stream_mod, "map_rows_for_fingerprint", _map_and_cancel)
    job_id, result = _run(store, src, dst)

    assert not result.success
    assert "cancel" in str(result.error or "").lower(), result.error
    # Stopped at the next page boundary: one page re-read, not all of them.
    assert pages["n"] <= 2, pages
    # Gate-8 never started on a cancelled run.
    assert not any(m.startswith(GATE8_ENTER) for m in store.messages)
    # The rows the writer committed stay committed, and the job says so.
    assert _count(dst) == ROWS
    assert int(store.jobs[job_id].get("records_processed") or 0) == ROWS
