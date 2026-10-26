"""D102: cancellation is honoured between write batches, not after the run.

Base behaviour: the only cancel check rode inside ``ThrottledCheckpoint``,
which drops intermediate post-commit callbacks — a cancelled job ran the
remaining batches to completion, and the COPY fast path never checked at
all. The job then reported 'Transfer cancelled by user' with no committed
row count, and progress text printed fake totals like 'Writing batch 7/1'.

Every test here fails on the pre-fix code.
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


# ---------------------------------------------------------------------------
# cancel_check helpers
# ---------------------------------------------------------------------------


def test_cancel_check_for_raises_after_flag() -> None:
    from services.cancel_check import (
        cancel_check_for,
        job_cancel_requested,
        raise_if_job_cancelled,
    )
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "d102-a", "status": "running"})

    assert job_cancel_requested(mongo, job_id) is False
    check = cancel_check_for(mongo, job_id)
    check(0)  # not cancelled yet — no raise

    assert mongo.request_job_cancel(job_id) is True
    assert job_cancel_requested(mongo, job_id) is True
    with pytest.raises(TransferCancelled) as ei:
        check(5)
    assert ei.value.rows_written == 5
    with pytest.raises(TransferCancelled):
        raise_if_job_cancelled(mongo, job_id, rows_written=7)


def test_cancel_probe_is_per_thread() -> None:
    from services.cancel_check import cancel_checkpoint, set_cancel_probe
    from services.error_handling import TransferCancelled

    try:
        cancel_checkpoint(0)  # no probe registered — no-op
        set_cancel_probe(lambda rows=None: (_ for _ in ()).throw(
            TransferCancelled("x", rows_written=rows)
        ))
        with pytest.raises(TransferCancelled):
            cancel_checkpoint(3)
    finally:
        set_cancel_probe(None)
    cancel_checkpoint(0)


# ---------------------------------------------------------------------------
# Writer-level: the next chunk must not land after cancel
# ---------------------------------------------------------------------------


def _sqlite_args(dest: Path, rows: list[list[str]]) -> dict:
    return {
        "host": "",
        "port": 0,
        "database": str(dest),
        "username": "",
        "password": "",
        "schema": "main",
        "connection_string": "",
        "ssl": False,
        "table_name": "t",
        "headers": ["id", "v"],
        "data_rows": rows,
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "v", "target": "v"},
        ],
        "column_types": {"id": "INTEGER", "v": "TEXT"},
    }


def _count(dest: Path, table: str = "t") -> int:
    conn = sqlite3.connect(str(dest))
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def test_sqlite_writer_stops_at_next_chunk_after_cancel(tmp_path, monkeypatch):
    """Cancel at batch 2 of 4 → only the first committed chunk lands."""
    import connectors.sqlite_writer as w
    from services.cancel_check import cancel_check_for, set_cancel_probe
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService

    monkeypatch.setattr(w, "CHUNK_SIZE", 3)
    dest = tmp_path / "dest.db"
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "d102-sqlite", "status": "running"})
    calls = {"n": 0}
    real_check = cancel_check_for(mongo, job_id)

    def probe(rows_written=None):
        calls["n"] += 1
        if calls["n"] >= 2:  # second batch boundary → user hit cancel
            mongo.request_job_cancel(job_id)
        real_check(rows_written)

    set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled) as ei:
            w.write_mapped_rows(
                **_sqlite_args(dest, [[str(i), f"v{i}"] for i in range(10)])
            )
    finally:
        set_cancel_probe(None)

    landed = _count(dest)
    assert landed == 3  # only the first committed chunk — not all 10
    assert ei.value.rows_written == landed


def test_pg_writer_stops_at_next_chunk_after_cancel(monkeypatch):
    """PostgreSQL writer honours the same boundary via the shared probe."""
    import connectors.postgresql_writer as w
    from services.cancel_check import cancel_check_for, set_cancel_probe
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService

    monkeypatch.setattr(w, "write_chunk_size", lambda *a, **k: 2)
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "d102-pg", "status": "running"})
    calls = {"n": 0}
    real_check = cancel_check_for(mongo, job_id)

    def probe(rows_written=None):
        calls["n"] += 1
        if calls["n"] >= 2:
            mongo.request_job_cancel(job_id)
        real_check(rows_written)

    class FakeCur:
        """Enough of a psycopg2 cursor for the writer's dense COPY path."""

        def __init__(self):
            self.executed = []
            self.last = ""
            self.copied_rows = 0

        def execute(self, sql, params=None):
            self.executed.append(str(sql))
            self.last = str(sql)

        def executemany(self, sql, batch):
            self.executed.append(("many", str(sql), len(batch)))
            self.last = "many"

        def copy_expert(self, sql, buf):
            data = buf.read()
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            n = len([ln for ln in data.splitlines() if ln.strip()])
            self.copied_rows += n
            self.executed.append(("copy", n))

        def fetchall(self):
            s = self.last
            if "numeric_precision" in s:
                return [
                    ("id", "integer", "int4", None, 32, 0),
                    ("v", "text", "text", None, None, None),
                ]
            if "information_schema.columns" in s:
                return [("id",), ("v",)]
            if "information_schema.tables" in s:
                return [("t",)]
            if "COUNT" in s.upper():
                return [(4,)]
            return []

        def fetchone(self):
            rows = self.fetchall()
            return rows[0] if rows else (0,)

        def fetchmany(self, n=1):
            return self.fetchall()[:n]

        def close(self):
            pass

    class FakeConn:
        def __init__(self):
            self.cur = FakeCur()

        def cursor(self):
            return self.cur

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    conn = FakeConn()
    set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled):
            w.write_mapped_rows(
                host="h",
                port=5432,
                database="db",
                username="u",
                password="p",
                schema="public",
                connection_string="",
                ssl=False,
                table_name="t",
                headers=["id", "v"],
                data_rows=[["1", "a"], ["2", "b"], ["3", "c"], ["4", "d"]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "v", "target": "v"},
                ],
                column_types={"id": "integer", "v": "text"},
                connection=conn,
                close_connection=False,
                create_table=False,
            )
    finally:
        set_cancel_probe(None)
    # The probe fired before the second chunk landed — the COPY landed at
    # most one committed chunk (2 rows), never the full 4.
    copies = [e for e in conn.cur.executed if isinstance(e, tuple) and e[0] == "copy"]
    assert sum(e[1] for e in copies) <= 2


# ---------------------------------------------------------------------------
# COPY fast path: sinks and merges honour the probe
# ---------------------------------------------------------------------------


def test_copy_executemany_sink_stops_after_cancel(monkeypatch):
    from services.cancel_check import cancel_check_for, set_cancel_probe
    from services.copy_pg_sqlserver import _CopyExecutemanySink
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "d102-sink", "status": "running"})
    calls = {"n": 0}
    real_check = cancel_check_for(mongo, job_id)

    def probe(rows_written=None):
        calls["n"] += 1
        if calls["n"] >= 2:
            mongo.request_job_cancel(job_id)
        real_check(rows_written)

    flushed: list[int] = []

    class FakeCur:
        def execute(self, *a, **k):
            pass

        def executemany(self, sql, batch):
            flushed.append(len(batch))

    sink = _CopyExecutemanySink(
        FakeCur(), "INSERT INTO t VALUES (%s,%s)", [str, str], 2, 2
    )
    set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled):
            # 6 rows → 3 batches; cancel lands after the first flush.
            sink.write(b"1\ta\n2\tb\n3\tc\n4\td\n5\te\n6\tf\n")
    finally:
        set_cancel_probe(None)
    assert flushed == [2]
    assert sink.rows == 2


def test_merge_staging_refused_after_cancel():
    """The one-statement staging→dest merge must not start after a cancel."""
    from services.cancel_check import cancel_check_for, set_cancel_probe
    from services.copy_upsert import merge_staging_into_dest
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"_id": "d102-merge", "status": "running"})
    mongo.request_job_cancel(job_id)

    ran: list[str] = []

    class FakeCur:
        def execute(self, sql, params=None):
            ran.append(str(sql))

        def fetchone(self):
            return (0,)

    set_cancel_probe(cancel_check_for(mongo, job_id))
    try:
        with pytest.raises(TransferCancelled):
            merge_staging_into_dest(
                FakeCur(),
                merge_sql="INSERT INTO d SELECT * FROM s",
                dest_q='"d"',
                staging_q='"s"',
                pk_ident='"id"',
            )
    finally:
        set_cancel_probe(None)
    # Only the pre-merge join count ran; the merge itself never executed.
    assert all("INSERT INTO d SELECT" not in s for s in ran)


# ---------------------------------------------------------------------------
# Progress text: never print a denominator we never knew
# ---------------------------------------------------------------------------


def test_batch_write_message_no_fake_total():
    from services.batch_progress import batch_write_message

    msg = batch_write_message(7, 1, 7000, checkpoint=None, stream_contracts=[])
    assert "/1" not in msg
    assert "batch 7" in msg
    # A known total still prints chunk/chunks.
    msg2 = batch_write_message(3, 4, 3000, checkpoint=None, stream_contracts=[])
    assert "3/4" in msg2


# ---------------------------------------------------------------------------
# Engine end-to-end: cancel mid-run reports the committed prefix
# ---------------------------------------------------------------------------


class _CancelMongo:
    """Job store that flips cancel_requested on the Nth get_job call.

    Counting get_job calls is deterministic on both sides of the fix: the
    batch-boundary cancel probes poll the store per chunk, while the
    pre-fix code only polls through dropped throttled checkpoints, so the
    flag lands too late and every row commits (the pre-fix failure).
    """

    def __init__(self, cancel_on_call: int) -> None:
        self.jobs: dict[str, dict] = {}
        self._cancel_on_call = cancel_on_call
        self._calls = 0

    def get_job(self, job_id: str):
        rec = self.jobs.get(job_id)
        if rec is not None and not rec.get("cancel_requested"):
            self._calls += 1
            if self._calls >= self._cancel_on_call:
                rec["cancel_requested"] = True
        return rec

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(fields or {})
        return True

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = job_data.get("job_id") or uuid.uuid4().hex[:24]
        self.jobs[job_id] = dict(job_data)
        return job_id

    def request_job_cancel(self, job_id: str) -> bool:
        rec = self.jobs.get(job_id)
        if not rec:
            return False
        rec["cancel_requested"] = True
        return True

    def is_cancel_requested(self, job_id: str) -> bool:
        rec = self.jobs.get(job_id)
        return bool(rec and rec.get("cancel_requested"))


def _exec(db: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(db))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


def _seed_source(src: Path, n: int) -> None:
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER, v TEXT)")
    _exec(
        src,
        "INSERT INTO t (seq, v) VALUES (?, ?)",
        [(i, f"v{i}") for i in range(n)],
    )


def _request(src: Path, dst: Path, sync_mode: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode=sync_mode,
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "seq",
                "sync_mode": sync_mode,
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    import services.sync_cursor as sc

    fake = _CancelMongo(cancel_on_call=5)
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    # The COPY fast path stages into a single INSERT SELECT — cover the row
    # path (the fallback every run may take) by declining it here.
    monkeypatch.setattr(stream_mod, "_try_copy_fast_path", lambda **kw: None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_incremental_append_cancel_mid_run_reports_committed(
    tmp_path: Path, isolated, monkeypatch
):
    """Cancel mid-append: bounded committed prefix + honest job report."""
    import connectors.sqlite_writer as w
    from src.transfer.engine import UniversalTransferEngine

    monkeypatch.setattr(w, "CHUNK_SIZE", 4)
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed_source(src, 12)

    job_id = "d102-append-" + uuid.uuid4().hex[:8]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    UniversalTransferEngine().execute_tracked(
        _request(src, dst, "incremental_append"), job_id
    )

    job = isolated.get_job(job_id)
    assert job["status"] == "cancelled"
    landed = _count(dst)
    assert landed < 12  # the batches after the cancel never landed
    outcome = job.get("cancel_outcome") or {}
    assert outcome.get("rows_committed") == landed
    assert outcome.get("partial_write") == (landed > 0)
    assert int(job.get("records_processed") or 0) == landed


def test_incremental_upsert_cancel_mid_run_reports_committed(
    tmp_path: Path, isolated, monkeypatch
):
    import connectors.sqlite_writer as w
    from src.transfer.engine import UniversalTransferEngine

    monkeypatch.setattr(w, "CHUNK_SIZE", 4)
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed_source(src, 12)
    # upsert needs a key: declare one in the contract.
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="incremental_upsert",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "seq",
                "primary_key": "id",
                "sync_mode": "incremental_upsert",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "d102-upsert-" + uuid.uuid4().hex[:8]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    UniversalTransferEngine().execute_tracked(req, job_id)

    job = isolated.get_job(job_id)
    assert job["status"] == "cancelled"
    landed = _count(dst)
    assert landed < 12
    outcome = job.get("cancel_outcome") or {}
    assert outcome.get("rows_committed") == landed
    assert outcome.get("partial_write") == (landed > 0)
    assert int(job.get("records_processed") or 0) == landed
