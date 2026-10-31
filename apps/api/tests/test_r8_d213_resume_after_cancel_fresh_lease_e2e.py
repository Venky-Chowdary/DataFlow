"""QA D213: a resume after a cancel ends "Lease lost to another worker".

QA on 557f556c: cancel an incremental job before its first write, resume it.
The resume reported "running" and then always ended ``cancelled`` with
"Lease lost to another worker; aborting to prevent dual writes". MySQL ->
SQLite had already landed all 31 rows correctly and still told the user to
undo them; SQL Server -> MySQL landed nothing.

Root cause: a cancel is honoured at the run's next checkpoint, so the
cancelled run is often still alive when Resume schedules the same job id.
The scheduler's lease store is one per process with one worker id, so the
resumed run re-acquired the lease the cancelled run still held (the memory
store even handed back the same fence). When the cancelled run then stopped,
its ``finally`` released "its" lease, which was the resumed run's. The
resumed run's next heartbeat was refused and ``on_lost`` cancelled the job.

Route here is QA's: the first run goes through the durable scheduler
(``run_transfer_async``, what /transfer and MCP confirm use), the operator
cancels through ``POST /api/v1/connectors/jobs/{id}/cancel`` and resumes
through ``POST /api/v1/connectors/jobs/{id}/resume`` while the cancelled run
is still stopping. Two cancel points: before any row was written, and after a
partial write. The resumed run must end ``completed`` with every source row
exactly once (NULL, '', whitespace and unicode intact), and its message must
not speak of a lost lease or of undoing rows.

Live MySQL + SQL Server (``MYSQL_*`` / ``DATAFLOW_TEST_MSSQL_*``); skipped
when absent.
"""
from __future__ import annotations

import contextvars
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

ROWS = 31
#: Short TTL so the heartbeat runs every 5 s and a lost lease shows quickly.
LEASE_TTL = 10
BEAT = max(5, LEASE_TTL // 2)


def _mssql_cfg() -> dict | None:
    host, password = os.environ.get("DATAFLOW_TEST_MSSQL_HOST"), os.environ.get(
        "DATAFLOW_TEST_MSSQL_PASSWORD"
    )
    if not (host and password):
        return None
    return {
        "host": host,
        "port": int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT") or 1433),
        "username": os.environ.get("DATAFLOW_TEST_MSSQL_USER") or "sa",
        "password": password,
        "database": os.environ.get("DATAFLOW_TEST_MSSQL_DB") or "dataflow",
    }


def _text(i: int) -> str | None:
    if i % 5 == 0:
        return None
    if i % 7 == 0:
        return ""
    if i % 11 == 0:
        return "  "
    if i % 3 == 0:
        return f"é漢{i}"
    return f"a{i}"


def _source_rows() -> list[tuple]:
    base = datetime(2026, 1, 1)
    return [
        (i, _text(i), Decimal(i % 97) / Decimal(4), base + timedelta(seconds=i))
        for i in range(1, ROWS + 1)
    ]


def _want() -> list[tuple]:
    return [(i, a, float(n)) for i, a, n, _ in _source_rows()]


@pytest.fixture
def env(tmp_path, monkeypatch):
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.transfer.stream as stream_mod
    from services.mongodb_service import MemoryMongoDBService

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_WORKER_LEASE_TTL", str(LEASE_TTL))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    store = MemoryMongoDBService()
    store.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    # Small pages so "after a partial write" has a committed page to stop on.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 10)
    return store


class _Route:
    """One source -> destination pair: create, read back, drop."""

    def __init__(self, name: str, tmp_path):
        from tests.helpers.live_env import mysql_creds, mysql_up

        self.name = name
        self.suffix = uuid.uuid4().hex[:8]
        self.src_t = f"r8d213s_{self.suffix}"
        self.dst_t = f"r8d213d_{self.suffix}"
        if not mysql_up():
            pytest.skip("live MySQL not configured")
        self.my = mysql_creds()
        self.ms = _mssql_cfg() if name == "sqlserver_to_mysql" else None
        if name == "sqlserver_to_mysql" and self.ms is None:
            pytest.skip("live SQL Server not configured")
        self.sqlite_path = str(tmp_path / "d213.db")

    def _mysql(self):
        import pymysql

        c = self.my
        return pymysql.connect(
            host=c["host"], port=int(c["port"]), user=c["username"],
            password=c["password"], database=c["database"], autocommit=True,
        )

    def _mssql_engine(self):
        from connectors.generic_sql import _engine

        return _engine({"type": "sqlserver", "schema": "dbo", "ssl": False,
                        "trust_server_certificate": True, **self.ms})

    def create_source(self) -> None:
        rows = _source_rows()
        if self.ms is None:
            con = self._mysql()
            with con.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE `{self.src_t}` (id INT PRIMARY KEY, a VARCHAR(40) "
                    "CHARACTER SET utf8mb4, n DECIMAL(12,2), updated_at DATETIME NOT NULL)"
                )
                cur.executemany(f"INSERT INTO `{self.src_t}` VALUES (%s,%s,%s,%s)", rows)  # nosec B608
            con.close()
            return
        import sqlalchemy as sa

        eng = self._mssql_engine()
        with eng.begin() as conn:
            conn.execute(sa.text(
                f"CREATE TABLE dbo.[{self.src_t}] (id INT PRIMARY KEY, a NVARCHAR(40), "
                "n DECIMAL(12,2), updated_at DATETIME2 NOT NULL)"
            ))
            conn.execute(
                sa.text(f"INSERT INTO dbo.[{self.src_t}] VALUES (:i, :a, :n, :u)"),  # nosec B608
                [{"i": i, "a": a, "n": n, "u": u} for i, a, n, u in rows],
            )

    def request(self):
        from src.transfer.models import EndpointConfig, TransferRequest

        my = self.my
        if self.ms is None:
            source = EndpointConfig(
                kind="database", format="mysql", host=str(my["host"]), port=int(my["port"]),
                database=str(my["database"]), username=str(my["username"]),
                password=str(my["password"]), table=self.src_t,
            )
            dest = EndpointConfig(
                kind="database", format="sqlite", database=self.sqlite_path, table=self.dst_t,
            )
        else:
            source = EndpointConfig(
                kind="database", format="sqlserver", table=self.src_t, schema="dbo",
                extra={"trust_server_certificate": True}, **self.ms,
            )
            dest = EndpointConfig(
                kind="database", format="mysql", host=str(my["host"]), port=int(my["port"]),
                database=str(my["database"]), username=str(my["username"]),
                password=str(my["password"]), table=self.dst_t,
            )
        mode = "incremental_deduped"
        return TransferRequest(
            source=source,
            destination=dest,
            sync_mode=mode,
            stream_contracts=[{
                "name": self.src_t, "selected": True, "sync_mode": mode,
                "primary_key": ["id"], "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
            }],
            validation_mode="balanced",
            # Keeps the job on the paged row writer (no bulk fast path), so the
            # partial-write leg has a committed page to stop on.
            source_filter={"column": "id", "op": "gt", "value": 0},
        )

    def dest_rows(self) -> list[tuple] | None:
        sql = f"SELECT id, a, n FROM {{t}} ORDER BY id"  # nosec B608
        try:
            if self.ms is None:
                con = sqlite3.connect(self.sqlite_path)
                try:
                    got = con.execute(sql.format(t=f'"{self.dst_t}"')).fetchall()
                finally:
                    con.close()
            else:
                con = self._mysql()
                with con.cursor() as cur:
                    cur.execute(sql.format(t=f"`{self.dst_t}`"))
                    got = cur.fetchall()
                con.close()
        except Exception:  # noqa: BLE001 - not created yet
            return None
        return [(int(i), a, None if n is None else float(n)) for i, a, n in got]

    def drop(self) -> None:
        con = self._mysql()
        with con.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{self.dst_t}`")
            cur.execute(f"DROP TABLE IF EXISTS `{self.src_t}`")
        con.close()
        if self.ms is not None:
            import sqlalchemy as sa

            with self._mssql_engine().begin() as conn:
                conn.execute(sa.text(
                    f"IF OBJECT_ID('dbo.{self.src_t}') IS NOT NULL DROP TABLE dbo.[{self.src_t}]"
                ))


# (cancel_at, timing). ``while_stopping`` is QA's race: Resume lands before
# the cancelled run reached its next checkpoint. ``after_stopped`` resumes once
# it is gone; before any write that case already passed on 557f556c, so only
# the partial-write one is kept (it failed Gate-8 on SQL Server and SQLite).
_CASES = [
    ("before_first_write", "while_stopping"),
    ("after_partial_write", "while_stopping"),
    ("after_partial_write", "after_stopped"),
]


@pytest.mark.parametrize("cancel_at,timing", _CASES)
@pytest.mark.parametrize("route", ["mysql_to_sqlite", "sqlserver_to_mysql"])
def test_resume_after_cancel_completes_with_every_row(
    env, tmp_path, monkeypatch, route, cancel_at, timing
):
    from fastapi.testclient import TestClient

    from services import transfer_scheduler
    from src.main import app
    from src.transfer.background import run_transfer_async
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict

    store = env
    r = _Route(route, tmp_path)
    r.create_source()
    request = r.request()
    job_id = store.create_transfer_job({
        "status": "pending", "sync_mode": request.sync_mode,
        "transfer_request": transfer_request_to_dict(request),
    })

    first_paused = threading.Event()  # the first run reached its cancel point
    resumed_entered = threading.Event()  # the resumed run started
    release_first = threading.Event()  # the cancelled run may go on to its checkpoint
    first_done = threading.Event()  # the cancelled run returned
    calls = {"n": 0}
    lock = threading.Lock()
    run_no: contextvars.ContextVar[int | None] = contextvars.ContextVar("d213_run", default=None)

    original_inner = UniversalTransferEngine._execute_tracked_inner

    def gated_inner(self, req, jid, *a, **kw):
        with lock:
            n = calls["n"]
            calls["n"] += 1
        run_no.set(n)
        if jid != job_id:
            return original_inner(self, req, jid, *a, **kw)
        if n == 0:
            try:
                if cancel_at == "before_first_write":
                    # Slow pre-write phase: still alive when the operator
                    # cancels and resumes.
                    first_paused.set()
                    release_first.wait(90)
                return original_inner(self, req, jid, *a, **kw)
            finally:
                first_done.set()
        resumed_entered.set()
        release_first.set()
        # The resumed run outlives the cancelled run's exit by more than one
        # heartbeat, as any real transfer does.
        first_done.wait(90)
        if timing == "while_stopping":
            time.sleep(BEAT + 2)
        return original_inner(self, req, jid, *a, **kw)

    monkeypatch.setattr(UniversalTransferEngine, "_execute_tracked_inner", gated_inner)

    if cancel_at == "after_partial_write":
        import src.transfer.engine as engine_mod

        original_raise = engine_mod._raise_if_job_cancelled
        paused = {"done": False}

        def gated_raise(mongo, jid, *a, rows_written=None, **kw):
            # The engine's per-chunk cancel check, after the chunk committed.
            if (
                jid == job_id
                and run_no.get() == 0
                and not paused["done"]
                and (rows_written or 0) > 0
            ):
                paused["done"] = True
                first_paused.set()
                release_first.wait(90)
            return original_raise(mongo, jid, *a, rows_written=rows_written, **kw)

        monkeypatch.setattr(engine_mod, "_raise_if_job_cancelled", gated_raise)

    client = TestClient(app)

    def _status() -> str:
        return str((store.get_job(job_id) or {}).get("status") or "")

    try:
        run_transfer_async(job_id, request)
        assert first_paused.wait(120), ("the first run never reached its cancel point", store.get_job(job_id))
        landed_at_cancel = len(r.dest_rows() or [])
        if cancel_at == "before_first_write":
            assert landed_at_cancel == 0
        else:
            assert 0 < landed_at_cancel < ROWS, landed_at_cancel

        resp = client.post(f"/api/v1/connectors/jobs/{job_id}/cancel")
        assert resp.status_code == 200, resp.text
        assert _status() == "cancelled"
        if timing == "after_stopped":
            release_first.set()
            assert first_done.wait(120), "the cancelled run never stopped"
            deadline = time.time() + 60
            while time.time() < deadline and job_id in transfer_scheduler.local_job_ids():
                time.sleep(0.1)
            job = store.get_job(job_id) or {}
            assert job.get("status") == "cancelled", job.get("status")
            assert len(r.dest_rows() or []) == landed_at_cancel

        resp = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
        assert resp.status_code == 200, resp.text
        assert resumed_entered.wait(120), ("resume never started a run", store.get_job(job_id))

        deadline = time.time() + 240
        while time.time() < deadline and (
            job_id in transfer_scheduler.local_job_ids()
            or _status() not in {"completed", "failed", "cancelled", "completed_with_quarantine"}
        ):
            time.sleep(0.25)
        job = store.get_job(job_id) or {}
        text = f"{job.get('message') or ''} {job.get('error') or ''}"
        assert job.get("status") == "completed", (route, cancel_at, job.get("status"), text)
        assert "lease" not in text.lower(), text
        assert "undo" not in text.lower(), text
        assert not job.get("cancel_requested"), job.get("cancel_requested")
        assert r.dest_rows() == _want(), (route, cancel_at, r.dest_rows())
        # The lease the resumed run took was released when it finished.
        from services.transfer_scheduler import _lease_store

        assert not _lease_store.is_held(job_id)
    finally:
        resumed_entered.set()
        release_first.set()
        first_done.wait(60)
        deadline = time.time() + 60
        while time.time() < deadline and job_id in transfer_scheduler.local_job_ids():
            time.sleep(0.25)
        r.drop()


def test_a_superseded_run_cannot_release_or_lose_the_newer_runs_lease(monkeypatch):
    """The lease mechanics under the e2e above, without services.

    Same worker id (one scheduler per process), two runs of one job: the
    second acquire must issue a new fence; the first run's release and
    heartbeat act on its own fence only, and its refused beat is "superseded",
    never ``on_lost`` (which cancels the job the newer run is writing).
    """
    from services.lease_heartbeat import start_lease_heartbeat
    from services.worker_leases import WorkerLeaseStore, active_fence

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    store = WorkerLeaseStore("d213-worker")
    monkeypatch.setattr(store, "_mongo_collection", lambda: None)
    monkeypatch.setattr(WorkerLeaseStore, "_job_fence_floor", staticmethod(lambda _j: 0))
    job = f"d213-{uuid.uuid4().hex[:8]}"

    assert store.acquire(job, ttl_seconds=30)
    old_fence = store.get_fence(job)
    assert store.acquire(job, ttl_seconds=30)  # Resume while the old run stops.
    new_fence = store.get_fence(job)
    assert new_fence is not None and old_fence is not None and new_fence > old_fence

    lost = threading.Event()
    stop_old = start_lease_heartbeat(
        store, job, ttl_seconds=30, interval_seconds=0.01, on_lost=lost.set,
        fence=old_fence,
    )
    time.sleep(0.2)
    stop_old()
    assert not lost.is_set(), "the superseded run's beat cancelled the newer run"

    store.release(job, fence=old_fence)  # the cancelled run's finally
    assert active_fence(job) == new_fence
    assert store.is_held(job)
    assert store.heartbeat(job, ttl_seconds=30, fence=new_fence)

    store.release(job, fence=new_fence)
    assert not store.is_held(job)
    assert active_fence(job) is None
