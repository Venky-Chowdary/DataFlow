"""D326: a run waiting on its source says so and keeps its heartbeat moving.

QA: a second job on a busy source sat at "running 5% ..." with a frozen
``updated_at`` and no heartbeat for 19 minutes, so nobody could tell what it
waited on. Two jobs on one source connector do run concurrently in the engine
(there is no per-connector lock or queue in the job path); what blocked the
waiter was a read the source database did not answer. The peek read and the
population-fit walk had no wait message and no pulse (the main read already
had one, ``stream.blocking_step``).

The live case holds an ACCESS EXCLUSIVE lock on the source table from a second
session (the stand-in for anything on the source that keeps a read waiting)
and runs the engine: while blocked, the job record must name the source wait.
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
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_WAIT_WORDS = "waiting for the source"


def _pg_params() -> dict | None:
    keys = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")
    if not all(os.environ.get(k) for k in keys):
        return None
    return {
        "host": os.environ["PGHOST"], "port": int(os.environ["PGPORT"]),
        "user": os.environ["PGUSER"], "password": os.environ["PGPASSWORD"],
        "dbname": os.environ["PGDATABASE"],
    }


@pytest.fixture()
def fast_pulse(monkeypatch):
    import services.cancel_check as cc

    monkeypatch.setattr(cc, "BLOCKING_STEP_PULSE_SECONDS", 0.2)


def test_population_walk_names_a_source_that_does_not_answer(fast_pulse):
    from services.cancel_check import ProgressMeter, set_progress_meter
    from src.transfer.source_peek import _first_row_waits, source_wait_message

    stamps: list[dict] = []

    class _Store:
        def get_job(self, job_id):
            return {"status": "running"}

        def update_job_fields(self, job_id, fields, **_kw):
            stamps.append(dict(fields))
            return True

        def update_job_status(self, *a, **k):
            return True

    def _slow_rows():
        time.sleep(0.9)
        yield {"id": 1}
        yield {"id": 2}

    msg = source_wait_message("postgresql", {"host": "db.internal", "port": 5432, "database": "sales"}, "Validating mapping and schema")
    # UX-34: users read (and paste) the event log: it names the engine and
    # database, never the host and port.
    assert "PostgreSQL · sales" in msg and _WAIT_WORDS in msg
    assert "db.internal" not in msg and "5432" not in msg
    set_progress_meter(ProgressMeter(_Store(), "job-d326"))
    try:
        assert list(_first_row_waits(_slow_rows(), msg)) == [{"id": 1}, {"id": 2}]
    finally:
        set_progress_meter(None)
    assert any(_WAIT_WORDS in str(s.get("message") or "") for s in stamps), stamps
    assert all(s.get("heartbeat_at") for s in stamps)


def test_peek_read_blocked_on_the_source_is_visible_on_the_job(tmp_path: Path, fast_pulse, monkeypatch):
    pg = _pg_params()
    if pg is None:
        pytest.skip("no local PostgreSQL configured (PG* env)")
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        admin = psycopg2.connect(connect_timeout=3, **pg)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local PostgreSQL unavailable: {exc}")
    admin.autocommit = True
    table = f"d326_wait_{uuid.uuid4().hex[:8]}"
    cur = admin.cursor()
    cur.execute(f"CREATE TABLE {table} (id int primary key, v text)")
    cur.execute(f"INSERT INTO {table} SELECT g, 'v' || g FROM generate_series(1, 5) g")

    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Fake(_FakeMongo):
        def __init__(self):
            super().__init__()
            self.messages: list[str] = []

        def update_job_fields(self, job_id, fields, **_kw):
            if fields.get("message"):
                self.messages.append(str(fields["message"]))
            job = self.get_job(job_id) or {}
            job.update(fields)
            return True

        def update_job_status(self, job_id, status, **kw):
            if kw.get("message"):
                self.messages.append(str(kw["message"]))
            return super().update_job_status(job_id, status, **kw)

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    import services.sync_cursor as sc

    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

    dst = tmp_path / "dst.sqlite"
    sqlite3.connect(str(dst)).close()
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host=pg["host"], port=pg["port"],
            database=pg["dbname"], username=pg["user"], password=pg["password"],
            table=table, schema="public",
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table=table),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "d326" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})

    locker = psycopg2.connect(**pg)
    lcur = locker.cursor()
    lcur.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    out: dict = {}
    worker = threading.Thread(
        target=lambda: out.setdefault(
            "result", engine_mod.UniversalTransferEngine().execute_tracked(request, job_id)
        ),
        daemon=True,
    )
    try:
        worker.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not any(_WAIT_WORDS in m for m in fake.messages):
            time.sleep(0.2)
        waiting = [m for m in fake.messages if _WAIT_WORDS in m]
    finally:
        locker.rollback()
        locker.close()
        worker.join(60)
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        admin.close()
    assert waiting, fake.messages
    assert "postgresql" in waiting[0].lower() and "Analyzing source table" in waiting[0]
    assert out["result"].success, out["result"].error
