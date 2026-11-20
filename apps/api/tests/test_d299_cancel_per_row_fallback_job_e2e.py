"""D299 at job level: a cancel stops the per-row fallback and the job reports the destination.

QA (PG q23_big -> new SQL Server, incremental_upsert): a NOCHECK CHECK
constraint made every batch fail, so the writer fell back to one statement +
COMMIT per row. After cancel_job the destination kept growing for minutes and
the job's records_processed stayed below the destination count.

``test_d299_cancel_stops_per_row_fallback`` drives the writers directly. This
test drives the whole engine (``execute_tracked``) on local PostgreSQL ->
MySQL: a CHECK constraint poisons one row in every chunk so every chunk falls
back to per-row commits, each per-row commit is slowed to imitate a remote
destination, and the operator cancels mid-fallback. Acceptance:

* AC-1: from the moment the job is cancelled, the destination grows by at most
  the one in-flight row, and stays flat after the worker returns;
* AC-2: records_processed and cancel_outcome.rows_committed equal the real
  destination count.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest


@pytest.mark.parametrize("workers", [1, 4])
def test_cancel_during_per_row_fallback_stops_within_one_row(monkeypatch, workers):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not configured")
    import psycopg2
    import pymysql
    from psycopg2.extras import execute_values

    import services.cancel_check as cancel_mod
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    # Each pooled chunk writer may finish the one row it is committing.
    monkeypatch.setenv("DATAFLOW_PARALLEL_WORKERS", str(workers))
    monkeypatch.delenv("DATAWRAP_PARALLEL_WORKERS", raising=False)
    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 500)

    # A slow remote destination: every per-row poll (one per committed row)
    # costs a few milliseconds, so the fallback is still running when the
    # operator cancels.
    real_checkpoint = cancel_mod.cancel_checkpoint

    def slow_checkpoint(rows_written=None):
        time.sleep(0.004)
        real_checkpoint(rows_written)

    monkeypatch.setattr(cancel_mod, "cancel_checkpoint", slow_checkpoint)

    p, c = pg_creds(), mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d299s_{suffix}", f"d299d_{suffix}"
    rows = 6000
    pg = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    pg.autocommit = True
    my = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    with pg.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, name VARCHAR(40), n INT)")
        execute_values(
            cur, f"INSERT INTO {src_t} VALUES %s",  # nosec B608 - test table
            [(i, f"n{i}", i % 97) for i in range(1, rows + 1)], page_size=5000,
        )
    with my.cursor() as cur:
        # Existing keyed destination; id % 400 = 0 is refused, so every
        # 500-row chunk fails as a batch and falls back to per-row commits.
        cur.execute(
            f"CREATE TABLE `{dst_t}` (id INT PRIMARY KEY, name VARCHAR(40), n INT, "
            f"CONSTRAINT ck_{dst_t} CHECK (id % 400 <> 0))"
        )

    def dest_count() -> int:
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst_t}`")  # nosec B608 - test table
            return int(cur.fetchone()[0])

    def ep(fmt: str, creds: dict, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database", format=fmt, host=str(creds["host"]), port=int(creds["port"]),
            database=str(creds["database"]), username=str(creds["username"]),
            password=str(creds["password"]), table=table,
        )

    request = TransferRequest(
        source=ep("postgresql", p, src_t),
        destination=ep("mysql", c, dst_t),
        sync_mode="upsert",
        validation_mode="balanced",
        source_filter={"column": "id", "op": "gt", "value": 0},
        skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result: dict = {}
    worker = threading.Thread(
        target=lambda: result.setdefault(
            "r", UniversalTransferEngine().execute_tracked(request, job_id)
        )
    )
    try:
        worker.start()
        deadline = time.time() + 120
        while time.time() < deadline and worker.is_alive() and dest_count() < 1200:
            time.sleep(0.01)
        assert worker.is_alive(), (result, store.get_job(job_id))
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        at_cancel = dest_count()
        worker.join(60)
        assert not worker.is_alive(), "the cancelled job is still writing"
        stopped = dest_count()
        time.sleep(1.0)
        settled = dest_count()
        # AC-1: only the row each writer has in flight may finish; nothing after.
        assert stopped - at_cancel <= workers, (at_cancel, stopped)
        assert settled == stopped, (stopped, settled)
        assert 0 < settled < rows, settled
        job = store.get_job(job_id) or {}
        outcome = job.get("cancel_outcome") or {}
        # AC-2: the job reports the rows the destination really holds.
        assert outcome.get("rows_committed") == settled, (outcome, settled, job.get("message"))
        assert int(job.get("records_processed") or 0) == settled, (
            job.get("records_processed"), settled,
        )
        assert str(job.get("status")) == "cancelled", job.get("status")
    finally:
        worker.join(30)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst_t}`")
        my.close()
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        pg.close()


@pytest.mark.parametrize("workers", [1, 4])
def test_cancel_during_postgresql_salvage_discards_the_chunk_and_counts_exactly(monkeypatch, workers):
    """The same job into a PostgreSQL destination, where the fallback is the
    single-transaction salvage (SAVEPOINT per row, one COMMIT per chunk).

    A cancel during that salvage must discard the open chunk inside the
    salvage itself (connectors/salvage_cancel.py), so: the cancel is observed
    by the salvage guard, the destination stops growing once the worker
    returns, and the job's counts equal the rows the destination holds,
    without the rows salvaged into the discarded chunk.
    """
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    import psycopg2
    from psycopg2.extras import execute_values

    import connectors.postgresql_writer as pw
    import services.cancel_check as cancel_mod
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from connectors.salvage_cancel import SalvageCancelGuard
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_PARALLEL_WORKERS", str(workers))
    monkeypatch.delenv("DATAWRAP_PARALLEL_WORKERS", raising=False)
    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 500)

    # A slow remote destination: each salvaged row costs a few milliseconds,
    # and the guard polls before every row, so a cancel lands mid-salvage.
    real_poll = cancel_mod.poll_cancel

    def slow_poll(rows_written=None):
        time.sleep(0.004)
        real_poll(rows_written)

    monkeypatch.setattr(cancel_mod, "poll_cancel", slow_poll)
    discarded: list[int] = []
    polls = [0]

    class _Guard(SalvageCancelGuard):
        def __init__(self, conn, cur, **_kw):
            super().__init__(conn, cur, interval=0.0)

        def check(self, rows_committed, *, held_overwrite):
            polls[0] += 1
            try:
                super().check(rows_committed, held_overwrite=held_overwrite)
            except TransferCancelled:
                discarded.append(rows_committed)
                raise

    monkeypatch.setattr(pw, "SalvageCancelGuard", _Guard)

    p = pg_creds()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d299ps_{suffix}", f"d299pd_{suffix}"
    rows = 6000
    pg = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    pg.autocommit = True
    with pg.cursor() as cur:
        # A chunk left open by the writer must fail this test, not hang it.
        cur.execute("SET lock_timeout = '10s'")
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, name VARCHAR(40), n INT)")
        execute_values(
            cur, f"INSERT INTO {src_t} VALUES %s",  # nosec B608 - test table
            [(i, f"n{i}", i % 97) for i in range(1, rows + 1)], page_size=5000,
        )
        # id % 400 = 0 is refused: every 500-row chunk fails as a batch and is salvaged.
        cur.execute(
            f"CREATE TABLE {dst_t} (id INT PRIMARY KEY, name VARCHAR(40), n INT, "
            f"CONSTRAINT ck_{dst_t} CHECK (id % 400 <> 0))"
        )

    def dest_count() -> int:
        with pg.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {dst_t}")  # nosec B608 - test table
            return int(cur.fetchone()[0])

    def ep(table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database", format="postgresql", host=str(p["host"]), port=int(p["port"]),
            database=str(p["database"]), username=str(p["username"]),
            password=str(p["password"]), table=table,
        )

    request = TransferRequest(
        source=ep(src_t),
        destination=ep(dst_t),
        sync_mode="upsert",
        validation_mode="balanced",
        source_filter={"column": "id", "op": "gt", "value": 0},
        skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result: dict = {}
    worker = threading.Thread(
        target=lambda: result.setdefault(
            "r", UniversalTransferEngine().execute_tracked(request, job_id)
        )
    )
    try:
        worker.start()
        deadline = time.time() + 120
        while time.time() < deadline and worker.is_alive() and dest_count() < 900:
            time.sleep(0.01)
        # Cancel in the middle of a salvage: some of its rows already sit in
        # the open chunk under released savepoints.
        mid = polls[0] + 60
        while time.time() < deadline and worker.is_alive() and polls[0] < mid:
            time.sleep(0.005)
        assert worker.is_alive(), (result, store.get_job(job_id))
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        worker.join(60)
        assert not worker.is_alive(), "the cancelled job is still writing"
        stopped = dest_count()
        time.sleep(1.0)
        settled = dest_count()
        # The cancel was seen inside the salvage, which discarded its chunk.
        assert discarded, "the salvage never observed the cancel"
        assert settled == stopped, (stopped, settled)
        assert 0 < settled < rows - rows // 400, settled
        job = store.get_job(job_id) or {}
        outcome = job.get("cancel_outcome") or {}
        assert outcome.get("rows_committed") == settled, (outcome, settled, job.get("message"))
        assert int(job.get("records_processed") or 0) == settled, (
            job.get("records_processed"), settled,
        )
        assert str(job.get("status")) == "cancelled", job.get("status")
    finally:
        worker.join(30)
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {dst_t}")
            cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        pg.close()
