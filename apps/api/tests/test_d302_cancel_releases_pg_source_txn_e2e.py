"""D302: a cancel ends the source PostgreSQL read transaction within 5 seconds.

QA (PG 150k rows -> MariaDB, overwrite): after a cancel at 7k-20k rows the
source session sat ``idle in transaction`` for 12+ minutes (last query
``CLOSE "_df_scan_..."``), holding AccessShareLock on the table and its
primary key and pinning backend_xmin; a DROP TABLE on the source waited.

End to end through ``UniversalTransferEngine().execute_tracked`` on local
PostgreSQL -> MySQL: cancel mid-load, then require that within 5 s no backend
other than the test's own holds a lock on the source table and no backend
is idle in transaction with a pinned xmin on it. Both the row/stream path
(a pass-through filter) and the default route are covered, and the same is
required after a run that fails and one that succeeds (edge case "same on
failure and success").
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest


def _source_lock_holders(pg, table: str) -> list[tuple]:
    with pg.cursor() as cur:
        cur.execute(
            "SELECT a.pid, a.state, a.query FROM pg_locks l "
            "JOIN pg_class c ON c.oid = l.relation "
            "JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE c.relname IN (%s, %s) AND l.pid <> pg_backend_pid()",
            (table, f"{table}_pkey"),
        )
        return list(cur.fetchall())


def _wait_released(pg, table: str, budget: float) -> list[tuple]:
    deadline = time.monotonic() + budget
    holders = _source_lock_holders(pg, table)
    while holders and time.monotonic() < deadline:
        time.sleep(0.1)
        holders = _source_lock_holders(pg, table)
    return holders


def _setup(rows: int):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not configured")
    import psycopg2
    import pymysql
    from psycopg2.extras import execute_values

    p, c = pg_creds(), mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d302s_{suffix}", f"d302d_{suffix}"
    pg = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
        execute_values(
            cur, f"INSERT INTO {src_t} VALUES %s",  # nosec B608 - test table
            [(i, f"a{i}", i % 97) for i in range(1, rows + 1)], page_size=10000,
        )
    my = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    return p, c, pg, my, src_t, dst_t


def _endpoint(fmt: str, creds: dict, table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format=fmt, host=str(creds["host"]), port=int(creds["port"]),
        database=str(creds["database"]), username=str(creds["username"]),
        password=str(creds["password"]), table=table,
    )


def _dest_count(my, table: str) -> int:
    try:
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608 - test table
            return int(cur.fetchone()[0])
    except Exception:  # noqa: BLE001 - not created yet
        return 0


def _teardown(pg, my, src_t: str, dst_t: str) -> None:
    with my.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{dst_t}`")
    my.close()
    with pg.cursor() as cur:
        cur.execute("SET lock_timeout = '10s'")
        cur.execute(f"DROP TABLE IF EXISTS {src_t}")
    pg.close()


def _engine(monkeypatch):
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from services.mongodb_service import MemoryMongoDBService

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 1024)
    return store


@pytest.mark.parametrize("row_path", [True, False])
def test_cancel_ends_the_source_read_transaction_within_5s(monkeypatch, row_path):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    store = _engine(monkeypatch)
    rows = 150_000
    p, c, pg, my, src_t, dst_t = _setup(rows)
    request = TransferRequest(
        source=_endpoint("postgresql", p, src_t),
        destination=_endpoint("mysql", c, dst_t),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        source_filter={"column": "id", "op": "gt", "value": 0} if row_path else None,
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
        deadline = time.time() + 180
        while time.time() < deadline and worker.is_alive() and _dest_count(my, dst_t) < 5000:
            time.sleep(0.02)
        if not worker.is_alive():
            pytest.fail(f"the load finished before it could be cancelled: {result}")
        # The read is live: the source table is held by the job's session.
        assert _source_lock_holders(pg, src_t), "no source session is reading"
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        holders = _wait_released(pg, src_t, 5.0)
        assert not holders, holders
        worker.join(120)
        assert not worker.is_alive(), "the cancelled job is still running"
        assert not _source_lock_holders(pg, src_t)
    finally:
        worker.join(30)
        _teardown(pg, my, src_t, dst_t)


@pytest.mark.parametrize("outcome", ["success", "failure"])
def test_a_finished_or_failed_run_leaves_no_source_transaction(monkeypatch, outcome):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    store = _engine(monkeypatch)
    p, c, pg, my, src_t, dst_t = _setup(3000)
    if outcome == "failure":
        # A destination that refuses every row: the run fails after reading.
        with my.cursor() as cur:
            cur.execute(
                f"CREATE TABLE `{dst_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT, "
                f"CONSTRAINT ck_{dst_t} CHECK (id < 0))"
            )
    request = TransferRequest(
        source=_endpoint("postgresql", p, src_t),
        destination=_endpoint("mysql", c, dst_t),
        sync_mode="full_refresh_overwrite" if outcome == "success" else "append",
        validation_mode="strict" if outcome == "failure" else "balanced",
        source_filter={"column": "id", "op": "gt", "value": 0},
        skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    try:
        UniversalTransferEngine().execute_tracked(request, job_id)
        job = store.get_job(job_id) or {}
        assert (str(job.get("status")) == "completed") == (outcome == "success"), job.get("status")
        holders = _wait_released(pg, src_t, 5.0)
        assert not holders, holders
    finally:
        _teardown(pg, my, src_t, dst_t)


@pytest.mark.parametrize("workers", [1, 4])
def test_cancel_releases_the_source_while_a_writer_is_stuck(monkeypatch, workers):
    """The writer is blocked on the destination (another session holds the
    table) when the operator cancels. The job cannot finish until that write
    returns, but the source read transaction must not wait for it: the
    customer's table stays locked and its xmin pinned for as long as the
    destination stalls (QA saw 12+ minutes)."""
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    monkeypatch.setenv("DATAFLOW_PARALLEL_WORKERS", str(workers))
    monkeypatch.delenv("DATAWRAP_PARALLEL_WORKERS", raising=False)
    store = _engine(monkeypatch)
    p, c, pg, my, src_t, dst_t = _setup(150_000)
    blocker = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=False,
    )
    request = TransferRequest(
        source=_endpoint("postgresql", p, src_t),
        destination=_endpoint("mysql", c, dst_t),
        sync_mode="full_refresh_overwrite",
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
    locked = False
    try:
        worker.start()
        deadline = time.time() + 180
        while time.time() < deadline and worker.is_alive() and _dest_count(my, dst_t) < 3000:
            time.sleep(0.02)
        if not worker.is_alive():
            pytest.fail(f"the load finished before it could be cancelled: {result}")
        # Stall every further destination write behind a row lock.
        with blocker.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst_t}` FOR UPDATE")  # nosec B608
        locked = True
        time.sleep(1.0)
        assert _source_lock_holders(pg, src_t), "no source session is reading"
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        holders = _wait_released(pg, src_t, 5.0)
        assert worker.is_alive(), "the writer was expected to still be stalled"
        assert not holders, holders
        # Once the destination answers, the run ends as cancelled, not failed
        # on the closed source session.
        blocker.rollback()
        locked = False
        worker.join(120)
        assert not worker.is_alive(), "the cancelled job is still running"
        job = store.get_job(job_id) or {}
        assert str(job.get("status")) == "cancelled", (job.get("status"), job.get("message"))
    finally:
        if locked:
            blocker.rollback()
        blocker.close()
        worker.join(120)
        _teardown(pg, my, src_t, dst_t)
