"""D407: a retry from zero after a cancelled large upsert reads at normal speed.

QA (PG 40k rows -> new SQL Server, upsert): the job was cancelled at 5% and
retry_job (a new job with the same request) sat at 'Reading from the source
... waiting for it to answer' with 0 rows for about ten minutes while no
source session was reading, then progressed. Suspected cause: a stale session
of the cancelled attempt reused, or still holding the destination.

SQL Server is not available locally. This runs the same shape through the
engine on PostgreSQL -> a new MySQL table: cancel the upsert at about 5%,
then immediately run the retry as a fresh job with the same request. The
cancelled attempt must leave no destination transaction behind, the retry's
first rows must commit within seconds, and the retry must land all 40,000
rows with sum(qty) = 800,020,000.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest


def test_retry_after_cancelled_upsert_starts_at_once_and_lands_every_row(monkeypatch):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not configured")
    import psycopg2
    import pymysql
    from psycopg2.extras import execute_values

    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 1024)

    p, c = pg_creds(), mysql_creds()
    tok = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d407s_{tok}", f"d407d_{tok}"
    rows = 40_000
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
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, name VARCHAR(40), qty BIGINT)")
        execute_values(
            cur, f"INSERT INTO {src_t} VALUES %s",  # nosec B608 - test table
            [(i, f"n{i}", i) for i in range(1, rows + 1)], page_size=10000,
        )

    def dest(sql: str):
        try:
            with my.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()
        except pymysql.err.ProgrammingError:  # table not created yet
            return None

    def count() -> int:
        row = dest(f"SELECT COUNT(*) FROM `{dst_t}`")  # nosec B608 - test table
        return int(row[0]) if row else 0

    def ep(fmt: str, creds: dict, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database", format=fmt, host=str(creds["host"]), port=int(creds["port"]),
            database=str(creds["database"]), username=str(creds["username"]),
            password=str(creds["password"]), table=table,
        )

    def request() -> TransferRequest:
        return TransferRequest(
            source=ep("postgresql", p, src_t),
            destination=ep("mysql", c, dst_t),
            sync_mode="upsert",
            validation_mode="balanced",
            source_filter={"column": "id", "op": "gt", "value": 0},
            skip_preflight=True,
        )

    def start(job_id: str) -> threading.Thread:
        store.update_job_status(job_id, "pending", transfer_request={})
        t = threading.Thread(
            target=lambda: UniversalTransferEngine().execute_tracked(request(), job_id)
        )
        t.start()
        return t

    first = uuid.uuid4().hex[:24]
    retry = uuid.uuid4().hex[:24]
    worker = retry_worker = None
    try:
        worker = start(first)
        deadline = time.time() + 120
        while time.time() < deadline and worker.is_alive() and count() < 2000:
            time.sleep(0.02)
        assert worker.is_alive(), store.get_job(first)
        store.request_job_cancel(first)
        store.update_job_status(first, "cancelled", operator_command=True)
        worker.join(60)
        assert not worker.is_alive(), "the cancelled attempt is still running"
        # The cancelled attempt left no open destination transaction: a
        # locking read of every row gets its locks within 2 s.
        with my.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 2")
            cur.execute("SET SESSION lock_wait_timeout = 2")
            cur.execute("START TRANSACTION")
            cur.execute(f"SELECT COUNT(*) FROM `{dst_t}` FOR UPDATE")  # nosec B608
            cur.fetchone()
            cur.execute("ROLLBACK")
        before = count()

        t0 = time.monotonic()
        retry_worker = start(retry)
        first_rows_at = None
        while retry_worker.is_alive() and time.monotonic() - t0 < 300:
            if first_rows_at is None and int(
                (store.get_job(retry) or {}).get("records_processed") or 0
            ) > 0:
                first_rows_at = time.monotonic() - t0
            time.sleep(0.05)
        retry_worker.join(10)
        assert not retry_worker.is_alive(), "the retry did not finish"
        job = store.get_job(retry) or {}
        assert str(job.get("status")) == "completed", (job.get("status"), job.get("message"))
        assert first_rows_at is not None and first_rows_at < 30, first_rows_at
        assert count() == rows, (before, count())
        total = dest(f"SELECT SUM(qty) FROM `{dst_t}`")  # nosec B608 - test table
        assert int(total[0]) == 800_020_000, total
    finally:
        for t in (worker, retry_worker):
            if t is not None:
                t.join(60)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst_t}`")
        my.close()
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        pg.close()
