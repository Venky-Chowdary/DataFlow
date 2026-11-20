"""D360: a query-mode transfer finalizes with no destination session left in a transaction.

QA (SQL Server query mode -> PostgreSQL, 25 rows): the job wrote 25/25 rows
then sat 10+ minutes at 'Writing batch 1/1' (98%) while the destination kept
two sessions idle in transaction; a repeat completed. Intermittent under
connector-pool pressure (D281).

SQL Server is not available locally. This regression test runs the same
shape on local engines: a MySQL source in query mode (``source_query``) into
a new PostgreSQL table, with the finalize step (reconciliation) delayed by
six seconds to stand in for a slow finalize. While the run waits in finalize
no destination session may sit idle in transaction (its writes are committed
and its sessions closed before verification starts), and the job completes
with every row once the step returns.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest


def test_delayed_finalize_holds_no_destination_transaction(monkeypatch):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not configured")
    import psycopg2
    import pymysql

    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    in_finalize = threading.Event()
    real_reconcile = engine_mod.run_reconciliation

    def slow_reconcile(*args, **kwargs):
        in_finalize.set()
        time.sleep(6.0)
        return real_reconcile(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", slow_reconcile)

    p, c = pg_creds(), mysql_creds()
    tok = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d360s_{tok}", f"d360d_{tok}"
    pg = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    pg.autocommit = True
    my = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    with my.cursor() as cur:
        cur.execute(f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, name VARCHAR(40), qty INT)")
        cur.executemany(
            f"INSERT INTO `{src_t}` VALUES (%s, %s, %s)",  # nosec B608 - test table
            [(i, f"n{i}", i * 10) for i in range(1, 26)],
        )
    with pg.cursor() as cur:
        cur.execute("SELECT now()")
        started = cur.fetchone()[0]

    def idle_in_txn() -> list[tuple]:
        # Only PostgreSQL sessions opened by this run: the source is MySQL, so
        # every one of them is a destination session.
        with pg.cursor() as cur:
            cur.execute(
                "SELECT pid, left(query, 100) FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid() AND backend_start > %s "
                "AND state LIKE 'idle in transaction%%' "
                "AND now() - state_change > interval '1 second'",
                (started,),
            )
            return list(cur.fetchall())

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), username=str(c["username"]),
            password=str(c["password"]),
            extra={
                "source_read_mode": "query",
                "source_query": f"SELECT id, name, qty FROM `{src_t}` WHERE qty > 0",
            },
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host=str(p["host"]), port=int(p["port"]),
            database=str(p["database"]), username=str(p["username"]),
            password=str(p["password"]), table=dst_t,
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    worker = threading.Thread(
        target=lambda: UniversalTransferEngine().execute_tracked(request, job_id)
    )
    try:
        worker.start()
        assert in_finalize.wait(120), "the run never reached finalize"
        seen: list[tuple] = []
        while worker.is_alive() and in_finalize.is_set() and not seen:
            seen = idle_in_txn()
            time.sleep(0.2)
            if not worker.is_alive():
                break
        assert seen == [], seen
        worker.join(120)
        assert not worker.is_alive(), "the run did not finish after finalize returned"
        job = store.get_job(job_id) or {}
        assert str(job.get("status")) == "completed", (job.get("status"), job.get("message"))
        assert int(job.get("records_processed") or 0) == 25
        assert idle_in_txn() == []
        with pg.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {dst_t}")  # nosec B608 - test table
            assert cur.fetchone()[0] == 25
    finally:
        worker.join(30)
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {dst_t}")
        pg.close()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_t}`")
        my.close()
