"""D168 — a cancelled job stops writing, including from the chunk-writer pool.

QA: a 60,000-row MySQL -> SQL Server job showed ``cancelled`` while the
destination kept growing (3,072 -> 6,144 -> 12,288 ...). Two gaps:

* the per-job cancel probe was a thread-local, so the pool threads that run the
  chunk writers never saw it — a writer past its first chunk could not be
  stopped (the probe is now a ContextVar the dispatcher's copied context carries);
* the generic SQL writer (SQL Server, Oracle, ...) had no checkpoint between its
  sub-chunks, and swallowed a cancel into a failed ``WriteResult``.

Live MySQL -> SQL Server leg skips when either service is not configured.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.cancel_check import cancel_checkpoint, set_cancel_probe  # noqa: E402
from services.error_handling import TransferCancelled  # noqa: E402
from services.parallel_chunks import ChunkDispatcher  # noqa: E402


def test_cancel_probe_reaches_chunk_writer_pool_threads():
    def probe(_rows):
        raise TransferCancelled("Transfer cancelled by user")

    seen: list[str] = []

    def write(idx, _item):
        try:
            cancel_checkpoint(idx)
        except TransferCancelled:
            seen.append("cancelled")
            raise
        seen.append("wrote")
        return idx

    set_cancel_probe(probe)
    try:
        with ChunkDispatcher(max_workers=2) as dispatcher:
            dispatcher.submit(1, object(), write)
            with pytest.raises(TransferCancelled):
                list(dispatcher.results())
    finally:
        set_cancel_probe(None)
    assert seen == ["cancelled"]


def test_probe_is_isolated_between_concurrent_jobs():
    out: dict[str, bool] = {}

    def job(name: str, cancelled: bool):
        def probe(_rows):
            if cancelled:
                raise TransferCancelled(name)

        set_cancel_probe(probe)
        time.sleep(0.05)
        try:
            cancel_checkpoint()
            out[name] = False
        except TransferCancelled:
            out[name] = True
        finally:
            set_cancel_probe(None)

    threads = [
        threading.Thread(target=job, args=("a", True)),
        threading.Thread(target=job, args=("b", False)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert out == {"a": True, "b": False}


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


def test_cancelled_mysql_to_sqlserver_job_stops_growing(monkeypatch):
    from tests.helpers.live_env import mysql_creds, mysql_up

    ms = _mssql_cfg()
    if ms is None or not mysql_up():
        pytest.skip("live MySQL + SQL Server not configured")
    import pymysql
    import sqlalchemy as sa

    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from connectors.generic_sql import _engine
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    # Small pages: the cancel must land between committed batches.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 1024)

    c = mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d168s_{suffix}", f"d168d_{suffix}"
    my = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    with my.cursor() as cur:
        cur.execute(f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
        cur.executemany(
            f"INSERT INTO `{src_t}` VALUES (%s, %s, %s)",
            [(i, f"a{i}", i % 97) for i in range(1, 40001)],
        )
    sql = _engine({"type": "sqlserver", "schema": "dbo", "ssl": False,
                   "trust_server_certificate": True, **ms})

    def dest_count() -> int | None:
        try:
            with sql.connect() as conn:
                return conn.execute(sa.text(f"SELECT COUNT(*) FROM dbo.[{dst_t}]")).scalar()
        except Exception:  # noqa: BLE001 - table not created yet / already dropped
            return None

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), username=str(c["username"]),
            password=str(c["password"]), table=src_t,
        ),
        destination=EndpointConfig(
            kind="database", format="sqlserver", table=dst_t, schema="dbo",
            extra={"trust_server_certificate": True}, **ms,
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        # A pass-through filter keeps the job on the row/stream writer.
        source_filter={"column": "id", "op": "gt", "value": 0},
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
        while time.time() < deadline and not worker.join(0.05) and worker.is_alive():
            if (dest_count() or 0) >= 2000:
                break
        assert (dest_count() or 0) >= 2000, "the job never started writing"
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        worker.join(60)
        assert not worker.is_alive(), "the cancelled job is still running"
        settled = dest_count()
        time.sleep(2)
        assert dest_count() == settled, "the destination kept growing after the cancel"
        assert settled is not None and settled < 40000
        assert "cancel" in str(result["r"].error).lower()
    finally:
        worker.join(30)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_t}`")
        my.close()
        with sql.begin() as conn:
            conn.execute(sa.text(f"IF OBJECT_ID('dbo.{dst_t}') IS NOT NULL DROP TABLE dbo.[{dst_t}]"))
