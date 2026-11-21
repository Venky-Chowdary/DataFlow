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

The cancel test reads through a paced view (:func:`_paced_source`), so the
source read is still running when the cancel lands and for well over the 5 s
budget after it: a release inside the budget can only come from the cancel.
It used to race the destination row count instead, which the default route
cannot support: the PG->MySQL COPY lands its rows with one LOAD DATA
statement, so none are visible until the whole table has been read and
committed, and the run ends its source transaction a few milliseconds
later. Whether the test still saw the source held was a coin toss, and when
it did, the cancel arrived after the read had finished.
"""

from __future__ import annotations

import contextvars
import threading
import time
import uuid

import pytest

from services.error_handling import TransferCancelled


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


def _copy_rows_sent(pg, table: str) -> int:
    """Rows a COPY that holds ``table`` has sent so far (pg_stat_progress_copy)."""
    with pg.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(p.tuples_processed), 0) FROM pg_stat_progress_copy p "
            "WHERE p.pid IN (SELECT l.pid FROM pg_locks l "
            "JOIN pg_class c ON c.oid = l.relation WHERE c.relname = %s)",
            (table,),
        )
        return int(cur.fetchone()[0])


#: The paced view sleeps this long once per this many rows it emits.
_PACE_EVERY_ROWS = 1000
_PACE_SECONDS = 0.1


def _paced_source(pg, table: str) -> str:
    """A view over ``table`` that emits its rows at a fixed, slow pace.

    ``_PACE_SECONDS`` per ``_PACE_EVERY_ROWS`` rows: 150k rows take ~15 s to
    read on any machine, so the read is deterministically still running when
    the test cancels it after 5k rows. The pacing function is declared STABLE
    so the planner drops it from ``COUNT(*)``/key probes that do not read the
    column; only reads that emit the rows (COPY, the cursor) are paced.
    """
    view, pace = f"{table}_v", f"{table}_pace"
    with pg.cursor() as cur:
        cur.execute(
            f"CREATE FUNCTION {pace}(i int, v varchar) RETURNS varchar "
            "LANGUAGE plpgsql STABLE AS $$ BEGIN "
            f"IF i % {_PACE_EVERY_ROWS} = 0 THEN PERFORM pg_sleep({_PACE_SECONDS}); END IF; "
            "RETURN v; END $$"
        )
        cur.execute(
            f"CREATE VIEW {view} AS "  # nosec B608 - test view
            f"SELECT id, {pace}(id, a)::varchar(40) AS a, n FROM {table}"
        )
    return view


def _assert_engine_cancelled(store, job_id: str, result: dict, my, dst_t: str) -> int:
    """The engine itself ended the run as cancelled, and its ledger is exact.

    The test writes ``status="cancelled"`` the way the API does, so the status
    alone proves nothing. These fields are written only by the engine's
    cancel path: the run's own result, and the cancel outcome counted on the
    destination. Returns the rows that landed.
    """
    r = result.get("r")
    assert r is not None and r.success is False, r
    raw = str((getattr(r, "error_details", None) or {}).get("raw") or "")
    assert raw.startswith("Transfer cancelled by user"), raw
    job = store.get_job(job_id) or {}
    assert job.get("phase") == "cancelled", (job.get("phase"), job.get("message"))
    outcome = job.get("cancel_outcome") or {}
    assert outcome, job.get("message")
    landed = _dest_count(my, dst_t)
    assert outcome.get("rows_committed") == landed, (outcome, landed)
    return landed


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
        cur.execute(f"DROP VIEW IF EXISTS {src_t}_v")
        cur.execute(f"DROP FUNCTION IF EXISTS {src_t}_pace(int, varchar)")
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
    view = _paced_source(pg, src_t)
    request = TransferRequest(
        source=_endpoint("postgresql", p, view),
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

    def rows_read() -> int:
        if row_path:
            # The row path commits batch by batch: the destination grows.
            return _dest_count(my, dst_t)
        # The default route's COPY lands every row with one LOAD DATA, so the
        # destination shows nothing until the read is over: count what the
        # source has sent instead (and keep MySQL out of the LOAD's way).
        return _copy_rows_sent(pg, src_t)

    try:
        worker.start()
        deadline = time.time() + 180
        while time.time() < deadline and worker.is_alive() and rows_read() < 5000:
            time.sleep(0.02)
        if not worker.is_alive():
            pytest.fail(f"the load finished before it could be cancelled: {result}")
        # The read is live, and the pace keeps it running for ~14 s more:
        # the source table is held by the job's session.
        assert _source_lock_holders(pg, src_t), "no source session is reading"
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        holders = _wait_released(pg, src_t, 5.0)
        assert not holders, holders
        worker.join(120)
        assert not worker.is_alive(), "the cancelled job is still running"
        assert not _source_lock_holders(pg, src_t)
        landed = _assert_engine_cancelled(store, job_id, result, my, dst_t)
        assert landed < rows, "the read finished before the cancel"
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
        _assert_engine_cancelled(store, job_id, result, my, dst_t)
    finally:
        if locked:
            blocker.rollback()
        blocker.close()
        worker.join(120)
        _teardown(pg, my, src_t, dst_t)


def _load_data_waiting(my, table: str) -> int:
    """LOAD DATA statements into ``table`` the server is running (processlist only)."""
    with my.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.processlist "
            "WHERE info LIKE %s AND id <> CONNECTION_ID()",
            (f"LOAD DATA LOCAL INFILE % INTO TABLE `{table}`%",),
        )
        return int(cur.fetchone()[0])


@pytest.mark.parametrize("shards", [1, 4], ids=["one-copy", "ctid-shards"])
def test_cancel_releases_the_source_while_a_copy_load_is_stuck(monkeypatch, shards):
    """The default route (PG->MySQL COPY into LOAD DATA) with its LOAD stalled
    behind a destination lock. The FIFO producer is then blocked on the pipe
    and polls nothing, so the snapshot session sat idle in transaction holding
    the customer's table until the destination answered (70+ s here), and the
    load was then committed although the cancel had landed long before. The
    source must be released within 5 s, and the run must end cancelled with
    nothing committed. ``ctid-shards`` analyzes the source first so the copy
    splits into per-shard sessions that share the exported snapshot."""
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", str(shards))
    monkeypatch.delenv("DATAWRAP_PG_MYSQL_COPY_WORKERS", raising=False)
    store = _engine(monkeypatch)
    p, c, pg, my, src_t, dst_t = _setup(150_000)
    if shards > 1:
        with pg.cursor() as cur:
            cur.execute(f"ANALYZE {src_t}")  # relpages > 0: ctid page ranges
    # An existing empty destination: the default route appends into it with
    # LOAD DATA, and a gap lock taken before the run stalls every insert.
    with my.cursor() as cur:
        cur.execute(f"CREATE TABLE `{dst_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
    blocker = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=False,
    )
    with blocker.cursor() as cur:
        cur.execute(f"SELECT * FROM `{dst_t}` FOR UPDATE")  # nosec B608
    locked = True
    request = TransferRequest(
        source=_endpoint("postgresql", p, src_t),
        destination=_endpoint("mysql", c, dst_t),
        sync_mode="append",
        validation_mode="balanced",
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
        while time.time() < deadline and worker.is_alive() and _load_data_waiting(my, dst_t) < 1:
            time.sleep(0.05)
        if not worker.is_alive():
            pytest.fail(f"the load finished before it could be cancelled: {result}")
        assert _load_data_waiting(my, dst_t) >= 1, "the COPY route never started its LOAD"
        time.sleep(1.0)
        assert _source_lock_holders(pg, src_t), "no source session is reading"
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        holders = _wait_released(pg, src_t, 5.0)
        assert worker.is_alive(), "the LOAD was expected to still be stalled"
        assert not holders, holders
        blocker.rollback()
        locked = False
        worker.join(120)
        assert not worker.is_alive(), "the cancelled job is still running"
        # The cancel landed before the LOAD returned: nothing is committed.
        assert _assert_engine_cancelled(store, job_id, result, my, dst_t) == 0
    finally:
        if locked:
            blocker.rollback()
        blocker.close()
        worker.join(120)
        _teardown(pg, my, src_t, dst_t)


def test_a_cancelled_overwrite_leaves_no_stage_table(monkeypatch):
    """A keyed overwrite stages into a run-scoped ``df_ow_*`` table and swaps
    it in at the end. A cancel (or failure) before the swap used to keep that
    stage "for resume" (PK mode), but its name is random per run and nothing
    resumes into it: a full copy of the load was orphaned beside the
    untouched destination. The stage must go; the destination stays as it was."""
    from services import cancel_check
    from services.copy_pg_mysql import copy_postgres_to_mysql

    p, c, pg, my, src_t, dst_t = _setup(3000)
    with my.cursor() as cur:
        cur.execute(f"CREATE TABLE `{dst_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
        cur.execute(f"INSERT INTO `{dst_t}` VALUES (-1, 'keep', 0), (-2, 'keep', 0)")

    def cfg(creds: dict) -> dict:
        return {
            "host": creds["host"], "port": int(creds["port"]),
            "database": creds["database"], "username": creds["username"],
            "password": creds["password"],
        }

    def cancelled(_rows=None):
        raise TransferCancelled("Transfer cancelled by user")

    def run():
        cancel_check.set_cancel_probe(cancelled)
        copy_postgres_to_mysql(
            source_cfg=cfg(p), source_schema="public", source_table=src_t,
            dest_cfg=cfg(c), dest_table=dst_t,
            pairs=[("id", "id"), ("a", "a"), ("n", "n")],
            mysql_ddls=["INT", "VARCHAR(40)", "INT"],
            replace_destination=True,
        )

    try:
        with pytest.raises(TransferCancelled):
            contextvars.copy_context().run(run)
        with my.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name LIKE %s",
                (f"df_ow_{dst_t}%",),
            )
            stages = [r[0] for r in cur.fetchall()]
            cur.execute(f"SELECT id FROM `{dst_t}` ORDER BY id")  # nosec B608
            kept = [r[0] for r in cur.fetchall()]
        assert stages == [], stages
        assert kept == [-2, -1], kept
    finally:
        _teardown(pg, my, src_t, dst_t)
