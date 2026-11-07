"""D168 / D230 / D166: the COPY fast paths that stream through a FIFO read the
source on a thread of their own, and that thread must belong to the job.

The cancel probe, the per-batch progress meter and the run-attempt fence are
ContextVars (D168 moved them off thread-locals so the chunk-writer pool sees
them). ``stream_between_cursors`` started its producer on a bare
``threading.Thread``, which starts with an empty context, so for MySQL->PG,
PG->MySQL and MySQL->MySQL:

* ``cancel_checkpoint()`` in the producer was a no-op: a cancelled job kept
  streaming every row to the destination (D168: "rows keep landing after the
  job is reported cancelled");
* ``report_rows()`` fed no meter: the job read 0 rows with a frozen
  ``updated_at`` until the whole copy returned (D168, D230: "records_processed
  0 for 60-120 s after the destination holds all rows");
* a superseded attempt could not see that it was superseded, so a cancelled
  run kept going next to the resumed one (D166).

Live MySQL -> PostgreSQL through the real fast path, plus the shared helper.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.cancel_check as cc  # noqa: E402
from services.error_handling import TransferCancelled  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402

ROWS = 30_000


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def test_fifo_producer_runs_in_the_jobs_context():
    """Whatever the pair, the producer sees the job's probe and meter."""
    from services.copy_fast_path import stream_between_cursors

    seen: dict[str, object] = {}
    marker = object()

    def probe(rows_written=None):
        return None

    cc.set_cancel_probe(probe)
    cc.set_progress_meter(marker)  # type: ignore[arg-type]
    try:

        def produce(path: str) -> None:
            seen["probe"] = cc.get_cancel_probe()
            seen["meter"] = cc._meter.get()
            with open(path, "wb") as out:
                out.write(b"x\n")

        def consume(path: str) -> None:
            with open(path, "rb") as src:
                src.read()

        stream_between_cursors(prefix="df_d168_", producer=produce, consumer=consume)
    finally:
        cc.set_cancel_probe(None)
        cc.set_progress_meter(None)
    assert seen["probe"] is probe
    assert seen["meter"] is marker


@pytest.fixture
def mysql_source():
    if not _reachable(3306):
        pytest.skip("MySQL 3306 not reachable")
    pymysql = pytest.importorskip("pymysql")
    cfg = {
        "type": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
        "username": os.environ.get("MYSQL_USER", "dataflow"),
        "password": os.environ.get("MYSQL_PASSWORD", "dataflow"),
    }
    table = f"d168_src_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["username"],
        password=cfg["password"], database=cfg["database"], autocommit=True,
    )
    labels = [None, "", "   ", "naïve ☃ 東京"]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, label TEXT)")
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, %s)",
            [(i, labels[i % 4] if i % 4 else f"r{i}") for i in range(1, ROWS + 1)],
        )
    try:
        yield cfg, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


@pytest.fixture
def pg_dest():
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg

    cfg = {"type": "postgresql", "schema": "public", **pg_test_cfg()}
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = f"d168_dst_{uuid.uuid4().hex[:8]}"

    def count() -> int | None:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            if cur.fetchone()[0] is None:
                return None
            cur.execute(f'SELECT count(*) FROM public."{table}"')
            return int(cur.fetchone()[0])

    try:
        yield cfg, table, count
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


def _copy(mysql_source, pg_dest):
    from services.copy_mysql_pg import copy_mysql_to_postgres

    (src_cfg, src_table), (dst_cfg, dst_table, _count) = mysql_source, pg_dest
    return copy_mysql_to_postgres(
        source_cfg={**src_cfg, "table": src_table},
        source_table=src_table,
        dest_cfg={**dst_cfg, "table": dst_table},
        dest_schema="public",
        dest_table=dst_table,
        pairs=[("id", "id"), ("label", "label")],
        pg_ddls=["BIGINT", "TEXT"],
        replace_destination=True,
    )


def test_cancelled_mysql_to_postgres_copy_stops_and_lands_nothing(mysql_source, pg_dest):
    calls = {"n": 0}

    def probe(rows_written=None):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise TransferCancelled("Transfer cancelled by user", rows_written=rows_written)

    cc.set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled):
            _copy(mysql_source, pg_dest)
    finally:
        cc.set_cancel_probe(None)
    # The producer polled the probe between fetch batches and stopped there;
    # the COPY transaction rolled back, so nothing landed.
    assert calls["n"] >= 2
    assert pg_dest[2]() in (None, 0)


def test_mysql_to_postgres_copy_moves_the_jobs_progress_per_batch(mysql_source, pg_dest):
    store = MemoryMongoDBService()
    store.update_job_status("j168", "running", total_rows=ROWS, records_processed=0)
    cc.set_progress_meter(cc.ProgressMeter(store, "j168", interval=0))
    try:
        result = _copy(mysql_source, pg_dest)
    finally:
        cc.set_progress_meter(None)
    assert result.rows_copied == ROWS
    assert pg_dest[2]() == ROWS
    job = store.get_job("j168")
    # Every fetch batch reached the job row while the copy ran.
    assert job["records_processed"] == ROWS, job.get("records_processed")
    assert float(job.get("progress_pct") or 0) > 5


def test_a_superseded_attempt_stops_its_copy_when_resume_takes_over(mysql_source, pg_dest):
    """D166: cancel -> resume while the first attempt was still copying. The old
    attempt's producer could not see the run-attempt fence, kept streaming, and
    fought the resumed run over the same table ("expected target 60000 vs
    target 0 ... previous destination table was restored"). Now it stops at
    its next fetch batch and its COPY rolls back."""
    store = MemoryMongoDBService()
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "running", total_rows=ROWS, records_processed=0)
    assert cc.begin_run_attempt(store, job_id)
    check = cc.cancel_check_for(store, job_id)
    polls = {"n": 0}

    def probe(rows_written=None):
        polls["n"] += 1
        if polls["n"] == 2:
            # resume_job stamps a fresh attempt while this one is mid-copy.
            store.update_job_fields(job_id, {"run_attempt": uuid.uuid4().hex})
        check(rows_written)

    cc.set_cancel_probe(probe)
    try:
        with pytest.raises(TransferCancelled) as stopped:
            _copy(mysql_source, pg_dest)
    finally:
        cc.set_cancel_probe(None)
        cc.end_run_attempt()
    assert getattr(stopped.value, "superseded", False) is True
    assert pg_dest[2]() in (None, 0)
