"""D199 — after a cancel, the job states how many rows the destination holds.

QA (production ac612203): a MySQL load was cancelled mid-run. The job said
23,552 row(s) were left behind; MySQL held 25,120. The count came from the
committed batches the progress reporter had seen, not from the destination:
pooled chunk writers that were already inside their commit when the cancel
landed added rows the reporter never counted.

End to end through ``UniversalTransferEngine().execute_tracked`` against a live
MySQL destination: cancel mid-load, wait for the writers to stop, then compare
the job's cancel message and ``cancel_outcome`` with ``SELECT COUNT(*)``.
"""

from __future__ import annotations

import re
import threading
import time
import uuid

import pytest


def _run_and_cancel(
    monkeypatch, *, source: str, rows: int, prefill: int = 0, operator_cancel: bool = True,
    sync: str = "", row_path: bool = True,
):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not mysql_up() or (source == "postgresql" and not pg_up()):
        pytest.skip("live MySQL (and PG) not configured")
    import pymysql

    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    # Small pages so the cancel lands between (and during) committed batches.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 1024)

    c = mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d199s_{suffix}", f"d199d_{suffix}"
    my = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )
    seed = [(i, f"a{i}" if i % 5 else ("" if i % 2 else None), i % 97) for i in range(1, rows + 1)]
    pg = None
    if source == "postgresql":
        import psycopg2

        p = pg_creds()
        pg = psycopg2.connect(
            host=p["host"], port=int(p["port"]), user=p["username"],
            password=p["password"], dbname=p["database"],
        )
        pg.autocommit = True
        with pg.cursor() as cur:
            cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
            from psycopg2.extras import execute_values

            execute_values(cur, f"INSERT INTO {src_t} VALUES %s", seed, page_size=5000)  # nosec B608
        src_ep = EndpointConfig(
            kind="database", format="postgresql", host=str(p["host"]), port=int(p["port"]),
            database=str(p["database"]), username=str(p["username"]),
            password=str(p["password"]), table=src_t,
        )
    else:
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
            cur.executemany(f"INSERT INTO `{src_t}` VALUES (%s, %s, %s)", seed)  # nosec B608
        src_ep = EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), username=str(c["username"]),
            password=str(c["password"]), table=src_t,
        )
    if prefill:
        # An append into an occupied table: rows that were there before the run
        # are not "left behind" by it.
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{dst_t}` (id INT PRIMARY KEY, a VARCHAR(40), n INT)")
            cur.executemany(
                f"INSERT INTO `{dst_t}` VALUES (%s, %s, %s)",  # nosec B608
                [(-i, "pre", 0) for i in range(1, prefill + 1)],
            )

    def dest_count() -> int | None:
        try:
            with my.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{dst_t}`")  # nosec B608
                return int(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - table not created yet
            return None

    request = TransferRequest(
        source=src_ep,
        destination=EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), username=str(c["username"]),
            password=str(c["password"]), table=dst_t,
        ),
        sync_mode=sync or ("append" if prefill else "full_refresh_overwrite"),
        validation_mode="balanced",
        # A pass-through filter keeps the job on the row/stream writer.
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
        while time.time() < deadline and worker.is_alive():
            if (dest_count() or 0) >= prefill + 3000:
                break
            time.sleep(0.02)
        assert (dest_count() or 0) >= prefill + 3000, "the job never started writing"
        if operator_cancel:
            store.request_job_cancel(job_id)
            store.update_job_status(job_id, "cancelled", operator_command=True)
        worker.join(120)
        assert not worker.is_alive(), "the cancelled job is still running"
        time.sleep(1)
        settled = dest_count()
        job = store.get_job(job_id) or {}
        return settled, job, result.get("r")
    finally:
        worker.join(30)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst_t}`")
            if pg is None:
                cur.execute(f"DROP TABLE IF EXISTS `{src_t}`")
        my.close()
        if pg is not None:
            with pg.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {src_t}")
            pg.close()


def _stated(text: str) -> int:
    m = re.search(r"([\d,]+) row\(s\)", text or "")
    assert m, text
    return int(m.group(1).replace(",", ""))


@pytest.mark.parametrize("source", ["mysql", "postgresql"])
def test_cancel_message_states_the_destination_count(monkeypatch, source):
    settled, job, _ = _run_and_cancel(monkeypatch, source=source, rows=60000)
    assert settled is not None and 0 < settled < 60000, settled
    outcome = job.get("cancel_outcome") or {}
    message = str(job.get("message") or "")
    assert outcome.get("rows_committed") == settled, (outcome, settled, message)
    assert _stated(message) == settled, (message, settled)
    assert outcome.get("rows_committed_source") == "destination_count", outcome


def test_append_cancel_counts_only_this_runs_rows(monkeypatch):
    settled, job, _ = _run_and_cancel(monkeypatch, source="mysql", rows=60000, prefill=500)
    written = settled - 500
    outcome = job.get("cancel_outcome") or {}
    message = str(job.get("message") or "")
    assert outcome.get("rows_committed") == written, (outcome, settled, message)
    assert _stated(message) == written, (message, written)


def test_pooled_writer_mid_commit_is_counted(monkeypatch):
    """QA's shape, made deterministic: 23,552 = 23 committed batches the
    reporter saw; MySQL held 1,568 more (one later batch plus 544 rows of the
    batch that noticed the cancel between sub-chunks). Chunk 6 notices the
    cancel after committing half its rows while chunks 7+ finish first; the
    ordered drain stops at chunk 6, so the tally never sees any of them."""
    import copy

    import src.transfer.stream as stream_mod
    from services.error_handling import TransferCancelled

    real = stream_mod._process_db_chunk
    gate = threading.Event()

    def _chunk(run, idx, batch):
        if idx == 6:
            gate.wait(10)  # later chunks commit while this one is "mid-write"
            time.sleep(0.5)
            half = copy.copy(batch)
            half.rows = list(batch.rows)[: len(batch.rows) // 2]
            real(run, idx, half)
            raise TransferCancelled(
                "Transfer cancelled by user", rows_written=len(half.rows)
            )
        out = real(run, idx, batch)
        if idx >= 8:
            gate.set()
        return out

    monkeypatch.setattr(stream_mod, "_process_db_chunk", _chunk)
    settled, job, _ = _run_and_cancel(
        monkeypatch, source="mysql", rows=60000, operator_cancel=False
    )
    outcome = job.get("cancel_outcome") or {}
    message = str(job.get("message") or "")
    assert outcome.get("rows_committed") == settled, (outcome, settled, message)
    assert _stated(message) == settled, (message, settled)
    assert int(job.get("records_processed") or 0) == settled, job.get("records_processed")
    # The tally the reporter saw really was short — this is the D199 gap.
    assert int(outcome.get("committed_batches_tally") or 0) < settled, outcome


def test_overwrite_of_an_existing_table_counts_the_replacement(monkeypatch):
    settled, job, _ = _run_and_cancel(
        monkeypatch, source="mysql", rows=60000, prefill=500, sync="full_refresh_overwrite"
    )
    outcome = job.get("cancel_outcome") or {}
    # The overwrite cleared the 500 old rows; every row now there is this run's.
    assert outcome.get("rows_committed") == settled, (outcome, settled)
    assert _stated(str(job.get("message") or "")) == settled


def test_copy_route_cancel_shows_the_destination_count(monkeypatch):
    """A server-to-server COPY route does not re-count: the tally stays, but
    the message says it is a lower bound and what the table holds."""
    settled, job, _ = _run_and_cancel(
        monkeypatch, source="postgresql", rows=200000, row_path=False
    )
    outcome = job.get("cancel_outcome") or {}
    message = str(job.get("message") or "")
    if outcome.get("rows_committed_source") in ("destination_count", "completed_run"):
        # Re-counted, or the COPY had finished (and been verified) first.
        assert outcome.get("rows_committed") == settled, (outcome, settled)
        assert _stated(message) == settled, message
    elif outcome.get("partial_write"):
        assert outcome.get("rows_committed_is_lower_bound") is True, outcome
        assert outcome.get("rows_committed") <= settled, (outcome, settled)
        assert outcome.get("destination_rows_now") == settled, (outcome, settled)
        assert f"now holds {settled:,} row(s)" in message, message


@pytest.mark.parametrize(
    "sync,keyed,atomic,existed,before,expect",
    [
        ("append", False, False, True, 4, 6),  # 10 now - 4 before
        ("full_refresh_overwrite", False, False, True, 99, 10),  # cleared first
        ("full_refresh_overwrite", False, True, True, 99, None),  # one txn: rolled back
        ("full_refresh_overwrite", False, True, False, None, 10),  # created this run
        ("upsert", True, False, True, 4, None),  # COUNT growth is new keys only
        ("append", False, False, True, None, None),  # before unknown
    ],
)
def test_landing_is_attributed_only_when_exact(tmp_path, sync, keyed, atomic, existed, before, expect):
    import sqlite3

    from services.error_handling import TransferCancelled
    from services.job_status import cancel_outcome_for, measure_cancel_landing

    db = tmp_path / "d.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    con.executemany("INSERT INTO t VALUES (?, ?)", [(i, None if i % 2 else "") for i in range(10)])
    con.commit()
    con.close()
    exc = TransferCancelled("Transfer cancelled by user", rows_written=3)
    measure_cancel_landing(
        exc, dest_type="sqlite", dest_cfg={"database": str(db)}, table="t",
        sync_mode=sync, rows_before=before, table_existed_before=existed,
        keyed=keyed, atomic_overwrite=atomic,
    )
    assert exc.destination_rows_now == 10
    assert getattr(exc, "destination_rows_by_run", None) == expect
    out = cancel_outcome_for(
        3,
        destination_rows_by_run=getattr(exc, "destination_rows_by_run", None),
        destination_rows_now=exc.destination_rows_now,
    )
    if expect is None:
        assert out["rows_committed"] == 3 and out["rows_committed_is_lower_bound"]
        assert "at least 3 row(s)" in out["message"] and "now holds 10 row(s)" in out["message"]
    else:
        assert out["rows_committed"] == expect
        assert out["rows_committed_source"] == "destination_count"
        assert out["committed_batches_tally"] == 3
