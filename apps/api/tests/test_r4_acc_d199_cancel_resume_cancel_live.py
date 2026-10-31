"""QA round 4, D199 acceptance: PG (800k rows) -> MySQL incremental_upsert,
cancel, resume, cancel. The message must equal the rows actually left.

QA on ac612203: the message said 23,552 and later 121,856 rows committed while
MySQL held 25,120 and 151,744. Route here: the run starts on the engine with
its saved transfer_request (what /transfer stores), the operator cancels
through ``POST /api/v1/connectors/jobs/{id}/cancel``, resumes through
``POST /api/v1/connectors/jobs/{id}/resume`` (same job id, gates re-run), and
cancels again. After each cancel the job message and ``cancel_outcome`` are
compared with ``SELECT COUNT(*)`` on MySQL. A final resume runs to completion:
800k rows, no duplicates, a cell check on a sample.

Live PostgreSQL + MySQL (``PG*`` / ``MYSQL_*`` env); skipped when absent.
Set ``R4_D199_ROWS`` to change the row count (default 800000).
"""
from __future__ import annotations

import os
import re
import threading
import time
import uuid

import pytest

ROWS = int(os.environ.get("R4_D199_ROWS") or 800_000)


@pytest.fixture
def env(tmp_path, monkeypatch):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not configured")
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    from services.mongodb_service import MemoryMongoDBService

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    store = MemoryMongoDBService()
    store.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return store, pg_creds(), mysql_creds()


def _stated_numbers(text: str) -> list[int]:
    return [int(m.replace(",", "")) for m in re.findall(r"([\d,]+) row\(s\)", text or "")]


def _assert_message_matches(job: dict, held: int, leg: str) -> None:
    message = str(job.get("message") or "")
    outcome = job.get("cancel_outcome") or {}
    stated = _stated_numbers(message)
    assert stated, (leg, message)
    # The job created the table, so every row there is the job's: the
    # headline number is the destination's count, not a lower bound.
    assert not outcome.get("rows_committed_is_lower_bound"), (leg, outcome, held, message)
    assert outcome.get("rows_committed") == held, (leg, outcome, held, message)
    assert stated[0] == held, (leg, message, held)

def test_cancel_resume_cancel_message_equals_destination(env, monkeypatch):
    import psycopg2
    import pymysql
    from fastapi.testclient import TestClient

    from src.main import app
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    store, p, c = env
    import src.transfer.stream as stream_mod

    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 1024)
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r4d199s_{suffix}", f"r4d199d_{suffix}"
    pg = psycopg2.connect(host=p["host"], port=int(p["port"]), user=p["username"],
                          password=p["password"], dbname=p["database"])
    pg.autocommit = True
    my = pymysql.connect(host=c["host"], port=int(c["port"]), user=c["username"],
                         password=c["password"], database=c["database"], autocommit=True)

    def held() -> int:
        try:
            with my.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{dst_t}`")  # nosec B608
                return int(cur.fetchone()[0])
        except Exception:  # noqa: BLE001 - not created yet
            return 0

    with pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {src_t} (id INT PRIMARY KEY, a VARCHAR(40), n NUMERIC(12,2), "
            "updated_at TIMESTAMP NOT NULL)"
        )
        cur.execute(
            f"INSERT INTO {src_t} SELECT g, CASE WHEN g % 5 = 0 THEN NULL WHEN g % 7 = 0 THEN '' "  # nosec B608
            "ELSE 'a' || g END, (g % 1000) / 4.0, TIMESTAMP '2026-01-01' + g * INTERVAL '1 second' "
            f"FROM generate_series(1, {ROWS}) g"
        )
    mode = "incremental_upsert"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host=str(p["host"]), port=int(p["port"]),
            database=str(p["database"]), username=str(p["username"]),
            password=str(p["password"]), table=src_t,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), username=str(c["username"]),
            password=str(c["password"]), table=dst_t,
        ),
        sync_mode=mode,
        stream_contracts=[{
            "name": src_t, "selected": True, "sync_mode": mode, "primary_key": ["id"],
            "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
        }],
        validation_mode="balanced",
        # QA's numbers (23,552 = 23 x 1,024) are the row/stream writer's
        # 1,024-row batches. A pass-through filter keeps the job off the
        # PG->MySQL LOAD DATA fast path, which loads 800k before a cancel lands.
        source_filter={"column": "id", "op": "gt", "value": 0},
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request=transfer_request_to_dict(request))
    client = TestClient(app)
    log: list[tuple[str, int, str]] = []

    def _running(worker: threading.Thread | None) -> bool:
        if worker is not None and worker.is_alive():
            return True
        status = str((store.get_job(job_id) or {}).get("status") or "")
        return status in {"pending", "queued", "running", "validating", "starting"}

    def _settle(worker: threading.Thread | None, timeout: float = 300) -> None:
        # The cancel endpoint turns the job "cancelled" at once; the run then
        # stops at its next checkpoint, re-counts the destination and only then
        # writes cancel_outcome and the message. Wait for the run itself to
        # exit: the first leg's thread, or a resumed run on the transfer
        # scheduler (local_job_ids), never a fixed sleep (CI read the
        # endpoint's interim "Transfer cancelled by user" 1.5 s after cancel).
        from services.transfer_scheduler import local_job_ids

        if worker is not None:
            worker.join(timeout)
            assert not worker.is_alive(), "still running after cancel"
        deadline = time.time() + timeout
        while time.time() < deadline and (job_id in local_job_ids() or _running(None)):
            time.sleep(0.2)
        assert job_id not in local_job_ids(), ("resumed run still running", store.get_job(job_id))

    def _cancel_when(threshold: int, worker: threading.Thread | None, leg: str) -> dict:
        deadline = time.time() + 900
        while time.time() < deadline and _running(worker) and held() < threshold:
            time.sleep(0.05)
        assert held() >= threshold, (leg, "the job never reached", threshold, held(), store.get_job(job_id))
        r = client.post(f"/api/v1/connectors/jobs/{job_id}/cancel")
        assert r.status_code == 200, r.text
        _settle(worker)
        job = store.get_job(job_id) or {}
        n = held()
        log.append((leg, n, str(job.get("message") or "")))
        assert str(job.get("status")) == "cancelled", (leg, job.get("status"), job.get("message"))
        _assert_message_matches(job, n, leg)
        return job

    def _resume() -> None:
        r = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
        assert r.status_code == 200, r.text
        deadline = time.time() + 60
        while time.time() < deadline and not _running(None):
            time.sleep(0.1)

    try:
        w1 = threading.Thread(target=lambda: UniversalTransferEngine().execute_tracked(request, job_id))
        w1.start()
        _cancel_when(20_000, w1, "cancel-1")
        first = held()

        _resume()
        _cancel_when(first + 60_000, None, "cancel-2")

        # Resume to the end: every row once, values intact.
        _resume()
        _settle(None, timeout=1800)
        job = store.get_job(job_id) or {}
        assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"), job.get("message"), log)
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM `{dst_t}`")  # nosec B608
            assert cur.fetchone() == (ROWS, ROWS, 1, ROWS), log
            cur.execute(f"SELECT id, a, n FROM `{dst_t}` WHERE id IN (5, 7, 35, 12345, {ROWS}) ORDER BY id")  # nosec B608
            got = [(int(r[0]), r[1], float(r[2])) for r in cur.fetchall()]

        def want(g):
            return (g, None if g % 5 == 0 else ("" if g % 7 == 0 else f"a{g}"), (g % 1000) / 4.0)

        assert got == [want(g) for g in sorted({5, 7, 35, 12345, ROWS})]
    finally:
        print("D199 legs:", log)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst_t}`")
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        my.close()
        pg.close()


@pytest.mark.parametrize(
    "existed,before,expect",
    [
        (False, None, 10),  # the job created the table: every row is its own
        (True, 0, 10),  # empty before the first pass (a resume restores this count)
        (True, 4, None),  # populated: COUNT growth is new keys only, stays a lower bound
        (None, None, None),  # unknown stays unknown
    ],
)
def test_keyed_upsert_landing_is_exact_on_a_table_the_job_filled(tmp_path, existed, before, expect):
    import sqlite3

    from services.error_handling import TransferCancelled
    from services.job_status import measure_cancel_landing

    db = tmp_path / "d.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    con.executemany("INSERT INTO t VALUES (?, ?)", [(i, None if i % 2 else "") for i in range(10)])
    con.commit()
    con.close()
    exc = TransferCancelled("Transfer cancelled by user", rows_written=3)
    measure_cancel_landing(
        exc, dest_type="sqlite", dest_cfg={"database": str(db)}, table="t",
        sync_mode="incremental_upsert", rows_before=before, table_existed_before=existed,
        keyed=True, atomic_overwrite=False,
    )
    assert getattr(exc, "destination_rows_by_run", None) == expect
