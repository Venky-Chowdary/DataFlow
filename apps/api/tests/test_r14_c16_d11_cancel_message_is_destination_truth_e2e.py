"""C16 / MySQL D11: a cancel states the destination's committed state as measured.

QA (production 8f7d246d): a 150,000-row transfer cancelled during execution
said "1,024 rows retained" (MySQL source: "2,048 rows written and retained")
while the destination table did not exist; retry then reported
``from_zero_no_writes``. The record must say what the destination holds —
counted after the writers stopped — never a writer-ack tally for rows a
rollback (or a dropped run-created table) removed; and retry must agree.

Live: PostgreSQL (``PG*``) and MySQL 8 (``MYSQL_*``) sources, PostgreSQL
destination. The run and the cancel go through REST.
"""

from __future__ import annotations

import os
import re
import time
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

ROWS = 150_000
PREFILL = 500
RETRY_TO_COMPLETION = os.environ.get("R14_C16_RETRY_FULL") == "1"
_TERMINAL = {"completed", "completed_with_quarantine", "failed", "partial", "cancelled"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _seed(kind: str, table: str) -> None:
    labels = {1: None, 2: "", 3: "   ", 4: "Zoë 東京 ✓"}
    rows = [(i, labels.get(i % 50, f"n{i}"), i % 997) for i in range(1, ROWS + 1)]
    if kind == "postgresql":
        h.execute(kind, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT, n INTEGER)")
        conn = h.pg_conn()
        try:
            from psycopg2.extras import execute_values

            with conn.cursor() as cur:
                execute_values(
                    cur, f"INSERT INTO {table} VALUES %s", rows, page_size=10000  # nosec B608
                )
        finally:
            conn.close()
    else:
        h.execute(
            kind,
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, label VARCHAR(64), n INT) "
            "CHARACTER SET utf8mb4",
        )
        conn = h.mysql_conn()
        try:
            with conn.cursor() as cur:
                for start in range(0, ROWS, 10000):
                    cur.executemany(
                        f"INSERT INTO `{table}` VALUES (%s, %s, %s)",  # nosec B608
                        rows[start:start + 10000],
                    )
        finally:
            conn.close()


def _dest_state(table: str) -> int | None:
    """Rows in the destination table, or None when the table does not exist."""
    exists = h.query("postgresql", f"SELECT to_regclass('public.\"{table}\"')")[0][0]
    if exists is None:
        return None
    return int(h.query("postgresql", f'SELECT COUNT(*) FROM "{table}"')[0][0])


def _wait_terminal(job_id: str, timeout_s: float = 600.0, settle_s: float = 60.0) -> dict:
    """The job once its worker recorded the outcome.

    The cancel route flips the status at once; the worker then records what
    it left behind (``cancel_outcome``). A worker that never records it gets
    ``settle_s`` after the status flipped — the record is what the operator
    reads, so a missing outcome is a failure, not a wait.
    """
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    cancelled_at: float | None = None
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        status = str(job.get("status") or "")
        if status in _TERMINAL and (status != "cancelled" or job.get("cancel_outcome")):
            return job
        if status == "cancelled":
            cancelled_at = cancelled_at or time.monotonic()
            if time.monotonic() - cancelled_at > settle_s:
                return job
        time.sleep(0.2)
    return job


def _writing(watch, dst_t: str) -> bool:
    with watch.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid() "
            "AND query ILIKE %s AND (query ILIKE 'copy%%' OR query ILIKE 'insert%%')",
            (f"%{dst_t}%",),
        )
        return cur.fetchone() is not None


def _cancel_mid_write(monkeypatch, client, dst_t: str) -> dict:
    """Cancel through REST from the job's own cancel probe, once it is writing.

    The probe runs on every COPY read/write block and between row batches; it
    fires while the destination session is inside its COPY / INSERT and the job
    already shows progress — mid-write on every route, deterministic, unlike a
    wall-clock race against a 150k-row COPY that takes about a second.
    """
    import services.cancel_check as cc

    real = cc.cancel_checkpoint
    state: dict = {"job_id": None, "polls": 0, "fired": False}
    watch = h.pg_conn()

    def probe(rows_written=None):
        if not state["fired"] and state["job_id"]:
            if _writing(watch, dst_t):
                state["polls"] += 1
                from services.mongodb_service import get_mongodb_service

                shown = int(
                    (get_mongodb_service().get_job(state["job_id"]) or {}).get("records_processed")
                    or 0
                )
                # Once the job shows progress (the writer's tally QA read back as
                # "rows retained"), or well into the COPY on a route with no meter.
                if state["polls"] >= 3 and (shown > 0 or state["polls"] >= 12):
                    state["fired"] = True
                    resp = client.post(f"/api/v1/connectors/jobs/{state['job_id']}/cancel")
                    assert resp.status_code == 200, resp.text
        return real(rows_written)

    monkeypatch.setattr(cc, "cancel_checkpoint", probe)
    state["watch"] = watch
    return state


def _run_and_cancel(client, monkeypatch, src_kind: str, sync_mode: str, existing: bool):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r14c16s_{token}", f"r14c16d_{token}"
    _seed(src_kind, src_t)
    if existing:
        h.execute(
            "postgresql",
            f'CREATE TABLE "{dst_t}" (id INTEGER PRIMARY KEY, label TEXT, n INTEGER)',
        )
        h.execute(
            "postgresql",
            f'INSERT INTO "{dst_t}" SELECT -g, \'pre\', 0 FROM generate_series(1, {PREFILL}) g',
        )
    src = create_connector(
        {"name": f"{src_kind}-{token}", **h.dest_config(src_kind)}
        | ({"schema": "public"} if src_kind == "postgresql" else {})
    )
    dst = create_connector({"name": f"pg-dst-{token}", **h.dest_config("postgresql")})
    form = {
        "source_kind": "database", "source_format": src_kind,
        "source_connector_id": src.id, "source_table": src_t,
        "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": dst.id, "dest_table": dst_t,
        "async_mode": "true", "sync_mode": sync_mode,
    }
    try:
        state = _cancel_mid_write(monkeypatch, client, dst_t)
        resp = client.post("/api/v1/transfer/run", data=form)
        assert resp.status_code == 200, resp.text
        job_id = resp.json()["job_id"]
        state["job_id"] = job_id
        job = _wait_terminal(job_id)
        # Disarmed: the retry runs with the real probe only.
        state["fired"] = True
        state["watch"].close()
        time.sleep(0.5)
        truth = _dest_state(dst_t)
        return job_id, job, truth, dst_t, src_t
    except BaseException:
        h.drop_table("postgresql", dst_t)
        h.drop_table(src_kind, src_t)
        raise


def _numbers(text: str) -> list[int]:
    return [int(m.replace(",", "")) for m in re.findall(r"(\d[\d,]*) row\(s\)", text or "")]


@pytest.mark.parametrize("src_kind", ["postgresql", "mysql"])
@pytest.mark.parametrize("existing", [False, True], ids=["create_new", "existing"])
@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_cancel_record_matches_destination_and_retry_agrees(
    client, monkeypatch, src_kind, existing, sync_mode
):
    if src_kind == "mysql":
        h.mysql_conn().close()
    job_id, job, truth, dst_t, src_t = _run_and_cancel(
        client, monkeypatch, src_kind, sync_mode, existing
    )
    try:
        status = job.get("status")
        print("JOB", job_id, status, "truth=", truth, "rp=", job.get("records_processed"),
              "msg=", job.get("message"), "err=", job.get("error"),
              "outcome=", job.get("cancel_outcome"))
        assert status == "cancelled", (status, job.get("error"))
        before = PREFILL if existing else 0
        held = truth or 0
        if existing and sync_mode == "full_refresh_append":
            by_run = held - before
        elif existing and truth is not None and held == PREFILL:
            by_run = 0  # overwrite rolled back: the old rows are back, none of this run's
        else:
            by_run = held
        outcome = job.get("cancel_outcome") or {}
        text = " ".join(str(job.get(k) or "") for k in ("message", "error"))
        assert outcome.get("rows_committed") == by_run, (outcome, truth, text)
        assert int(job.get("records_processed") or 0) == by_run, (job.get("records_processed"), truth)
        # No row count in the message other than what the destination holds.
        for n in _numbers(text):
            assert n in (by_run, held), (n, truth, text)
        if by_run == 0:
            assert outcome.get("partial_write") is False, outcome
            assert "were not removed" not in text and "already written" not in text, text
            if truth is None:
                assert outcome.get("destination_table_exists") is False, outcome
        # Retry agrees with the record: nothing committed -> a from-zero
        # retry that cannot duplicate; committed append rows -> refused with
        # the destination's number; committed overwrite rows -> convergent.
        resp = client.post(f"/api/v1/connectors/jobs/{job_id}/retry")
        if by_run == 0:
            assert resp.status_code == 200, resp.text
            assert resp.json()["retry_decision"]["kind"] == "from_zero_no_writes", resp.json()
        elif sync_mode == "full_refresh_append":
            assert resp.status_code == 409, resp.text
            assert resp.json()["detail"]["rows_committed"] == by_run, resp.text
        else:
            assert resp.status_code == 200, resp.text
        if resp.status_code == 200:
            retry_id = resp.json()["job_id"]
            if RETRY_TO_COMPLETION:
                retried = _wait_terminal(retry_id, timeout_s=1500)
                assert retried.get("status") == "completed", (
                    retried.get("status"), retried.get("error"),
                )
                final = _dest_state(dst_t)
                assert final == ROWS + (before if sync_mode == "full_refresh_append" else 0), final
            else:
                # The decision is what this test owns; a full 150k reload per
                # leg is minutes on a shared engine. R14_C16_RETRY_FULL=1 runs it.
                client.post(f"/api/v1/connectors/jobs/{retry_id}/cancel")
                _wait_terminal(retry_id, settle_s=30)
    finally:
        h.drop_table("postgresql", dst_t)
        h.drop_table(src_kind, src_t)
