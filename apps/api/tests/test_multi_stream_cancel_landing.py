"""A cancelled multi-stream run is measured on every stream's own object.

D199 / C16 count the destination once a cancelled run has stopped, against the
pre-run count. A multi-stream run writes each stream into its own object, but
that measurement saw one object:

* the route that saw the cancel counted the stream it was writing, so the
  tables that had already committed were left out — two whole tables landed
  and the job said "No row of this run was kept ... A retry starts from zero"
  with ``records_processed`` 0 (a retry of the append would duplicate them);
* a route that did not count (a COPY fast path, an adapter) fell back to the
  request's primary table against ``destination.extra["cancel_rows_before"]``,
  which ``setdefault`` had pinned to the FIRST stream's pre-run count (or
  ``dest_rows_before``, the LAST stream's) — one table's count against
  another's.

Each stream now pins its pre-run count under its own destination object, and
the cancel measures every object the run reached against its own count. The
pre-run counts differ per table (4 / 7 / 2) so one cannot pass for another.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

# name -> (source rows, rows the destination table held before the run)
TABLES = {"people": (6, 4), "orders": (3, 7), "items": (5, 2)}


def _contracts() -> list[dict]:
    return [{"name": n, "selected": True, "primary_key": ["id"]} for n in TABLES]


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services import audit_log
    from services.mongodb_service import MemoryMongoDBService

    mem = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mem)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: mem)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return mem


@pytest.fixture()
def pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"], dbname=creds["database"],
        user=creds["username"], password=creds["password"],
    )
    conn.autocommit = True
    token = uuid.uuid4().hex[:8]
    src, dst = f"ms2_s_{token}", f"ms2_d_{token}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{src}"')
            cur.execute(f'CREATE SCHEMA "{dst}"')
            for name, (new, old) in TABLES.items():
                cur.execute(f'CREATE TABLE "{src}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
                cur.execute(
                    f'INSERT INTO "{src}"."{name}" '  # noqa: S608 - fixed test names
                    f"SELECT g, 'new-' || g FROM generate_series(1, {new}) g"
                )
                cur.execute(f'CREATE TABLE "{dst}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
                if old:
                    cur.execute(
                        f'INSERT INTO "{dst}"."{name}" '  # noqa: S608 - fixed test names
                        f"SELECT 1000 + g, 'old' FROM generate_series(1, {old}) g"
                    )
        ep = {k: creds[k] for k in ("host", "port", "database", "username", "password")}
        yield conn, src, dst, ep
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{src}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{dst}" CASCADE')
        conn.close()


def _pg_counts(conn, schema: str) -> dict[str, int]:
    out = {}
    with conn.cursor() as cur:
        for name in TABLES:
            cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')  # noqa: S608
            out[name] = cur.fetchone()[0]
    return out


def _cancel_after(monkeypatch, store, job_id: str, stream: str) -> None:
    """Cancel the job as soon as ``stream`` has committed (the next stream sees it)."""
    import src.transfer.stream_multi as stream_multi

    real = stream_multi._publish_stream_written

    def _then_cancel(jid, name, rows):
        real(jid, name, rows)
        if name == stream:
            store.request_job_cancel(job_id)
            store.update_job_status(job_id, "cancelled", operator_command=True)

    monkeypatch.setattr(stream_multi, "_publish_stream_written", _then_cancel)


def _run_cancelled(monkeypatch, store, pg, sync_mode: str) -> tuple[dict, dict[str, int]]:
    from src.transfer.engine import UniversalTransferEngine

    conn, src, dst, ep = pg
    job_id = "ms2c" + uuid.uuid4().hex[:12]
    _cancel_after(monkeypatch, store, job_id, "orders")
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema=src, table="people", **ep),
        destination=EndpointConfig(kind="database", format="postgresql", schema=dst, table="people", **ep),
        sync_mode=sync_mode,
        stream_contracts=_contracts(),
        skip_preflight=True,
        validation_mode="strict",
    )
    store.update_job_status(job_id, "pending", transfer_request={})
    UniversalTransferEngine().execute_tracked(request, job_id)
    return store.get_job(job_id) or {}, _pg_counts(conn, dst)


def _by_table(outcome: dict) -> dict[str, dict]:
    return {str(s.get("table")): s for s in outcome.get("streams") or []}


def test_live_cancelled_append_counts_every_stream_that_landed(monkeypatch, store, pg):
    job, counts = _run_cancelled(monkeypatch, store, pg, "full_refresh_append")
    assert job.get("status") == "cancelled", (job.get("status"), job.get("error"))
    # people and orders committed in full before the cancel reached items.
    assert counts["people"] == 6 + 4 and counts["orders"] == 3 + 7, counts
    landed = {n: counts[n] - old for n, (_new, old) in TABLES.items()}
    out = job.get("cancel_outcome") or {}
    assert out.get("rows_committed_source") == "destination_count", out
    assert out.get("rows_committed") == sum(landed.values()), (out, landed)
    assert job.get("records_processed") == sum(landed.values())
    assert out.get("destination_rows_before") == sum(old for _n, old in TABLES.values()), out
    assert out.get("destination_rows_now") == sum(counts.values()), out
    assert "No row of this run was kept" not in str(job.get("message")), job.get("message")
    per = _by_table(out)
    assert {n: per[n]["rows_before"] for n in per} == {n: old for n, (_new, old) in TABLES.items() if n in per}
    assert per["people"]["rows_by_run"] == 6 and per["orders"]["rows_by_run"] == 3, per


def test_live_cancelled_overwrite_pins_each_stream_before_its_own_clear(monkeypatch, store, pg):
    """The overwrite pin was counted once, on the first stream, and reused for every stream."""
    job, counts = _run_cancelled(monkeypatch, store, pg, "full_refresh_overwrite")
    assert job.get("status") == "cancelled", (job.get("status"), job.get("error"))
    assert counts["people"] == 6 and counts["orders"] == 3, counts
    per = _by_table(job.get("cancel_outcome") or {})
    assert per["people"]["rows_before"] == 4 and per["orders"]["rows_before"] == 7, per
    if "items" in per:
        assert per["items"]["rows_before"] == 2, per
    assert per["people"]["rows_by_run"] == 6 and per["orders"]["rows_by_run"] == 3, per
    assert "No row of this run was kept" not in str(job.get("message")), job.get("message")


def _sqlite_run(tmp_path: Path, store) -> TransferRequest:
    """A completed 3-stream append into occupied SQLite tables; returns its request."""
    from src.transfer.engine import UniversalTransferEngine

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    for path in (src, dst):
        conn = sqlite3.connect(str(path))
        try:
            with conn:
                for name, (new, old) in TABLES.items():
                    conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")
                    rows = (
                        [(i, f"new-{i}") for i in range(1, new + 1)]
                        if path == src
                        else [(1000 + i, "old") for i in range(1, old + 1)]
                    )
                    conn.executemany(f"INSERT INTO {name} VALUES (?, ?)", rows)  # noqa: S608
        finally:
            conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people"),
        sync_mode="full_refresh_append",
        stream_contracts=_contracts(),
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "ms2r" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return request


def test_unmeasured_cancel_counts_each_stream_against_its_own_pre_run_count(tmp_path, store):
    """The route that did not count (COPY fast path / adapter) after a multi-stream run.

    The request is the one a real 3-stream append just ran with, so its
    destination carries exactly what the run pinned. The cancel reaches the job
    record with only the writer's tally, as a COPY route raises it.
    """
    from services.error_handling import TransferCancelled
    from src.transfer.job_failure import _fail_runtime_job

    request = _sqlite_run(tmp_path, store)
    job_id = "ms2u" + uuid.uuid4().hex[:12]
    store.create_transfer_job({"_id": job_id, "status": "running", "records_processed": 5})
    store.request_job_cancel(job_id)
    store.update_job_status(job_id, "cancelled", phase="cancelled", message="Transfer cancelled by user")
    _fail_runtime_job(store, job_id, TransferCancelled("Transfer cancelled by user", rows_written=5),
                      request=request)
    job = store.get_job(job_id) or {}
    out = job.get("cancel_outcome") or {}
    total_new = sum(new for new, _old in TABLES.values())
    assert out.get("rows_committed_source") == "destination_count", out
    assert out.get("rows_committed") == total_new, out
    assert job.get("records_processed") == total_new
    assert out.get("destination_rows_before") == sum(old for _new, old in TABLES.values()), out
    assert out.get("destination_rows_now") == sum(new + old for new, old in TABLES.values()), out
    per = _by_table(out)
    assert {n: (s["rows_before"], s["rows_now"], s["rows_by_run"]) for n, s in per.items()} == {
        n: (old, new + old, new) for n, (new, old) in TABLES.items()
    }, per
