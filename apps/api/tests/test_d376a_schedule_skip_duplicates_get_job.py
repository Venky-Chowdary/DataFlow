"""D376 (a) — a skip_duplicates schedule's kept rows are counted where QA reads them.

QA (schedule QAV3-sched-skip, PostgreSQL -> MongoDB, skip_duplicates): the
second run kept all 30 stored keys; get_job read records_processed 0 and the
trust report "0 event(s) merged" with the 30 skipped rows uncounted. AC-1:
get_job and the trust report read rows_skipped 30.

PR #433's live test drives ``execute_tracked`` with ``request.duplicate_policy``
and reads the internal job store. QA's route is different on both ends: the
schedule builds the request (the policy rides only on the route extras) and
the operator reads the MCP ``get_job`` tool. Both legs run here: the schedule's
own ``build_schedule_request`` into the engine, read back through
``DataPilotTools.get_job`` and the parity (trust) report.

The source is PostgreSQL. The SQLite leg runs on every machine (a writer
route like MongoDB's, not a server-side copy); the MongoDB leg needs the
replica set on :27017 (CI ``start_ci_databases.sh``).
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.conftest import pg_test_cfg  # noqa: E402

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")
ROWS = 30


def _pg_sql(statement: str, params=None) -> list[tuple]:
    import psycopg2

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"],
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"PostgreSQL is required for the D376(a) schedule e2e: {exc}")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if params is not None:
                cur.executemany(statement, params)
            else:
                cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _pg_conn() -> dict:
    cfg = pg_test_cfg()
    return {"type": "postgresql", "host": cfg["host"], "port": cfg["port"],
            "database": cfg["database"], "username": cfg["username"],
            "password": cfg["password"], "schema": "public"}


@pytest.fixture
def store(tmp_path, monkeypatch):
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: memory)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return memory


def _destination(kind: str, tag: str, tmp_path):
    """(connector dict, dest table, read-back of (id, name), cleanup)."""
    if kind == "sqlite":
        # Another engine than the source, like QA's MongoDB: the rows go
        # through the writer, not a server-side copy.
        import sqlite3

        path = tmp_path / f"d376a_{tag}.db"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER)")

        def rows() -> dict:
            with sqlite3.connect(path) as db:
                return dict(db.execute("SELECT id, name FROM c").fetchall())

        return {"type": "sqlite", "database": str(path)}, "c", rows, lambda: None
    import socket

    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
    except OSError:
        pytest.skip("no MongoDB replica set on :27017 (CI runs this leg)")
    import pymongo

    db = f"d376a_{tag}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    def rows() -> dict:
        return {d["id"]: d["name"] for d in client[db].c.find({})}

    def cleanup() -> None:
        client.drop_database(db)
        client.close()

    return {"type": "mongodb", "connection_string": MONGO_URI, "database": db}, "c", rows, cleanup


@pytest.mark.parametrize("sync_mode", ["full_refresh_append", "incremental_deduped"])
@pytest.mark.parametrize("dest_kind", ["sqlite", "mongodb"])
def test_schedule_second_run_reports_kept_rows_on_get_job_and_trust_report(
    store, dest_kind, sync_mode, tmp_path
):
    from services.parity_report import build_parity_report
    from services.schedule_runner import build_schedule_request
    from services.schedule_store import PipelineSchedule
    from src.ai.copilot.tools import DataPilotTools
    from src.transfer.engine import UniversalTransferEngine

    tag = uuid.uuid4().hex[:8]
    src = f"d376a_src_{tag}"
    _pg_sql(f'CREATE TABLE "{src}" (id INT PRIMARY KEY, name TEXT, amount INT)')
    _pg_sql(
        f'INSERT INTO "{src}" VALUES (%s, %s, %s)',  # nosec B608
        [(i, f"n{i}", i * 10) for i in range(1, ROWS + 1)],
    )
    dst_conn, dst_table, dest_rows, cleanup = _destination(dest_kind, tag, tmp_path)
    sched = PipelineSchedule(
        id=f"sched-{tag}", name="QAV3-sched-skip", source_connector_id="",
        source_table=src, dest_connector_id="", dest_table=dst_table, interval="daily",
        sync_mode=sync_mode, validation_mode="balanced", primary_key="id",
        duplicate_policy="skip_duplicates",
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in ("id", "name", "amount")],
    )
    try:
        jobs = []
        for run in (1, 2):
            if run == 2:
                # New images of stored keys: skip_duplicates keeps the stored ones.
                _pg_sql(f"UPDATE \"{src}\" SET name = name || '-v2'")  # nosec B608
            request = build_schedule_request(sched, _pg_conn(), dst_conn)
            request.skip_preflight = True
            job_id = uuid.uuid4().hex[:24]
            store.update_job_status(job_id, "pending", transfer_request={})
            result = UniversalTransferEngine().execute_tracked(request, job_id)
            assert result.success, result.error
            jobs.append(job_id)
        assert dest_rows() == {i: f"n{i}" for i in range(1, ROWS + 1)}

        # AC-1 on the surface QA reads: the MCP get_job tool.
        shown = DataPilotTools().execute("get_job", {"job_id": jobs[1]})
        assert shown.success, shown.error
        out = dict(shown.output or {})
        assert out.get("rows_skipped") == ROWS, {
            k: out.get(k) for k in ("rows_skipped", "records_processed", "message")
        }
        # "0 event(s) merged" alone read as nothing happened: the line names the kept rows.
        assert f"{ROWS} row(s) kept as already stored" in str(out.get("message") or ""), out.get("message")
        first = DataPilotTools().execute("get_job", {"job_id": jobs[0]}).output or {}
        assert first.get("rows_skipped") == 0, first.get("rows_skipped")

        # AC-1 on the trust report: the 30 kept rows are named and explained.
        job = store.get_job(jobs[1])
        report = build_parity_report(job, reread=False, job_id=jobs[1])
        policy = [
            d for d in report.get("differences") or []
            if "duplicate policy" in str((d.get("cause") or {}).get("detail"))
        ]
        assert [(d["kind"], d["rows"]) for d in policy] == [("missing_in_destination", ROWS)], report
        assert all((d.get("cause") or {}).get("explained") for d in report.get("differences") or [])
    finally:
        cleanup()
        _pg_sql(f'DROP TABLE IF EXISTS "{src}"')
