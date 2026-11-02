"""D187 end to end: PostgreSQL / MySQL -> a NEW Snowflake table through MCP
plan_transfer / start_transfer / confirm / job, with a real Snowflake's costs.

QA (production ac612203): every load into Snowflake failed at the 45s tool
deadline ("source unreachable/slow: <PG host> ... step: running the
preflight checks"), from a local PG and from a cloud Neon PG alike, while the
same source staged into SQL Server and Snowflake test_connector/run_query
answered. Before #339 (37c0e268) the same plan staged: #339 added the 45s
deadline around a plan whose work was unbounded and serial.

Real Snowflake is not available here. fakesnow stands in, slowed to a real
account's costs (``tests/injected_latency.py``): 3s per login, 1s per
statement, 8s for the suspended warehouse's resume, 0.3s per cloud-PG
connect. What it measured on the base (integration 9ff6c1e7): the plan sent
16 Snowflake statements serially after the PG reads (ten of them repeated
``ALTER SESSION``/``USE``), counted the PG source with COUNT(*) four times, and
ran past 45s. Only QA can confirm on a real Snowflake account.
"""

from __future__ import annotations

import os
import time
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

pytest.importorskip("fakesnow")

from injected_latency import slow_postgres, slow_snowflake  # noqa: E402

ROWS = 30


def _seed_rows() -> list[tuple]:
    rows = []
    for i in range(1, ROWS + 1):
        name = {3: None, 5: "", 7: "  padded  ", 9: "Zoë 東京 ✓"}.get(i, f"name {i}")
        doc = None if i == 11 else {"k": i, "tags": ["a", str(i)], "nested": {"ok": i % 2 == 0}}
        rows.append((i, name, Decimal(f"{i}.25"), doc))
    return rows


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn, cfg


def _mysql():
    pymysql = pytest.importorskip("pymysql")
    cfg = {
        "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
        "username": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
        "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", "dataflow"),
    }
    try:
        conn = pymysql.connect(
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            user=cfg["username"], password=cfg["password"], autocommit=True,
            connect_timeout=3,
        )
    except pymysql.MySQLError as exc:
        pytest.skip(f"MySQL not reachable: {exc}")
    return conn, cfg


@pytest.fixture
def route(request, tmp_path, monkeypatch):
    import json

    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    engine = request.param
    sfx = uuid.uuid4().hex[:8]
    table = f"d187_src_{sfx}"
    if engine == "postgresql":
        db, cfg = _pg()
        with db.cursor() as cur:
            cur.execute(
                f"CREATE TABLE public.{table} (id INTEGER PRIMARY KEY, name TEXT, "
                "amount NUMERIC(12,2), doc JSONB, updated_at TIMESTAMP DEFAULT '2026-01-02 03:04:05')"
            )
            for row in _seed_rows():
                cur.execute(
                    f"INSERT INTO public.{table} (id, name, amount, doc) VALUES (%s, %s, %s, %s)",
                    (*row[:3], json.dumps(row[3]) if row[3] is not None else None),
                )
        src = create_connector({"name": f"pg-{sfx}", "type": "postgresql", "schema": "public", **cfg})
    else:
        db, cfg = _mysql()
        with db.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, "
                "amount DECIMAL(12,2), doc JSON, updated_at DATETIME DEFAULT '2026-01-02 03:04:05')"
            )
            for row in _seed_rows():
                cur.execute(
                    f"INSERT INTO {table} (id, name, amount, doc) VALUES (%s, %s, %s, %s)",
                    (*row[:3], json.dumps(row[3]) if row[3] is not None else None),
                )
        src = create_connector({"name": f"my-{sfx}", "type": "mysql", **cfg})
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    dst = create_connector({
        "name": f"sf-{sfx}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    slow_postgres(monkeypatch, connect=0.3)
    # QA's real-account range: login 2-4s, metadata 0.5-2s, resume 5-10s.
    calls = slow_snowflake(monkeypatch, login=3.0, query=1.5, warehouse_resume=8.0)
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "calls": calls, "engine": engine}
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        leaked = []
        with db.cursor() as cur:
            if engine == "postgresql":
                cur.execute(
                    "SELECT pid, left(query, 160) FROM pg_stat_activity WHERE state = "
                    "'idle in transaction' AND pid <> pg_backend_pid() AND query ILIKE %s",
                    (f"%{table}%",),
                )
                leaked = cur.fetchall()
                cur.execute("SET lock_timeout = '15s'")
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        db.close()
        # A session left open in a transaction on the source would block the
        # operator's DDL (it blocked this DROP for 25 minutes once).
        assert not leaked, leaked


def _call(tool: str, route: dict, dest: str, mode: str):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"], "source_table": route["table"],
        "dest_connector_id": route["dst"], "dest_table": dest, "sync_mode": mode,
    }
    if "upsert" in mode:
        args.update(primary_key="id", cursor_column="updated_at",
                    cursor_semantics="modification_timestamp")
    started = time.monotonic()
    result = get_pilot_tools().execute(tool, args)
    return result, time.monotonic() - started


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    job_id = spend_pilot_ack(ack_id, "d187-test")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _snowflake_rows(dest: str) -> dict[int, tuple]:
    import json

    import connectors.snowflake_conn as sc

    conn = sc._open_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM DATAFLOW.PUBLIC."{dest}"')
        cols = [d[0].lower() for d in cur.description]
        out = {}
        for raw in cur.fetchall():
            row = dict(zip(cols, raw))
            doc = row["doc"]
            parsed = json.loads(doc) if isinstance(doc, str) else doc
            amount = row["amount"]
            out[int(row["id"])] = (
                row["name"], Decimal(str(amount)) if amount is not None else None, parsed,
            )
        return out
    finally:
        conn.close()


def _assert_cells(dest: str) -> None:
    got = _snowflake_rows(dest)
    want = {r[0]: (r[1], r[2], r[3]) for r in _seed_rows()}
    diffs = [(k, got.get(k), v) for k, v in want.items() if got.get(k) != v]
    assert sorted(got) == sorted(want) and not diffs, diffs[:5]


@pytest.mark.parametrize("route", ["postgresql", "mysql"], indirect=True)
@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_load_into_a_new_snowflake_table_fits_the_deadline_three_times(route, mode):
    from services.connector_executor import snapshot
    from src.ai.copilot.tool_deadline import deadline_seconds

    assert deadline_seconds("plan_transfer") == 45.0, "the deadline is not raised"
    dest = f"D187_DST_{uuid.uuid4().hex[:6].upper()}"
    for attempt in range(3 if route["engine"] == "postgresql" else 1):
        plan, plan_s = _call("plan_transfer", route, dest, mode)
        assert plan.success, plan.error
        assert plan_s < 45, (attempt, plan_s)
        staged, start_s = _call("start_transfer", route, dest, mode)
        assert staged.success, staged.error
        assert start_s < 45, (attempt, start_s)
        # The job is not deadline-bound: it runs without the injected latency.
        route["calls"].active.clear()
        job = _job(staged.output["ack_id"])
        route["calls"].active.set()
        assert job.get("status") == "completed", job
        _assert_cells(dest)
    pool = snapshot()
    # No call was abandoned, so nothing can leak.
    assert pool["abandoned_total"] == 0 and pool["abandoned_lifetime"] == 0, pool
    assert pool["in_flight"] == 0, pool


@pytest.mark.parametrize("route", ["postgresql"], indirect=True)
def test_one_snowflake_login_and_no_repeated_session_statements(route):
    calls = route["calls"]
    dest = f"D187_DST_{uuid.uuid4().hex[:6].upper()}"
    plan, plan_s = _call("plan_transfer", route, dest, "full_refresh_overwrite")
    assert plan.success, plan.error
    assert calls.counts["sf_login"] == 1, calls.counts
    # Base: 16 statements per plan, ten of them repeated session settings.
    # (fakesnow rejects ALTER SESSION, so each consumer re-sends that one; a
    # real Snowflake session answers its repeat locally.)
    assert calls.counts["sf_statement"] <= 12, calls.counts
    pre = plan.output["preflight"]
    assert pre["source_row_count"] == ROWS
    assert pre["source_row_count_basis"] == "exact"
