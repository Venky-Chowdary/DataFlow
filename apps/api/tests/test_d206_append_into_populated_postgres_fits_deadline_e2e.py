"""D206 end to end: MySQL -> an EXISTING, populated PostgreSQL table, append,
through MCP start_transfer -> /copilot/confirm -> job, with realistic costs.

QA (production 557f556c, post-#357): appending MySQL -> PG into a table that
already holds rows timed out at the 45s tool deadline in the pre-run check,
3 of 3, with and without a signed contract; an overwrite on the same route
staged. Overwrite never reads the destination's rows in the plan; append does:
the pre-write collision probe compares the batch with the stored keys.

What it measured on 557f556c: the probe ran ``CAST(key AS TEXT) IN (...)``,
which no index can answer, so every append plan read the WHOLE destination
(a sequential scan whose cost grows with the rows already stored), under a
statement timeout equal to the call's entire remaining budget. A slow scan
or a lock queue on the destination therefore always spent the whole 45s.

The local table answers that scan in milliseconds, so the scan is made to
cost what it costs on a big production table (``slow_destination_scans``:
0.5 ms per stored row, i.e. 60s for 120k rows; an index lookup costs nothing
extra), plus a remote connect for both systems. Only QA can confirm the
production table's own size and timings.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from injected_latency import slow_destination_scans, slow_mysql  # noqa: E402

DEST_ROWS = 120_000
SRC_ROWS = 1_000
FIRST_NEW_ID = 5_000_000


def _seed(i: int) -> tuple:
    name = {0: None, 1: "", 2: "  padded  ", 3: "Zoë 東京 ✓", 4: " "}.get(i % 9, f"name {i}")
    doc = None if i % 11 == 0 else {"k": i, "tags": ["a", str(i)], "s": "ü"}
    return (FIRST_NEW_ID + i, name, Decimal(f"{i}.25"), doc)


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


def _insert_source(my, table: str, rows: list[tuple], updated_at: str) -> None:
    with my.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {table} (id, name, amount, doc, updated_at) VALUES (%s, %s, %s, %s, %s)",
            [(*r[:3], json.dumps(r[3]) if r[3] is not None else None, updated_at) for r in rows],
        )


@pytest.fixture
def route(monkeypatch):
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:8]
    src_table, dst_table = f"d206_src_{sfx}", f"d206_dst_{sfx}"
    my, my_cfg = _mysql()
    pg, pg_cfg = _pg()
    with my.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT, "
            "amount DECIMAL(12,2), doc JSON, updated_at DATETIME)"
        )
    _insert_source(my, src_table, [_seed(i) for i in range(SRC_ROWS)], "2026-01-02 03:04:05")
    with pg.cursor() as cur:
        # The operator's own table, already holding rows.
        cur.execute(
            f"CREATE TABLE public.{dst_table} (id BIGINT PRIMARY KEY, name TEXT, "
            "amount NUMERIC(12,2), doc JSONB, updated_at TIMESTAMP)"
        )
        cur.execute(
            f"INSERT INTO public.{dst_table} SELECT g, 'stored ' || g, g + 0.5, "
            "jsonb_build_object('k', g), '2025-06-01' FROM generate_series(1, %s) g",
            (DEST_ROWS,),
        )
        cur.execute(f"ANALYZE public.{dst_table}")
    src = create_connector({"name": f"my-{sfx}", "type": "mysql", **my_cfg})
    dst = create_connector({"name": f"pg-{sfx}", "type": "postgresql", "schema": "public", **pg_cfg})
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    # Pooled sessions from an earlier test were opened without the injected
    # costs; every session this test uses must carry them.
    from services import engine_pool

    engine_pool.reset_for_tests()
    scans = slow_destination_scans(monkeypatch, table=dst_table, seconds_per_row=0.0005, connect=0.3)
    remote = slow_mysql(monkeypatch, connect=0.3, statement=0.02)
    try:
        yield {
            "src": src.id, "dst": dst.id, "src_table": src_table, "dst_table": dst_table,
            "scans": scans, "remote": remote, "my": my, "pg": pg,
        }
    finally:
        scans.active.clear()
        remote.active.clear()
        engine_pool.reset_for_tests()
        delete_connector(src.id)
        delete_connector(dst.id)
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_table}")
        my.close()
        with pg.cursor() as cur:
            cur.execute("SET lock_timeout = '60s'")
            cur.execute(f"DROP TABLE IF EXISTS public.{dst_table}")
        pg.close()


def _args(route: dict, mode: str, contract: bool) -> dict:
    args = {
        "source_connector_id": route["src"], "source_table": route["src_table"],
        "dest_connector_id": route["dst"], "dest_table": route["dst_table"], "sync_mode": mode,
    }
    if mode == "incremental_append":
        args["cursor_column"] = "updated_at"
    if contract:
        args["risk_acceptance"] = {
            "execution_policy": "CAST_AND_CONTINUE", "approved_by": "qa", "reason": "D206 repro",
        }
    return args


def _start(route: dict, mode: str, contract: bool):
    from src.ai.copilot.tools import get_pilot_tools

    route["scans"].active.set()
    route["remote"].active.set()
    started = time.monotonic()
    result = get_pilot_tools().execute("start_transfer", _args(route, mode, contract))
    return result, time.monotonic() - started


def _job(route: dict, ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    # The job is not deadline-bound: it runs without the injected costs.
    route["scans"].active.clear()
    route["remote"].active.clear()
    job_id = spend_pilot_ack(ack_id, "d206-test")["job_id"]
    deadline = time.time() + 300
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _assert_cells(route: dict, ids: list[int]) -> None:
    want = {r[0]: r for r in (_seed(i - FIRST_NEW_ID) for i in ids)}
    with route["pg"].cursor() as cur:
        cur.execute(
            f"SELECT id, name, amount, doc FROM public.{route['dst_table']} WHERE id >= %s",
            (FIRST_NEW_ID,),
        )
        got = {int(r[0]): (int(r[0]), r[1], r[2], r[3]) for r in cur.fetchall()}
        cur.execute(f"SELECT count(*) FROM public.{route['dst_table']} WHERE id < %s", (FIRST_NEW_ID,))
        kept = cur.fetchone()[0]
    assert kept == DEST_ROWS, "the rows already stored must stay"
    diffs = [(k, got.get(k), v) for k, v in want.items() if got.get(k) != v]
    assert sorted(got) == sorted(want) and not diffs, diffs[:5]


@pytest.mark.parametrize(
    "mode,contract",
    [
        ("full_refresh_append", False),
        ("full_refresh_append", True),
        ("incremental_append", False),
    ],
)
def test_append_into_a_populated_table_fits_the_deadline_and_loads(route, mode, contract):
    from src.ai.copilot.tool_deadline import deadline_seconds

    assert deadline_seconds("start_transfer") == 45.0, "the deadline is not raised"
    staged, seconds = _start(route, mode, contract)
    print(f"D206 start_transfer run 1 {mode} contract={contract}: {seconds:.1f}s")
    assert staged.success, (round(seconds, 1), staged.error)
    assert seconds < 45, seconds
    # The batch was compared with the stored keys through the key's index,
    # never by reading the whole destination.
    assert route["scans"].counts["dest_seq_scan"] == 0, route["scans"].counts
    # ...and the comparison really ran: no collision is proven, not assumed.
    assert "probed against stored" in json.dumps(staged.output, default=str), staged.output
    job = _job(route, staged.output["ack_id"])
    assert job.get("status") == "completed", job
    _assert_cells(route, [FIRST_NEW_ID + i for i in range(SRC_ROWS)])

    # Run 2 into the same table.
    if mode == "incremental_append":
        late = [_seed(i) for i in range(SRC_ROWS, SRC_ROWS + 5)]
        _insert_source(route["my"], route["src_table"], late, "2026-03-04 05:06:07")
        again, seconds = _start(route, mode, contract)
        print(f"D206 start_transfer run 2 {mode}: {seconds:.1f}s")
        assert again.success, (round(seconds, 1), again.error)
        assert seconds < 45, seconds
        job = _job(route, again.output["ack_id"])
        assert job.get("status") == "completed", job
        _assert_cells(route, [FIRST_NEW_ID + i for i in range(SRC_ROWS + 5)])
    else:
        # The same rows again: the plan proves the collision, inside the budget.
        again, seconds = _start(route, mode, contract)
        print(f"D206 start_transfer run 2 {mode} contract={contract}: {seconds:.1f}s")
        assert seconds < 45, seconds
        assert not again.success
        assert "already" in str(again.error) and "id" in str(again.error), again.error
    assert route["scans"].counts["dest_seq_scan"] == 0, route["scans"].counts

