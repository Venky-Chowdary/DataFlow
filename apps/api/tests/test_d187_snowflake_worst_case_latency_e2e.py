"""D187: PostgreSQL -> Snowflake plans and stages well inside the 45s deadline
at a real account's WORST costs, on run 1 and on run 2 into the table
DataFlow created.

Real Snowflake is not available here: fakesnow is slowed to the top of QA's
measured range (``tests/injected_latency.py``): 4s per login, 2s per
statement, 8s for a suspended warehouse's first compute statement, 0.3s per
cloud-PG connect. Round 4's own measurement left run 2 of an upsert at 37.9s
of the 45s: every statement after the login was a serial round trip, and
several only restated what the login had already set (warehouse, database,
schema, statement timeout) or what an earlier step had already read.

The target is 25s worst case, a margin against a slower account than these
numbers. Only QA can confirm on a real Snowflake account.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

pytest.importorskip("fakesnow")

from injected_latency import slow_postgres, slow_snowflake  # noqa: E402
from test_d187_snowflake_destination_fits_the_budget_e2e import (  # noqa: E402
    _assert_cells,
    _pg,
    _seed_rows,
    _snowflake_rows,
)

TARGET_SECONDS = 25.0


@pytest.fixture
def pg_route(tmp_path, monkeypatch):
    from services import connector_executor, engine_pool
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:8]
    table = f"d187w_src_{sfx}"
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
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    dst = create_connector({
        "name": f"sf-{sfx}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    engine_pool.reset_for_tests()
    slow_postgres(monkeypatch, connect=0.3)
    calls = slow_snowflake(monkeypatch, login=4.0, query=2.0, warehouse_resume=8.0)
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "calls": calls, "db": db}
    finally:
        calls.active.clear()
        delete_connector(src.id)
        delete_connector(dst.id)
        engine_pool.reset_for_tests()
        with db.cursor() as cur:
            cur.execute("SET lock_timeout = '15s'")
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        db.close()


def _call(tool: str, route: dict, dest: str, mode: str):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"], "source_table": route["table"],
        "dest_connector_id": route["dst"], "dest_table": dest, "sync_mode": mode,
    }
    if "upsert" in mode:
        args.update(primary_key="id", cursor_column="updated_at",
                    cursor_semantics="modification_timestamp")
    route["calls"].active.set()
    before = dict(route["calls"].counts)
    started = time.monotonic()
    result = get_pilot_tools().execute(tool, args)
    seconds = time.monotonic() - started
    after = route["calls"].counts
    used = {k: after[k] - before.get(k, 0) for k in after if after[k] - before.get(k, 0)}
    print(f"D187 {tool} {mode}: {seconds:.1f}s {used}")
    return result, seconds


def _run_job(route: dict, ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    # The job is not deadline-bound: it runs without the injected costs.
    route["calls"].active.clear()
    job_id = spend_pilot_ack(ack_id, "d187-worst-case")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_run_1_and_run_2_fit_well_inside_the_deadline_at_worst_costs(pg_route, mode):
    from src.ai.copilot.tool_deadline import deadline_seconds

    assert deadline_seconds("start_transfer") == 45.0, "the deadline is not raised"
    dest = f"D187W_{uuid.uuid4().hex[:6].upper()}"
    for run in (1, 2):
        if run == 2:
            # Tamper the source, then load again into the table run 1 created.
            with pg_route["db"].cursor() as cur:
                cur.execute(
                    f"UPDATE public.{pg_route['table']} SET name = 'tampered ✓', "
                    "updated_at = '2026-02-03 04:05:06' WHERE id IN (2, 4)"
                )
        plan, plan_s = _call("plan_transfer", pg_route, dest, mode)
        assert plan.success, plan.error
        assert plan_s < TARGET_SECONDS, (run, "plan_transfer", round(plan_s, 1))
        staged, start_s = _call("start_transfer", pg_route, dest, mode)
        assert staged.success, staged.error
        assert start_s < TARGET_SECONDS, (run, "start_transfer", round(start_s, 1))
        job = _run_job(pg_route, staged.output["ack_id"])
        assert job.get("status") == "completed", job
        if run == 1:
            _assert_cells(dest)
        else:
            got = _snowflake_rows(dest)
            assert got[2][0] == "tampered ✓" and got[4][0] == "tampered ✓", (got[2], got[4])
            want = {r[0]: (r[1], r[2], r[3]) for r in _seed_rows() if r[0] not in (2, 4)}
            diffs = [(k, got.get(k), v) for k, v in want.items() if got.get(k) != v]
            assert len(got) == len(_seed_rows()) and not diffs, diffs[:5]
