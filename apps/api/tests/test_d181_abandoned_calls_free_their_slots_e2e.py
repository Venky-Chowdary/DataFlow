"""D181 end to end: stuck connector calls end by themselves and never lock a
workspace out.

QA (production ac612203): ``/health`` ``connector_pool.abandoned_total``
climbed from 6 to 14 during a pass; the "limit 6 in flight" cap then refused
every connector call of the workspace until the stuck calls finally died (the
6:12 PM lockout cleared on its own).

What actually hung, measured here: not the connect (drivers time out), but
statements on a connected source. PostgreSQL reader sessions carry
``statement_timeout = 0`` and the query route allowed 120s, so after the tool
deadline the abandoned worker kept scanning (a view over a slow function stood
in for a big table; the worker was still busy 180s after the answer). Each one
held its workspace slot the whole time.

Now every statement sent for a tool call carries the call's remaining budget,
so the server cancels it at the deadline and the worker returns; and an
abandoned call gives its workspace slot back at once.

Real PostgreSQL, a black-holed host (TCP accepts, never answers), and twenty
parallel ``test_connector`` / ``run_query`` calls through the real chokepoint.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

#: A short query deadline keeps the test quick. It is lowered, never raised.
DEADLINE = 6


@pytest.fixture
def black_hole():
    held: list[socket.socket] = []
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(64)
    stop = threading.Event()

    def accept() -> None:
        server.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
                held.append(conn)
            except OSError:
                continue

    t = threading.Thread(target=accept, daemon=True)
    t.start()
    yield server.getsockname()[1]
    stop.set()
    for c in held:
        c.close()
    server.close()


@pytest.fixture
def world(monkeypatch, black_hole):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    try:
        pg = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    pg.autocommit = True
    view = f"d181_slow_{uuid.uuid4().hex[:6]}"
    with pg.cursor() as cur:
        # Any read of it costs 600s of server work: a stand-in for a huge scan.
        cur.execute(
            f"CREATE VIEW public.{view} AS SELECT g AS id FROM generate_series(1, 10) g "
            "CROSS JOIN (SELECT pg_sleep(600)) s"
        )
    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", str(DEADLINE))
    monkeypatch.delenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", raising=False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_POOL_WORKERS", raising=False)
    connector_executor.reset_for_tests()
    made = []
    ids: dict[str, dict[str, str]] = {}
    for ws in ("ws-a", "ws-b", "ws-c", "ws-d"):
        good = create_connector({"name": f"good-{ws}-{view}", "type": "postgresql",
                                 "schema": "public", "workspace_id": ws, **cfg})
        hole = create_connector({"name": f"hole-{ws}-{view}", "type": "postgresql",
                                 "host": "127.0.0.1", "port": black_hole, "database": "x",
                                 "username": "u", "password": "p", "workspace_id": ws})
        made += [good.id, hole.id]
        ids[ws] = {"good": good.id, "hole": hole.id}
    try:
        yield {"ids": ids, "view": view}
    finally:
        for cid in made:
            delete_connector(cid)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE query ILIKE '%{view}%' AND pid <> pg_backend_pid()"
            )
            cur.execute(f"DROP VIEW IF EXISTS public.{view}")
        pg.close()
        connector_executor.reset_for_tests()


def _call(ws: str, tool: str, args: dict):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only(ws), ws, "tester"):
        return DataPilotTools().execute(tool, args)


def _wave(world, *, stuck: bool) -> list:
    """Twenty calls at once, five per workspace; ``stuck`` adds the black hole
    and two never-ending queries in ws-a."""
    calls = []
    for ws, ids in world["ids"].items():
        for i in range(5):
            if stuck and ws == "ws-a" and i == 0:
                calls.append((ws, "test_connector", {"connector_id": ids["hole"]}))
            elif stuck and ws == "ws-a" and i in (1, 2):
                calls.append((ws, "run_query", {
                    "connector_id": ids["good"], "query": f"SELECT count(*) FROM {world['view']}",
                }))
            elif i % 2:
                calls.append((ws, "test_connector", {"connector_id": ids["good"]}))
            else:
                calls.append((ws, "run_query", {"connector_id": ids["good"], "query": "SELECT 1 AS one"}))
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as pool:
        futures = [pool.submit(_call, *c) for c in calls]
        return [(c, f.result(timeout=120)) for c, f in zip(calls, futures)]


def _healthy(result) -> bool:
    out = result.output if isinstance(result.output, dict) else {}
    return bool(result.success) and out.get("ok", True) is not False


def test_twenty_parallel_calls_with_a_black_hole_leave_no_leak(world):
    from services.connector_executor import snapshot

    first = _wave(world, stuck=True)
    refused = [c for c, r in first if "in flight" in str(r.error or "")]
    assert not refused, refused
    stuck_calls = [(c, r) for c, r in first if c[0] == "ws-a" and c[2].get("query", "").startswith("SELECT count")]
    for _c, r in stuck_calls:
        assert not r.success
    hole = [r for c, r in first if c[2].get("connector_id") == world["ids"]["ws-a"]["hole"]]
    assert hole and not _healthy(hole[0])
    # The black hole is named, not "a system it reads from".
    assert "127.0.0.1" in str(hole[0].error or hole[0].output), hole[0]
    healthy = [r for c, r in first if r not in [x for _, x in stuck_calls] and r is not hole[0]]
    assert all(_healthy(r) for r in healthy), [r.error for r in healthy if not _healthy(r)]

    # Abandoned workers end on their own: the server cancels their statements
    # at the call's deadline. The gauge returns to baseline within seconds.
    end = time.monotonic() + 15
    while time.monotonic() < end and (snapshot()["abandoned_total"] or snapshot()["in_flight"]):
        time.sleep(0.25)
    pool = snapshot()
    assert pool["abandoned_total"] == 0 and pool["in_flight"] == 0, pool

    # And nothing is refused afterwards: every slot came back.
    second = _wave(world, stuck=False)
    assert all(_healthy(r) for _, r in second), [r.error for _, r in second if not _healthy(r)]


def test_an_abandoned_call_releases_its_workspace_slot_at_the_deadline(world):
    """Six never-ending queries in one workspace (its whole share): the
    seventh call runs as soon as they are abandoned, not when they die."""
    from services.connector_executor import snapshot

    stuck = {"connector_id": world["ids"]["ws-b"]["good"],
             "query": f"SELECT count(*) FROM {world['view']}"}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_call, "ws-b", "run_query", stuck) for _ in range(6)]
        results = [f.result(timeout=60) for f in futures]
    assert all(not r.success for r in results)
    after = _call("ws-b", "run_query", {"connector_id": world["ids"]["ws-b"]["good"], "query": "SELECT 1 AS one"})
    assert after.success, after.error
    end = time.monotonic() + 15
    while time.monotonic() < end and snapshot()["abandoned_total"]:
        time.sleep(0.25)
    assert snapshot()["abandoned_total"] == 0, snapshot()


def test_pooled_sqlalchemy_sessions_carry_the_budget_only_inside_a_tool():
    """Pooled engines (the duplicate probe, the query route) get the call's
    budget as statement_timeout at checkout and lose it at checkin, so a
    transfer job that reuses the session later is not bounded by a tool."""
    sa = pytest.importorskip("sqlalchemy")
    from conftest import pg_test_cfg
    from services import statement_budget
    from src.ai.copilot.tool_deadline import deadline_context_for_tests

    statement_budget.install()
    cfg = pg_test_cfg()
    engine = sa.create_engine(
        f"postgresql+psycopg2://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}",
        pool_pre_ping=True,
        pool_size=1,
    )
    try:
        with engine.connect() as c:
            assert c.exec_driver_sql("SHOW statement_timeout").scalar() == "0"
        with deadline_context_for_tests(7):
            with engine.connect() as c:
                bound = c.exec_driver_sql("SHOW statement_timeout").scalar()
                # The consumer's own first statement still opens the transaction.
                c.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert bound.endswith("ms") and 5000 <= int(bound[:-2]) <= 7000, bound
        with engine.connect() as c:
            assert c.exec_driver_sql("SHOW statement_timeout").scalar() == "0"
    finally:
        engine.dispose()
