"""D189 / D159 — an SFTP CSV into an EXISTING Postgres table, through the MCP route.

QA (production ac612203): a 29-row SFTP CSV loaded into a NEW Postgres table
29/29, but every attempt into an existing table timed out after the 45s tool
deadline with an error that blamed the SFTP source.

What the existing-table path does that the new-table path does not is map the
file columns onto the table's columns. Scoring a pair asked the schematic index
for a canonical name, and the first lookup in a process built that index: 2.5
million name variants enumerated in Python (about 15s of CPU here, longer on a
small production container, and a fresh build on every deploy because the
pickle cache lives in the data dir). The planner was still labelled "sampling
source table", so the deadline named the SFTP host.

The plan also mapped with no sample rows: the planner's fallback sampler
resolved the saved SFTP connector by id and dropped its pinned host key (saved
under ``extra``), so the read failed and Map scored ``name`` VARCHAR -> TEXT at
0.78, under the G4 floor (D159).

These tests drive ``plan_transfer`` and ``start_transfer`` the way QA does, on
the local SFTP test server and the local PostgreSQL, with injected latency on
every SFTP login and every PostgreSQL connect, the deadline left at its
default, and a second run into the table the first run wrote.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

#: Injected cost of one SFTP login / one PostgreSQL connect (a remote server).
SFTP_LOGIN_LATENCY = 1.0
PG_CONNECT_LATENCY = 0.2

#: 29 rows, with NULL (unquoted empty), an empty string (quoted ""),
#: surrounding whitespace and non-ASCII text.
_SPECIAL = {
    3: ("", "3.25", "2024-01-04"),
    5: ('"  padded  "', "5.25", "2024-01-06"),
    7: ('"Zoë 東京 ✓"', "7.25", "2024-01-08"),
    9: ("name 9", "", "2024-01-10"),
    11: ("name 11", "11.25", ""),
    13: ('""', "13.25", "2024-01-14"),
}


def _csv() -> str:
    lines = ["id,name,amount,joined"]
    for i in range(1, 30):
        name, amount, joined = _SPECIAL.get(i, (f"name {i}", f"{i}.25", f"2024-01-{(i % 28) + 1:02d}"))
        lines.append(f"{i},{name},{amount},{joined}")
    return "\n".join(lines) + "\n"


def _pg_cfg() -> dict:
    from conftest import pg_test_cfg

    return pg_test_cfg()


def _pg_connect():
    psycopg2 = pytest.importorskip("psycopg2")
    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


def _cold_mapping_caches() -> None:
    """A freshly deployed process: nothing about column names is warm."""
    import services.schematic_index as si

    for name in dir(si):
        cache_clear = getattr(getattr(si, name), "cache_clear", None)
        if callable(cache_clear):
            cache_clear()


@pytest.fixture
def route(tmp_path, monkeypatch):
    import paramiko
    import psycopg2

    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    pg = _pg_connect()
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "people.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    cfg = server.endpoint_config("/people.csv")
    sfx = uuid.uuid4().hex[:8]
    existing, fresh = f"d189_existing_{sfx}", f"d189_new_{sfx}"
    with pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE public.{existing} (id INTEGER PRIMARY KEY, name TEXT, "
            "amount NUMERIC(12,2), joined DATE)"
        )
    src = create_connector({
        "name": f"sftp-{sfx}", "type": "sftp", "role": "source", "host": cfg["host"],
        "port": cfg["port"], "username": cfg["username"], "password": cfg["password"],
        "database": cfg["database"], "host_key": cfg["host_key"],
    })
    pg_cfg = _pg_cfg()
    dst = create_connector({
        "name": f"pg-{sfx}", "type": "postgresql", "role": "destination",
        "host": pg_cfg["host"], "port": pg_cfg["port"], "database": pg_cfg["database"],
        "username": pg_cfg["username"], "password": pg_cfg["password"], "schema": "public",
    })

    logins: list[float] = []
    real_auth = paramiko.Transport.auth_password

    def slow_auth(self, *args, **kwargs):
        logins.append(time.monotonic())
        time.sleep(SFTP_LOGIN_LATENCY)
        return real_auth(self, *args, **kwargs)

    real_connect = psycopg2.connect

    def slow_connect(*args, **kwargs):
        time.sleep(PG_CONNECT_LATENCY)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(paramiko.Transport, "auth_password", slow_auth)
    monkeypatch.setattr(psycopg2, "connect", slow_connect)
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    _cold_mapping_caches()
    try:
        yield {
            "src": src.id, "dst": dst.id, "existing": existing, "new": fresh,
            "pg": pg, "logins": logins,
        }
    finally:
        runner.stop()
        delete_connector(src.id)
        delete_connector(dst.id)
        with pg.cursor() as cur:
            for table in (existing, fresh):
                cur.execute(f"DROP TABLE IF EXISTS public.{table}")
                cur.execute(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
        pg.close()


def _call(tool: str, route: dict, table: str, mode: str) -> tuple[object, float]:
    from src.ai.copilot.tools import get_pilot_tools

    started = time.monotonic()
    result = get_pilot_tools().execute(tool, {
        "source_connector_id": route["src"], "source_table": "people.csv",
        "dest_connector_id": route["dst"], "dest_table": table,
        "sync_mode": mode, "primary_key": "id",
    })
    return result, time.monotonic() - started


def _load(route: dict, table: str, mode: str) -> dict:
    """plan_transfer, then start_transfer, then the job, each timed."""
    from conftest import spend_pilot_ack
    from src.ai.copilot.tool_deadline import deadline_seconds
    from src.ai.copilot.tools import get_pilot_tools

    limit = deadline_seconds("plan_transfer")
    assert limit == 45.0, "the deadline is the production default, not raised"
    plan, plan_s = _call("plan_transfer", route, table, mode)
    assert plan.success, plan.error
    preflight = (plan.output or {}).get("preflight") or {}
    held = [
        (g.get("id"), g.get("summary") or g.get("message"))
        for g in preflight.get("gates", [])
        if isinstance(g, dict) and g.get("status") in {"block", "fail"}
    ]
    assert preflight.get("passed") is True, held
    assert plan_s < limit, f"plan_transfer took {plan_s:.1f}s"

    staged, start_s = _call("start_transfer", route, table, mode)
    assert staged.success, staged.error
    assert start_s < limit, f"start_transfer took {start_s:.1f}s"
    job_id = spend_pilot_ack(staged.output["ack_id"], "d189-test")["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    assert job.get("status") == "completed", job
    return {"plan_s": plan_s, "start_s": start_s, "job": job}


def _rows(route: dict, table: str) -> dict[int, tuple]:
    with route["pg"].cursor() as cur:
        cur.execute(f"SELECT id, name, amount, joined FROM public.{table} ORDER BY id")
        return {r[0]: r[1:] for r in cur.fetchall()}


def _assert_values(rows: dict[int, tuple]) -> None:
    from decimal import Decimal

    assert sorted(rows) == list(range(1, 30))
    assert rows[1][0] == "name 1" and rows[1][1] == Decimal("1.25")
    assert rows[3][0] is None  # unquoted empty -> NULL
    assert rows[13][0] == ""  # quoted "" -> empty string
    assert rows[5][0] == "  padded  "
    assert rows[7][0] == "Zoë 東京 ✓"
    assert rows[9][1] is None
    assert rows[11][2] is None


def test_sftp_csv_into_an_existing_postgres_table_fits_the_deadline_twice(route):
    first = _load(route, route["existing"], "full_refresh_append")
    _assert_values(_rows(route, route["existing"]))
    # A file rerun into the same table (D189: reruns / schedules / replays).
    second = _load(route, route["existing"], "full_refresh_overwrite")
    _assert_values(_rows(route, route["existing"]))
    for run in (first, second):
        assert run["plan_s"] < 45 and run["start_s"] < 45, run


def test_existing_table_plan_costs_what_a_new_table_plan_costs(route):
    """The existing-table path adds Map onto real columns. That must not add a
    cold-start build that the new-table path never pays."""
    fresh = _load(route, route["new"], "full_refresh_append")
    _assert_values(_rows(route, route["new"]))
    _cold_mapping_caches()
    # Second run into the table the first run created: now an existing table.
    again = _load(route, route["new"], "full_refresh_overwrite")
    _assert_values(_rows(route, route["new"]))
    _cold_mapping_caches()
    existing = _load(route, route["existing"], "full_refresh_append")
    extra = existing["plan_s"] - fresh["plan_s"]
    assert extra < 5.0, (fresh["plan_s"], again["plan_s"], existing["plan_s"])
    assert again["plan_s"] - fresh["plan_s"] < 5.0, (fresh["plan_s"], again["plan_s"])


def test_a_slow_map_step_is_not_blamed_on_the_sftp_source(route, monkeypatch):
    """Attribution: the deadline names the step that was really running."""
    import services.mapping_pipeline as mapping_pipeline

    real = mapping_pipeline.run_mapping_pipeline

    def slow_map(*args, **kwargs):
        time.sleep(30)
        return real(*args, **kwargs)

    monkeypatch.setattr(mapping_pipeline, "run_mapping_pipeline", slow_map)
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "12")
    result, elapsed = _call("plan_transfer", route, route["existing"], "full_refresh_append")
    assert not result.success
    error = str(result.error)
    assert elapsed < 20, elapsed
    assert "source unreachable/slow" not in error, error
    assert "sampling source table" not in error, error
    assert "mapping" in error.lower(), error
