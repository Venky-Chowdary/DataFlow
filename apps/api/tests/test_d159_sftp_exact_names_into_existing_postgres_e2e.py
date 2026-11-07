"""D159 — SFTP CSV into an EXISTING Postgres table: exact-name columns map at the top band.

QA (production ac612203): an SFTP CSV loaded into a typed Postgres table that
already existed was blocked at g4 "ambiguous mapping name->name (gap 0.94),
city->city", even with a signed contract. Exact name plus a compatible type
must map with full confidence, whatever the sample evidence.

Root cause: two sample-evidence rules overrode the exact-name grade.
``_demote_untyped_varchar_confidence`` capped every VARCHAR column with no
sample rows at 0.78 and set ``requires_review`` (QA's plan had no samples:
the planner's sampler dropped the saved SFTP host key, fixed in 8e008b24), and
the wrong-role demotion in ``classify_mapping_confidence`` graded a ``city``
column whose values look numeric "weak or conflicted". Onto a text column of
the same name neither changes what lands: text holds any value byte for byte.

This drives QA's route (plan_transfer / start_transfer -> confirm -> job) on the
local SFTP test server and the local PostgreSQL, twice (a file rerun), once with
the sampler returning the file's rows and once with no sample evidence at all.
"""

from __future__ import annotations

import time
import uuid

import pytest

#: id, name, city — NULL (unquoted empty), "" (quoted), whitespace, unicode, and
#: city values that look numeric (postal-style codes) next to real names.
_ROWS = [
    ("1", "Alice", "Paris"),
    ("2", "Bob", "10001"),
    ("3", "", "94105"),
    ("4", '""', '""'),
    ("5", '"  padded  "', '"  Lyon  "'),
    ("6", '"Zoë 東京 ✓"', '"東京"'),
    ("7", "Eve", ""),
    ("8", "name 8", "60601"),
]
_EXPECTED = {
    1: ("Alice", "Paris"),
    2: ("Bob", "10001"),
    3: (None, "94105"),
    4: ("", ""),
    5: ("  padded  ", "  Lyon  "),
    6: ("Zoë 東京 ✓", "東京"),
    7: ("Eve", None),
    8: ("name 8", "60601"),
}


def _csv() -> str:
    return "id,name,city\n" + "\n".join(",".join(r) for r in _ROWS) + "\n"


def _pg_connect():
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


@pytest.fixture
def route(tmp_path, monkeypatch):
    pytest.importorskip("paramiko")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    pg, pg_cfg = _pg_connect()
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "people.csv").write_text(_csv(), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    cfg = server.endpoint_config("/people.csv")
    table = f"d159_exact_{uuid.uuid4().hex[:8]}"
    with pg.cursor() as cur:
        cur.execute(
            f"CREATE TABLE public.{table} (id INTEGER PRIMARY KEY, name TEXT, city TEXT)"
        )
    src = create_connector({
        "name": f"sftp-{table}", "type": "sftp", "role": "source", "host": cfg["host"],
        "port": cfg["port"], "username": cfg["username"], "password": cfg["password"],
        "database": cfg["database"], "host_key": cfg["host_key"],
    })
    dst = create_connector({
        "name": f"pg-{table}", "type": "postgresql", "role": "destination",
        "host": pg_cfg["host"], "port": pg_cfg["port"], "database": pg_cfg["database"],
        "username": pg_cfg["username"], "password": pg_cfg["password"], "schema": "public",
    })
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "pg": pg}
    finally:
        runner.stop()
        delete_connector(src.id)
        delete_connector(dst.id)
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
        pg.close()


def _call(tool: str, route: dict, mode: str):
    from src.ai.copilot.tools import get_pilot_tools

    return get_pilot_tools().execute(tool, {
        "source_connector_id": route["src"], "source_table": "people.csv",
        "dest_connector_id": route["dst"], "dest_table": route["table"],
        "sync_mode": mode, "primary_key": "id",
    })


def _load(route: dict, mode: str) -> None:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    plan = _call("plan_transfer", route, mode)
    assert plan.success, plan.error
    out = plan.output or {}
    by_source = {str(m.get("source")): m for m in out.get("mappings") or []}
    for col in ("id", "name", "city"):
        m = by_source[col]
        assert m.get("target") == col, m
        assert not m.get("requires_review"), m
        assert float(m.get("confidence") or 0) >= 0.99, m
    preflight = out.get("preflight") or {}
    held = [
        (g.get("id"), g.get("summary") or g.get("message"))
        for g in preflight.get("gates", [])
        if isinstance(g, dict) and g.get("status") in {"block", "fail"}
    ]
    assert preflight.get("passed") is True, held

    staged = _call("start_transfer", route, mode)  # no contract, no ack
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d159-test")["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    assert job.get("status") == "completed", job
    with route["pg"].cursor() as cur:
        cur.execute(f"SELECT id, name, city FROM public.{route['table']} ORDER BY id")
        got = {r[0]: r[1:] for r in cur.fetchall()}
    assert got == _EXPECTED


def test_exact_name_columns_map_at_full_confidence_with_samples(route):
    _load(route, "full_refresh_append")
    _load(route, "full_refresh_overwrite")  # a file rerun into the same table


def test_exact_name_columns_map_at_full_confidence_with_no_sample_evidence(route, monkeypatch):
    """QA's plan had no sample rows. That is no reason to doubt name -> name."""
    import src.ai.copilot.transfer_tools as tt

    monkeypatch.setattr(tt, "_sample_rows", lambda *a, **k: [])
    _load(route, "full_refresh_append")
