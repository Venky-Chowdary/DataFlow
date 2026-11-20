"""D72 / D73 / D77: PostgreSQL -> Snowflake through QA's route (MCP plan/start/job).

The earlier fixes for these stories were proven through ``execute_tracked``
or on the writer's SQL. QA met them through ``plan_transfer`` /
``start_transfer``, where the Map confidence floor (G4), the fidelity gates
and the read-back verification all run. Each scenario here is QA's.

* D72: a UUID primary key, incremental_upsert, new table. QA: G4 refused
  ``u->u (0.70)``. Snowflake has no UUID type; the canonical 36-character text
  is lossless (https://docs.snowflake.com/en/sql-reference/functions/uuid_string).
* D73: JSON numbers ``3.0`` vs ``3``: a VARIANT stores a number by value
  (https://docs.snowflake.com/en/sql-reference/data-types-semistructured),
  so the read-back must compare by value and key set, keep a correct load, and
  still fail, naming the row, on a real difference.
* D77: ``text[]`` / ``int[]`` in table mode and in query mode must both land as
  arrays with exact values (empty array and NULL elements included); query
  mode was refused as a "fidelity collapse".

Snowflake is fakesnow with Snowflake's case-sensitive quoted identifiers
restored (``test_d120_snowflake_strict_e2e.snowflake_semantics``).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
pytest.importorskip("fakesnow")

from tests import test_d120_snowflake_strict_e2e as _strict  # noqa: E402

snowflake_semantics = _strict.snowflake_semantics


@pytest.fixture
def route(tmp_path, monkeypatch, snowflake_semantics):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    try:
        db = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    db.autocommit = True
    sfx = uuid.uuid4().hex[:8]
    src = create_connector({"name": f"pg-{sfx}", "type": "postgresql", "schema": "public", **cfg})
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    dst = create_connector({
        "name": f"sf-{sfx}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    connector_executor.reset_for_tests()
    made: list[str] = []
    try:
        yield {"src": src.id, "dst": dst.id, "db": db, "sfx": sfx, "made": made}
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        with db.cursor() as cur:
            for table in made:
                cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        db.close()


def _source(route: dict, ddl: str, rows: list[tuple], prefix: str) -> str:
    table = f"{prefix}_{route['sfx']}"
    route["made"].append(table)
    with route["db"].cursor() as cur:
        cur.execute(f"CREATE TABLE public.{table} ({ddl})")
        if rows:
            marks = ", ".join(["%s"] * len(rows[0]))
            cur.executemany(f"INSERT INTO public.{table} VALUES ({marks})", rows)
    return table


def _tool(tool: str, args: dict) -> Any:
    from src.ai.copilot.tools import get_pilot_tools

    return get_pilot_tools().execute(tool, {k: v for k, v in args.items() if v not in (None, "")})


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack

    job_id = spend_pilot_ack(ack_id, "sf-mcp-test")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(_tool("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _run(args: dict) -> tuple[Any, dict]:
    plan = _tool("plan_transfer", args)
    assert plan.success, plan.error
    staged = _tool("start_transfer", args)
    assert staged.success, staged.error
    return plan, _job(staged.output["ack_id"])


def _sf(sql: str) -> list[tuple]:
    import connectors.snowflake_conn as sc

    conn = sc._open_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _mapping(plan: Any, source: str) -> dict:
    for m in plan.output.get("engine_mappings") or []:
        if m.get("source") == source:
            return m
    raise AssertionError(f"no mapping for {source}")


# ---------------------------------------------------------------- D72

# Random-looking and digit-run UUIDs alike (nil-prefixed / sequential keys look
# like the third): a UUID column's content is never a phone (D72 follow-up).
_U = [
    "6f1c2a9e-3b4d-4e8f-9a0b-c1d2e3f4a5b6",
    "00000000-0000-0000-1111-222233334444",
    "00000000-0000-0000-2222-444466668888",
]


def test_d72_uuid_primary_key_upserts_into_snowflake_at_full_confidence(route):
    src = _source(
        route,
        "u UUID PRIMARY KEY, parent UUID, n INT, updated_at TIMESTAMP NOT NULL",
        [(_U[0], None, 1, "2026-01-01 00:00:00"), (_U[1], _U[0], 2, "2026-01-01 00:00:00"),
         (_U[2], _U[1], 3, "2026-01-01 00:00:00")],
        "d72s",
    )
    dest = f"HQ2200_JS_SF_{route['sfx'].upper()}"
    args = {
        "source_connector_id": route["src"], "source_table": src,
        "dest_connector_id": route["dst"], "dest_table": dest,
        "sync_mode": "incremental_upsert", "primary_key": "u",
        "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
    }
    plan, job = _run(args)
    assert job.get("status") == "completed", job
    u = _mapping(plan, "u")
    assert u["target"].upper() == "U" and u.get("transform") in (None, "", "none"), u
    assert str(u.get("dest_native_type") or u.get("target_type")).upper() == "VARCHAR(36)", u
    assert not u.get("requires_review") and float(u["confidence"]) >= 0.9, u
    gates = {g["id"]: g["status"] for g in plan.output["preflight"]["gates"]}
    assert gates["g4_mapping_confidence"] == "pass", gates

    with route["db"].cursor() as cur:
        cur.execute(f"UPDATE public.{src} SET n = 20, updated_at = '2026-02-01' WHERE u = %s", (_U[1],))
    _plan2, job2 = _run(args)
    assert job2.get("status") == "completed", job2
    got = sorted(_sf(f'SELECT "U", "PARENT", "N" FROM DATAFLOW.PUBLIC."{dest}"'))
    assert got == sorted([(_U[0], None, 1), (_U[1], _U[0], 20), (_U[2], _U[1], 3)])


# ---------------------------------------------------------------- D73

_DOCS = {
    1: '{"x": 3.0, "b": 1, "a": [1.0, [2.50, {"z": 0.0, "y": -0.0}]], "s": "3.0"}',
    2: '{"x": 3, "nested": {"q": [], "r": {}}}',
    3: '[{"k": 1e2}, 10.000, null, true]',
}


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_d73_json_with_trailing_zero_numbers_verifies_and_is_kept(route, mode):
    src = _source(
        route,
        "id INT PRIMARY KEY, payload JSONB, updated_at TIMESTAMP NOT NULL",
        [(k, v, "2026-01-01") for k, v in _DOCS.items()],
        "d73s",
    )
    dest = f"D73_DST_{route['sfx'].upper()}"
    args = {
        "source_connector_id": route["src"], "source_table": src,
        "dest_connector_id": route["dst"], "dest_table": dest, "sync_mode": mode,
    }
    if mode == "incremental_upsert":
        args.update(primary_key="id", cursor_column="updated_at",
                    cursor_semantics="modification_timestamp")
    _plan, job = _run(args)
    assert job.get("status") == "completed", job
    got = {int(r[0]): json.loads(r[1]) for r in _sf(f'SELECT "ID", "PAYLOAD" FROM DATAFLOW.PUBLIC."{dest}"')}
    assert got == {k: json.loads(v) for k, v in _DOCS.items()}  # 3.0 == 3 by value


def test_d73_a_real_json_difference_fails_and_names_the_row(route, monkeypatch):
    src = _source(
        route,
        "id INT PRIMARY KEY, payload JSONB",
        [(k, v) for k, v in _DOCS.items()],
        "d73m",
    )
    dest = f"D73_BAD_{route['sfx'].upper()}"
    # A deliberately wrong write of row 2 (R9: the check must still catch it).
    import connectors.snowflake_writer as sw

    real = sw._load_rows_into_table

    def corrupt(cur, table, cols, types, rows, *a, **kw):
        idx = [c.upper() for c in cols].index("PAYLOAD")
        rows = [
            tuple(('{"x": 4, "nested": {"q": [], "r": {}}}' if (i == idx and str(r[0]) == "2") else v)
                  for i, v in enumerate(r))
            for r in rows
        ]
        return real(cur, table, cols, types, rows, *a, **kw)

    monkeypatch.setattr(sw, "_load_rows_into_table", corrupt)
    staged = _tool("start_transfer", {
        "source_connector_id": route["src"], "source_table": src,
        "dest_connector_id": route["dst"], "dest_table": dest,
        "sync_mode": "full_refresh_overwrite",
    })
    assert staged.success, staged.error
    job = _job(staged.output["ack_id"])
    assert job.get("status") == "failed", job
    assert "row id='2' column 'payload'" in str(job.get("error") or ""), job.get("error")


# ---------------------------------------------------------------- D77

_ARRAYS = [
    (1, ["a", "b,c", 'q"uote'], [1, 2, 3]),
    (2, [], []),
    (3, None, None),
    (4, ["x", None], [None, 7]),
]


@pytest.mark.parametrize("read_mode", ["table", "query"])
def test_d77_pg_arrays_land_in_snowflake_in_table_and_query_mode(route, read_mode):
    src = _source(route, "id INT PRIMARY KEY, tags TEXT[], nums INT[]", _ARRAYS, "d77s")
    dest = f"D77_{read_mode.upper()}_{route['sfx'].upper()}"
    args = {
        "source_connector_id": route["src"], "source_table": src,
        "dest_connector_id": route["dst"], "dest_table": dest,
        "sync_mode": "full_refresh_overwrite",
    }
    if read_mode == "query":
        args["source_query"] = f"SELECT id, tags, nums FROM public.{src}"
    _plan, job = _run(args)
    assert job.get("status") == "completed", job
    rows = _sf(f'SELECT "ID", "TAGS", "NUMS" FROM DATAFLOW.PUBLIC."{dest}" ORDER BY "ID"')

    def _arr(v: Any) -> Any:
        return None if v is None else (json.loads(v) if isinstance(v, str) else list(v))

    got = {int(r[0]): (_arr(r[1]), _arr(r[2])) for r in rows}
    assert got == {i: (t, n) for i, t, n in _ARRAYS}
