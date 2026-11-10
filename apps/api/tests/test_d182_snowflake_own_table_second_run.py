"""D182 — a second run into a Snowflake table DataFlow itself created was blocked.

Run 1 creates a PostgreSQL ``bytea`` column as Snowflake ``BINARY`` (no
length: Snowflake's 8 MB maximum). ``DESC TABLE`` describes that column as
``BINARY(8388608)``, and the catalog reader kept the number as a declared
width, so run 2's plan saw ``bytea -> BINARY(8388608)`` as a narrowing:
G3/G9 "Lossy coercion", plus a ``narrow_type`` schema-drift pause on an
incremental run. Run 2 into the table run 1 made could never start.

PostgreSQL -> Snowflake (fakesnow, with Snowflake's case-sensitive quoted
identifiers) through MCP ``plan_transfer`` / ``start_transfer`` and the job,
twice, with a JSON column and a bytea column.
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

from tests import test_d120_snowflake_strict_e2e as _strict  # noqa: E402

snowflake_semantics = _strict.snowflake_semantics


@pytest.mark.parametrize(
    ("described", "logical"),
    [
        ("BINARY(8388608)", "BINARY"),
        ("BINARY", "BINARY"),
        ("BINARY(16)", "BINARY(16)"),
    ],
)
def test_snowflake_binary_maximum_reads_as_the_unbounded_column(described, logical):
    from services.schema_introspect import _sf_to_logical
    from services.type_system import is_lossy_coercion

    assert _sf_to_logical(described) == logical
    lossy = is_lossy_coercion(
        "BYTEA", _sf_to_logical(described), dest_db="snowflake", dest_table_exists=True
    )
    assert lossy is (logical != "BINARY")


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
    table = f"d182_src_{sfx}"
    with db.cursor() as cur:
        cur.execute(
            f"CREATE TABLE public.{table} (id INT PRIMARY KEY, doc JSONB, bin BYTEA, "
            "updated_at TIMESTAMP DEFAULT '2026-01-02 03:04:05')"
        )
        for i in range(1, 6):
            cur.execute(
                f"INSERT INTO public.{table} (id, doc, bin) VALUES (%s, %s, %s)",
                (i, json.dumps({"n": i, "nest": {"x": 3.0}}), psycopg2.Binary(bytes([0, 255, i]))),
            )
    src = create_connector({"name": f"pg-{sfx}", "type": "postgresql", "schema": "public", **cfg})
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    dst = create_connector({
        "name": f"sf-{sfx}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    connector_executor.reset_for_tests()
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "db": db}
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        with db.cursor() as cur:
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
    return get_pilot_tools().execute(tool, args)


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    job_id = spend_pilot_ack(ack_id, "d182-test")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _snowflake_cells(dest: str) -> dict[int, tuple]:
    import connectors.snowflake_conn as sc

    conn = sc._open_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT "ID", "DOC", "BIN" FROM DATAFLOW.PUBLIC."{dest}"')
        return {int(r[0]): (json.loads(r[1]), bytes(r[2])) for r in cur.fetchall()}
    finally:
        conn.close()


@pytest.mark.parametrize("mode", ["incremental_upsert", "full_refresh_overwrite"])
def test_second_run_into_the_table_run_one_created(route, mode):
    dest = f"D182_DST_{uuid.uuid4().hex[:6].upper()}"
    for run in (1, 2):
        if run == 2:
            with route["db"].cursor() as cur:
                cur.execute(
                    f"UPDATE public.{route['table']} SET bin = '\\x0102'::bytea, "
                    "updated_at = '2026-03-03 00:00:00' WHERE id = 2"
                )
        plan = _call("plan_transfer", route, dest, mode)
        assert plan.success, (run, plan.error)
        staged = _call("start_transfer", route, dest, mode)
        assert staged.success, (run, staged.error)
        job = _job(staged.output["ack_id"])
        assert job.get("status") == "completed", (run, job)
    cells = _snowflake_cells(dest)
    assert cells[2] == ({"n": 2, "nest": {"x": 3}}, b"\x01\x02")
    assert cells[5] == ({"n": 5, "nest": {"x": 3}}, bytes([0, 255, 5]))
