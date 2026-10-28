"""D47 — SCD2 and mirror into Snowflake failed every run.

The SCD2/mirror engines build their own text SQL and quoted the logical
column names raw (``"id"``). On Snowflake every table they touch stores the
folded name: the staging spool is written by the Snowflake writer (``ID``)
and the SCD2/mirror target is created through SQLAlchemy, whose Snowflake
dialect emits lower-case names unquoted (``ID`` again). Quoted identifiers
are case-sensitive there, so the first staging read failed with
``000904 invalid identifier '"id"'``.

PostgreSQL -> Snowflake (fakesnow, with Snowflake's case-sensitive quoted
identifiers restored) with preflight on, two runs each so the SCD2
expire/insert and the mirror delete paths run as well as the first load.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d120_snowflake_strict_e2e import PG_CFG

# Real-Snowflake rules over fakesnow (see test_d120_snowflake_strict_e2e).
snowflake_semantics = _strict.snowflake_semantics
engine_env = _strict.engine_env


def _pg() -> Any:
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture()
def pg_source():
    conn = _pg()
    table = "d47_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, amt NUMERIC(12,2))")
        cur.execute(f"INSERT INTO {table} VALUES (1, 'a', 1.50), (2, 'b', 2.00), (3, 'c', 3.25)")
    try:
        yield conn, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _run(engine_env, src: str, dst: str, mode: str, **contract: Any):
    job_id = "d47" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public", table=src, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode=mode,
        stream_contracts=[{
            "name": src, "sync_mode": mode, "primary_key": "id", "selected": True,
            **contract,
        }],
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is True, result.error
    return result


def _dst(table: str, columns: str, where: str = "") -> list[tuple]:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT {columns} FROM "PUBLIC"."{table.upper()}" {where} ORDER BY "ID"'
            )
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def test_d47_scd2_into_snowflake(engine_env, pg_source):
    conn, src = pg_source
    dst = "d47_scd2_" + uuid.uuid4().hex[:8]
    first = _run(engine_env, src, dst, "scd2")
    assert first.records_transferred == 3
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {src} SET name = 'b2' WHERE id = 2")
    _run(engine_env, src, dst, "scd2")
    history = _dst(dst, '"ID", "NAME"')
    assert history == [(1, "a"), (2, "b"), (2, "b2"), (3, "c")]


def test_d47_mirror_into_snowflake(engine_env, pg_source):
    conn, src = pg_source
    dst = "d47_mirror_" + uuid.uuid4().hex[:8]
    first = _run(engine_env, src, dst, "mirror", delete_mode="hard")
    assert first.records_transferred == 3
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {src} WHERE id = 3")
        cur.execute(f"UPDATE {src} SET name = 'a2' WHERE id = 1")
    _run(engine_env, src, dst, "mirror", delete_mode="hard")
    assert _dst(dst, '"ID", "NAME"') == [(1, "a2"), (2, "b")]
