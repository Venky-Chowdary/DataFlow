"""D77 — a load into a Snowflake table that already has ARRAY columns failed
with ``001003 (42000) syntax error ... unexpected '<'``.

Snowflake describes a column DataFlow created as ``ARRAY(VARCHAR)`` as
``ARRAY(VARCHAR(16777216))``; the catalog reader turns that into the logical
``ARRAY<VARCHAR(16777216)>``. The writer re-used the live type for the next
``CREATE TABLE`` / ``PARSE_JSON(..)::<type>`` and passed the logical spelling
through as if it were a physical stamp, so the SQL carried ``ARRAY<...>``,
which Snowflake cannot parse. fakesnow has no structured types (it stores
VARIANT and accepts ``<``), so its DESC answer and its parser are made to act
like Snowflake's here.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import psycopg2
import pytest

from connectors import snowflake_writer as sf
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests import test_d120_snowflake_strict_e2e as _strict
from tests.test_d120_snowflake_strict_e2e import PG_CFG
from tests.test_d77_snowflake_structured_array import _snowflake_rows_columns

snowflake_semantics = _strict.snowflake_semantics
engine_env = _strict.engine_env

#: What Snowflake's ``DESC TABLE`` reports for the columns DataFlow creates.
_SNOWFLAKE_DESC = {"NUMS": "ARRAY(NUMBER(38,0))", "TAGS": "ARRAY(VARCHAR(16777216))"}
_GENERIC = re.compile(r"\b(?:ARRAY|MAP|OBJECT|STRUCT)\s*<|\w\[\]", re.I)


@pytest.mark.parametrize(
    ("live", "ddl"),
    [
        ("ARRAY<VARCHAR(16777216)>", "ARRAY(VARCHAR)"),
        ("ARRAY<DECIMAL(38,0)>", "ARRAY(NUMBER(38,0))"),
        ("MAP<TEXT,INTEGER>", "MAP(VARCHAR, NUMBER(38,0))"),
        ("STRUCT<a:INTEGER>", "OBJECT(a NUMBER(38,0))"),
        ("TEXT[]", "ARRAY(VARCHAR)"),
        ("ARRAY(VARCHAR)", "ARRAY(VARCHAR)"),
    ],
)
def test_live_nested_type_is_spelled_the_snowflake_way(live, ddl):
    assert sf.sf_type(live) == ddl
    assert sf._sf_resolve_create_types(["C"], {}, ["VARIANT"], live_dest_types={"C": live}) == [ddl]


@pytest.fixture()
def snowflake_nested_types(monkeypatch, snowflake_semantics):
    import snowflake.connector
    from fakesnow.cursor import FakeSnowflakeCursor

    import connectors.snowflake_conn as sc

    real_desc = sc._snowflake_desc_column_rows

    def desc(cur: Any, schema: str, table: str):
        rows, missing = real_desc(cur, schema, table)
        return [
            (r[0], _SNOWFLAKE_DESC.get(str(r[0]).upper(), r[1]), *r[2:]) for r in rows
        ], missing

    strict_execute = FakeSnowflakeCursor.execute
    sent: list[str] = []

    def execute(self, command: str, *args: Any, **kwargs: Any) -> Any:
        text = " ".join(str(command or "").split())
        sent.append(text)
        if _GENERIC.search(text):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error: syntax error line 1 unexpected '<'.",
                errno=1003,
                sqlstate="42000",
            )
        return strict_execute(self, command, *args, **kwargs)

    monkeypatch.setattr(sc, "_snowflake_desc_column_rows", desc)
    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    return sent


@pytest.fixture()
def pg_arrays():
    try:
        conn = psycopg2.connect(
            host=PG_CFG["host"], port=PG_CFG["port"], dbname=PG_CFG["database"],
            user=PG_CFG["username"], password=PG_CFG["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = "d77r_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, nums INT[], tags TEXT[])")
        cur.execute(f"INSERT INTO {table} VALUES (1, '{{1,2,3}}', '{{a,b}}'), (2, NULL, NULL)")
    try:
        yield conn, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _run(source: str, dst: str, sync_mode: str, engine_env) -> Any:
    job_id = "d77r" + uuid.uuid4().hex[:16]
    engine_env.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public", table=source, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test",
            schema="public", table=dst,
        ),
        sync_mode=sync_mode,
        stream_contracts=[{
            "name": source, "sync_mode": sync_mode, "primary_key": "id", "selected": True,
        }],
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "upsert"])
def test_second_load_into_a_snowflake_table_with_array_columns(
    engine_env, snowflake_nested_types, pg_arrays, sync_mode
):
    db, source = pg_arrays
    dst = "d77r_dst_" + uuid.uuid4().hex[:8]
    first = _run(source, dst, sync_mode, engine_env)
    assert first.success is True, first.error
    with db.cursor() as cur:
        cur.execute(f"UPDATE {source} SET tags = '{{c,d,e}}', nums = '{{4}}' WHERE id = 2")
    second = _run(source, dst, sync_mode, engine_env)
    assert second.success is True, second.error
    assert (second.reconciliation or {}).get("passed") is True, second.reconciliation
    assert not [s for s in snowflake_nested_types if _GENERIC.search(s)]
    rows = _snowflake_rows_columns(dst)
    assert rows[1] == ("[1,2,3]", '["a","b"]')
    assert rows[2] == ("[4]", '["c","d","e"]')
