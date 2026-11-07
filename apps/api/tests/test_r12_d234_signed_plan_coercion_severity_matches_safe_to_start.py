"""D234: a signed plan must not report severity="block" coercions beside safe_to_start.

QA on prod: S3 csv -> Mongo, Qdrant -> Weaviate, pgvector -> Kafka ... signed
with CAST_AND_CONTINUE: preflight passed and safe_to_start=true while
coercion_issues still said lossy=true / severity="block". The plan graded the
coercions before it signed the contracts and never re-graded them.

Live: PostgreSQL (PGPASSWORD) and MongoDB on 127.0.0.1; skipped otherwise.
"""
from __future__ import annotations

import os
import socket
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
pymongo = pytest.importorskip("pymongo")

from tests.test_d147_d150_mongo_mcp_plan_route import store  # noqa: E402,F401

_ACCEPT = {"approved_by": "qa@example.com", "reason": "ms is enough", "execution_policy": "CAST_AND_CONTINUE"}


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture
def pg_table():
    if not (_up(5432) and _up(27017) and os.environ.get("PGPASSWORD")):
        pytest.skip("PostgreSQL / MongoDB not configured")
    conn = psycopg2.connect(host="127.0.0.1", port=5432, user="dataflow",
                            password=os.environ["PGPASSWORD"], dbname="dataflow")
    conn.autocommit = True
    name = "d234_" + uuid.uuid4().hex[:8]
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, ts TIMESTAMPTZ(6))")  # nosec B608
    cur.executemany(f"INSERT INTO {name} VALUES (%s, %s)",  # nosec B608
                    [(i, f"2024-03-10 02:30:00.0123{i}+00") for i in range(1, 6)])
    try:
        yield name
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {name}")  # nosec B608
        conn.close()
        pymongo.MongoClient("mongodb://127.0.0.1:27017")["dataflow_test"].drop_collection(name)


def _plan(store, table, **extra):  # noqa: F811
    from src.ai.copilot import transfer_tools

    src = store.create_connector({
        "name": "d234-pg-" + uuid.uuid4().hex[:6], "type": "postgresql", "role": "source",
        "host": "127.0.0.1", "port": 5432, "database": "dataflow", "username": "dataflow",
        "password": os.environ["PGPASSWORD"], "workspace_id": "", "ssl": False})
    dst = store.create_connector({
        "name": "d234-mg-" + uuid.uuid4().hex[:6], "type": "mongodb", "role": "destination",
        "host": "127.0.0.1", "port": 27017, "database": "dataflow_test", "workspace_id": "", "ssl": False})
    result = transfer_tools.plan_transfer(
        source_connector_id=str(src.id), source_table=table,
        dest_connector_id=str(dst.id), dest_table=table,
        sync_mode="full_refresh_overwrite", validation_mode="strict", **extra)
    assert result.success, result.error
    return result.output


def test_unsigned_lossy_instant_blocks_and_says_so(store, pg_table):  # noqa: F811
    plan = _plan(store, pg_table)
    ts = [i for i in plan["coercion_issues"] if i["source"] == "ts"]
    assert ts and ts[0]["severity"] == "block" and ts[0]["lossy"] is True
    assert plan["safe_to_start"] is False


def test_signed_plan_downgrades_the_cleared_coercion_it_lets_start(store, pg_table):  # noqa: F811
    plan = _plan(store, pg_table, risk_acceptance=dict(_ACCEPT))
    assert plan["safe_to_start"] is True, (plan.get("preflight") or {}).get("blockers")
    ts = [i for i in plan["coercion_issues"] if i["source"] == "ts"]
    assert ts and ts[0]["lossy"] is True
    assert ts[0]["severity"] == "warn" and ts[0]["cleared_by_risk_contract"] is True
    assert not [i for i in plan["coercion_issues"] if i["severity"] == "block"]
