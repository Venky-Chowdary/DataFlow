"""D198 — run 2 into the table DataFlow created is never blocked on its JSON column.

QA (production ac612203): a second run into an Oracle table DataFlow itself had
created was blocked on the JSON column and only went through with two separate
overrides. The same family surfaced on SQL Server (D158), Mongo and Snowflake and
was patched one engine at a time.

Root cause: run 2 grades the source JSON against the destination column type that
*introspection* reads back, and asked "is this DataFlow's own JSON carrier?" by
matching that read-back spelling against a token list. Introspection does not
return the DDL spelling. Oracle's writer creates JSON as ``CLOB`` and Oracle's
introspection reads ``CLOB`` back as the logical ``TEXT``: not in the list, so
JSON -> TEXT graded as a document collapse. That one answer drove both gates
QA had to override: the type gate / risk contract (``is_lossy_coercion``) and the
G4 mapping-confidence gate (the mapper demoted the exact-name pair to "ambiguous").

This module drives QA's route end to end on every engine available locally:
``start_transfer`` (plan + preflight) -> ``/api/v1/copilot/confirm`` -> job, twice.
Run 1 creates the destination table; run 2 plans and executes into it with no
risk acceptance, no acknowledgement, no override of any kind, and every JSON
value (nested object, array, unicode, JSON null inside a document, empty object,
empty array, JSON strings "" and whitespace, SQL NULL) must read back equal.

There is no Oracle instance here: Oracle is covered in
``test_d198_json_carrier_recognition.py`` against the exact DDL and introspected
types DataFlow's Oracle writer/introspection produce. The real Oracle run is QA's.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from decimal import Decimal
from typing import Any

import pytest

psycopg2 = pytest.importorskip("psycopg2")

_PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"),
    "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", ""),
}

# id -> JSON document. Every shape QA named, plus the empty/blank cases.
_DOCS: dict[int, Any] = {
    1: {"a": {"b": [1, 2, {"c": "x"}]}, "n": None, "ok": True},
    2: [1, "two", None, {"k": []}],
    3: {"name": "Łukasz 山田 🎉", "é": "ü", "nested": {"emoji": "👩‍💻"}},
    4: None,  # SQL NULL
    5: {},
    6: [],
    7: {"empty": "", "blank": "   ", "tab": "\t"},
    8: ["", "   "],
    9: {"deep": {"deeper": {"deepest": [[], {}, [None]]}}},
}
_NOTES: dict[int, Any] = {1: "plain", 2: "", 3: "Łukasz 山田", 4: None, 5: "   "}

# Run 2 changes two documents and adds one row.
_RUN2_CHANGES: dict[int, Any] = {
    2: {"changed": ["x", {"y": "中文"}]},
    5: {"was": "empty", "now": {}},
    10: {"added": True, "list": [1.5, -2, 0]},
}


def _up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _pg_conn():
    if not _PG["password"] or not _up(_PG["port"], _PG["host"]):
        pytest.skip("PostgreSQL source not configured (PG* env)")
    try:
        conn = psycopg2.connect(
            host=_PG["host"], port=_PG["port"], user=_PG["username"],
            password=_PG["password"], dbname=_PG["database"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Destinations: connector config, read-back, cleanup.
# --------------------------------------------------------------------------- #


def _dest_config(kind: str) -> dict[str, Any]:
    if kind == "postgresql":
        return dict(type="postgresql", host=_PG["host"], port=_PG["port"],
                    database=_PG["database"], username=_PG["username"],
                    password=_PG["password"], schema="public")
    if kind == "mysql":
        pw = os.environ.get("MYSQL_PASSWORD", "")
        if not pw or not _up(3306):
            pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
        return dict(type="mysql", host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"), password=pw)
    if kind == "sqlserver":
        pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
        if not pw or not _up(1433):
            pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
        pytest.importorskip("pyodbc")
        return dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"), password=pw,
                    extra={"trust_server_certificate": True})
    if kind == "mongodb":
        if not _up(27017):
            pytest.skip("MongoDB not reachable on :27017")
        pytest.importorskip("pymongo")
        return dict(type="mongodb", host="127.0.0.1", port=27017, database="dataflow_test")
    if kind == "snowflake":
        pytest.importorskip("fakesnow")
        return dict(type="snowflake", host="localhost", port=443, database="dataflow",
                    username="test", password="test", schema="public")
    raise AssertionError(kind)


def _mssql():
    import pyodbc

    if not os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD"):
        pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD)")
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID={os.environ.get('DATAFLOW_TEST_MSSQL_USER', 'sa')};"
        f"PWD={os.environ['DATAFLOW_TEST_MSSQL_PASSWORD']};TrustServerCertificate=yes",
        timeout=10,
    )


def _mysql():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1", port=3306, user=os.environ.get("MYSQL_USER", "dataflow"),
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ.get("MYSQL_DATABASE", "dataflow"), charset="utf8mb4",
    )


def _snowflake():
    import connectors.snowflake_conn as sc

    return sc.get_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="public", warehouse="", connection_string="",
    )


def _decode(value: Any) -> Any:
    """A JSON cell as the destination hands it back, decoded to a document."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return value


def _plain(value: Any) -> Any:
    """Mongo/driver numbers (Decimal, Int64) to plain JSON numbers."""
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _read_back(kind: str, table: str) -> dict[int, tuple[Any, Any]]:
    """id -> (payload document, note) from the destination."""
    if kind == "mongodb":
        import pymongo

        client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
        try:
            docs = list(client["dataflow_test"][table].find({}))
        finally:
            client.close()
        out: dict[int, tuple[Any, Any]] = {}
        for d in docs:
            key = d.get("id", d.get("_id"))
            out[int(key)] = (_plain(_decode(d.get("payload"))), d.get("note"))
        return out
    sql = f"SELECT id, payload, note FROM {{t}}"  # nosec B608 - test-owned table
    if kind == "postgresql":
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute(sql.format(t=f'public."{table}"'))
        rows = cur.fetchall()
        conn.close()
    elif kind == "mysql":
        conn = _mysql()
        cur = conn.cursor()
        cur.execute(sql.format(t=f"`{table}`"))
        rows = cur.fetchall()
        conn.close()
    elif kind == "sqlserver":
        conn = _mssql()
        rows = conn.execute(sql.format(t=f"[{table}]")).fetchall()
        conn.close()
    else:
        conn = _snowflake()
        cur = conn.cursor()
        cur.execute(sql.format(t=table))
        rows = cur.fetchall()
        conn.close()
    return {int(r[0]): (_plain(_decode(r[1])), r[2]) for r in rows}


def _drop(kind: str, table: str) -> None:
    try:
        if kind == "postgresql":
            conn = _pg_conn()
            conn.cursor().execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
            conn.close()
        elif kind == "mysql":
            conn = _mysql()
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
            conn.commit()
            conn.close()
        elif kind == "sqlserver":
            conn = _mssql()
            conn.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
            conn.commit()
            conn.close()
        elif kind == "mongodb":
            import pymongo

            client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
            client["dataflow_test"][table].drop()
            client.close()
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


# --------------------------------------------------------------------------- #
# The route QA drives: start_transfer -> confirm -> job.
# --------------------------------------------------------------------------- #


def _wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return job


def _run(client, src_id: str, dst_id: str, table: str, dest_table: str) -> dict:
    from src.ai.copilot.transfer_tools import start_transfer

    # No risk_acceptance, no acknowledgement, no drift ack: run 2 must need none.
    staged = start_transfer(
        source_connector_id=src_id,
        source_table=table,
        dest_connector_id=dst_id,
        dest_table=dest_table,
        sync_mode="incremental_upsert",
        primary_key="id",
        cursor_column="updated_at",
        cursor_semantics="monotonic_sequence",
        validation_mode="strict",
    )
    plan = (staged.output or {}).get("plan") or staged.output or {}
    preflight = plan.get("preflight") or {}
    blockers = [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in (preflight.get("blockers") or [])
    ]
    assert staged.success, {"error": staged.error, "blockers": blockers}
    assert not blockers, blockers
    payload_maps = [
        m for m in (plan.get("mappings") or []) if str(m.get("source")) == "payload"
    ]
    for m in payload_maps:
        assert not m.get("requires_review"), m
        assert not m.get("requires_risk_contract"), m
    resp = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "d198"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return job


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver", "mongodb", "snowflake"])
def test_second_run_into_own_table_needs_no_override_and_json_round_trips(client, kind):
    from services.connector_store import create_connector

    dest_cfg = _dest_config(kind)
    pg = _pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"d198_{kind[:5]}_{token}"
    dest_table = table + "_out"
    cur = pg.cursor()
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload JSONB, "  # nosec B608
            "note TEXT, updated_at BIGINT NOT NULL)"
        )
        for i, doc in _DOCS.items():
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, %s)",  # nosec B608
                (i, None if doc is None else json.dumps(doc), _NOTES.get(i, f"n{i}"), i),
            )
        src = create_connector(
            {"name": f"d198-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **{k: v for k, v in _PG.items()},
             "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d198-{kind}-{token}", "role": "destination", "workspace_id": "",
             "ssl": False, **dest_cfg}
        )

        _run(client, src.id, dst.id, table, dest_table)  # run 1 creates the table
        first = _read_back(kind, dest_table)
        assert {i: v[0] for i, v in first.items()} == _DOCS

        for i, doc in _RUN2_CHANGES.items():
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, %s) "  # nosec B608
                "ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, "
                "updated_at = EXCLUDED.updated_at",
                (i, json.dumps(doc), _NOTES.get(i, f"n{i}"), 100 + i),
            )

        _run(client, src.id, dst.id, table, dest_table)  # run 2 into DataFlow's table
        second = _read_back(kind, dest_table)
        expected = {**_DOCS, **_RUN2_CHANGES}
        assert {i: v[0] for i, v in second.items()} == expected
        # Text beside the document keeps NULL / "" / whitespace / unicode apart.
        for i, note in _NOTES.items():
            got = second[i][1]
            if kind == "mongodb" and note is None:
                assert got is None
            else:
                assert got == note, (i, got, note)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)
