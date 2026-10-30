"""Shared harness for the round-4 route tests (D55, D58, D119, D139).

Drives the route a user drives: the streamable MCP endpoint (``/api/v1/mcp``
``tools/call``) for plan/stage, ``/api/v1/copilot/confirm`` for Confirm, and the
job the confirm starts. Connector, ack, cursor and schedule stores are isolated
to ``tmp_path``; the job store is the in-memory one. Engines are the real local
ones (PG, MySQL 8, SQL Server 2022); a test skips when its engine is absent.
"""

from __future__ import annotations

import json
import os
import socket
import time
from typing import Any

import pytest

PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"),
    "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", ""),
}


def port_up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    if not PG["password"] or not port_up(PG["port"], PG["host"]):
        pytest.skip("PostgreSQL not configured (PG* env)")
    try:
        conn = psycopg2.connect(
            host=PG["host"], port=PG["port"], user=PG["username"],
            password=PG["password"], dbname=PG["database"], connect_timeout=3,
            # A live test never holds a lock longer than this.
            options="-c statement_timeout=60000 -c idle_in_transaction_session_timeout=60000",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


def mysql_conn():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("MYSQL_PASSWORD", "")
    if not pw or not port_up(3306):
        pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
    return pymysql.connect(
        host="127.0.0.1", port=3306, user=os.environ.get("MYSQL_USER", "dataflow"),
        password=pw, database=os.environ.get("MYSQL_DATABASE", "dataflow"),
        charset="utf8mb4", autocommit=True, connect_timeout=5,
        read_timeout=60, write_timeout=60,
    )


def mssql_conn():
    pyodbc = pytest.importorskip("pyodbc")
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
    if not pw or not port_up(1433):
        pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID={os.environ.get('DATAFLOW_TEST_MSSQL_USER', 'sa')};PWD={pw};"
        "TrustServerCertificate=yes",
        timeout=10, autocommit=True,
    )
    conn.timeout = 60  # per-statement: a hung test never holds a session open
    return conn


def dest_config(kind: str) -> dict[str, Any]:
    if kind == "postgresql":
        pg_conn().close()
        return dict(type="postgresql", host=PG["host"], port=PG["port"],
                    database=PG["database"], username=PG["username"],
                    password=PG["password"], schema="public")
    if kind == "mysql":
        mysql_conn().close()
        return dict(type="mysql", host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"),
                    password=os.environ["MYSQL_PASSWORD"])
    if kind == "sqlserver":
        mssql_conn().close()
        return dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
                    password=os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"],
                    extra={"trust_server_certificate": True})
    raise AssertionError(kind)


def query(kind: str, sql: str) -> list[tuple]:
    conn = {"postgresql": pg_conn, "mysql": mysql_conn, "sqlserver": mssql_conn}[kind]()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def execute(kind: str, sql: str) -> None:
    conn = {"postgresql": pg_conn, "mysql": mysql_conn, "sqlserver": mssql_conn}[kind]()
    try:
        conn.cursor().execute(sql)
    finally:
        conn.close()


def quote(kind: str, name: str) -> str:
    if kind == "mysql":
        return f"`{name}`"
    if kind == "sqlserver":
        return f"[{name}]"
    return f'"{name}"'


def drop_table(kind: str, table: str) -> None:
    try:
        execute(kind, f"DROP TABLE IF EXISTS {quote(kind, table)}")  # nosec B608
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


def isolate(tmp_path, monkeypatch) -> None:
    """Every store a route touches lives under tmp_path; the job store is memory."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.schedule_store as schedule_store
    import services.sync_cursor as sc
    import src.ai.copilot.ack_ledger as ack_mod
    from services.mongodb_service import MemoryMongoDBService

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)


def mcp_call(client, name: str, arguments: dict) -> tuple[bool, Any]:
    """One ``tools/call`` on the streamable MCP endpoint. Returns (ok, payload)."""
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": name, "arguments": arguments}},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "error" not in body, body
    result = body["result"]
    text = result["content"][0]["text"]
    if result["isError"]:
        return False, text
    try:
        return True, json.loads(text)
    except ValueError:
        return True, text


def blockers_of(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    plan = payload.get("plan") or payload
    preflight = plan.get("preflight") or payload.get("preflight") or {}
    return [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in (preflight.get("blockers") or [])
    ]


def wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return job


def confirm(client, ack_id: str, reason: str = "r4") -> dict:
    """``/api/v1/copilot/confirm`` then wait for the job it started."""
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": reason})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "phase": job.get("phase"),
    }
    return job


def stage_and_run(client, tool: str, arguments: dict) -> dict:
    """Stage through MCP, assert it cleared preflight, confirm, return the job."""
    ok, staged = mcp_call(client, tool, arguments)
    assert ok, {"error": staged}
    assert not blockers_of(staged), blockers_of(staged)
    assert staged.get("ack_id"), staged
    return confirm(client, staged["ack_id"])


def use_upload_dir(monkeypatch, uploads) -> None:
    """Point the Pilot's dataset analyst at ``uploads`` (files QA would upload)."""
    import src.ai.copilot.data_analyst as data_analyst
    from src.ai.training.universal_data_feeder import UniversalDataFeeder

    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(uploads))
    analyst = data_analyst.CopilotDataAnalyst()
    analyst.feeder = UniversalDataFeeder([str(uploads)])
    monkeypatch.setattr(data_analyst, "_analyst", analyst)
