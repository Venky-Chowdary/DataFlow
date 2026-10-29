"""Shared harness for the round-4 value-fidelity end-to-end tests.

Drives the route QA uses: ``start_transfer`` (plan + preflight) ->
``/api/v1/copilot/confirm`` -> job, against the real local engines (PG from
``PG*``, MySQL from ``MYSQL_*``, SQL Server from ``DATAFLOW_TEST_MSSQL_*``).
Each engine is skipped when it is not reachable. Not a test module itself.
"""

from __future__ import annotations

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


def up(port: int, host: str = "127.0.0.1") -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    if not PG["password"] or not up(PG["port"], PG["host"]):
        pytest.skip("PostgreSQL not configured (PG* env)")
    try:
        conn = psycopg2.connect(
            host=PG["host"], port=PG["port"], user=PG["username"],
            password=PG["password"], dbname=PG["database"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


def mysql_conn():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("MYSQL_PASSWORD", "")
    if not pw or not up(3306):
        pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
    return pymysql.connect(
        host="127.0.0.1", port=3306, user=os.environ.get("MYSQL_USER", "dataflow"),
        password=pw, database=os.environ.get("MYSQL_DATABASE", "dataflow"),
        charset="utf8mb4", autocommit=True,
    )


def mssql_conn():
    pyodbc = pytest.importorskip("pyodbc")
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
    if not pw or not up(1433):
        pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID={os.environ.get('DATAFLOW_TEST_MSSQL_USER', 'sa')};PWD={pw};"
        "TrustServerCertificate=yes",
        timeout=10,
    )
    conn.autocommit = True
    return conn


def connect(kind: str):
    return {"postgresql": pg_conn, "mysql": mysql_conn, "sqlserver": mssql_conn}[kind]()


def quote(kind: str, name: str) -> str:
    if kind == "mysql":
        return f"`{name}`"
    if kind == "sqlserver":
        return f"[{name}]"
    return f'"{name}"'


def execute(kind: str, sql: str, params: Any = None) -> None:
    conn = connect(kind)
    try:
        cur = conn.cursor()
        cur.execute(sql, params) if params is not None else cur.execute(sql)
    finally:
        conn.close()


def query(kind: str, sql: str) -> list[tuple]:
    conn = connect(kind)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def drop(kind: str, table: str) -> None:
    try:
        execute(kind, f"DROP TABLE IF EXISTS {quote(kind, table)}")  # nosec B608
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


def dest_config(kind: str) -> dict[str, Any]:
    connect(kind).close()  # skips when the engine is not reachable
    if kind == "postgresql":
        return dict(type="postgresql", **PG, schema="public")
    if kind == "mysql":
        return dict(type="mysql", host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"),
                    password=os.environ["MYSQL_PASSWORD"])
    if kind == "sqlserver":
        return dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
                    password=os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"],
                    extra={"trust_server_certificate": True}, schema="dbo")
    raise AssertionError(kind)


def make_client(tmp_path, monkeypatch):
    """The API app with an in-memory job store; yields a TestClient."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
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
    return TestClient(app)


def connectors(token: str, dest_kind: str, src_kind: str = "postgresql") -> tuple[str, str]:
    from services.connector_store import create_connector

    src = create_connector(
        {"name": f"r4-src-{src_kind}-{token}", "role": "source", "workspace_id": "",
         "ssl": False, **dest_config(src_kind)}
    )
    dst = create_connector(
        {"name": f"r4-{dest_kind}-{token}", "role": "destination", "workspace_id": "",
         "ssl": False, **dest_config(dest_kind)}
    )
    return src.id, dst.id


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


def stage(**kwargs: Any) -> tuple[Any, dict, list[str]]:
    """``start_transfer`` -> (tool result, plan, blocker strings)."""
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    plan = (staged.output or {}).get("plan") or staged.output or {}
    preflight = plan.get("preflight") or {}
    blockers = [
        f"{b.get('gate') or b.get('id')}: {b.get('message')}"
        for b in (preflight.get("blockers") or [])
    ]
    return staged, plan, blockers


def confirm_and_wait(client, staged) -> dict:
    resp = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "r4-value-fidelity"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
        "quarantined": job.get("rows_quarantined"),
    }
    return job


def run(client, **kwargs: Any) -> dict:
    """Plan, assert no blockers, confirm, and wait for a completed job."""
    staged, plan, blockers = stage(**kwargs)
    assert staged.success, {"error": staged.error, "blockers": blockers}
    assert not blockers, blockers
    return confirm_and_wait(client, staged)
