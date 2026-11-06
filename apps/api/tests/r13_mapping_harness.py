"""Shared harness for the round-13 mapping/fidelity engine tests.

Drives ``UniversalTransferEngine().execute_tracked`` with preflight ON (the
route QA runs) against the real local engines. The job store is the in-process
``_FakeMongo``; a test skips when its engine is not configured.
"""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


def _port_up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def pg_cfg() -> dict[str, Any]:
    pw = os.environ.get("PGPASSWORD", "")
    if not pw or not _port_up(5432):
        pytest.skip("PostgreSQL not configured (PG* env)")
    return dict(
        host="127.0.0.1", port=5432, database=os.environ.get("PGDATABASE", "dataflow"),
        username=os.environ.get("PGUSER", "dataflow"), password=pw, schema="public",
    )


def mysql_cfg() -> dict[str, Any]:
    pw = os.environ.get("MYSQL_PASSWORD", "")
    if not pw or not _port_up(3306):
        pytest.skip("MySQL not configured (MYSQL_PASSWORD, :3306)")
    return dict(
        host="127.0.0.1", port=3306, database=os.environ.get("MYSQL_DATABASE", "dataflow"),
        username=os.environ.get("MYSQL_USER", "dataflow"), password=pw,
    )


def mssql_cfg() -> dict[str, Any]:
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
    if not pw or not _port_up(1433):
        pytest.skip("SQL Server not configured (DATAFLOW_TEST_MSSQL_PASSWORD, :1433)")
    return dict(
        host="127.0.0.1", port=1433, database="dataflow",
        username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"), password=pw,
        extra={"trust_server_certificate": True},
    )


CFG = {"postgresql": pg_cfg, "mysql": mysql_cfg, "sqlserver": mssql_cfg}


def connect(kind: str):
    cfg = CFG[kind]()
    if kind == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"],
        )
    elif kind == "mysql":
        import pymysql

        conn = pymysql.connect(
            host=cfg["host"], port=cfg["port"], user=cfg["username"],
            password=cfg["password"], database=cfg["database"], charset="utf8mb4",
        )
    else:
        import pyodbc

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;"
            f"DATABASE=dataflow;UID={cfg['username']};PWD={cfg['password']};"
            "TrustServerCertificate=yes",
            timeout=10,
        )
    conn.autocommit = True
    return conn


def run_sql(kind: str, *statements: str, params: list | None = None) -> list[tuple]:
    conn = connect(kind)
    try:
        cur = conn.cursor()
        rows: list[tuple] = []
        for sql in statements:
            if params is not None and sql.lstrip().upper().startswith("INSERT"):
                cur.executemany(sql, params)
            else:
                cur.execute(sql)
            if cur.description:
                rows = [tuple(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def quote(kind: str, name: str) -> str:
    return {"mysql": f"`{name}`", "sqlserver": f"[{name}]"}.get(kind, f'"{name}"')


def drop(kind: str, table: str) -> None:
    try:
        run_sql(kind, f"DROP TABLE IF EXISTS {quote(kind, table)}")  # nosec B608
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


def token() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    import src.transfer.engine as engine_mod
    import services.sync_cursor as sc
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def endpoint(kind: str, table: str, **extra):
    from src.transfer.models import EndpointConfig

    cfg = dict(CFG[kind]())
    cfg.update(extra)
    return EndpointConfig(kind="database", format=kind, table=table, **cfg)


def sqlite_endpoint(path, table: str = "t"):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def execute(fake, source, destination, **request):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    request.setdefault("validation_mode", "strict")
    job_id = "r13m" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(source=source, destination=destination, **request), job_id
    )
