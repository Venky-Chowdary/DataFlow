"""Shared harness for the round-11 PostgreSQL-writer end-to-end tests.

QA's handover run (build 9c75ada4) hit these through MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job and through the Studio's REST upload / run.
This module drives the same routes against the real local engines: PostgreSQL
(``PG*``), MySQL 8 on :3306 (``MYSQL_*``) and MariaDB on :3307
(``DATAFLOW_TEST_MARIADB_*``). Each engine is skipped when it is not reachable.
Not a test module itself.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

import pytest

from tests import r4_route_harness as h

PG = h.PG


def mysql_family(kind: str) -> dict[str, Any]:
    """Connector settings for ``mysql`` (:3306) or ``mariadb`` (:3307)."""
    if kind == "mysql":
        port, pw = 3306, os.environ.get("MYSQL_PASSWORD", "")
        user = os.environ.get("MYSQL_USER", "dataflow")
        db = os.environ.get("MYSQL_DATABASE", "dataflow")
        host = "127.0.0.1"
    else:
        port = 3307
        pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "") or os.environ.get("MYSQL_PASSWORD", "")
        user = os.environ.get("DATAFLOW_TEST_MARIADB_USER", "") or os.environ.get("MYSQL_USER", "root")
        db = os.environ.get("DATAFLOW_TEST_MARIADB_DB", "") or os.environ.get("MYSQL_DATABASE", "dataflow")
        host = os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1")
    if not pw or not h.port_up(port, host):
        pytest.skip(f"{kind} not configured on :{port}")
    return dict(type=kind, host=host, port=port, database=db, username=user, password=pw)


def mysql_execute(kind: str, *statements: str, params: Any = None) -> list[tuple]:
    pymysql = pytest.importorskip("pymysql")
    cfg = mysql_family(kind)
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["username"], password=cfg["password"],
        database=cfg["database"], charset="utf8mb4", autocommit=True,
    )
    try:
        cur = conn.cursor()
        out: list[tuple] = []
        for stmt in statements:
            cur.execute(stmt, params) if params is not None else cur.execute(stmt)
            out = [tuple(r) for r in cur.fetchall()] if cur.description else []
        return out
    finally:
        conn.close()


def pg(sql: str, params: Any = None, *, database: str | None = None, user: str | None = None,
       password: str | None = None) -> list[tuple]:
    psycopg2 = pytest.importorskip("psycopg2")
    h.pg_conn().close()  # skips when PG is absent
    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], user=user or PG["username"],
        password=password if password is not None else PG["password"],
        dbname=database or PG["database"], connect_timeout=5,
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def pg_is_superuser() -> bool:
    rows = pg("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
    return bool(rows and rows[0][0])


@contextmanager
def scratch_database(*, owner: str | None = None) -> Iterator[str]:
    """A fresh PostgreSQL database with no extensions; dropped afterwards."""
    if not pg_is_superuser():
        pytest.skip("needs a PostgreSQL role that can CREATE DATABASE")
    name = f"r11_novec_{uuid.uuid4().hex[:8]}"
    pg(f'CREATE DATABASE "{name}"' + (f' OWNER "{owner}"' if owner else ""))
    try:
        yield name
    finally:
        pg(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (name,),
        )
        pg(f'DROP DATABASE IF EXISTS "{name}"')


@contextmanager
def plain_role() -> Iterator[tuple[str, str]]:
    """A LOGIN role with no superuser and no CREATEDB; dropped afterwards."""
    if not pg_is_superuser():
        pytest.skip("needs a PostgreSQL role that can CREATE ROLE")
    name = f"r11_plain_{uuid.uuid4().hex[:8]}"
    secret = uuid.uuid4().hex
    pg(f"CREATE ROLE \"{name}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{secret}'")
    try:
        yield name, secret
    finally:
        pg(f'DROP ROLE IF EXISTS "{name}"')


def connector(role: str, cfg: dict[str, Any], **extra: Any) -> str:
    from services.connector_store import create_connector

    payload = {
        "name": f"r11-{cfg['type']}-{role}-{uuid.uuid4().hex[:6]}",
        "role": role, "workspace_id": "", "ssl": False, **cfg, **extra,
    }
    return str(create_connector(payload).id)


def pg_cfg(**overrides: Any) -> dict[str, Any]:
    cfg = dict(h.dest_config("postgresql"))
    cfg.update(overrides)
    return cfg


def stage(client, arguments: dict) -> tuple[bool, Any]:
    return h.mcp_call(client, "start_transfer", {"validation_mode": "strict", **arguments})


def confirm_job(client, staged: Any, *, expect: str | None = "completed") -> dict:
    """Confirm an MCP-staged transfer; return the finished job (any status)."""
    assert isinstance(staged, dict) and staged.get("ack_id"), staged
    resp = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r11"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    job = h.wait_job(str(body.get("job_id") or ""), timeout_s=300)
    if expect:
        assert str(job.get("status")) == expect, {
            "status": job.get("status"),
            "error": job.get("error") or job.get("message"),
            "phase": job.get("phase"),
        }
    return job


def run(client, arguments: dict) -> dict:
    ok, staged = stage(client, arguments)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return confirm_job(client, staged)


def columns(table: str, *, database: str | None = None) -> dict[str, tuple[str, Any]]:
    """name -> (format_type, column default) of a PostgreSQL table."""
    rows = pg(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid) "
        "FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
        "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        (f"public.{table}",),
        database=database,
    )
    return {str(n): (str(t), d) for n, t, d in rows}


def table_exists(table: str, *, database: str | None = None, schema: str = "public") -> bool:
    return bool(pg(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        (schema, table), database=database,
    ))


def job_text(job: dict) -> str:
    return " ".join(str(job.get(k) or "") for k in ("error", "message", "status"))
