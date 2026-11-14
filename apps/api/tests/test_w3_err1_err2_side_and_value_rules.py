"""ERR-1 and ERR-2: a job failure names the side that failed and the real cause.

ERR-1: a source role without SELECT used to read "Destination rejected the
write for privileges/auth … confirm the role can INSERT/UPDATE".
ERR-2: NOT NULL and type errors used to read "Write referenced a column that
is not on the destination", because a ``column "`` needle ran before any value
rule. Both are raised for real against PostgreSQL 16 and MySQL 8.
"""

from __future__ import annotations

import secrets
import uuid

import pytest

from tests import r4_route_harness as h

TOK = uuid.uuid4().hex[:6]
SRC = f"w3err_e1_src_{TOK}"
DST = f"w3err_e1_dst_{TOK}"
VAL = f"w3err_e2_val_{TOK}"
ROLE = f"w3err_e1_np_{TOK}"
ROLE_PW = secrets.token_hex(8)


class _MP:
    def __init__(self, mp):
        self._mp = mp

    def setenv(self, k, v, *a, **kw):
        if k == "DATAFLOW_SEED_DEMO" and v == "":
            v = "false"
        return self._mp.setenv(k, v, *a, **kw)

    def __getattr__(self, n):
        return getattr(self._mp, n)


@pytest.fixture(scope="module")
def pg_objects():
    h.execute("postgresql", f'CREATE TABLE "{SRC}" (id INT PRIMARY KEY, name TEXT, val TEXT)')
    h.execute("postgresql", f"INSERT INTO \"{SRC}\" VALUES (1,'a','abc')")
    h.execute("postgresql", f'CREATE TABLE "{VAL}" (id INT PRIMARY KEY, name VARCHAR(3), v INT NOT NULL)')
    h.execute("postgresql", f"CREATE ROLE {ROLE} LOGIN PASSWORD '{ROLE_PW}'")
    h.execute("postgresql", f"GRANT CONNECT ON DATABASE {h.PG['database']} TO {ROLE}")
    yield
    for t in (SRC, DST, VAL):
        h.drop_table("postgresql", t)
    h.execute("postgresql", f'REVOKE ALL ON DATABASE {h.PG["database"]} FROM {ROLE}')
    h.execute("postgresql", f"DROP OWNED BY {ROLE}")
    h.execute("postgresql", f"DROP ROLE IF EXISTS {ROLE}")


@pytest.fixture(scope="module")
def my_objects():
    h.execute("mysql", f"CREATE TABLE `{VAL}` (id INT PRIMARY KEY, name VARCHAR(3), v INT NOT NULL)")
    yield
    h.drop_table("mysql", VAL)


def _raise(kind: str, sql: str, **over):
    from sqlalchemy import text

    from connectors.generic_sql import get_sqlalchemy_engine

    eng = get_sqlalchemy_engine({**h.dest_config(kind), **over})
    try:
        with eng.begin() as conn:
            if kind == "mysql":
                conn.execute(text("SET SESSION sql_mode='STRICT_ALL_TABLES'"))
            conn.execute(text(sql))
    except Exception as exc:  # noqa: BLE001 — the driver exception is the subject
        return exc
    raise AssertionError(f"{sql} did not raise")


def test_err1_source_read_denied_is_not_a_destination_write(pg_objects, tmp_path, monkeypatch):
    """Execute with a source role that has no SELECT: the job names the source."""
    h.isolate(tmp_path, _MP(monkeypatch))
    from fastapi.testclient import TestClient

    from services.connector_store import create_connector
    from src.main import app

    def conn(role, **over):
        cfg = {**h.dest_config("postgresql"), **over}
        return create_connector(
            {"name": f"w3err-{uuid.uuid4().hex[:6]}", "role": role, "workspace_id": "", "ssl": False, **cfg}
        )

    src = conn("source", username=ROLE, password=ROLE_PW)
    dst = conn("destination")
    cols = ["id", "name", "val"]
    with TestClient(app) as client:
        r = client.post(
            "/api/v1/transfer/execute",
            json={
                "source": {"kind": "database", "format": "postgresql", "connector_id": src.id, "table": SRC},
                "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id, "table": DST},
                "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in cols],
                "sync_mode": "append",
                "stream_contracts": [{"name": SRC, "selected": True, "sync_mode": "append", "primary_key": "id"}],
                "validation_mode": "strict",
                "async_mode": False,
                "skip_preflight": True,
            },
        )
    body = r.json()
    payload = body.get("detail") if isinstance(body.get("detail"), dict) else body
    details = payload.get("error_details") or {}
    text = f"{payload.get('error')} {details.get('fix')}"
    assert details.get("code") == "access.source_read_denied", details
    assert "Destination" not in text and "INSERT" not in text, text
    assert "address capacity" not in text
    assert "SELECT" in text and SRC in text


@pytest.mark.parametrize(
    "case,sql,expected",
    [
        ("pg_not_null", f'insert into "{VAL}" (id, v) values (4, NULL)', "value.not_null"),
        ("pg_type_mismatch", f"insert into \"{VAL}\" (id, v) values (5, 'abc')", "value.type_mismatch"),
        ("pg_type_mismatch_expr", f"insert into \"{VAL}\" (id, v) select 6, 'x'::text", "value.type_mismatch"),
        ("pg_too_long", f"insert into \"{VAL}\" (id, name, v) values (7, 'abcdef', 1)", "value.too_long"),
    ],
)
def test_err2_pg_value_errors_are_not_missing_columns(pg_objects, case, sql, expected):
    from services.error_handling import humanize_transfer_failure

    exc = _raise("postgresql", sql)
    human = humanize_transfer_failure(exc)
    assert human["code"] == expected, (case, human)
    assert "not on the destination" not in human["title"]
    assert "address capacity" not in human["message"]


@pytest.mark.parametrize(
    "case,sql,expected",
    [
        ("mysql_1048_not_null", f"insert into `{VAL}` values (3,'z',NULL)", "value.not_null"),
        ("mysql_1366_type", f"insert into `{VAL}` (id, name, v) values (2,'b','abc')", "value.type_mismatch"),
        ("mysql_1406_too_long", f"insert into `{VAL}` values (4,'abcdef',1)", "value.too_long"),
    ],
)
def test_err2_mysql_value_errors_are_not_missing_columns(my_objects, case, sql, expected):
    from services.error_handling import humanize_transfer_failure

    exc = _raise("mysql", sql)
    human = humanize_transfer_failure(exc)
    assert human["code"] == expected, (case, human)
    assert "not on the destination" not in human["title"]
    assert "address capacity" not in human["message"]


def test_err1_a_replication_grant_is_not_a_table_read_or_write_grant():
    """42501 for walsender/replication is CDC setup, not \"Grant SELECT\" or \"INSERT\"."""
    from services.error_taxonomy import classify

    class _Pg(Exception):
        pgcode = "42501"

    _Pg.__module__ = "psycopg2.errors"
    named = classify(_Pg("must be superuser or replication role to start walsender"),
                     side="source", fallback=False)
    assert named is None or not named.code.startswith("access."), named
