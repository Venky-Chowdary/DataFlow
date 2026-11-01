"""QA round 12: MySQL query mode with ``source_timezone`` blocked as a lossy
``TIMESTAMPTZ -> TIMESTAMP`` collapse; table mode with the same declaration ran.

Root cause: a query extract is not an authoritative catalog, so Map let the
profile of the samples re-type the column. A declared-zone wall clock renders
without an offset, profiled as zoneless ``TIMESTAMP``, and the destination was
planned ``TIMESTAMP`` while the source stayed the declared ``TIMESTAMPTZ``.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, MySQL ->
PostgreSQL, table mode and query mode, ``source_timezone`` Europe/London and
Asia/Kolkata. Round-5 policy (services/timezone_policy.py): a ``DATETIME`` is a
wall clock in the declared zone and lands as that instant; a ``TIMESTAMP`` is
already an instant and the declaration does not move it. Values sit on both
sides of the 2019 London DST changes; NULL rides along. Run 2 lands in the
table run 1 created. Every stored instant is checked exactly.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from tests import r4_route_harness as h

# (id, DATETIME wall clock, TIMESTAMP instant written in a UTC session)
_ROWS = [
    (1, "2019-03-31 00:59:59", "2019-03-31 00:59:59"),  # last GMT second
    (2, "2019-03-31 02:00:00", "2019-03-31 01:00:00"),  # first BST wall clock
    (3, "2019-07-01 12:00:00", "2019-07-01 12:00:00"),  # summer
    (4, "2019-10-27 00:59:59", "2019-10-27 00:59:59"),  # BST, before fall-back
    (5, "2019-10-27 02:00:00", "2019-10-27 02:00:00"),  # GMT again
    (6, "2020-01-15 23:30:00", None),                   # winter, NULL instant
]

# DATETIME wall clock -> the UTC instant it names in the declared zone.
_EXPECTED_WALL = {
    "Europe/London": {
        1: "2019-03-31 00:59:59", 2: "2019-03-31 01:00:00", 3: "2019-07-01 11:00:00",
        4: "2019-10-26 23:59:59", 5: "2019-10-27 02:00:00", 6: "2020-01-15 23:30:00",
        7: "2021-06-30 23:00:00",
    },
    "Asia/Kolkata": {
        1: "2019-03-30 19:29:59", 2: "2019-03-30 20:30:00", 3: "2019-07-01 06:30:00",
        4: "2019-10-26 19:29:59", 5: "2019-10-26 20:30:00", 6: "2020-01-15 18:00:00",
        7: "2021-06-30 18:30:00",
    },
}


def _utc(text: str | None) -> datetime | None:
    if text is None:
        return None
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _mysql(statement: str, params=None) -> None:
    conn = h.mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            cur.execute(statement, params)
    finally:
        conn.close()


def _connector(kind: str) -> str:
    from services.connector_store import create_connector

    if kind == "mysql":
        h.mysql_conn().close()
        cfg = dict(host="127.0.0.1", port=3306,
                   database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                   username=os.environ.get("MYSQL_USER", "dataflow"),
                   password=os.environ["MYSQL_PASSWORD"])
    else:
        h.pg_conn().close()
        cfg = dict(host=h.PG["host"], port=h.PG["port"], database=h.PG["database"],
                   username=h.PG["username"], password=h.PG["password"], schema="public")
    return str(create_connector({
        "name": f"r12tz-{kind}-{uuid.uuid4().hex[:6]}", "type": kind, "role": "x",
        "workspace_id": "", "ssl": False, **cfg,
    }).id)


def _landed(table: str) -> dict[int, tuple]:
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(f'SELECT id, created, updated FROM "{table}"')
        out = {}
        for rid, created, updated in cur.fetchall():
            out[int(rid)] = tuple(
                None if v is None else (
                    v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
                ) for v in (created, updated)
            )
        return out
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)
    monkeypatch.delenv("DATAWRAP_ENGINE_DIGEST", raising=False)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("zone", ["Europe/London", "Asia/Kolkata"])
@pytest.mark.parametrize("mode", ["table", "query"])
def test_mysql_source_timezone_lands_exact_instants(client, mode, zone):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r12tz_{token}", f"r12tzd_{token}"
    _mysql(f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, created DATETIME NOT NULL, "
           f"updated TIMESTAMP NULL DEFAULT NULL)")
    try:
        for row in _ROWS:
            _mysql(f"INSERT INTO `{src_t}` VALUES (%s, %s, %s)", row)
        src, dst = _connector("mysql"), _connector("postgresql")
        source = ({"source_table": src_t} if mode == "table"
                  else {"source_query": f"SELECT id, created, updated FROM `{src_t}`"})
        args = {"source_connector_id": src, "dest_connector_id": dst, "dest_table": dst_t,
                "sync_mode": "full_refresh_overwrite", "primary_key": "id",
                "validation_mode": "strict", "source_timezone": zone, **source}

        h.stage_and_run(client, "start_transfer", args)
        want_wall = _EXPECTED_WALL[zone]
        expected = {rid: (_utc(want_wall[rid]), _utc(ts)) for rid, _wall, ts in _ROWS}
        assert _landed(dst_t) == expected

        # Run 2 into the table run 1 created.
        _mysql(f"INSERT INTO `{src_t}` VALUES (7, '2021-07-01 00:00:00', NULL)")
        h.stage_and_run(client, "start_transfer", args)
        expected[7] = (_utc(want_wall[7]), None)
        assert _landed(dst_t) == expected
    finally:
        _mysql(f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table("postgresql", dst_t)
