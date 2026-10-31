"""QA round 11, item 1 — hosted SQL tiles transfer end to end on their real engine.

QA on 9c75ada4 (P2): PostgreSQL -> ``alloydb`` / ``amazon_rds_postgresql`` /
``azure_database_for_postgresql`` / ``google_cloud_sql_postgresql`` was blocked
("Migration Risk Contract required", "lossy TIMESTAMPTZ→TEXT", "not a
supported destination", "existence unknown"), and the same tiles as a source
were blocked with "Duplicate-key probe not implemented for source type …".
Execution already dialed them through the PostgreSQL driver; preflight
dispatched on the raw tile id.

The route is QA's: the streamable MCP endpoint ``tools/call start_transfer``
-> ``/api/v1/copilot/confirm`` -> job. Every hosted tile of each family is
pointed at that family's real engine:

* PostgreSQL tiles on :5432, as a source and as a destination, with run 2
  into the table DataFlow created;
* MySQL tiles on :3306 (``mariadb`` on the MariaDB server at :3307);
* SQL Server tiles on :1433;
* Oracle tiles on :1521 where reachable.

Each run compares every cell — NULL, '', whitespace, unicode and a
timezone-aware timestamp — between source and destination. A leg skips when its
engine is not configured on the host.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

import pytest

from tests import r4_route_harness as h

# --------------------------------------------------------------- engines


def _oracle_conn():
    oracledb = pytest.importorskip("oracledb")
    pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
    if not pw or not h.port_up(1521):
        pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
    oracledb.defaults.fetch_lobs = False
    oracledb.defaults.fetch_decimals = True
    svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
    try:
        conn = oracledb.connect(
            user=os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system"), password=pw,
            dsn=f"{os.environ.get('DATAFLOW_TEST_ORACLE_HOST', '127.0.0.1')}:1521/{svc}",
        )
    except Exception as exc:  # environmental: Oracle still starting
        pytest.skip(f"Oracle not reachable: {exc}")
    conn.autocommit = True
    return conn


def _mariadb_conn():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    port = int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307"))
    if not pw or not h.port_up(port):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_PASSWORD, :3307)")
    return pymysql.connect(
        host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"), port=port,
        user=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"), password=pw,
        database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
        charset="utf8mb4", autocommit=True, connect_timeout=5,
    )


def _mysql_session(conn):
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return conn


def _mssql():
    conn = h.mssql_conn()
    conn.add_output_converter(-155, _datetimeoffset)
    return conn


def _datetimeoffset(raw: bytes):
    import struct

    y, mo, d, hh, mi, sec, ns, oh, om = struct.unpack("<6hI2h", raw)
    return datetime(y, mo, d, hh, mi, sec, ns // 1000, timezone(timedelta(hours=oh, minutes=om)))


_ENGINES: dict[str, Callable[[], Any]] = {
    "postgresql": h.pg_conn,
    "mysql": lambda: _mysql_session(h.mysql_conn()),
    "mariadb": lambda: _mysql_session(_mariadb_conn()),
    "sqlserver": _mssql,
    "oracle": _oracle_conn,
}


def _sql(engine: str, statement: str, params: Any = None) -> list[tuple]:
    conn = _ENGINES[engine]()
    try:
        cur = conn.cursor()
        if params is None:
            cur.execute(statement)
        else:
            cur.execute(statement, params)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _connector_cfg(engine: str) -> dict[str, Any]:
    _ENGINES[engine]().close()  # skips when the engine is absent
    if engine == "postgresql":
        return dict(host=h.PG["host"], port=h.PG["port"], database=h.PG["database"],
                    username=h.PG["username"], password=h.PG["password"], schema="public")
    if engine == "mysql":
        return dict(host="127.0.0.1", port=3306,
                    database=os.environ.get("MYSQL_DATABASE", "dataflow"),
                    username=os.environ.get("MYSQL_USER", "dataflow"),
                    password=os.environ["MYSQL_PASSWORD"])
    if engine == "mariadb":
        return dict(host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
                    port=int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307")),
                    database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
                    username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
                    password=os.environ["DATAFLOW_TEST_MARIADB_PASSWORD"])
    if engine == "sqlserver":
        return dict(host="127.0.0.1", port=1433, database="dataflow", schema="dbo",
                    username=os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
                    password=os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"],
                    extra={"trust_server_certificate": True})
    if engine == "oracle":
        svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
        user = os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system")
        return dict(host=os.environ.get("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1"), port=1521,
                    database=svc, service_name=svc, username=user,
                    password=os.environ["DATAFLOW_TEST_ORACLE_PASSWORD"],
                    schema=os.environ.get("DATAFLOW_TEST_ORACLE_SCHEMA", user.upper()))
    raise AssertionError(engine)


def _connector(kind: str, engine: str, role: str) -> str:
    from services.connector_store import create_connector

    payload = {
        "name": f"r11-{kind}-{role}-{uuid.uuid4().hex[:6]}", "type": kind, "role": role,
        "workspace_id": "", "ssl": False, **_connector_cfg(engine),
    }
    return str(create_connector(payload).id)


# ------------------------------------------------------------ the tables

_TS1 = datetime(2026, 3, 29, 1, 30, 15, 123456, tzinfo=timezone.utc)
_TS2 = datetime(2026, 10, 25, 23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=5, minutes=30)))
_ROWS = [
    (1, "Zoë ✓ 日本語 🚀", "plain", _TS1, Decimal("12.50")),
    (2, None, "", _TS2, None),
    (3, "", None, None, Decimal("-0.01")),
    (4, "  ", "  padded  ", _TS1, Decimal("99999999.99")),
    (5, "ünïcødé — ‘quotes’", "x", _TS2, Decimal("0.00")),
]
# Run 2 edits the source: value -> NULL, NULL -> value, '' -> unicode, a new row.
_EDITS = [
    ("UPDATE {t} SET name = NULL, note = 'changed' WHERE id = 1", None),
    ("UPDATE {t} SET name = 'was null' WHERE id = 2", None),
    ("UPDATE {t} SET name = '東京 é' WHERE id = 3", None),
]

_DDL = {
    "postgresql": "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name TEXT, note VARCHAR(40), "
                  "ts TIMESTAMPTZ, amt NUMERIC(10,2))",
    "mysql": "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name TEXT, note VARCHAR(40), "
             "ts TIMESTAMP(6) NULL, amt DECIMAL(10,2)) DEFAULT CHARSET=utf8mb4",
    "sqlserver": "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name NVARCHAR(MAX), "
                 "note NVARCHAR(40), ts DATETIMEOFFSET(6), amt DECIMAL(10,2))",
    "oracle": "CREATE TABLE {t} (id NUMBER(19) PRIMARY KEY, name NVARCHAR2(200), "
              "note NVARCHAR2(40), ts TIMESTAMP(6) WITH TIME ZONE, amt NUMBER(10,2))",
}
_DDL["mariadb"] = _DDL["mysql"]
_PARAM = {"postgresql": "%s", "mysql": "%s", "mariadb": "%s", "sqlserver": "?", "oracle": None}


def _quote(engine: str, name: str) -> str:
    if engine in ("mysql", "mariadb"):
        return f"`{name}`"
    if engine == "sqlserver":
        return f"[{name}]"
    return f'"{name}"'


def _source_quote(engine: str, table: str) -> str:
    # A source table as an Oracle user creates it: unquoted, so upper case.
    return _quote(engine, table.upper() if engine == "oracle" else table)


def _seed(engine: str, table: str) -> None:
    t = _source_quote(engine, table)
    _sql(engine, _DDL[engine].format(t=t))
    mark = _PARAM[engine]
    values = ", ".join([mark] * 5) if mark else ":1, :2, :3, :4, :5"
    for row in _ROWS:
        cells = list(row)
        if engine in ("mysql", "mariadb") and cells[3] is not None:
            cells[3] = cells[3].astimezone(timezone.utc).replace(tzinfo=None)
        _sql(engine, f"INSERT INTO {t} (id, name, note, ts, amt) VALUES ({values})", cells)


def _edit(engine: str, table: str) -> None:
    t = _source_quote(engine, table)
    for statement, _ in _EDITS:
        _sql(engine, statement.format(t=t))
    mark = _PARAM[engine]
    values = ", ".join([mark] * 5) if mark else ":1, :2, :3, :4, :5"
    ts = _TS1 if engine not in ("mysql", "mariadb") else _TS1.replace(tzinfo=None)
    _sql(engine, f"INSERT INTO {t} (id, name, note, ts, amt) VALUES ({values})",
         [6, "nouveau ✓", "  ", ts, Decimal("1.00")])


def _table_name(engine: str, table: str) -> str:
    if engine != "oracle":
        return table
    names = {r[0] for r in _sql("oracle", "SELECT table_name FROM user_tables "
                                "WHERE UPPER(table_name) = UPPER(:1)", [table])}
    return table if table in names else table.upper()


def _norm_cell(engine: str, value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:  # MySQL TIMESTAMP read in a UTC session
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    if engine == "oracle" and isinstance(value, (int, float)) and not isinstance(value, bool):
        return Decimal(str(value)).quantize(Decimal("0.01")) if value != int(value) else int(value)
    return value


def _cells(engine: str, table: str) -> list[tuple]:
    name = _table_name(engine, table)
    rows = _sql(engine, f"SELECT * FROM {_quote(engine, name)}")
    conn = _ENGINES[engine]()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_quote(engine, name)} WHERE 1 = 0")
        cols = [str(d[0]).lower() for d in cur.description]
    finally:
        conn.close()
    want = ["id", "name", "note", "ts", "amt"]
    out = []
    for row in rows:
        by_name = dict(zip(cols, row))
        out.append(tuple(_norm_cell(engine, by_name.get(c)) for c in want))
    out = [(int(r[0]),) + r[1:] for r in out]
    return sorted(out)


def _drop(engine: str, table: str) -> None:
    for name in {table, table.upper()}:
        try:
            if engine == "oracle":
                _sql(engine, f'DROP TABLE "{name}" PURGE')  # exact spelling
            else:
                _sql(engine, f"DROP TABLE IF EXISTS {_quote(engine, name)}")
        except Exception:
            pass


# ------------------------------------------------------------- the route


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _transfer(client, engine: str, src: str, src_table: str, dst: str, dst_table: str) -> dict:
    args = {
        "source_connector_id": src,
        "source_table": src_table.upper() if engine == "oracle" else src_table,
        "dest_connector_id": dst, "dest_table": dst_table,
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "validation_mode": "strict", "source_timezone": "UTC",
    }
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    assert staged.get("ack_id"), staged
    return h.confirm(client, staged["ack_id"], reason="r11-family")


# Every hosted tile pointed at its family's real engine. Distinct engines that
# merely speak a family's wire (CockroachDB, YugabyteDB, TiDB, Vitess,
# SingleStore, OceanBase, PolarDB, GaussDB, GoldenDB) are not that engine and
# are covered by the dispatch matrix, not by a run against a different server.
_PG_TILES = [
    "alloydb", "amazon_rds_postgresql", "azure_database_for_postgresql",
    "google_cloud_sql_postgresql", "amazon_aurora_postgresql", "timescaledb", "neon",
    "supabase", "openshift", "postgresql_rds", "postgresql_cloud_sql", "postgresql_azure",
    "postgresql_supabase", "postgresql_neon", "postgresql_aurora_global",
]
_MYSQL_TILES = [
    ("amazon_aurora", "mysql"), ("amazon_rds_mysql", "mysql"),
    ("azure_database_for_mysql", "mysql"), ("google_cloud_sql_mysql", "mysql"),
    ("mysql_rds", "mysql"), ("mysql_cloud_sql", "mysql"), ("mysql_azure", "mysql"),
    ("mysql_aurora_global", "mysql"), ("planetscale", "mysql"),
    ("mysql_planetscale", "mysql"), ("percona_mysql", "mysql"), ("mariadb", "mariadb"),
]
_MSSQL_TILES = ["sql_server", "amazon_rds_sql_server", "azure_sql_database",
                "google_cloud_sql_sql_server", "azure_sql"]
_ORACLE_TILES = ["amazon_rds_oracle", "oracle_autonomous_warehouse"]

_CASES = (
    [(t, "postgresql", "postgresql") for t in _PG_TILES]
    + [(t, engine, "mysql") for t, engine in _MYSQL_TILES]
    + [(t, "sqlserver", "sqlserver") for t in _MSSQL_TILES]
    + [(t, "oracle", "oracle") for t in _ORACLE_TILES]
)
_IDS = [c[0] for c in _CASES]


def _canonical_kind(engine: str) -> str:
    return "mysql" if engine == "mariadb" else engine


@pytest.mark.parametrize("tile,engine,family", _CASES, ids=_IDS)
def test_tile_as_source_transfers_every_cell(client, tile, engine, family):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r11s_{token}", f"r11d_{token}"
    try:
        _seed(engine, src_t)
        src = _connector(tile, engine, "source")
        dst = _connector(_canonical_kind(engine), engine, "destination")
        _transfer(client, engine, src, src_t, dst, dst_t)
        assert _cells(engine, dst_t) == _cells(engine, src_t)
    finally:
        _drop(engine, src_t)
        _drop(engine, dst_t)


@pytest.mark.parametrize("tile,engine,family", _CASES, ids=_IDS)
def test_tile_as_destination_run1_and_run2_into_created_table(client, tile, engine, family):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r11s_{token}", f"r11d_{token}"
    try:
        _seed(engine, src_t)
        src = _connector(_canonical_kind(engine), engine, "source")
        dst = _connector(tile, engine, "destination")
        _transfer(client, engine, src, src_t, dst, dst_t)
        assert _cells(engine, dst_t) == _cells(engine, src_t)

        _edit(engine, src_t)
        _transfer(client, engine, src, src_t, dst, dst_t)  # run 2 into the table run 1 created
        got = _cells(engine, dst_t)
        assert got == _cells(engine, src_t)
        assert len(got) == 6
    finally:
        _drop(engine, src_t)
        _drop(engine, dst_t)
