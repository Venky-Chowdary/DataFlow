"""D155 end to end — Oracle TIMESTAMP WITH [LOCAL] TIME ZONE keeps its instant.

Oracle source (live) -> PostgreSQL timestamptz / SQL Server DATETIMEOFFSET /
MySQL DATETIME. Offsets +05:30, -04:00 and +00:00 must survive as the same
instants (MySQL DATETIME stores the UTC clock).
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timezone

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ORA = dict(
    host=os.getenv("DATAFLOW_ORACLE_HOST") or "127.0.0.1",
    port=int(os.getenv("DATAFLOW_ORACLE_PORT") or 1521),
    database=os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)
PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")
MY = dict(host="127.0.0.1", port=3306, database="dataflow", username="dataflow", password="dataflow")
MS = dict(
    host="127.0.0.1", port=1433, database="dataflow", username="sa",
    password=os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD") or "Dataflow_CI_2026!",
)


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(ORA["port"]), reason="Oracle not reachable")

UTC = timezone.utc
#: id -> the instant (UTC) each Oracle literal denotes.
INSTANTS = {
    1: datetime(2024, 3, 1, 3, 1, 0, tzinfo=UTC),  # 08:31+05:30
    2: datetime(2024, 6, 2, 3, 15, 0, tzinfo=UTC),  # 23:15-04:00
    3: datetime(2024, 12, 31, 0, 0, 0, tzinfo=UTC),  # 00:00+00:00
}


def _ora(sql: str) -> list[tuple]:
    import oracledb

    conn = oracledb.connect(
        user=ORA["username"], password=ORA["password"],
        dsn=f"{ORA['host']}:{ORA['port']}/{ORA['database']}",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


@pytest.fixture()
def oracle_source():
    name = "D155_" + uuid.uuid4().hex[:8].upper()
    _ora(
        f'CREATE TABLE "{name}" (ID NUMBER(10) PRIMARY KEY, TS_TZ TIMESTAMP(6) WITH TIME ZONE, '
        "TS_LTZ TIMESTAMP(6) WITH LOCAL TIME ZONE)"
    )
    for i, lit in {
        1: "2024-03-01 08:31:00 +05:30",
        2: "2024-06-01 23:15:00 -04:00",
        3: "2024-12-31 00:00:00 +00:00",
    }.items():
        _ora(
            f"INSERT INTO \"{name}\" VALUES ({i}, TIMESTAMP '{lit}', TIMESTAMP '{lit}')"
        )
    yield name
    _ora(f'DROP TABLE "{name}" PURGE')


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, src: str, destination: EndpointConfig, mode: str = "full_refresh_overwrite"):
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="oracle", table=src, **ORA),
        destination=destination,
        sync_mode=mode,
        validation_mode="strict",
    )
    job_id = "d155" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.skipif(not _up(5432), reason="PostgreSQL not reachable")
def test_oracle_tz_columns_land_as_the_same_instants_in_postgres(isolated, oracle_source):
    import psycopg2

    dst = "d155_pg_" + uuid.uuid4().hex[:8]
    result = _run(
        isolated, oracle_source,
        EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG),
    )
    conn = psycopg2.connect(
        host=PG["host"], dbname=PG["database"], user=PG["username"], password=PG["password"]
    )
    conn.autocommit = True
    try:
        assert result.success, result.error
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = '{dst}'"
            )
            types = {n.lower(): t for n, t in cur.fetchall()}
            cur.execute(f'SELECT "ID", "TS_TZ", "TS_LTZ" FROM public."{dst}" ORDER BY "ID"')
            rows = cur.fetchall()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
        conn.close()
    assert types["ts_tz"] == "timestamp with time zone"
    assert [(int(r[0]), r[1].astimezone(UTC)) for r in rows] == [
        (i, INSTANTS[i]) for i in sorted(INSTANTS)
    ]
    assert [(int(r[0]), r[2].astimezone(UTC)) for r in rows] == [
        (i, INSTANTS[i]) for i in sorted(INSTANTS)
    ]


@pytest.mark.skipif(not _up(3306), reason="MySQL not reachable")
def test_oracle_tz_columns_land_as_utc_clock_in_mysql(isolated, oracle_source):
    import pymysql

    dst = "d155_my_" + uuid.uuid4().hex[:8]
    result = _run(
        isolated, oracle_source, EndpointConfig(kind="database", format="mysql", table=dst, **MY)
    )
    conn = pymysql.connect(
        host=MY["host"], user=MY["username"], password=MY["password"],
        database=MY["database"], autocommit=True,
    )
    try:
        assert result.success, result.error
        with conn.cursor() as cur:
            cur.execute(f"SELECT ID, TS_TZ, TS_LTZ FROM `{dst}` ORDER BY ID")
            rows = cur.fetchall()
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        conn.close()
    # LTZ stores an instant: DATETIME holds its UTC clock. TZ pins an offset
    # that DATETIME cannot keep, so it lands as offset text — same instant.
    assert [(int(r[0]), r[2]) for r in rows] == [
        (i, INSTANTS[i].replace(tzinfo=None)) for i in sorted(INSTANTS)
    ]
    assert [
        (int(r[0]), datetime.fromisoformat(str(r[1]).replace("Z", "+00:00")).astimezone(UTC))
        for r in rows
    ] == [(i, INSTANTS[i]) for i in sorted(INSTANTS)]


@pytest.mark.skipif(not _up(1433), reason="SQL Server not reachable")
def test_oracle_tz_columns_land_as_the_same_instants_in_sqlserver(isolated, oracle_source):
    import pyodbc

    dst = "d155_ms_" + uuid.uuid4().hex[:8]
    result = _run(
        isolated, oracle_source,
        EndpointConfig(
            kind="database", format="sqlserver", schema="dbo", table=dst,
            extra={"trust_server_certificate": True}, **MS,
        ),
    )
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID=sa;PWD={MS['password']};TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        assert result.success, result.error
        cur = conn.cursor()
        cur.execute(
            f"SELECT [ID], CONVERT(varchar(40), SWITCHOFFSET([TS_TZ], '+00:00'), 126), "
            f"CONVERT(varchar(40), SWITCHOFFSET([TS_LTZ], '+00:00'), 126) FROM dbo.[{dst}] ORDER BY [ID]"
        )
        rows = cur.fetchall()
    finally:
        conn.cursor().execute(f"DROP TABLE IF EXISTS dbo.[{dst}]")
        conn.close()
    for rid, tz_text, ltz_text in rows:
        want = INSTANTS[int(rid)].strftime("%Y-%m-%dT%H:%M:%S")
        assert tz_text.startswith(want) and ltz_text.startswith(want), (rid, tz_text, ltz_text)
    assert len(rows) == 3


def _connector(payload: dict):
    from services.connector_store import create_connector

    created = create_connector({"name": payload["type"] + uuid.uuid4().hex[:6], **payload})
    return getattr(created, "id", None) or created["id"]


@pytest.fixture()
def saved_oracle():
    from services.connector_store import delete_connector

    cid = _connector({
        "type": "oracle", "host": ORA["host"], "port": ORA["port"], "database": ORA["database"],
        "username": ORA["username"], "password": ORA["password"], "schema": "SYSTEM",
    })
    yield cid
    delete_connector(cid)


def test_pilot_sample_of_an_oracle_table_keeps_the_offset(oracle_source, saved_oracle):
    """The Pilot/MCP preflight sample is a raw query: it must not read TSTZ naive."""
    from src.ai.copilot.query_tools import sample_connector_object

    res = sample_connector_object(
        connector_id=saved_oracle, table=oracle_source, limit=5, analyze=False
    )
    assert res.success, res.error
    rows = {int(r["id"]): r for r in res.output["rows"]}
    for i, instant in INSTANTS.items():
        for col in ("ts_tz", "ts_ltz"):
            got = datetime.fromisoformat(str(rows[i][col]).replace("Z", "+00:00"))
            assert got.tzinfo is not None, (col, rows[i][col])
            assert got.astimezone(UTC) == instant


@pytest.mark.skipif(not _up(5432), reason="PostgreSQL not reachable")
def test_plan_transfer_oracle_to_postgres_is_not_blocked_by_a_naive_sample(
    oracle_source, saved_oracle
):
    from services.connector_store import delete_connector
    from src.ai.copilot import transfer_tools

    pg_id = _connector({
        "type": "postgresql", "host": PG["host"], "port": PG["port"],
        "database": PG["database"], "username": PG["username"], "password": PG["password"],
    })
    try:
        planned = transfer_tools.plan_transfer(
            source_connector_id=saved_oracle, source_table=oracle_source,
            dest_connector_id=pg_id, dest_table=oracle_source.lower() + "_plan",
            sync_mode="full_refresh_overwrite",
        )
    finally:
        delete_connector(pg_id)
    preflight = (planned.output or {}).get("preflight") or {}
    assert preflight.get("passed") is True, preflight.get("blockers")
