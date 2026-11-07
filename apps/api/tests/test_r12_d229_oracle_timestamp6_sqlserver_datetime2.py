"""QA D229 (prod 38eae820, job 6ae8f8a14fb8a205cd02ecfe): Oracle TIMESTAMP(6)
-> new SQL Server table (DATETIME2(6)) through the engine landed milliseconds
(20:53:33.307449 -> .307000) on every row, and the failure message said both
"value mismatch" and "count-only, no source value digest".
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ORA = dict(
    host="127.0.0.1",
    port=1521,
    database=os.getenv("DATAFLOW_TEST_ORACLE_SERVICE") or os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_TEST_ORACLE_USER") or os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_TEST_ORACLE_PASSWORD") or os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)
MS = dict(
    host="127.0.0.1", port=1433, database="dataflow", username="sa",
    password=os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD") or "Dataflow_CI_2026!",
)
MICROS = [307449, 1, 999999, 500001, 123456, 654321]
BASE = datetime(2026, 11, 2, 20, 53, 33)  # noqa: DTZ001 — TIMESTAMP(6) is zone-less


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not (_up(1521) and _up(1433)), reason="Oracle and SQL Server required"
)


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


def _ms(sql: str) -> list[tuple]:
    import pymssql

    conn = pymssql.connect(
        server=MS["host"], port=MS["port"], user=MS["username"],
        password=MS["password"], database=MS["database"], autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


EXPECTED = {
    i: (BASE.replace(microsecond=us) + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S.%f")
    for i, us in enumerate(MICROS, start=1)
}


@pytest.fixture()
def oracle_source():
    name = "D229_" + uuid.uuid4().hex[:8].upper()
    _ora(f'CREATE TABLE "{name}" (ID NUMBER(10) PRIMARY KEY, TS TIMESTAMP(6))')
    for i, lit in EXPECTED.items():
        _ora(f"INSERT INTO \"{name}\" VALUES ({i}, TIMESTAMP '{lit}')")
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


@pytest.mark.parametrize("driver", ["pyodbc", "pymssql"])
def test_oracle_timestamp6_lands_every_microsecond_in_new_sqlserver_table(
    isolated, oracle_source, driver, monkeypatch
):
    if driver == "pymssql":
        # The production image ships no Microsoft ODBC driver: every SQL
        # Server connection is pymssql (FreeTDS).
        pytest.importorskip("pymssql")
        import pyodbc

        monkeypatch.setattr(pyodbc, "drivers", lambda: [])
    # The engine pool keys on the endpoint, not the DBAPI: drop engines an
    # earlier test opened under the other driver so this leg runs on its own.
    from services import engine_pool

    engine_pool.dispose_all()
    dst = "d229_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="oracle", table=oracle_source, **ORA),
        destination=EndpointConfig(
            kind="database", format="sqlserver", schema="dbo", table=dst,
            # Lab container is self-signed; the product default stays verify-or-fail.
            extra={"trust_server_certificate": True},
            **MS,
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "d229" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        rows = _ms(
            f"SELECT [ID], CONVERT(varchar(30), [TS], 121) FROM dbo.[{dst}] ORDER BY [ID]"
        )
        types = _ms(
            "SELECT DATA_TYPE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_NAME = '{dst}' AND COLUMN_NAME = 'TS'"
        )
        assert types == [("datetime2", 6)]
        assert {int(r[0]): str(r[1]) for r in rows} == EXPECTED
        assert result.success, result.error
    finally:
        _ms(f"IF OBJECT_ID(N'dbo.[{dst}]', 'U') IS NOT NULL DROP TABLE dbo.[{dst}]")
        engine_pool.dispose_all()
