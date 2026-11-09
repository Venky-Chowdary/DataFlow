"""D266 — a table the login cannot see is not a dropped table.

SQL Server (metadata visibility) and MySQL (``SHOW TABLES``) list only the
objects the login holds a privilege on. A login that lost SELECT on the
destination saw it absent from the catalog, so the append continuity check
refused with "the table no longer exists ... It was dropped" while the table
sat there with its rows. The fix re-asks the server with a zero-row SELECT and
reads only a missing-relation error as "dropped".

Live cases need SQL Server (``MSSQL_SA_PASSWORD``) and MySQL root
(``DATAFLOW_TEST_MYSQL_ROOT_PASSWORD``); they skip without them.
"""

from __future__ import annotations

import os
import uuid

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest  # noqa: E402

from services import append_continuity as ac  # noqa: E402
from services.preflight_cursor_gate import dest_object_missing  # noqa: E402
from services.sql_object_identity import is_missing_relation_error  # noqa: E402

_LOW_PW = "Low!Pass1234"


class _Orig(Exception):
    def __init__(self, *args, pgcode=None):
        super().__init__(*args)
        self.pgcode = pgcode


class _Wrapped(Exception):
    def __init__(self, orig):
        super().__init__(str(orig))
        self.orig = orig


@pytest.mark.parametrize(
    "orig, missing",
    [
        (_Orig("relation does not exist", pgcode="42P01"), True),
        (_Orig("schema does not exist", pgcode="3F000"), True),
        (_Orig("permission denied for table t", pgcode="42501"), False),
        (_Orig(1146, "Table 'd.t' doesn't exist"), True),
        (_Orig(1142, "SELECT command denied to user"), False),
        (_Orig("42S02", "[42S02] Invalid object name 't'. (208)"), True),
        (_Orig("42000", "The SELECT permission was denied on the object 't'. (229)"), False),
        (_Orig(208, b"Invalid object name 't'."), True),
        (_Orig(229, b"The SELECT permission was denied"), False),
        (_Orig("no such table: t"), True),
        (_Orig("connection refused"), False),
    ],
)
def test_only_a_missing_relation_error_means_absent(orig, missing):
    assert is_missing_relation_error(_Wrapped(orig)) is missing


def _baseline_then_check(monkeypatch, tmp_path, fmt, port, database, schema, admin, low):
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    monkeypatch.setattr(ac, "_collection", lambda: None)

    def _req(user, pw):
        dest = EndpointConfig(
            kind="database", format=fmt, host="127.0.0.1", port=port, database=database,
            username=user, password=pw, schema=schema, table="pv_t",
            extra={"trust_server_certificate": True},
        )
        src = EndpointConfig(kind="database", format="postgresql", host="127.0.0.1", port=5432,
                             database="x", table="s")
        return TransferRequest(source=src, destination=dest, sync_mode="incremental_append",
                               skip_preflight=True)

    first = _req(*admin)
    target = ac.resolve_target(first.destination)
    fp = ac.fingerprint(target)
    assert fp is not None and fp["rows"] == 6
    ac.save_baseline(target.key, fp, "job-run1")
    assert ac.check_before_write(first) is None
    second = _req(*low)
    low_target = ac.resolve_target(second.destination)
    assert low_target.key == target.key
    assert ac.destination_missing(low_target) is None
    violation = ac.check_before_write(second)
    assert violation is not None
    assert violation.details["mismatch"] == "unreadable"
    assert "no longer exists" not in violation.message
    assert "It was dropped" not in violation.message
    gate = ac.continuity_preflight_gate(second.destination, sync="incremental_append",
                                        contracts=None, pass_status="pass", block_status="block")
    assert gate is None or "dropped" not in gate["message"]
    return low_target


def test_sqlserver_login_without_select_is_unreadable_not_dropped(monkeypatch, tmp_path):
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get("MSSQL_SA_PASSWORD") or ""
    try:
        import pyodbc

        cn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=master;"
            f"UID=sa;PWD={pw};TrustServerCertificate=yes", timeout=5, autocommit=True,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"SQL Server unavailable: {exc}")
    tag = uuid.uuid4().hex[:6]
    db, login = f"d266_{tag}", f"d266_low_{tag}"
    cur = cn.cursor()
    cur.execute(f"CREATE DATABASE [{db}]")
    try:
        cur.execute(f"CREATE LOGIN [{login}] WITH PASSWORD='{_LOW_PW}', CHECK_POLICY=OFF")
        cur.execute(
            f"USE [{db}]; CREATE TABLE dbo.pv_t (id INT PRIMARY KEY, v NVARCHAR(20)); "
            "INSERT INTO dbo.pv_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'); "
            f"CREATE USER [{login}] FOR LOGIN [{login}]; USE master;"
        )
        low_target = _baseline_then_check(
            monkeypatch, tmp_path, "sqlserver", 1433, db, "dbo", ("sa", pw), (login, _LOW_PW),
        )
        cur.execute(f"USE [{db}]; DROP TABLE dbo.pv_t; USE master;")
        # A real drop is still a drop, for the same login.
        assert ac.destination_missing(low_target) is True
    finally:
        cur.execute(f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        cur.execute(f"DROP DATABASE [{db}]")
        cur.execute(f"IF SUSER_ID('{login}') IS NOT NULL DROP LOGIN [{login}]")
        cn.close()


def test_mysql_login_without_select_is_unreadable_not_dropped(monkeypatch, tmp_path):
    root_pw = os.environ.get("DATAFLOW_TEST_MYSQL_ROOT_PASSWORD") or ""
    port = int(os.environ.get("MYSQL_PORT") or 3306)
    try:
        import pymysql

        cn = pymysql.connect(host="127.0.0.1", port=port, user="root", password=root_pw,
                             autocommit=True, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL root unavailable: {exc}")
    tag = uuid.uuid4().hex[:6]
    db, user = f"d266_{tag}", f"d266_low_{tag}"
    cur = cn.cursor()
    cur.execute(f"CREATE DATABASE `{db}`")
    try:
        cur.execute(f"CREATE TABLE `{db}`.pv_t (id INT PRIMARY KEY, v VARCHAR(20))")
        cur.execute(f"INSERT INTO `{db}`.pv_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')")
        cur.execute(f"CREATE TABLE `{db}`.other (x INT)")
        cur.execute(f"CREATE USER '{user}'@'%' IDENTIFIED BY '{_LOW_PW}'")
        cur.execute(f"GRANT SELECT ON `{db}`.other TO '{user}'@'%'")
        low_target = _baseline_then_check(
            monkeypatch, tmp_path, "mysql", port, db, "", ("root", root_pw), (user, _LOW_PW),
        )
        cur.execute(f"GRANT SELECT ON `{db}`.pv_t TO '{user}'@'%'")
        cur.execute(f"DROP TABLE `{db}`.pv_t")
        assert ac.destination_missing(low_target) is True
    finally:
        cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
        cur.execute(f"DROP USER IF EXISTS '{user}'@'%'")
        cn.close()


def test_postgres_true_drop_is_still_missing():
    try:
        import psycopg2

        cn = psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"),
                              port=int(os.environ.get("PGPORT") or 5432),
                              user=os.environ.get("PGUSER", "dataflow"),
                              password=os.environ.get("PGPASSWORD", "dataflow"),
                              dbname=os.environ.get("PGDATABASE", "dataflow"), connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    cn.close()
    cfg = {
        "type": "postgresql", "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": int(os.environ.get("PGPORT") or 5432),
        "database": os.environ.get("PGDATABASE", "dataflow"),
        "username": os.environ.get("PGUSER", "dataflow"),
        "password": os.environ.get("PGPASSWORD", "dataflow"), "schema": "public",
    }
    os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    assert dest_object_missing("postgresql", cfg, f"d266_gone_{uuid.uuid4().hex[:6]}") is True
