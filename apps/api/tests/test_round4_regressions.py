"""Round-4 review: session retune, key parsing, classification, collation."""

from __future__ import annotations

import uuid

import pymssql
import pytest


def test_a_dead_sqlserver_host_is_a_connect_timeout():
    from services.error_taxonomy import classify

    down = pymssql.OperationalError(
        20009,
        b"DB-Lib error message 20009, severity 9:\n"
        b"Unable to connect: Adaptive Server is unavailable or does not exist (host)\n",
    )
    classified = classify(down)
    assert classified is not None
    assert classified.code == "network.connect_timeout"


def test_a_password_may_contain_service_and_an_extra_key_may_not():
    from services.connector_store import server_side_dsn_refusal

    assert server_side_dsn_refusal({"password": "service=prod"}) == ""
    assert server_side_dsn_refusal(
        "postgresql://reader:service%3Dprod@db.example/app"
    ) == ""
    refused = server_side_dsn_refusal({"extra": {"service": "prod"}})
    assert "service" in refused
    assert "dsn" in server_side_dsn_refusal({"extra": {"dsn": "Prod"}})


def test_mysql_and_sqlserver_keys_ignore_case_and_trailing_space():
    from services.duplicate_policy import duplicate_key_token

    assert duplicate_key_token("Ada ", "mysql") == duplicate_key_token("ada", "mysql")
    assert duplicate_key_token("Ada ", "sqlserver") == duplicate_key_token("ADA", "mssql")
    assert duplicate_key_token("Ada ", "postgresql") != duplicate_key_token("ada", "postgresql")


def test_reused_mysql_login_takes_the_new_purpose_timeout():
    import connectors.mysql_conn as mysql_conn

    seen: list[int] = []

    class _Cur:
        def __init__(self, conn):
            self.conn = conn
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def execute(self, sql, *_a):
            self.sql = str(sql)

        def fetchone(self):
            if "net_read_timeout" in self.sql:
                seconds = int(self.conn._read_timeout)
                seen.append(seconds)
                return (seconds, seconds, "STRICT_TRANS_TABLES")
            return ("STRICT_TRANS_TABLES",)

    class _Conn:
        def __init__(self):
            self._read_timeout = 40
            self._write_timeout = 40

        def cursor(self):
            return _Cur(self)

        def ping(self, reconnect=False):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    conn = _Conn()
    mysql_conn._retune_mysql_session(
        conn, io_timeout=1800, lock_wait_s=120, require_strict=False
    )
    assert conn._read_timeout == 1800
    assert seen[-1] == 1800
    mysql_conn._retune_mysql_session(
        conn, io_timeout=45, lock_wait_s=30, require_strict=True
    )
    assert conn._read_timeout == 45
    assert seen[-1] == 45


def test_mysql_reconcile_sleep_past_the_introspect_timeout(monkeypatch):
    """Real MySQL: a reconcile login must outlive the 40s schema-read timeout."""
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    pymysql = pytest.importorskip("pymysql")
    from services.endpoint_session import endpoint_session_scope

    import connectors.mysql_conn as mysql_conn

    try:
        probe = pymysql.connect(
            host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
            database="dataflow", connect_timeout=2,
        )
    except Exception as exc:
        pytest.skip(f"MySQL not reachable: {exc}")
    probe.close()
    name = f"d308_{uuid.uuid4().hex[:8]}"
    with endpoint_session_scope(engines=("mysql",)):
        first = mysql_conn.get_connection(
            host="127.0.0.1", port=3306, database="dataflow",
            username="dataflow", password="dataflow", connection_string="",
            ssl=False, purpose="introspect",
        )
        try:
            with first.cursor() as cur:
                cur.execute(f"CREATE TABLE `{name}` (id INT)")
            first.commit()
        finally:
            first.close()
        second = mysql_conn.get_connection(
            host="127.0.0.1", port=3306, database="dataflow",
            username="dataflow", password="dataflow", connection_string="",
            ssl=False, purpose="reconcile",
        )
        try:
            with second.cursor() as cur:
                cur.execute("SELECT SLEEP(45)")
                assert cur.fetchone()[0] == 0
        finally:
            with second.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{name}`")
            second.commit()
            second.close()
