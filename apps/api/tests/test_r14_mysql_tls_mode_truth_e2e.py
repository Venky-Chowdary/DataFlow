"""QA MySQL D10: ``ssl=false`` but the session was TLS-encrypted.

PyMySQL negotiates TLS whenever the server offers it, so ``ssl=false`` never
meant "no TLS" — it meant "TLS not required" — and nothing said which one a
session got. The contract is now explicit and the test result states the
truth read from the server (``Ssl_cipher``):

* ``ssl=true``  -> REQUIRED (TLS or no connection), PyMySQL and SQLAlchemy;
* ``ssl=false`` -> PREFERRED, the secure default: TLS whenever offered;
* ``ssl-mode=DISABLED`` in the connection string (the MySQL client's URI
  option) -> plaintext, only where the server allows it; a server with
  ``require_secure_transport=ON`` is re-dialled with TLS and that is recorded.

Live MySQL 8 (:3306) and MariaDB (:3307), both of which offer TLS.
"""

from __future__ import annotations

import os

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg


def _dial(engine: str, *, ssl: bool, dsn: str = ""):
    from connectors.mysql_conn import get_connection

    cfg = _cfg(engine)
    return get_connection(host=cfg["host"], port=cfg["port"], database=cfg["database"],
                          username=cfg["username"], password=cfg["password"],
                          connection_string=dsn, ssl=ssl, purpose="probe")


@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
@pytest.mark.parametrize("ssl, dsn, encrypted, requested", [
    (False, "", True, "preferred"),
    (True, "", True, "required"),
    (False, "?ssl-mode=DISABLED", False, "disabled"),
    (False, "?sslmode=disable", False, "disabled"),
])
def test_session_encryption_matches_the_requested_mode(engine, ssl, dsn, encrypted, requested):
    from connectors.mysql_conn import session_encryption

    conn = _dial(engine, ssl=ssl, dsn=dsn)
    try:
        enc = session_encryption(conn)
    finally:
        conn.close()
    assert enc["requested"] == requested
    assert enc["encrypted"] is encrypted, enc
    assert bool(enc["cipher"]) is encrypted


@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
def test_sqlalchemy_engine_honours_the_same_modes(engine):
    from connectors.generic_sql import _build_engine

    cfg = {**_cfg(engine), "type": engine}
    full = (f"mysql://{cfg['username']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            "?ssl-mode=DISABLED&charset=utf8mb4")
    for ssl, dsn, encrypted in [(False, "", True), (True, "", True),
                                (False, "?ssl-mode=DISABLED", False), (False, full, False)]:
        eng = _build_engine({**cfg, "ssl": ssl, "connection_string": dsn})
        try:
            with eng.connect() as cx:
                cipher = cx.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").fetchone()[1]
        finally:
            eng.dispose()
        assert bool(cipher) is encrypted, (ssl, dsn, cipher)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_connector_test_states_the_real_session_encryption(client):
    cfg = _cfg("mysql")
    body = {"type": "mysql", "host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
            "username": cfg["username"], "password": cfg["password"]}
    out = client.post("/api/v1/connectors/test", json={**body, "ssl": False}).json()
    assert out["success"] and "session encrypted" in out["message"], out
    assert "not required" in out["message"], out
    out = client.post("/api/v1/connectors/test", json={
        **body, "ssl": False, "connection_string": "?ssl-mode=DISABLED"}).json()
    assert out["success"] and "session NOT encrypted" in out["message"], out
    out = client.post("/api/v1/connectors/test", json={**body, "ssl": True}).json()
    assert out["success"] and "TLS required" in out["message"], out


def test_dsn_tls_option_never_reaches_the_driver():
    from connectors.mysql_conn import requested_tls_mode, strip_tls_mode_params

    assert strip_tls_mode_params("?ssl-mode=DISABLED") == ""
    assert strip_tls_mode_params("mysql://u@h/db?ssl-mode=DISABLED&charset=utf8mb4") == \
        "mysql://u@h/db?charset=utf8mb4"
    assert strip_tls_mode_params("mysql://u@h/db") == "mysql://u@h/db"
    # A public proxy host keeps TLS whatever the DSN asks.
    assert requested_tls_mode(ssl=False, connection_string="?ssl-mode=DISABLED",
                              public_proxy=True) == "required"
    assert requested_tls_mode(ssl=True, connection_string="?ssl-mode=DISABLED",
                              public_proxy=False) == "required"


def test_disabled_on_a_server_requiring_tls_is_upgraded_and_recorded():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MYSQL_ROOT_PASSWORD", "")
    if not pw or not h.port_up(3306):
        pytest.skip("needs DATAFLOW_TEST_MYSQL_ROOT_PASSWORD to SET GLOBAL require_secure_transport")
    from connectors.mysql_conn import describe_session_encryption, session_encryption

    root = pymysql.connect(host="127.0.0.1", port=3306, user="root", password=pw, autocommit=True)
    with root.cursor() as cur:
        cur.execute("SELECT @@GLOBAL.require_secure_transport")
        before = int(cur.fetchone()[0])
        cur.execute("SET GLOBAL require_secure_transport = ON")
    try:
        conn = _dial("mysql", ssl=False, dsn="?ssl-mode=DISABLED")
        try:
            enc = session_encryption(conn)
        finally:
            conn.close()
        assert enc["encrypted"] and "requires secure transport" in enc["note"], enc
        assert "session encrypted" in describe_session_encryption(enc)
    finally:
        with root.cursor() as cur:
            cur.execute(f"SET GLOBAL require_secure_transport = {before}")
        root.close()
