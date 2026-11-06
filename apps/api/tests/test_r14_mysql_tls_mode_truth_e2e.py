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

``require_secure_transport`` is flipped only on a PRIVATE MySQL
(``tests.r4_route_harness.globals_mysql_server``), never the shared one.

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


def test_dsn_tls_options_never_reach_the_driver_and_proxies_keep_tls():
    from connectors.mysql_tls import TlsModeError, mysql_tls_plan, strip_tls_params

    assert strip_tls_params("?ssl-mode=DISABLED") == ""
    assert strip_tls_params("mysql://u@h/db?ssl-mode=DISABLED&charset=utf8mb4") == \
        "mysql://u@h/db?charset=utf8mb4"
    assert strip_tls_params("mysql://u@h/db?ssl_disabled=true") == "mysql://u@h/db"
    assert strip_tls_params("mysql://u@h/db") == "mysql://u@h/db"
    # A public proxy host keeps TLS whatever the DSN asks; ssl=True wins over the DSN.
    assert mysql_tls_plan(ssl=False, connection_string="?ssl-mode=DISABLED",
                          host="x.proxy.rlwy.net").mode == "required"
    assert mysql_tls_plan(ssl=True, connection_string="?ssl_disabled=true").mode == "required"
    assert mysql_tls_plan(ssl=False, connection_string="?ssl_disabled=1").mode == "disabled"
    with pytest.raises(TlsModeError):
        mysql_tls_plan(ssl=False, connection_string="?ssl-mode=VERIFY_CA")


@pytest.fixture
def secure_transport_on():
    """require_secure_transport=ON on the PRIVATE server only; restored in finally."""
    root = h.globals_mysql_root()
    with root.cursor() as cur:
        cur.execute("SELECT @@GLOBAL.require_secure_transport")
        before = int(cur.fetchone()[0])
        cur.execute("SET GLOBAL require_secure_transport = ON")
    try:
        yield h.globals_mysql_server()
    finally:
        with root.cursor() as cur:
            cur.execute(f"SET GLOBAL require_secure_transport = {before}")
            cur.execute("SELECT @@GLOBAL.require_secure_transport")
            assert int(cur.fetchone()[0]) == before
        root.close()


def _dial_cfg(cfg: dict, *, ssl: bool, dsn: str = ""):
    from connectors.mysql_conn import get_connection

    return get_connection(host=cfg["host"], port=cfg["port"], database=cfg["database"],
                          username=cfg["username"], password=cfg["password"],
                          connection_string=dsn, ssl=ssl, purpose="probe")


def test_disabled_on_a_server_requiring_tls_is_upgraded_on_every_path(secure_transport_on,
                                                                     monkeypatch):
    from connectors.generic_sql import _build_engine
    from connectors.mysql_change_stream import MySqlChangeStreamCdc
    from connectors.mysql_conn import describe_session_encryption, session_encryption
    import pymysql

    srv = secure_transport_on
    notes: list[str] = []
    monkeypatch.setattr("services.job_tls_note.record_job_tls_note", notes.append)
    # PyMySQL path (readers, writers, LOAD DATA, connector test).
    conn = _dial_cfg(srv, ssl=False, dsn="?ssl-mode=DISABLED")
    try:
        enc = session_encryption(conn)
    finally:
        conn.close()
    assert enc["encrypted"] and "requires secure transport" in enc["note"], enc
    assert "session encrypted" in describe_session_encryption(enc)
    # SQLAlchemy path: the same dial, no raw 3159.
    cfg = {**srv, "type": "mysql", "ssl": False, "connection_string": "?ssl-mode=DISABLED"}
    eng = _build_engine(cfg)
    try:
        with eng.connect() as cx:
            cipher = cx.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").fetchone()[1]
            raw = cx.connection.dbapi_connection
            assert cipher and "requires secure transport" in raw._dataflow_tls_note
    finally:
        eng.dispose()
    # Binlog path.
    settings = MySqlChangeStreamCdc(cfg, "t", "id")._binlog_connection_settings()
    assert settings.get("ssl"), settings
    probe = pymysql.connect(**settings)
    try:
        with probe.cursor() as cur:
            cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            assert cur.fetchone()[1]
    finally:
        probe.close()
    assert len(notes) >= 3 and all("requires secure transport" in n for n in notes), notes


def test_ssl_true_beats_a_url_ssl_disabled_on_sqlalchemy():
    from connectors.generic_sql import _build_engine

    cfg = _cfg("mysql")
    dsn = f"mysql://{cfg['username']}@{cfg['host']}:{cfg['port']}/{cfg['database']}?ssl_disabled=true"
    eng = _build_engine({**cfg, "type": "mysql", "ssl": True, "connection_string": dsn})
    try:
        with eng.connect() as cx:
            assert cx.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").fetchone()[1]
    finally:
        eng.dispose()


def test_dsn_only_config_to_a_proxy_host_keeps_tls(monkeypatch):
    from connectors.generic_sql import _build_engine

    import connectors.write_resilience as wr

    real = wr.is_public_proxy_host
    monkeypatch.setattr(wr, "is_public_proxy_host",
                        lambda host: str(host or "") == "127.0.0.1" or real(host))
    cfg = _cfg("mysql")
    dsn = f"mysql://{cfg['username']}@127.0.0.1:{cfg['port']}/{cfg['database']}?ssl-mode=DISABLED"
    eng = _build_engine({"type": "mysql", "host": "", "port": 0, "database": "",
                         "username": "", "password": cfg["password"], "ssl": False,
                         "connection_string": dsn})
    try:
        with eng.connect() as cx:
            assert cx.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").fetchone()[1]
    finally:
        eng.dispose()


def test_verify_ca_verifies_and_says_so_unverified_otherwise():
    from connectors.mysql_conn import describe_session_encryption, session_encryption

    ca = os.environ.get("DATAFLOW_TEST_MYSQL_CA", "")
    if not ca or not os.path.exists(ca):
        pytest.skip("DATAFLOW_TEST_MYSQL_CA (the server's ca.pem) not provided")
    conn = _dial("mysql", ssl=False, dsn=f"?ssl-mode=VERIFY_CA&ssl-ca={ca}")
    try:
        text = describe_session_encryption(session_encryption(conn))
    finally:
        conn.close()
    assert "certificate verified" in text and "VERIFY_CA" in text, text
    conn = _dial("mysql", ssl=True)
    try:
        assert "certificate unverified" in describe_session_encryption(session_encryption(conn))
    finally:
        conn.close()
    # The auto-generated server certificate does not name 127.0.0.1: identity fails closed.
    with pytest.raises(Exception):
        _dial("mysql", ssl=False, dsn=f"?ssl-mode=VERIFY_IDENTITY&ssl-ca={ca}").close()


def test_binlog_settings_follow_the_plan():
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    cfg = {**_cfg("mysql"), "type": "mysql"}
    assert MySqlChangeStreamCdc({**cfg, "ssl": True}, "t", "id")._binlog_connection_settings()[
        "ssl"]
    plain = MySqlChangeStreamCdc({**cfg, "ssl": False, "connection_string": "?ssl-mode=DISABLED"},
                                 "t", "id")._binlog_connection_settings()
    assert plain.get("ssl_disabled") is True and "ssl" not in plain


def test_saved_connector_ssl_mode_round_trips_and_disable_is_honoured(client):
    cfg = _cfg("mysql")
    body = {"name": "r14-tls-mode", "type": "mysql", "host": cfg["host"], "port": cfg["port"],
            "database": cfg["database"], "username": cfg["username"],
            "password": cfg["password"], "ssl": False, "ssl_mode": "disable"}
    resp = client.post("/api/v1/connectors/saved", json=body)
    assert resp.status_code == 200, resp.text
    saved = resp.json()
    assert saved.get("ssl_mode") == "disable" and saved.get("ssl") is False, saved
    out = client.post(f"/api/v1/connectors/saved/{saved['id']}/test").json()
    assert out["success"] and "session NOT encrypted" in out["message"], out
    # The transfer path dials the same way.
    from src.transfer.adapters import _lookup_saved_connector
    from connectors.mysql_conn import get_connection, session_encryption

    looked = _lookup_saved_connector(saved["id"])
    conn = get_connection(host=looked["host"], port=looked["port"], database=looked["database"],
                          username=looked["username"], password=looked["password"],
                          connection_string=looked["connection_string"], ssl=looked["ssl"],
                          purpose="read")
    try:
        assert not session_encryption(conn)["encrypted"]
    finally:
        conn.close()
    # require / prefer round-trip; disable is refused where it cannot be honoured.
    for mode, ssl in (("require", True), ("prefer", False)):
        r = client.post("/api/v1/connectors/saved", json={**body, "name": f"r14-{mode}",
                                                          "ssl_mode": mode})
        assert r.status_code == 200 and (r.json()["ssl_mode"], r.json()["ssl"]) == (mode, ssl), r.text
    pg = {**body, "name": "r14-pg-disable", "type": "postgresql", "port": 5432}
    assert client.post("/api/v1/connectors/saved", json=pg).status_code == 422
    assert client.post("/api/v1/connectors/test", json=pg).status_code == 422


def test_legacy_saved_connectors_keep_their_meaning():
    from services.connector_store import SavedConnector, resolve_tls

    assert resolve_tls("mysql", {"ssl": True}) == (True, "")      # require
    assert resolve_tls("mysql", {"ssl": False}) == (False, "")    # prefer
    assert resolve_tls("mysql", {}) == (True, "")                  # legacy default
    assert resolve_tls("mysql", {"ssl": True, "ssl_mode": "disable"}) == (False, "disable")
    assert resolve_tls("postgresql", {"ssl": True}) == (False, "")  # PG keeps its rule
    doc = {"id": "x", "name": "n", "type": "mysql", "role": "source", "ssl": True}
    assert SavedConnector.from_dict(doc).ssl is True
