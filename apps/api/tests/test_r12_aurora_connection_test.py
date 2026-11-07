"""QA round 12, bug 8: the ``amazon_aurora`` connection Test said "Cannot reach host".

QA (8f7d246d): an ``amazon_aurora`` connector given the same reachable
endpoint and credentials as a working PostgreSQL connector answered "Cannot
reach host". Re-tested against the real local MySQL :3306 and PostgreSQL :5432
with exactly the host, port and credentials of working ``mysql`` /
``postgresql`` connectors, three different code paths showed up:

* **Port.** The connector store "healed" an explicit 5432 on any non-PostgreSQL
  driver to that driver's default, so a saved ``amazon_aurora`` (Aurora MySQL)
  connector pointed at 5432 dialed 3306 instead. Locally that reached a
  *different server* and reported "MySQL connected"; on an Aurora PostgreSQL
  host, where 3306 is closed, it reported "Cannot reach the host on the
  specified port" — QA's message — and the round-11 wire sniff, run against the
  healed 3306, could not explain it.
* **Latency.** The form Test dialed 5432, but pymysql waited out its 45 s
  handshake read on a server that never speaks first before anything was named.
* **TLS.** The store keyed "PostgreSQL negotiates TLS" on the raw tile id, so
  ``amazon_aurora_postgresql`` stored ``ssl=True`` (TLS *required*) when the
  operator never asked for TLS, while the ``postgresql`` tile negotiated.

And the humaniser folded refused / unreachable / down into one "Cannot reach
the host" slogan, an unresolved name with "Temporary failure in name
resolution" into a raw dump, and a server that never spoke into "Connection
timed out". Each class is now named from the real exception (real sockets
below: a closed port, an unresolvable host, a silent listener, plaintext and
TLS-requiring servers, bad credentials).

Decision on auto-detect: the catalog has one tile per engine
(``amazon_aurora`` = Aurora MySQL, ``amazon_aurora_postgresql``), and the
driver, type mapping and every later run follow the saved tile. The Test does
not switch engines silently; it names the protocol the port speaks and the
tile to pick.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _endpoint(kind: str) -> dict:
    cfg = h.dest_config(kind)
    return {k: cfg[k] for k in ("host", "port", "database", "username", "password")}


def _form_test(client, tile: str, ep: dict, **extra) -> tuple[bool, str, float]:
    started = time.monotonic()
    body = client.post("/api/v1/connectors/test", json={**ep, "type": tile, **extra}).json()
    return bool(body["success"]), str(body["message"]), time.monotonic() - started


def _saved(tile: str, ep: dict, **extra):
    from services.connector_store import create_connector

    return create_connector({
        "name": f"r12a-{tile}-{uuid.uuid4().hex[:6]}", "role": "source", "workspace_id": "",
        **ep, "type": tile, **extra,
    })


def _saved_test(client, conn) -> tuple[bool, str, float]:
    started = time.monotonic()
    body = client.post(f"/api/v1/connectors/saved/{conn.id}/test").json()
    return bool(body["success"]), str(body["message"]), time.monotonic() - started


# ------------------------------------------------------------ real endpoints

def test_aurora_mysql_tile_passes_on_the_endpoint_a_mysql_connector_passes_on(client):
    ep = _endpoint("mysql")
    for tile in ("mysql", "amazon_aurora", "amazon_aurora_mysql"):
        for extra in ({}, {"ssl": False}, {"ssl": True}):
            ok, msg, _ = _form_test(client, tile, ep, **extra)
            assert ok and msg.startswith("MySQL connected"), (tile, extra, msg)
            ok, msg, _ = _saved_test(client, _saved(tile, ep, **extra))
            assert ok and msg.startswith("MySQL connected"), (tile, extra, msg)


def test_aurora_mysql_tile_on_a_postgres_endpoint_names_the_protocol_at_the_configured_port(client):
    """Same host, port and credentials as a working postgresql connector."""
    ep = _endpoint("postgresql")
    ok, msg, _ = _form_test(client, "postgresql", ep)
    assert ok, msg
    for tile in ("amazon_aurora", "amazon_aurora_mysql"):
        conn = _saved(tile, ep)
        assert conn.port == ep["port"], (tile, conn.port)  # the operator's port, not 3306
        for label, (ok, msg, took) in (
            ("form", _form_test(client, tile, ep)),
            ("saved", _saved_test(client, conn)),
        ):
            assert not ok, (tile, label, msg)
            assert "speaks the PostgreSQL protocol" in msg, (tile, label, msg)
            assert f":{ep['port']}" in msg, (tile, label, msg)
            assert "Amazon Aurora PostgreSQL" in msg, (tile, label, msg)
            assert "Cannot reach" not in msg, (tile, label, msg)
            # Named up front, not after pymysql's 45 s handshake read.
            assert took < 10, (tile, label, took)


def test_aurora_postgresql_tile_matches_the_postgresql_tile_on_both_endpoints(client):
    pg, my = _endpoint("postgresql"), _endpoint("mysql")
    for tile in ("postgresql", "amazon_aurora_postgresql"):
        conn = _saved(tile, pg)
        assert conn.ssl is False and conn.ssl_mode == "", (tile, conn.ssl, conn.ssl_mode)
        for ok, msg, _ in (_form_test(client, tile, pg), _saved_test(client, conn)):
            assert ok and msg.startswith("PostgreSQL connected"), (tile, msg)
        for ok, msg, took in (_form_test(client, tile, my), _saved_test(client, _saved(tile, my))):
            assert not ok, (tile, msg)
            assert "speaks the MySQL protocol" in msg and ":3306" in msg, (tile, msg)
            assert took < 10, (tile, took)


# ------------------------------------------------------ protocol-level servers

def _serve(handler) -> int:
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)

    def loop():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=handler, args=(conn,), daemon=True).start()

    threading.Thread(target=loop, daemon=True).start()
    return listener.getsockname()[1]


def _plaintext_postgres(conn: socket.socket) -> None:
    """A PostgreSQL that offers no TLS: 'N' to SSLRequest, then FATAL 28P01 on login."""
    try:
        while True:
            header = conn.recv(8)
            if len(header) < 8:
                return
            length, code = struct.unpack("!ii", header)
            if code == 80877103:  # SSLRequest
                conn.sendall(b"N")
                continue
            conn.recv(length - 8)
            fields = (b"SFATAL\0VFATAL\0C28P01\0"
                      b"Mpassword authentication failed for user \"r12\"\0\0")
            conn.sendall(b"E" + struct.pack("!i", len(fields) + 4) + fields)
            return
    finally:
        conn.close()


def _mysql_requiring_tls(conn: socket.socket) -> None:
    """A MySQL 8 with require_secure_transport=ON answering a plaintext login."""
    try:
        caps = 0x000FF7FF & ~0x0800  # CLIENT_PROTOCOL_41 etc.; no CLIENT_SSL offered
        salt = b"abcdefgh" + b"ijklmnopqrst"
        payload = (
            b"\x0a" + b"8.0.36\0" + struct.pack("<I", 7) + salt[:8] + b"\0"
            + struct.pack("<H", caps & 0xFFFF) + b"\x21" + struct.pack("<H", 2)
            + struct.pack("<H", caps >> 16) + bytes([21]) + b"\0" * 10
            + salt[8:] + b"\0" + b"mysql_native_password\0"
        )
        conn.sendall(struct.pack("<I", len(payload))[:3] + b"\0" + payload)
        conn.recv(4096)  # the client's handshake response
        message = (b"Connections using insecure transport are prohibited while "
                   b"--require_secure_transport=ON.")
        err = b"\xff" + struct.pack("<H", 3159) + b"#HY000" + message
        conn.sendall(struct.pack("<I", len(err))[:3] + b"\x02" + err)
        time.sleep(0.2)
    finally:
        conn.close()


def _silent(conn: socket.socket) -> None:
    time.sleep(20)
    conn.close()


def _closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_aurora_postgresql_tile_negotiates_tls_like_the_postgresql_tile(client):
    """Plaintext server: both tiles reach its login; only an explicit SSL requires TLS."""
    ep = {"host": "127.0.0.1", "port": _serve(_plaintext_postgres), "database": "d",
          "username": "r12", "password": "wrong"}
    for tile in ("postgresql", "amazon_aurora_postgresql"):
        ok, msg, _ = _saved_test(client, _saved(tile, ep))
        assert not ok and msg.startswith("Authentication failed"), (tile, msg)
        ok, msg, _ = _form_test(client, tile, ep)
        assert not ok and msg.startswith("Authentication failed"), (tile, msg)
        # The operator turned SSL on: the mismatch names which side lacks TLS.
        ok, msg, _ = _saved_test(client, _saved(tile, ep, ssl=True))
        assert not ok and msg.startswith("TLS mismatch"), (tile, msg)
        assert "requires TLS, but the server does not offer it" in msg, (tile, msg)


def test_humaniser_names_the_real_failure_for_both_aurora_tiles(client):
    from src.transfer.connector_registry import run_probe

    my, pg = _endpoint("mysql"), _endpoint("postgresql")
    closed = _closed_port()
    for driver, tile, ep in (("mysql", "amazon_aurora", my),
                             ("postgresql", "amazon_aurora_postgresql", pg)):
        ok, msg = run_probe(driver, {**ep, "port": closed, "type": tile, "ssl": False})
        assert not ok and msg.startswith("Connection refused"), (tile, msg)
        assert "Cannot reach" not in msg, msg
        ok, msg = run_probe(driver, {**ep, "password": "definitely-wrong", "type": tile,
                                     "ssl": False})
        assert not ok and msg.startswith("Authentication failed"), (tile, msg)
        assert "Driver:" in msg, msg
        ok, msg = run_probe(driver, {**ep, "host": "no-such-host.invalid", "type": tile,
                                     "ssl": False})
        assert not ok and "DNS" in msg, (tile, msg)

    ok, msg = run_probe("mysql", {**my, "port": _serve(_mysql_requiring_tls),
                                  "type": "amazon_aurora", "ssl": False})
    assert not ok and msg.startswith("TLS mismatch"), msg
    assert "server requires TLS" in msg and "Turn SSL on" in msg, msg


def test_humaniser_classifies_real_driver_exceptions():
    """The exception each driver really raises, straight into the humaniser."""
    import psycopg2
    import pymysql

    from src.transfer.connector_registry import humanize_connection_error

    def raised(fn) -> Exception:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            return exc
        raise AssertionError("connect unexpectedly succeeded")

    closed, silent = _closed_port(), _serve(_silent)
    cases = {
        "mysql": {
            "Connection refused": lambda: pymysql.connect(
                host="127.0.0.1", port=closed, user="u", password="p", connect_timeout=2),
            "DNS lookup failed": lambda: pymysql.connect(
                host="no-such-host.invalid", port=3306, user="u", password="p", connect_timeout=2),
            "Handshake timed out": lambda: pymysql.connect(
                host="127.0.0.1", port=silent, user="u", password="p",
                connect_timeout=2, read_timeout=1),
        },
        "postgresql": {
            "Connection refused": lambda: psycopg2.connect(
                host="127.0.0.1", port=closed, user="u", password="p", dbname="d",
                connect_timeout=2),
            "DNS lookup failed": lambda: psycopg2.connect(
                host="no-such-host.invalid", port=5432, user="u", password="p", dbname="d",
                connect_timeout=2),
            "Connection timed out": lambda: psycopg2.connect(
                host="127.0.0.1", port=silent, user="u", password="p", dbname="d",
                connect_timeout=2),
        },
    }
    for driver, expected in cases.items():
        for prefix, connect in expected.items():
            exc = raised(connect)
            for tile in (driver, "amazon_aurora" if driver == "mysql" else "amazon_aurora_postgresql"):
                msg = humanize_connection_error(tile, exc)
                assert msg.startswith(prefix), (tile, prefix, str(exc), msg)
                assert "Driver:" in msg, msg
                assert "Cannot reach the host" not in msg, msg


@pytest.mark.parametrize("raw, prefix", [
    ('could not translate host name "db.example" to address: Temporary failure in name resolution',
     "DNS lookup failed"),
    ("(2003, \"Can't connect to MySQL server on '10.0.0.9' ([Errno 113] No route to host)\")",
     "Network unreachable"),
    ("(2003, \"Can't connect to MySQL server on '10.0.0.9' (timed out)\")",
     "Connection timed out"),
    ("(2026, \"SSL connection error: SSL is required but the server doesn't support it\")",
     "TLS mismatch"),
])
def test_humaniser_driver_texts_seen_off_this_host(raw, prefix):
    """Network failures the local sandbox cannot produce (no routed blackhole)."""
    from src.transfer.connector_registry import humanize_connection_error

    msg = humanize_connection_error("amazon_aurora", raw)
    assert msg.startswith(prefix), msg
    assert "Driver:" in msg, msg


def test_store_keeps_the_legacy_5432_heal_for_drivers_that_never_listen_there():
    """Redis/Elasticsearch/Neo4j stamped 5432 still dial their own port (unchanged)."""
    from services.connector_store import default_ssl, listen_port_for_connector

    assert listen_port_for_connector("redis", 5432) == 6379
    assert listen_port_for_connector("elasticsearch", 5432) == 9200
    assert listen_port_for_connector("amazon_aurora", 5432) == 5432
    assert listen_port_for_connector("mysql", 5432) == 5432
    assert listen_port_for_connector("amazon_aurora", None) == 3306
    assert default_ssl("amazon_aurora_postgresql") is False
    assert default_ssl("postgresql") is False
    assert default_ssl("amazon_aurora") is True  # MySQL family keeps its historical default
