"""D353 (QA round 38): an endpoint that accepts TCP and never answers read "Network unreachable".

A PostgreSQL or SQL Server connection Test against a proxy / tunnel that took
the TCP connection and never forwarded it was reported as "Network
unreachable". The driver text alone cannot say what happened: libpq's
"timeout expired" and ODBC's "Login timeout expired" (HYT00) read the same for
a dropped SYN and for a silent peer, a dual-stack host adds its unroutable
address's "Network is unreachable" (classified before the timeout), and HYT00
contains "login", which the humaniser read as a refused password.

After a timed-out SQL probe the Test now opens one bare TCP connection to the
configured endpoint: accepted-and-silent, refused and unreachable each get
their own copy. Real local endpoints: a "black hole" that accepts and never
sends a byte, and a closed port.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

pytestmark = pytest.mark.timeout(120)


@pytest.fixture(autouse=True)
def _loopback_egress(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")


@pytest.fixture
def black_hole():
    """Accepts every TCP connection and never sends a byte."""
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    held: list[socket.socket] = []

    def loop():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            held.append(conn)

    threading.Thread(target=loop, daemon=True).start()
    yield listener.getsockname()[1]
    listener.close()
    for conn in held:
        conn.close()


@pytest.fixture
def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _cfg(kind: str, port: int, **extra) -> dict:
    return {"type": kind, "host": "127.0.0.1", "port": port, "database": "d",
            "username": "u", "password": "p", "ssl": False, **extra}


def _assert_silent_copy(msg: str, port: int) -> None:
    # W3 error taxonomy: the copy names the connection, never its host:port.
    del port
    assert msg.startswith("Handshake timed out: the server accepted the "
                          "TCP connection but did not answer the database handshake within "), msg
    assert "proxy, tunnel or firewall" in msg, msg
    for wrong in ("Network unreachable", "Authentication failed", "Connection timed out",
                  "Connection refused"):
        assert wrong not in msg, msg


def test_postgresql_silent_endpoint_says_accepted_but_no_handshake(black_hole):
    """Real libpq against the black hole (connect_timeout capped by the scope)."""
    from src.ai.copilot.tool_deadline import deadline_scope
    from src.transfer.connector_registry import run_probe

    started = time.monotonic()
    with deadline_scope(3):
        ok, msg = run_probe("postgresql", _cfg("postgresql", black_hole))
    assert not ok
    _assert_silent_copy(msg, black_hole)
    assert "timeout expired" in msg, msg  # the driver's own text stays as the cause
    assert time.monotonic() - started < 15


def test_dual_stack_unreachable_address_does_not_hide_a_silent_endpoint(black_hole, monkeypatch):
    """libpq's text for a host with an unroutable AAAA and a silent A record."""
    import connectors.postgresql as pg

    from src.transfer.connector_registry import run_probe

    raw = (
        'connection to server at "db.example" (2001:db8::5), port {p} failed: Network is '
        'unreachable\n\tIs the server running on that host and accepting TCP/IP connections?\n'
        'connection to server at "db.example" (127.0.0.1), port {p} failed: timeout expired\n'
    ).format(p=black_hole)
    monkeypatch.setattr(pg, "test_postgresql", lambda **_kw: (False, raw))
    ok, msg = run_probe("postgresql", _cfg("postgresql", black_hole))
    assert not ok
    _assert_silent_copy(msg, black_hole)


_HYT00 = ("(pyodbc.OperationalError) ('HYT00', '[HYT00] [Microsoft][ODBC Driver 18 for SQL "
          "Server]Login timeout expired (0) (SQLDriverConnect)')")


def test_sqlserver_login_timeout_is_not_an_authentication_failure(black_hole, closed_port,
                                                                 monkeypatch):
    """ODBC says HYT00 for the black hole and for a refused port alike."""
    import connectors.sqlserver as ss

    from src.transfer.connector_registry import humanize_connection_error, run_probe

    monkeypatch.setattr(ss, "test_sqlserver", lambda **_kw: (False, _HYT00))
    ok, msg = run_probe("sqlserver", _cfg("sqlserver", black_hole))
    assert not ok
    _assert_silent_copy(msg, black_hole)
    assert "HYT00" in msg, msg

    ok, msg = run_probe("sqlserver", _cfg("sqlserver", closed_port))
    assert not ok and msg.startswith("Connection refused:"), msg

    # Text-only classification (no endpoint to check): a timeout, never a password.
    text_only = humanize_connection_error("sqlserver", _HYT00)
    assert text_only.startswith("Connection timed out"), text_only


def test_sqlserver_live_odbc_against_the_black_hole(black_hole):
    """The real ODBC driver, when this host has it (it does on the dev box)."""
    pyodbc = pytest.importorskip("pyodbc")
    if not any("SQL Server" in d for d in pyodbc.drivers()):
        pytest.skip("no Microsoft ODBC driver for SQL Server on this host")
    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("sqlserver", _cfg("sqlserver", black_hole, connect_timeout=2))
    assert not ok
    _assert_silent_copy(msg, black_hole)


def test_refused_and_unreachable_keep_their_own_copy(closed_port, monkeypatch):
    import connectors.postgresql as pg
    import services.wire_protocol_sniff as sniff

    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("postgresql", _cfg("postgresql", closed_port))
    assert not ok and msg.startswith("Connection refused:"), msg

    # No route: the TCP check fails the same way, so the driver's class stands.
    raw = 'connection to server at "10.9.9.9", port 5432 failed: No route to host\n'
    monkeypatch.setattr(pg, "test_postgresql", lambda **_kw: (False, raw))

    def no_route(*_a, **_kw):
        raise OSError(113, "No route to host")

    monkeypatch.setattr(sniff.socket, "create_connection", no_route)
    ok, msg = run_probe("postgresql", _cfg("postgresql", 5432))
    assert not ok and msg.startswith("Network unreachable:") and "10.9.9.9" not in msg, msg
