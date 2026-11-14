"""D354 (QA round 38): Oracle took 122 s to give up on an endpoint that accepts and never answers.

python-oracledb (thin) applies ``tcp_connect_timeout`` to the TCP connect only
and clears the socket timeout before the TNS handshake, so a connection Test
against a tunnel or proxy that accepted the connection and stayed silent ran
until the 120 s tool deadline (PostgreSQL and SQL Server give up at the 15 s
connect timeout). The handshake now ends at the same connect timeout.

The black hole is a local socket that accepts and never sends a byte:
python-oracledb connects to it like to any listener; no Oracle server needed.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

pytestmark = pytest.mark.timeout(120)

oracledb = pytest.importorskip("oracledb")

_SLACK = 4.0


@pytest.fixture(autouse=True)
def _loopback_egress(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")


@pytest.fixture
def black_hole():
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


def _cfg(port: int, **extra) -> dict:
    return {"type": "oracle", "host": "127.0.0.1", "port": port, "database": "FREEPDB1",
            "username": "u", "password": "p", "ssl": False, **extra}


def test_oracle_connection_test_gives_up_at_the_connect_timeout(black_hole):
    """The router's path: run_probe on the connector executor under the tool deadline."""
    from src.ai.copilot.tool_deadline import run_with_deadline
    from src.transfer.connector_registry import run_probe

    started = time.monotonic()
    ok, msg = run_with_deadline(
        "test_connector",
        lambda: run_probe("oracle", _cfg(black_hole, connect_timeout=2)),
        lambda message: (False, f"DEADLINE: {message}"),
        seconds=30,
    )
    took = time.monotonic() - started
    assert not ok
    assert took < 2 + _SLACK, (took, msg)
    # W3 error taxonomy: the copy names the connection, never its host:port.
    assert msg.startswith("Handshake timed out: the server accepted "
                          "the TCP connection but did not answer the database handshake "
                          "within 2 seconds"), msg
    assert str(black_hole) not in msg.split(" Driver: ")[0], msg


def test_oracle_engine_connect_outside_a_tool_call_is_bounded_too(black_hole):
    """A transfer's engine connect (no tool call) raises at the connect timeout."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from services import statement_budget
    from src.transfer.connector_registry import humanize_connection_error

    statement_budget.install()
    engine = _engine(_cfg(black_hole, connect_timeout=2))
    outcome: dict = {}

    def connect():
        try:
            with engine.connect():
                outcome["connected"] = True
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = exc

    started = time.monotonic()
    worker = threading.Thread(target=connect, daemon=True)
    worker.start()
    worker.join(2 + _SLACK)
    took = time.monotonic() - started
    assert not worker.is_alive(), "the Oracle handshake against a silent peer was not bounded"
    error = outcome.get("error")
    assert isinstance(error, sa.exc.OperationalError), outcome
    assert isinstance(error.orig, oracledb.OperationalError), error
    assert ("the server accepted the TCP connection but did not answer the Oracle "
            "handshake within 2 seconds") in str(error), error
    assert took < 2 + _SLACK
    assert humanize_connection_error("oracle", error).startswith("Handshake timed out"), error
    engine.dispose()


def test_budget_is_the_connect_timeout_and_defaults_to_the_platform_default():
    from connectors.generic_sql import DEFAULT_CONNECT_TIMEOUT_SECONDS
    from services.oracle_handshake_bound import _budget_seconds

    assert _budget_seconds({"tcp_connect_timeout": 7}) == 7.0
    assert _budget_seconds({}) == float(DEFAULT_CONNECT_TIMEOUT_SECONDS) == 15.0


class _Dialect:
    """A dialect whose connect dials the given port and waits for a greeting."""

    class loaded_dbapi:  # noqa: N801 - mirrors the DBAPI module attribute
        OperationalError = oracledb.OperationalError

    def __init__(self, port: int, greet: bool = False) -> None:
        self.port = port
        self.greet = greet

    def connect(self, **_kw):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        sock.settimeout(None)  # what python-oracledb does before the handshake
        if not self.greet:
            if not sock.recv(1):
                sock.close()
                raise oracledb.OperationalError("DPY-4011: closed")
        return sock


def test_a_connect_that_finishes_inside_the_budget_is_returned_untouched(black_hole):
    from services.oracle_handshake_bound import connect_within_timeout

    conn = connect_within_timeout(_Dialect(black_hole, greet=True), (), {"tcp_connect_timeout": 1})
    time.sleep(1.5)  # past the budget: the timer was cancelled, the socket is intact
    conn.settimeout(0.2)
    with pytest.raises(socket.timeout):
        conn.recv(1)  # still connected (a shut socket would return b"")
    conn.close()


def test_a_silent_handshake_is_ended_at_the_budget(black_hole):
    from services.oracle_handshake_bound import connect_within_timeout

    started = time.monotonic()
    with pytest.raises(oracledb.OperationalError, match="did not answer the Oracle handshake"):
        connect_within_timeout(_Dialect(black_hole), (), {"tcp_connect_timeout": 1,
                                                          "dsn": f"(HOST=127.0.0.1)(PORT={black_hole})"})
    assert time.monotonic() - started < 1 + _SLACK
