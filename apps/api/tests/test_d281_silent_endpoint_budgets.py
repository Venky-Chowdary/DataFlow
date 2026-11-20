"""D281 / D353 / D354: a silent or black-holed endpoint fails inside its budget.

Two local stand-ins for a dead tunnel (the sandbox proxy turns a routed black
hole into "refused", so these are emulated on loopback):

* **silent**: a listener that accepts the TCP connection and never sends a
  byte (a tunnel or proxy that accepts and does not forward);
* **black hole**: a listener whose accept backlog is full, so the kernel drops
  every further SYN and the client's connect never completes.

Rule R10: every external call has a driver-level connect timeout, and the
database handshake is part of connecting. PyMySQL reads the server greeting
under ``read_timeout``, not ``connect_timeout``, so a silent MySQL host held
the dial for the purpose's socket timeout: 40 s for a schema read, 120 s for a
write and 1800 s for a reconcile, instead of the 10-15 s connect budget.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest


def _silent() -> tuple[socket.socket, int, list]:
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(64)
    held: list = []

    def _accept() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            held.append(conn)

    threading.Thread(target=_accept, daemon=True).start()
    return server, server.getsockname()[1], held


def _black_hole() -> tuple[socket.socket, int, list]:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(0)
    port = server.getsockname()[1]
    fill: list = []
    for _ in range(8):
        client = socket.socket()
        client.setblocking(False)
        try:
            client.connect(("127.0.0.1", port))
        except BlockingIOError:
            pass
        fill.append(client)
    time.sleep(0.3)
    return server, port, fill


def _dial_mysql(port: int, purpose: str) -> tuple[float, str]:
    from connectors.mysql_conn import get_connection

    started = time.monotonic()
    try:
        conn = get_connection(
            host="127.0.0.1", port=port, database="x", username="u", password="p",
            connection_string="", ssl=False, purpose=purpose,
        )
        conn.close()
        error = ""
    except Exception as exc:  # noqa: BLE001 - the error text is the assertion
        error = str(exc)
    return time.monotonic() - started, error


@pytest.mark.parametrize("purpose, budget", [("introspect", 10), ("write", 15)])
def test_a_silent_mysql_host_fails_at_the_connect_budget(purpose, budget) -> None:
    server, port, _held = _silent()
    try:
        elapsed, error = _dial_mysql(port, purpose)
    finally:
        server.close()
    assert error, "a silent server must not log in"
    assert elapsed < budget + 4, (elapsed, error)


def test_the_restored_socket_wait_still_applies_after_login(monkeypatch) -> None:
    """The handshake is bounded by connect; queries keep the purpose's wait."""
    from connectors import mysql_conn

    seen: dict = {}

    class _Conn:
        _read_timeout = None
        _write_timeout = None

        def close(self) -> None:
            pass

    def _fake_dial(kwargs, **_kw):
        seen["handshake"] = (kwargs["read_timeout"], kwargs["write_timeout"])
        return _Conn()

    monkeypatch.setattr(mysql_conn, "dial_pymysql", _fake_dial)
    monkeypatch.setattr(
        "connectors.write_resilience.apply_mysql_session_guards", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "services.egress_guard.assert_connector_dial_allowed", lambda *a, **k: None
    )
    conn = mysql_conn.get_connection(
        host="db.example.com", port=3306, database="x", username="u", password="p",
        connection_string="", ssl=False, purpose="reconcile",
    )
    assert seen["handshake"] == (15, 15)
    assert (conn._read_timeout, conn._write_timeout) == (1800, 1800)


def test_a_black_holed_mysql_host_fails_at_the_connect_budget() -> None:
    server, port, _fill = _black_hole()
    try:
        elapsed, error = _dial_mysql(port, "write")
    finally:
        server.close()
    assert error
    assert elapsed < 15 + 4, (elapsed, error)


def _oracle_connect(port: int, seconds: float) -> tuple[float, str]:
    oracledb = pytest.importorskip("oracledb")
    from types import SimpleNamespace

    from services.oracle_handshake_bound import connect_within_timeout

    dialect = SimpleNamespace(connect=oracledb.connect, loaded_dbapi=oracledb)
    dsn = (
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT={port}))"
        "(CONNECT_DATA=(SERVICE_NAME=XEPDB1)))"
    )
    started = time.monotonic()
    try:
        connect_within_timeout(
            dialect, (), {"user": "u", "password": "p", "dsn": dsn,
                          "tcp_connect_timeout": seconds, "retry_count": 0},
        )
        error = ""
    except Exception as exc:  # noqa: BLE001 - the error text is the assertion
        error = str(exc)
    return time.monotonic() - started, error


def test_a_silent_oracle_host_ends_at_the_connect_budget_as_a_handshake_timeout() -> None:
    server, port, _held = _silent()
    try:
        elapsed, error = _oracle_connect(port, 3)
    finally:
        server.close()
    assert elapsed < 3 + 3, (elapsed, error)
    assert "accepted the TCP connection but did not answer" in error, error


def test_a_black_holed_oracle_host_is_a_connect_timeout_not_an_accepted_one() -> None:
    from services.error_taxonomy import classify

    server, port, _fill = _black_hole()
    try:
        elapsed, error = _oracle_connect(port, 3)
    finally:
        server.close()
    assert elapsed < 3 + 3, (elapsed, error)
    assert "accepted" not in error, error
    assert classify(error, engine="oracle").code == "network.connect_timeout", error


def _sqlserver_on(driver: str, monkeypatch) -> None:
    """Pin the SQL Server DBAPI the product picks (production ships pymssql)."""
    from connectors import generic_sql
    from services import engine_pool

    if driver == "pymssql":
        pytest.importorskip("pymssql")
        monkeypatch.setattr(generic_sql, "_mssql_odbc_driver", lambda: None)
    elif not generic_sql._mssql_odbc_driver():
        pytest.skip("Microsoft ODBC Driver 17/18 for SQL Server is not installed here")
    assert generic_sql._mssql_drivername() == f"mssql+{driver}"
    engine_pool.dispose_all()


@pytest.mark.parametrize("driver", ["pymssql", "pyodbc"])
def test_a_silent_sqlserver_host_fails_at_the_connect_budget_on_every_driver(
    driver, monkeypatch
) -> None:
    """D281 / R10 on the SQL Server leg CI runs (api-tests has no Microsoft driver).

    FreeTDS's default protocol version is "auto": after the TDS 7.4 login
    timed out it dialled the host again with a TDS 5.0 (Sybase ASE) login,
    which no SQL Server speaks, and waited about twice more. A 15 s Test
    took ~35 s on every pymssql host (PR #433 CI, api-tests 2/6).
    """
    from connectors.sqlserver import test_sqlserver

    _sqlserver_on(driver, monkeypatch)
    server, port, held = _silent()
    started = time.monotonic()
    try:
        ok, error = test_sqlserver(
            host="127.0.0.1", port=port, database="d", username="u", password="p",
            schema="dbo", connection_string="", ssl=False, connect_timeout=4,
        )
    finally:
        elapsed = time.monotonic() - started
        server.close()
    assert not ok and error, "a silent server must not log in"
    assert elapsed < 4 + 2, (elapsed, error)
    assert len(held) == 1, f"the login dialled the silent host {len(held)} times"


def test_every_direct_pymssql_login_pins_the_sql_server_protocol() -> None:
    """The product's own ``pymssql.connect`` calls get the same one-attempt login."""
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    unpinned = []
    for folder in ("services", "connectors", "src"):
        for path in (root / folder).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if "pymssql.connect(" not in text:
                continue
            for node in ast.walk(ast.parse(text)):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "connect"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "pymssql"
                    and "tds_version" not in {kw.arg for kw in node.keywords}
                ):
                    unpinned.append(f"{path.relative_to(root)}:{node.lineno}")
    assert not unpinned, unpinned


@pytest.fixture
def api(tmp_path, monkeypatch):
    import services.config  # noqa: F401 - settings parse before the harness blanks env
    from services import call_lanes, connector_executor
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    # Many tenants at once: lift the per-workspace cap so the pool fills.
    monkeypatch.setenv("DATAFLOW_CONNECTOR_WORKSPACE_MAX", "64")
    connector_executor.reset_for_tests()
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as client:
        yield client
    connector_executor.reset_for_tests()
    assert call_lanes.snapshot()["connector"]["busy"] == 0


def test_connector_tests_on_a_silent_host_do_not_starve_metadata_routes(api) -> None:
    """D321 AC-1 / D281 AC-2, measured in-process through the real routes.

    Before the lanes, 16 tests against a silent host held the event loop's
    default executor (8 threads on 4 CPUs): ``GET /api/v1/jobs`` took 28.5 s
    and the tests themselves ran 31-33 s against a 15 s budget.
    """
    from services import connector_executor

    server, port, _held = _silent()
    timings: list[float] = []
    lock = threading.Lock()

    def _probe(i: int) -> None:
        kind = ("postgresql", "oracle", "sqlserver")[i % 3]
        started = time.monotonic()
        api.post("/api/v1/connectors/test", json={
            "type": kind, "host": "127.0.0.1", "port": port, "database": "XEPDB1",
            "username": "u", "password": "p",
        })
        with lock:
            timings.append(time.monotonic() - started)

    threads = [threading.Thread(target=_probe, args=(i,)) for i in range(16)]
    try:
        for t in threads:
            t.start()
        time.sleep(3)
        for path in ("/api/v1/jobs", "/api/v1/connectors/saved", "/api/v1/health"):
            started = time.monotonic()
            response = api.get(path)
            assert response.status_code == 200, (path, response.text[:200])
            assert time.monotonic() - started < 1.0, path
        ready = api.get("/health/ready")
        # The pool is full of stuck calls: readiness says so (D321 AC-2).
        assert ready.status_code == 503, ready.text[:300]
        for t in threads:
            t.join(timeout=60)
    finally:
        server.close()
    assert len(timings) == 16
    assert max(timings) < 15 + 6, sorted(timings)
    deadline = time.monotonic() + 10
    while connector_executor.snapshot()["in_flight"] and time.monotonic() < deadline:
        time.sleep(0.2)
    assert connector_executor.snapshot()["in_flight"] == 0
