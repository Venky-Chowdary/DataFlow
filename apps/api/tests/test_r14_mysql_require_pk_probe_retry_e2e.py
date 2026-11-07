"""QA MySQL D2 follow-up: one dropped connection must not become a blocking G6.

The sql_require_primary_key probe opens its own connection to the destination.
A single transient failure (the server drops the handshake, a connect times
out) used to come back as "unreadable", which Validate reports as a blocking
finding on a destination that is fine. The probe now retries once on a
transient connect failure; a refusal that will not change (egress policy, a
malformed setting) is still reported at once.

The live half puts a TCP forwarder in front of a PRIVATE MySQL whose
``sql_require_primary_key`` is ON (restored in a finally block). The
forwarder drops the first connection and proxies the rest
(``tests.r4_route_harness.globals_mysql_server``; never the shared :3306).
"""

from __future__ import annotations

import socket
import threading

from tests import r4_route_harness as h


class _DropFirstForwarder:
    """Accepts on 127.0.0.1:<free>, closes the first ``drops`` connections, proxies the rest."""

    def __init__(self, target_host: str, target_port: int, drops: int = 1) -> None:
        self.target = (target_host, target_port)
        self.drops = drops
        self.accepted = 0
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self._stop = False
        threading.Thread(target=self._serve, daemon=True).start()

    def _pipe(self, a: socket.socket, b: socket.socket) -> None:
        try:
            while True:
                data = a.recv(65536)
                if not data:
                    break
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.close()
                except OSError:
                    pass

    def _serve(self) -> None:
        while not self._stop:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            self.accepted += 1
            if self.accepted <= self.drops:
                client.close()
                continue
            upstream = socket.create_connection(self.target, timeout=5)
            threading.Thread(target=self._pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pipe, args=(upstream, client), daemon=True).start()

    def close(self) -> None:
        self._stop = True
        self.sock.close()


def test_one_dropped_connection_does_not_make_the_variable_unreadable():
    from services.dest_require_primary_key import server_requires_primary_key

    srv = h.globals_mysql_server()
    root = h.globals_mysql_root()
    fwd = _DropFirstForwarder(srv["host"], srv["port"], drops=1)
    try:
        with root.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.sql_require_primary_key")
            before = cur.fetchone()[0]
            cur.execute("SET GLOBAL sql_require_primary_key = ON")
        try:
            got = server_requires_primary_key({**srv, "host": "127.0.0.1", "port": fwd.port})
            assert fwd.accepted >= 2, fwd.accepted
            assert got.unreadable == "", got
            assert got.variable == "sql_require_primary_key", got
        finally:
            with root.cursor() as cur:
                cur.execute(f"SET GLOBAL sql_require_primary_key = {int(before)}")
    finally:
        fwd.close()
        root.close()


def test_a_server_that_stays_down_is_still_reported_unreadable():
    from services.dest_require_primary_key import server_requires_primary_key

    srv = h.globals_mysql_server()
    fwd = _DropFirstForwarder(srv["host"], srv["port"], drops=10)
    try:
        got = server_requires_primary_key({**srv, "host": "127.0.0.1", "port": fwd.port})
        assert got.unreadable.startswith("could not connect"), got
        assert fwd.accepted == 2, fwd.accepted  # one try, one retry, no more
    finally:
        fwd.close()


def test_a_deterministic_refusal_is_not_retried(monkeypatch):
    from connectors import mysql_conn
    from services import dest_require_primary_key as rpk
    from services.egress_guard import EgressRefused

    calls = []

    def _refuse(**kwargs):
        calls.append(kwargs)
        raise EgressRefused("blocked by policy")

    monkeypatch.setattr(mysql_conn, "get_connection", _refuse)
    got = rpk.server_requires_primary_key({"host": "db.example", "port": 3306})
    assert got.unreadable.startswith("could not connect"), got
    assert len(calls) == 1, calls
