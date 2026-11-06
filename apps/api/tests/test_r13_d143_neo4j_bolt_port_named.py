"""D143: a Neo4j connector on a Bolt port says "use the HTTP port", not RemoteDisconnected.

QA: test_connector on the Bolt port failed "RemoteDisconnected"; only the HTTP
port 7474 worked. e369c11b2 maps the literal 7687 to 7474, but a Bolt port that
is not 7687 (a tunnel or a remapped container port) was still dialed as HTTP,
and the Bolt listener drops the request. The stub below is a Bolt listener: it
answers the Bolt handshake and closes an HTTP request without a reply.
"""

from __future__ import annotations

import socket
import threading

import pytest

from connectors.neo4j import test_connection as neo4j_test_connection

_MAGIC = bytes.fromhex("6060B017")


@pytest.fixture
def bolt_port():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    stop = threading.Event()

    def serve():
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except OSError:
                continue
            with conn:
                conn.settimeout(2)
                try:
                    head = conn.recv(20)
                except OSError:
                    continue
                if head.startswith(_MAGIC):
                    conn.sendall(bytes.fromhex("00000005"))  # agreed Bolt 5.0
                # anything else (an HTTP request): close without a reply

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    yield srv.getsockname()[1]
    stop.set()
    srv.close()


def test_a_bolt_port_is_named_and_the_http_port_asked_for(bolt_port):
    ok, msg = neo4j_test_connection(host="127.0.0.1", port=bolt_port, username="neo4j", password="pw")
    assert not ok
    assert "RemoteDisconnected" not in msg
    assert f"127.0.0.1:{bolt_port} is a Neo4j Bolt port" in msg
    assert "7474" in msg and "HTTP" in msg


def test_an_unreachable_http_port_says_the_connector_speaks_http_not_bolt():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    free = s.getsockname()[1]
    s.close()
    ok, msg = neo4j_test_connection(host="127.0.0.1", port=free, username="neo4j", password="pw")
    assert not ok
    assert f"HTTP API at 127.0.0.1:{free}" in msg
    assert "7474" in msg and "7687" in msg


def test_speaks_bolt_is_false_for_an_http_reply():
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from services.wire_protocol_sniff import speaks_bolt

    class H(BaseHTTPRequestHandler):
        def log_message(self, *_a):
            pass

        def handle(self):  # answer anything with an HTTP status line at once
            self.request.recv(64)
            self.request.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        assert speaks_bolt("127.0.0.1", srv.server_address[1]) is False
    finally:
        srv.shutdown()
