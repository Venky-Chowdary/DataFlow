"""Rogue-server LOCAL INFILE: only bulk writes may send a file, and only the staged one."""

from __future__ import annotations

import os
import socket
import struct
import sys
import threading
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pymysql = pytest.importorskip("pymysql")

from connectors import mysql_conn  # noqa: E402
from connectors.mysql_conn import (  # noqa: E402
    LOCAL_INFILE_PURPOSES,
    get_connection,
    local_infile_allowed,
)
from connectors.mysql_load_data import execute_load_data_local  # noqa: E402

_CLIENT_LOCAL_FILES = 0x80
_TRIGGER = "SELECT 'df_rogue_probe'"


def _packet(seq: int, payload: bytes) -> bytes:
    return struct.pack("<I", len(payload))[:3] + bytes([seq & 0xFF]) + payload


def _read_packet(sock: socket.socket) -> tuple[int, bytes] | None:
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    length = int.from_bytes(header[:3], "little")
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return header[3], body


_OK = b"\x00\x00\x00\x02\x00\x00\x00"


class RogueMySQLServer:
    """Answers every query with OK; ``_TRIGGER`` gets a LOAD LOCAL request for ``target``."""

    def __init__(self, target: str) -> None:
        self.target = target.encode()
        self.received = bytearray()
        self.load_requests = 0
        self.client_caps = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _handshake(self) -> bytes:
        caps = 0x1 | 0x8 | _CLIENT_LOCAL_FILES | 0x200 | 0x2000 | 0x8000 | 0x80000
        salt = b"abcdefghijklmnopqrst"
        return (
            b"\x0a" + b"8.0.0-rogue\x00" + struct.pack("<I", 1) + salt[:8] + b"\x00"
            + struct.pack("<H", caps & 0xFFFF) + b"\x2d" + struct.pack("<H", 2)
            + struct.pack("<H", caps >> 16) + bytes([21]) + b"\x00" * 10
            + salt[8:] + b"\x00" + b"mysql_native_password\x00"
        )

    def _serve(self) -> None:
        conn, _ = self._sock.accept()
        conn.settimeout(10)
        try:
            conn.sendall(_packet(0, self._handshake()))
            got = _read_packet(conn)
            if got is None:
                return
            self.client_caps = struct.unpack("<I", got[1][:4])[0]
            conn.sendall(_packet(got[0] + 1, _OK))
            while True:
                got = _read_packet(conn)
                if got is None or got[1][:1] == b"\x01":
                    return
                command, text = got[1][:1], got[1][1:].decode("utf-8", "replace")
                if command == b"\x03" and text == _TRIGGER:
                    self.load_requests += 1
                    conn.sendall(_packet(1, b"\xfb" + self.target))
                    seq = 2
                    while True:
                        data = _read_packet(conn)
                        if data is None or data[0] == 0:
                            # seq 0 is a new command (e.g. COM_QUIT), not file data.
                            return
                        seq = data[0] + 1
                        if not data[1]:
                            break
                        self.received.extend(data[1])
                    conn.sendall(_packet(seq, _OK))
                else:
                    conn.sendall(_packet(1, _OK))
        except (OSError, socket.timeout):
            return
        finally:
            conn.close()

    def close(self) -> None:
        self._sock.close()
        self._thread.join(timeout=5)


@pytest.fixture()
def secret_file(tmp_path: Path) -> Path:
    path = tmp_path / "connector_store.json"
    path.write_text("SENTINEL-TENANT-SECRET")
    return path


@pytest.fixture(autouse=True)
def _no_dial_or_session_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    import connectors.write_resilience as wr
    import services.egress_guard as eg

    monkeypatch.setattr(eg, "assert_connector_dial_allowed", lambda *_a, **_k: None)
    monkeypatch.setattr(wr, "apply_mysql_session_guards", lambda *_a, **_k: None)


def _connect(port: int, purpose: str):
    return get_connection(
        host="127.0.0.1",
        port=port,
        database="dataflow",
        username="mallory",
        password="x",
        connection_string="",
        ssl=False,
        purpose=purpose,
    )


@pytest.mark.parametrize(
    "purpose", ["read", "probe", "reconcile", "ddl", "setup", "drop", "introspect"]
)
def test_non_bulk_purposes_do_not_advertise_or_send_local_files(purpose, secret_file):
    server = RogueMySQLServer(str(secret_file))
    try:
        conn = _connect(server.port, purpose)
        assert not conn._local_infile
        assert not server.client_caps & _CLIENT_LOCAL_FILES
        with pytest.raises(Exception):
            conn.cursor().execute(_TRIGGER)
        conn.close()
    finally:
        server.close()
    assert server.load_requests == 1
    assert bytes(server.received) == b""


@pytest.mark.parametrize("purpose", sorted(LOCAL_INFILE_PURPOSES))
def test_bulk_write_refuses_a_file_it_did_not_stage(purpose, secret_file):
    server = RogueMySQLServer(str(secret_file))
    try:
        conn = _connect(server.port, purpose)
        assert conn._local_infile
        with pytest.raises(pymysql.err.OperationalError, match="did not stage"):
            conn.cursor().execute(_TRIGGER)
        # Protocol stays in sync after the refusal.
        conn.cursor().execute("SELECT 1")
        conn.close()
    finally:
        server.close()
    assert server.load_requests == 1
    assert b"SENTINEL" not in bytes(server.received)
    assert bytes(server.received) == b""


def test_bulk_write_refuses_other_path_while_a_staging_file_is_allowed(
    secret_file, tmp_path
):
    staged = tmp_path / "df_mysql_ld_stage.tsv"
    staged.write_text("1\tok\n")
    server = RogueMySQLServer(str(secret_file))
    try:
        conn = _connect(server.port, "write")
        with local_infile_allowed(conn, str(staged)):
            with pytest.raises(pymysql.err.OperationalError, match="did not stage"):
                conn.cursor().execute(_TRIGGER)
        conn.close()
    finally:
        server.close()
    assert bytes(server.received) == b""


def test_bulk_write_sends_only_the_staged_file_and_only_inside_the_block(tmp_path):
    staged = tmp_path / "df_mysql_ld_stage.tsv"
    staged.write_text("1\tok\n")
    server = RogueMySQLServer(str(staged))
    try:
        conn = _connect(server.port, "write")
        with local_infile_allowed(conn, str(staged)):
            conn.cursor().execute(_TRIGGER)
        assert bytes(server.received) == b"1\tok\n"
        with pytest.raises(pymysql.err.OperationalError, match="did not stage"):
            conn.cursor().execute(_TRIGGER)
        conn.close()
    finally:
        server.close()
    assert server.load_requests == 2
    assert bytes(server.received) == b"1\tok\n"


def test_execute_load_data_local_allows_only_its_own_path(tmp_path):
    staged = tmp_path / "df_mysql_ld_stage.tsv"
    staged.write_text("x\n")

    class _Conn:
        pass

    seen: list[set] = []

    class _Cursor:
        connection = _Conn()

        def execute(self, sql):
            assert f"LOCAL INFILE '{staged}'" in sql
            seen.append(set(self.connection._df_local_infile_allowed))

    cur = _Cursor()
    execute_load_data_local(cur, path=str(staged), table_q="`t`", columns=["a"])
    assert seen == [{os.fsencode(str(staged))}]
    assert cur.connection._df_local_infile_allowed == set()


def test_legacy_load_local_file_hook_is_guarded(monkeypatch, secret_file):
    """PyMySQL 1.1.0 sends via ``LoadLocalFile.send_data``, not ``_send_local_file``."""
    import types

    writes: list[bytes] = []

    class LoadLocalFile:
        def __init__(self, filename, connection):
            self.filename = filename
            self.connection = connection

        def send_data(self):
            with open(self.filename, "rb") as fh:
                self.connection.write_packet(fh.read())
            self.connection.write_packet(b"")

    fake = types.SimpleNamespace(LoadLocalFile=LoadLocalFile)
    import pymysql as real_pymysql

    monkeypatch.setattr(real_pymysql, "connections", fake, raising=False)
    monkeypatch.setitem(sys.modules, "pymysql.connections", fake)
    monkeypatch.setattr(mysql_conn, "_guard_installed", None)
    assert mysql_conn._install_local_infile_guard() is True

    conn = types.SimpleNamespace(_closed=False, write_packet=writes.append)
    with pytest.raises(real_pymysql.err.OperationalError, match="did not stage"):
        fake.LoadLocalFile(os.fsencode(str(secret_file)), conn).send_data()
    assert writes == [b""]

    writes.clear()
    with local_infile_allowed(conn, str(secret_file)):
        fake.LoadLocalFile(os.fsencode(str(secret_file)), conn).send_data()
    assert writes == [b"SENTINEL-TENANT-SECRET", b""]


def test_unknown_pymysql_hook_keeps_local_infile_off(monkeypatch):
    import types

    fake = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "pymysql.connections", fake)
    import pymysql as real_pymysql

    monkeypatch.setattr(real_pymysql, "connections", fake, raising=False)
    monkeypatch.setattr(mysql_conn, "_guard_installed", None)
    assert mysql_conn._install_local_infile_guard() is False
    captured: dict = {}
    monkeypatch.setattr(
        real_pymysql, "connect", lambda **kw: captured.update(kw) or types.SimpleNamespace()
    )
    _connect(3306, "write")
    assert captured["local_infile"] is False
