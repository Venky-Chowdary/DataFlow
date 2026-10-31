"""Name the protocol a port really speaks when a SQL handshake fails.

QA round 11: an ``amazon_aurora`` connector (Aurora MySQL) pointed at a
PostgreSQL port waited for a MySQL greeting that never came and reported
"Cannot reach host" — the host was reachable; the protocol was wrong.

The two wires are told apart without logging in:

* MySQL speaks first: the server sends its handshake greeting as soon as the
  TCP connection opens (protocol version byte ``10`` after a 4-byte header).
* PostgreSQL never speaks first. Sent an ``SSLRequest`` it answers exactly one
  byte, ``S`` or ``N``.

The sniff runs only after a failed probe, opens one connection to the address
the operator configured (the caller already holds egress approval for it),
sends at most eight bytes and reads at most a few.
"""

from __future__ import annotations

import socket
import struct

from src.transfer.engine_family import MYSQL, POSTGRESQL, engine_family

_SSL_REQUEST = struct.pack("!ii", 8, 80877103)
_MYSQL_PROTOCOL_V10 = 10


def sniff_wire(host: str, port: int, *, timeout: float = 2.0) -> str:
    """``"postgresql"``, ``"mysql"`` or ``""`` when the port's wire is unknown."""
    if not host or not port:
        return ""
    try:
        sock = socket.create_connection((host, int(port)), timeout=timeout)
    except OSError:
        return ""
    try:
        sock.settimeout(min(0.5, timeout))
        try:
            greeting = sock.recv(5)
        except socket.timeout:
            greeting = b""
        except OSError:
            return ""
        if greeting:
            if len(greeting) == 5 and greeting[4] == _MYSQL_PROTOCOL_V10:
                return MYSQL
            return ""
        sock.settimeout(timeout)
        sock.sendall(_SSL_REQUEST)
        answer = sock.recv(2)
        return POSTGRESQL if answer in (b"S", b"N") else ""
    except OSError:
        return ""
    finally:
        try:
            sock.close()
        except OSError:
            pass


_LABEL = {POSTGRESQL: "PostgreSQL", MYSQL: "MySQL"}
_TILE = {POSTGRESQL: "Amazon Aurora PostgreSQL", MYSQL: "Amazon Aurora MySQL"}


def protocol_mismatch_message(kind: str, host: str, port: int) -> str:
    """Operator copy when ``kind``'s handshake reached a port of the other wire.

    Empty when ``kind`` is not a MySQL- or PostgreSQL-family tile, or when the
    port does not answer as the other one.
    """
    expected = engine_family(kind)
    if expected not in _LABEL:
        return ""
    spoken = sniff_wire(host, port)
    if not spoken or spoken == expected:
        return ""
    tile = (kind or "").strip().lower()
    hint = (
        f" '{tile}' is the {_LABEL[expected]} tile; for {_LABEL[spoken]} pick the "
        f"{_TILE[spoken]} tile"
        if "aurora" in tile
        else f" Pick a {_LABEL[spoken]} connector type"
    )
    return (
        f"Reached {host}:{port}, but this port speaks the {_LABEL[spoken]} protocol, "
        f"not {_LABEL[expected]}.{hint}, or point this connector at the "
        f"{_LABEL[expected]} port."
    )
