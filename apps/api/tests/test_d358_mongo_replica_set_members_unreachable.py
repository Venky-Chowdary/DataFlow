"""D358: a MongoDB replica set reached through a tunnel was reported as "Connection refused".

The seed host behind the tunnel answers ``hello``, but as a replica-set
member it advertises the set's own (internal) member host names. pymongo
drops the seed, dials those members, cannot reach them, and the
ServerSelectionTimeoutError names only the member's socket error ("[Errno
111] Connection refused" for an internal ``localhost:27017``, a DNS failure
for an internal name). The Test now says the seed answered, names the
members it advertised, and points at ``directConnection=true`` for a single
node behind a tunnel (never added silently) or at exposing the members.

Reproduced first against a real single-node replica set (``mongod --replSet``
initiated with an internal member host that this side cannot reach): the base
Test said "Connection refused" after 20 s, and ``directConnection=true``
pinged. The tests below use a local fake seed that answers the driver's real
``hello`` handshake (OP_QUERY and OP_MSG) as a replica-set primary advertising
``internal-member.invalid:27017``, so the real pymongo discovery path runs in
a couple of seconds without a mongod.
"""

from __future__ import annotations

import datetime as dt
import socket
import struct
import threading

import pytest

bson = pytest.importorskip("bson")
pymongo = pytest.importorskip("pymongo")

pytestmark = pytest.mark.timeout(120)

_MEMBER = "internal-member.invalid:27017"


@pytest.fixture(autouse=True)
def _loopback_egress(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def _hello() -> dict:
    return {
        "ok": 1.0, "isWritablePrimary": True, "ismaster": True, "helloOk": True,
        "setName": "rs0", "setVersion": 1, "hosts": [_MEMBER], "me": _MEMBER,
        "minWireVersion": 0, "maxWireVersion": 21, "maxBsonObjectSize": 16777216,
        "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000,
        "localTime": dt.datetime.now(dt.timezone.utc), "logicalSessionTimeoutMinutes": 30,
        "connectionId": 1, "readOnly": False,
    }


def _answer(conn: socket.socket) -> None:
    """Answer every hello (legacy OP_QUERY or OP_MSG) as the replica-set primary."""
    try:
        while True:
            length, request_id, _to, opcode = struct.unpack("<iiii", _recv_exact(conn, 16))
            _recv_exact(conn, length - 16)
            doc = bson.encode(_hello())
            if opcode == 2004:  # OP_QUERY -> OP_REPLY
                payload = struct.pack("<iqii", 0, 0, 0, 1) + doc
                conn.sendall(struct.pack("<iiii", 16 + len(payload), 1, request_id, 1) + payload)
            elif opcode == 2013:  # OP_MSG
                payload = struct.pack("<I", 0) + b"\0" + doc
                conn.sendall(struct.pack("<iiii", 16 + len(payload), 1, request_id, 2013) + payload)
            else:
                return
    except (EOFError, OSError):
        pass
    finally:
        conn.close()


@pytest.fixture
def replica_set_seed():
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
            threading.Thread(target=_answer, args=(conn,), daemon=True).start()

    threading.Thread(target=loop, daemon=True).start()
    yield listener.getsockname()[1]
    listener.close()


@pytest.fixture
def quick_selection(monkeypatch):
    """The probe's 10 s server selection, lowered for the test (never raised)."""
    import connectors.mongodb_common as common

    real = common.guarded_mongo_client

    def quick(conn_str, **kwargs):
        kwargs["serverSelectionTimeoutMS"] = 1500
        return real(conn_str, **kwargs)

    monkeypatch.setattr(common, "guarded_mongo_client", quick)
    yield
    for conn_str in list(common._mongo_client_cache):
        if "127.0.0.1" in conn_str:
            common.close_mongo_client(conn_str)


def test_tunnelled_replica_set_names_its_unreachable_members(replica_set_seed, quick_selection):
    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("mongodb", {"type": "mongodb", "host": "127.0.0.1",
                                    "port": replica_set_seed, "database": "d358"})
    assert not ok
    assert msg.startswith("MongoDB replica set 'rs0': the seed 127.0.0.1:"), msg
    assert f"advertises its members as {_MEMBER}" in msg, msg
    assert "directConnection=true" in msg, msg
    assert "Connection refused" not in msg and "DNS lookup failed" not in msg, msg


def _description(set_name: str, servers: dict):
    from pymongo.errors import AutoReconnect
    from pymongo.server_description import ServerDescription
    from pymongo.topology_description import TOPOLOGY_TYPE, TopologyDescription

    try:  # pymongo >= 4.9
        from pymongo.synchronous.settings import TopologySettings
    except ImportError:  # pragma: no cover - older pymongo
        from pymongo.settings import TopologySettings

    sds = {
        address: ServerDescription(address, error=AutoReconnect(f"{address[0]}:{address[1]}: {err}"))
        for address, err in servers.items()
    }
    return TopologyDescription(TOPOLOGY_TYPE.ReplicaSetNoPrimary, sds, set_name, None, None,
                               TopologySettings())


class _Client:
    def __init__(self, description) -> None:
        self.topology_description = description


def test_classifier_from_pymongo_topology_objects():
    from pymongo.errors import ServerSelectionTimeoutError

    from connectors.mongodb_common import replica_set_members_unreachable

    timeout = ServerSelectionTimeoutError("Could not reach any servers")
    tunnel = "mongodb://user:secret@localhost:27018/app?authSource=admin"
    members = _description("rs0", {
        ("mongo-0.internal", 27017): "[Errno 111] Connection refused",
        ("mongo-1.internal", 27017): "[Errno -2] Name or service not known",
    })
    msg = replica_set_members_unreachable(_Client(members), tunnel, timeout)
    assert msg.startswith("MongoDB replica set 'rs0': the seed localhost:27018 answered"), msg
    assert "mongo-0.internal:27017, mongo-1.internal:27017" in msg, msg
    assert "directConnection=true" in msg and "secret" not in msg and "user" not in msg, msg

    # The seed itself is the unreachable server (replicaSet named in the URI,
    # nothing answered): that is not this case, the driver's own class stands.
    seed_only = _description("rs0", {("localhost", 27018): "[Errno 111] Connection refused"})
    assert replica_set_members_unreachable(
        _Client(seed_only), "mongodb://localhost:27018/?replicaSet=rs0", timeout) == ""
    # Already a direct connection, another error type, or no topology: unchanged.
    assert replica_set_members_unreachable(
        _Client(members), "mongodb://localhost:27018/?directConnection=true", timeout) == ""
    assert replica_set_members_unreachable(_Client(members), tunnel, ValueError("x")) == ""
    assert replica_set_members_unreachable(None, tunnel, timeout) == ""
