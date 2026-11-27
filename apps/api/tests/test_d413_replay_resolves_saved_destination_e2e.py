"""D413 — quarantine replay dials the saved destination connector as it is now.

QA r55-r75: a lenient SFTP CSV → PostgreSQL job quarantined two rows; the PG
host then moved and the saved connector was updated (its test passed). Every
replay was refused 422 dns_not_found / network.refused: the replay rebuilt its
request from the parent job's frozen ``transfer_request`` and dialled the old
host. Retry and rerun already refresh endpoints (D172, audit
``job.endpoint_refreshed``); the replay path did not.

Here the "old host" is a live TCP relay in front of the local PostgreSQL: the
parent job writes through it, then the saved connector is repointed straight
at the server. The relay keeps answering (AC-3: the old host still resolves),
and counts every connection it is asked to carry.

* AC-1/AC-3: the replay lands the edited rows on the connector's current
  endpoint, never opens a connection to the old one, and audits the refresh.
* AC-2: a deleted connector refuses the replay by naming the connection;
  nothing is written and the ledger stays open.
* AC-4: a replay that cannot connect names the endpoint it dialled and that it
  came from the saved connection.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import socket
import threading
import uuid

import pytest
from fastapi import HTTPException

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.conftest import pg_test_cfg  # noqa: E402

SOURCE = [
    (11, "alice", "10"),
    (12, "BAD_QTY", "NOTANUMBER"),
    (13, "bob", "20"),
    (14, "BAD_QTY2", "TOO_BIG_99999999999"),
]
FIXES = {12: {"qty": "30"}, 14: {"qty": "40"}}


class _Relay:
    """A TCP relay to the local PostgreSQL: the destination's 'old host'."""

    def __init__(self, target: tuple[str, int]):
        self.target = target
        self.connections = 0
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self._stop = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while not self._stop:
            try:
                client, _ = self._sock.accept()
            except OSError:
                return
            self.connections += 1
            upstream = socket.create_connection(self.target)
            for a, b in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(a: socket.socket, b: socket.socket) -> None:
        try:
            while data := a.recv(65536):
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self) -> None:
        self._stop = True
        self._sock.close()


def _pg():
    import psycopg2

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"],
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"PostgreSQL is required for the D413 replay e2e: {exc}")
    conn.autocommit = True
    return conn


def _sql(statement: str, params=None) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            if params is not None:
                cur.executemany(statement, params)
            else:
                cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class _Req:
    class state:  # noqa: N801 - mimics starlette's Request.state
        pass

    headers: dict = {}


@pytest.fixture()
def parent(tmp_path, monkeypatch):
    """A balanced append that quarantined ids 12 and 14, written via the relay."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.quarantine_dlq as dlq
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: memory)
    router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    audits: list[dict] = []
    import services.audit_log as audit_log

    real_append = audit_log.append_audit_event

    def _capture(**kw):
        audits.append(kw)
        return real_append(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", _capture)

    cfg = pg_test_cfg()
    relay = _Relay((cfg["host"], int(cfg["port"])))
    tag = uuid.uuid4().hex[:8]
    src_table, dst_table = f"d413s_{tag}", f"d413d_{tag}"
    _sql(f'CREATE TABLE "{src_table}" (id INT PRIMARY KEY, name TEXT, qty TEXT)')
    _sql(f'INSERT INTO "{src_table}" VALUES (%s, %s, %s)', SOURCE)  # nosec B608
    _sql(f'CREATE TABLE "{dst_table}" (id INT PRIMARY KEY, name VARCHAR(20), qty INT)')
    dest_conn = connector_store.create_connector({
        "name": f"QAR55-PG-{tag}", "type": "postgresql", "role": "destination",
        "host": "127.0.0.1", "port": relay.port, "database": cfg["database"],
        "username": cfg["username"], "password": cfg["password"], "schema": "public",
        "ssl": False,
    })
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host=cfg["host"], port=int(cfg["port"]),
            database=cfg["database"], username=cfg["username"], password=cfg["password"],
            schema="public", table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", connector_id=dest_conn.id,
            schema="public", table=dst_table,
        ),
        mappings=[{"source": c, "target": c} for c in ("id", "name", "qty")],
        sync_mode="full_refresh_append",
        validation_mode="balanced",
        skip_preflight=True,
    )
    try:
        result = UniversalTransferEngine().execute(request)
        assert result.success, result.error
        job = memory.get_job(result.job_id)
        assert job["status"] == "completed_with_quarantine", (job["status"], job.get("message"))
        stored = (job.get("transfer_request") or {}).get("destination") or {}
        assert int(stored.get("port") or 0) == relay.port, stored  # the frozen endpoint
        assert relay.connections > 0
        yield {
            "job_id": result.job_id, "table": dst_table, "relay": relay, "connector": dest_conn,
            "store": connector_store, "memory": memory, "audits": audits, "cfg": cfg,
        }
    finally:
        relay.close()
        _sql(f'DROP TABLE IF EXISTS "{src_table}"')
        _sql(f'DROP TABLE IF EXISTS "{dst_table}"')


def _replay(job_id: str, *, acknowledge: bool = False) -> dict:
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine

    rows = [{"source_pk": "id", "pk_value": pk, "values": {"id": pk, **fix}} for pk, fix in FIXES.items()]
    body = QuarantineReplayRequest(rows=rows, acknowledge_endpoint_change=acknowledge)
    return asyncio.run(replay_job_quarantine(job_id, body, _Req()))


def _open_findings(job_id: str) -> list:
    from services.quarantine_dlq import canonical_replay_details, open_quarantine_details

    return open_quarantine_details(canonical_replay_details({"id": job_id}))


def test_replay_writes_through_the_connectors_current_endpoint(parent) -> None:
    cfg, relay = parent["cfg"], parent["relay"]
    parent["store"].update_connector(parent["connector"].id, {"port": int(cfg["port"])})
    dialled_before = relay.connections

    # The connector now names another server: refused until the operator says so.
    with pytest.raises(HTTPException) as caught:
        _replay(parent["job_id"])
    assert caught.value.status_code == 409, caught.value.detail
    detail = str(caught.value.detail)
    assert f"127.0.0.1:{cfg['port']}/" in detail and f"127.0.0.1:{relay.port}/" in detail, detail
    assert "acknowledge_endpoint_change" in detail, detail
    assert len(_open_findings(parent["job_id"])) == 2
    assert relay.connections == dialled_before

    resp = _replay(parent["job_id"], acknowledge=True)

    assert resp.get("success") is True and resp.get("rows_written") == 2, resp
    assert relay.connections == dialled_before, "the replay dialled the old endpoint"
    got = dict(_sql(f'SELECT id, qty FROM "{parent["table"]}"'))  # nosec B608
    assert got == {11: 10, 12: 30, 13: 20, 14: 40}, got
    assert _open_findings(parent["job_id"]) == []
    refreshed = [a for a in parent["audits"] if a.get("action") == "job.endpoint_refreshed"]
    assert refreshed, [a.get("action") for a in parent["audits"]]
    change = refreshed[-1]["details"]["changes"][0]
    assert change["role"] == "destination"
    assert change["from"].startswith(f"127.0.0.1:{relay.port}/"), change
    assert change["to"].startswith(f"127.0.0.1:{cfg['port']}/"), change
    assert "password" not in str(refreshed[-1]).lower()
    shown = resp.get("endpoint_refresh") or {}
    assert shown.get("origin") == "saved_connector", resp.keys()
    assert str(shown.get("endpoint") or "").startswith(f"127.0.0.1:{cfg['port']}/"), shown
    child = parent["memory"].get_job(resp["job_id"]) or {}
    assert (child.get("endpoint_refresh") or {}).get("origin") == "saved_connector", child.keys()
    assert int(((child.get("transfer_request") or {}).get("destination") or {}).get("port") or 0) == int(cfg["port"])


def test_replay_after_the_connector_is_deleted_names_it_and_writes_nothing(parent) -> None:
    parent["store"].delete_connector(parent["connector"].id)
    dialled_before = parent["relay"].connections
    with pytest.raises(HTTPException) as caught:
        _replay(parent["job_id"])
    assert caught.value.status_code == 409, caught.value.detail
    detail = str(caught.value.detail)
    assert parent["connector"].id in detail and "no longer exists" in detail, detail
    assert "dns" not in detail.lower() and "host not found" not in detail.lower(), detail
    assert parent["relay"].connections == dialled_before
    got = dict(_sql(f'SELECT id, qty FROM "{parent["table"]}"'))  # nosec B608
    assert got == {11: 10, 13: 20}, got
    assert len(_open_findings(parent["job_id"])) == 2


def test_a_replay_that_cannot_connect_names_the_endpoint_and_its_origin(parent) -> None:
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead_port = closed.getsockname()[1]
    closed.close()  # nothing listens here now
    parent["store"].update_connector(parent["connector"].id, {"port": dead_port})
    with pytest.raises(HTTPException) as caught:
        _replay(parent["job_id"], acknowledge=True)
    detail = str(caught.value.detail)
    assert f"127.0.0.1:{dead_port}" in detail, detail
    assert "saved connection" in detail and parent["connector"].id in detail, detail
    assert len(_open_findings(parent["job_id"])) == 2


def test_a_credential_rotation_replays_without_an_acknowledgement(parent) -> None:
    """Same host and database, new password: the replay uses the saved credential."""
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    cfg, relay = parent["cfg"], parent["relay"]
    # The connector is repointed at the server directly, and the job's stored
    # snapshot names that same endpoint with the password before the rotation.
    parent["store"].update_connector(parent["connector"].id, {"port": int(cfg["port"])})
    job = parent["memory"].get_job(parent["job_id"])
    stored = transfer_request_from_dict(job["transfer_request"])
    stored.destination.host, stored.destination.port = cfg["host"], int(cfg["port"])
    stored.destination.database = cfg["database"]
    stored.destination.password = "rotated-away"  # nosec B105 - synthetic old secret
    parent["memory"].update_job_fields(
        parent["job_id"], {"transfer_request": transfer_request_to_dict(stored)}
    )
    dialled_before = relay.connections

    resp = _replay(parent["job_id"])

    assert resp.get("success") is True and resp.get("rows_written") == 2, resp
    assert relay.connections == dialled_before
    assert (resp.get("endpoint_refresh") or {}).get("changes") == [], resp.get("endpoint_refresh")
    got = dict(_sql(f'SELECT id, qty FROM "{parent["table"]}"'))  # nosec B608
    assert got == {11: 10, 12: 30, 13: 20, 14: 40}, got
