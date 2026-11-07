"""D208 (provisional) end to end: an SFTP host typed as ``sftp://h:PORT`` with
no ``port`` field must dial PORT, not the port the connector had before.

QA: update_connector(host="sftp://h:2200") on a connector saved with port 22
kept 22, so Test and every transfer dialled the old port. The URL's port is
authoritative unless an explicit port is given as well; when both are given
and disagree the edit is refused with a message naming both.

Real paramiko SFTP server (tests/sftp_test_server.py), MCP update_connector ->
/api/v1/copilot/confirm, MCP test_connector, and a start_transfer job into a
new PostgreSQL table, run twice.
"""

from __future__ import annotations

import os
import socket
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

CSV = "id,name\n1,alpha\n2,\n3,\"\"\n4,\"  padded  \"\n5,Zoë 東京 ✓\n"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def sftp_route(tmp_path):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg

    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    cfg = pg_test_cfg()
    try:
        db = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                              user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    db.autocommit = True
    root = tmp_path / "sftp"
    root.mkdir()
    (root / "people.csv").write_text(CSV, encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    scfg = server.endpoint_config("/people.csv")
    sfx = uuid.uuid4().hex[:8]
    stale_port = _free_port()  # nothing listens here: the "old" port
    src = create_connector({
        "name": f"d208-sftp-{sfx}", "type": "sftp", "role": "source", "host": scfg["host"],
        "port": stale_port, "username": scfg["username"], "password": scfg["password"],
        "database": scfg["database"], "host_key": scfg["host_key"],
    })
    dst = create_connector({"name": f"d208-pg-{sfx}", "type": "postgresql", "role": "destination",
                            **cfg, "schema": "public"})
    table = f"d208_{sfx}"
    try:
        yield {"src": src.id, "dst": dst.id, "server": scfg, "stale": stale_port,
               "table": table, "db": db}
    finally:
        runner.stop()
        delete_connector(src.id)
        delete_connector(dst.id)
        try:
            db.cursor().execute(f"DROP TABLE IF EXISTS public.{table}")
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        db.close()


def _tools():
    from src.ai.copilot.tools import get_pilot_tools

    return get_pilot_tools()


def _confirm(ack_id: str) -> dict:
    from conftest import spend_pilot_ack

    return spend_pilot_ack(ack_id, "d208-test")


def _update(route: dict, **fields):
    return _tools().execute("update_connector", {"connector_id": route["src"], **fields})


def _job(ack_id: str) -> dict:
    job_id = _confirm(ack_id)["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def test_url_port_wins_over_the_stored_port_for_test_and_transfer(sftp_route):
    from services.connector_store import get_connector

    route = sftp_route
    port = route["server"]["port"]
    # Sanity: the stored port is dead, so Test fails before the edit.
    before = _tools().execute("test_connector", {"connector_id": route["src"]})
    assert not (before.success and (before.output or {}).get("ok", True) is not False), before

    staged = _update(route, host=f"sftp://127.0.0.1:{port}")
    assert staged.success, staged.error
    _confirm(staged.output["ack_id"])
    saved = get_connector(route["src"])
    assert saved.host == "127.0.0.1", saved.host
    assert int(saved.port) == port, (saved.port, port, route["stale"])

    probe = _tools().execute("test_connector", {"connector_id": route["src"]})
    assert probe.success, probe.error
    assert (probe.output or {}).get("ok", True) is not False, probe.output
    assert str(port) in str(probe.output), probe.output

    args = {
        "source_connector_id": route["src"], "source_table": "people.csv",
        "dest_connector_id": route["dst"], "dest_table": route["table"],
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
    }
    for run in (1, 2):  # run 2 goes into the table run 1 created
        start = _tools().execute("start_transfer", args)
        assert start.success, (run, start.error)
        job = _job(start.output["ack_id"])
        assert job.get("status") == "completed", (run, job)
        cur = route["db"].cursor()
        cur.execute(f"SELECT id, name FROM public.{route['table']} ORDER BY id")
        rows = [(int(r[0]), r[1]) for r in cur.fetchall()]
        assert rows == [(1, "alpha"), (2, None), (3, ""), (4, "  padded  "), (5, "Zoë 東京 ✓")], (run, rows)


def test_url_port_and_a_disagreeing_port_field_are_refused(sftp_route):
    from services.connector_store import get_connector

    route = sftp_route
    port = route["server"]["port"]
    refused = _update(route, host=f"sftp://127.0.0.1:{port}", port=port + 1)
    assert not refused.success
    msg = str(refused.error)
    assert str(port) in msg and str(port + 1) in msg, msg
    assert int(get_connector(route["src"]).port) == route["stale"]

    agreed = _update(route, host=f"sftp://127.0.0.1:{port}", port=port)
    assert agreed.success, agreed.error
    _confirm(agreed.output["ack_id"])
    saved = get_connector(route["src"])
    assert (saved.host, int(saved.port)) == ("127.0.0.1", port)


def test_rest_save_with_a_url_host_and_a_disagreeing_port_is_refused(sftp_route):
    """The REST/UI path takes the same rule (the DTO always carries a port)."""
    from fastapi import HTTPException

    from services.connector_store import get_connector
    from src.routers import saved_connectors_router as r

    route = sftp_route
    port = route["server"]["port"]
    existing = get_connector(route["src"])

    class _State:
        pass

    class _Req:
        state = _State()
        headers: dict[str, str] = {}

    with pytest.raises((HTTPException, ValueError)) as exc:
        body = r.ConnectorSaveDTO(name=existing.name, type="sftp", role="source",
                                  host=f"sftp://127.0.0.1:{port}", port=port + 7,
                                  username=existing.username)
        r.update_saved_connector(route["src"], body, _Req(), "")
    assert str(port + 7) in str(exc.value), exc.value
    # The form resends the stored (stale) port with the new URL host: that is
    # "unchanged", so the URL's port is taken.
    body = r.ConnectorSaveDTO(name=existing.name, type="sftp", role="source",
                              host=f"sftp://127.0.0.1:{port}", port=route["stale"],
                              username=existing.username)
    r.update_saved_connector(route["src"], body, _Req(), "")
    saved = get_connector(route["src"])
    assert (saved.host, int(saved.port)) == ("127.0.0.1", port)
    probe = _tools().execute("test_connector", {"connector_id": route["src"]})
    assert probe.success and (probe.output or {}).get("ok", True) is not False, probe


def test_kafka_url_shaped_bootstrap_host_dials_its_own_port():
    """Sibling: a Kafka host ``PLAINTEXT://h:9092`` / ``h:9092`` with a stale
    port field must dial the host's port (the reader's bootstrap used to be
    ``PLAINTEXT://h:9092:<port>``). Real broker on 127.0.0.1:9092."""
    from connectors.kafka_reader import _bootstrap

    assert _bootstrap({"host": "PLAINTEXT://127.0.0.1:9092", "port": 19092}) == "127.0.0.1:9092"
    assert _bootstrap({"host": "kafka://b1:9093,kafka://b2:9094", "port": 1}) == "b1:9093,b2:9094"
    assert _bootstrap({"host": "127.0.0.1:9092", "port": 19092}) == "127.0.0.1:9092"
    assert _bootstrap({"host": "broker", "port": 9095}) == "broker:9095"
    with socket.socket() as s:
        s.settimeout(1)
        if s.connect_ex(("127.0.0.1", 9092)) != 0:
            pytest.skip("Kafka broker not on 127.0.0.1:9092")
    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("kafka", {"host": "PLAINTEXT://127.0.0.1:9092", "port": 19092,
                                  "type": "kafka"})
    assert ok, msg
    assert "19092" not in msg, msg



def test_mongodb_uri_typed_into_the_host_field_dials_its_own_port():
    """Sibling: ``mongodb://h:27017`` in the host field with a stale port field
    used to become ``mongodb://mongodb://h:27017:<port>/``. Real local Mongo."""
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or ""
    if not uri.startswith("mongodb://"):
        pytest.skip("no local MongoDB URI")
    from connectors.mongodb_common import normalize_mongodb_connection_string
    from src.transfer.connector_registry import run_probe

    built = normalize_mongodb_connection_string(host="mongodb://db1:27018", port=27999, database="d")
    assert built.startswith("mongodb://db1:27018/d"), built
    ok, msg = run_probe("mongodb", {"host": uri, "port": 1, "type": "mongodb", "database": "dataflow"})
    assert ok, msg
