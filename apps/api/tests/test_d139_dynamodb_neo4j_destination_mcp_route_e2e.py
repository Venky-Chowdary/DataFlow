"""D139 — every advertised destination loads; nothing unloadable is advertised.

QA (production): DynamoDB and Neo4j were advertised as transfer-live
destinations, yet every load was blocked (all columns "collapse"; Neo4j "not a
supported destination"). The earlier fix proved DynamoDB through
``execute_tracked`` only.

DynamoDB here is a real HTTP endpoint (moto's server: no DynamoDB Local image
fits this workstation's disk), driven by DataFlow's own boto3 probe, reader
and writer through QA's route: MCP ``start_transfer`` -> ``/api/v1/copilot/
confirm`` -> job, run 1 creating the table and run 2 upserting into it.

Neo4j has no writer. It must not be offered as a destination anywhere a user
or an agent picks one, and naming it as one must say why at once.
"""

from __future__ import annotations

import decimal
import socket
import uuid

import pytest

from tests import r4_route_harness as h

# Live engines: a hung run fails instead of holding a session (pytest-timeout).
pytestmark = pytest.mark.timeout(300)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def moto_endpoint(monkeypatch):
    server_mod = pytest.importorskip("moto.server")
    pytest.importorskip("boto3")
    port = _free_port()
    server = server_mod.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    try:
        yield f"http://127.0.0.1:{port}", port
    finally:
        server.stop()


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _items(endpoint: str, table: str) -> dict[int, dict]:
    import boto3
    from boto3.dynamodb.types import TypeDeserializer

    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test")
    des = TypeDeserializer()
    out: dict[int, dict] = {}
    for page in ddb.get_paginator("scan").paginate(TableName=table):
        for item in page["Items"]:
            row = {k: des.deserialize(v) for k, v in item.items()}
            out[int(row["id"])] = row
    return out


def test_pg_rows_load_into_dynamodb_twice_through_mcp(client, moto_endpoint):
    from services.connector_store import create_connector

    endpoint, port = moto_endpoint
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table = f"d139_{token}"
    dest_table = f"d139_out_{token}"
    try:
        cur.execute(
            f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT, "  # nosec B608
            "amount NUMERIC(12,2), qty INTEGER, updated_at BIGINT NOT NULL)"
        )
        cur.executemany(
            f"INSERT INTO {src_table} VALUES (%s, %s, %s, %s, %s)",  # nosec B608
            [(1, "Ada", "12.50", 3, 1), (2, "Łukasz 山田 🎉", None, None, 2),
             (3, "   ", "0.00", 0, 3)],
        )
        src = create_connector(
            {"name": f"d139-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d139-ddb-{token}", "type": "dynamodb", "role": "destination",
             "workspace_id": "", "ssl": False, "host": endpoint, "port": port,
             "database": "us-east-1", "username": "test", "password": "test"}
        )
        args = {
            "source_connector_id": src.id,
            "source_table": src_table,
            "dest_connector_id": dst.id,
            "dest_table": dest_table,
            "sync_mode": "incremental_upsert",
            "upsert_key": "id",
            "cursor_column": "updated_at",
            "cursor_semantics": "monotonic_sequence",
        }
        h.stage_and_run(client, "start_transfer", args)  # run 1 creates the table
        first = _items(endpoint, dest_table)
        assert sorted(first) == [1, 2, 3]

        cur.execute(
            f"UPDATE {src_table} SET name = 'Zoë', amount = 7.25, updated_at = 10 WHERE id = 2"  # nosec B608
        )
        cur.execute(f"INSERT INTO {src_table} VALUES (4, '', 1.10, 9, 11)")  # nosec B608
        h.stage_and_run(client, "start_transfer", args)  # run 2 into DataFlow's table
        got = _items(endpoint, dest_table)
        assert sorted(got) == [1, 2, 3, 4]
        assert got[1]["name"] == "Ada" and got[1]["amount"] == decimal.Decimal("12.5")
        assert got[2]["name"] == "Zoë" and got[2]["amount"] == decimal.Decimal("7.25")
        assert got[3]["name"] == "   "
        assert got[4]["qty"] == 9
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        pg.close()


def test_neo4j_is_never_offered_as_a_destination(client):
    """Every surface that lists destinations agrees with the writers."""
    from src.ai.copilot.tools import DataPilotTools
    from src.transfer.connector_capabilities import dest_live_driver_types
    from src.transfer.registry import LIVE_DEST_DATABASES, get_capabilities

    assert "neo4j" not in dest_live_driver_types()
    assert "neo4j" not in LIVE_DEST_DATABASES
    assert "neo4j" not in get_capabilities()["destination_databases"]

    rest = client.get("/api/v1/transfer/capabilities")
    if rest.status_code == 200:
        assert "neo4j" not in (rest.json().get("destination_databases") or [])

    # An agent asking for the route is told the truth, not handed a gate list.
    route = DataPilotTools().execute(
        "plan_transfer_route", {"source": "postgresql", "destination": "neo4j"}
    )
    assert not route.success, route.output
    assert "source" in str(route.error).lower() and "neo4j" in str(route.error).lower()


def test_neo4j_destination_connector_is_refused_with_the_reason(client):
    """A saved Neo4j connector named as the destination is refused before preflight."""
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    pg = h.pg_conn()
    cur = pg.cursor()
    src_table = f"d139n_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        src = create_connector(
            {"name": f"d139n-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        neo = create_connector(
            {"name": f"d139n-neo-{token}", "type": "neo4j", "role": "both",
             "workspace_id": "", "ssl": False, "host": "127.0.0.1", "port": 7474,
             "database": "neo4j", "username": "neo4j", "password": "x"}
        )
        for tool in ("plan_transfer", "start_transfer"):
            ok, out = h.mcp_call(client, tool, {
                "source_connector_id": src.id, "source_table": src_table,
                "dest_connector_id": neo.id, "dest_table": "Person",
            })
            assert not ok, out
            text = str(out).lower()
            assert "neo4j" in text and "source-only" in text, out
            assert "collapse" not in text, out
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        pg.close()
