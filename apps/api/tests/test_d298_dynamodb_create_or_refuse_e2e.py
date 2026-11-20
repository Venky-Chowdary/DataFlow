"""D298: PostgreSQL -> a new DynamoDB table, overwrite, with no upsert_key.

QA (Nov 9-10): preflight was green ("CREATE table allowed"), then the job failed
0 rows "DynamoDB table ... does not exist and no columns were provided to
create it". A pre-created table loaded 30/30.

A DynamoDB table cannot exist without its key schema (CreateTable requires
``KeySchema`` with a HASH attribute,
https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html).
So an overwrite into a new table either creates it with the HASH key the
source declares (its primary key) and writes every row, or, when the source
declares no key and none is named, Validate refuses before Confirm and nothing
is created. A key is never invented.

DynamoDB is moto's HTTP server (the D139 fixture); the route is QA's: MCP
``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job.
"""

from __future__ import annotations

import uuid

import pytest

import services.config  # noqa: F401 - built before the harness blanks DATAFLOW_SEED_DEMO
from tests import r4_route_harness as h
from tests.test_d139_dynamodb_neo4j_destination_mcp_route_e2e import (  # noqa: F401 - fixtures
    _items,
    client,
    moto_endpoint,
)

pytestmark = pytest.mark.timeout(300)


def _route(endpoint: str, port: int, src_table: str, token: str) -> tuple[str, str]:
    from services.connector_store import create_connector

    src = create_connector(
        {"name": f"d298-pg-{token}", "type": "postgresql", "role": "source",
         "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
    )
    dst = create_connector(
        {"name": f"d298-ddb-{token}", "type": "dynamodb", "role": "destination",
         "workspace_id": "", "ssl": False, "host": endpoint, "port": port,
         "database": "us-east-1", "username": "test", "password": "test"}
    )
    return src.id, dst.id


def _table_names(endpoint: str) -> list[str]:
    import boto3

    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test")
    return list(ddb.list_tables().get("TableNames") or [])


def _key_schema(endpoint: str, table: str) -> list[dict]:
    import boto3

    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test")
    return ddb.describe_table(TableName=table)["Table"]["KeySchema"]


def test_overwrite_creates_the_table_from_the_source_primary_key(client, moto_endpoint):
    endpoint, port = moto_endpoint
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"d298_{token}", f"q21_pg2dy_{token}"
    try:
        cur.execute(
            f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, note TEXT, amount NUMERIC(12,2))"  # nosec B608
        )
        cur.executemany(
            f"INSERT INTO {src_table} VALUES (%s, %s, %s)",  # nosec B608
            [(i, "" if i == 2 else (None if i == 3 else f"n{i}"), f"{i}.25") for i in range(1, 31)],
        )
        src, dst = _route(endpoint, port, src_table, token)
        job = h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src, "source_table": src_table,
            "dest_connector_id": dst, "dest_table": dest_table,
            "sync_mode": "full_refresh_overwrite",
        })
        assert str(job.get("status")) == "completed", job
        assert _key_schema(endpoint, dest_table) == [{"AttributeName": "id", "KeyType": "HASH"}]
        items = _items(endpoint, dest_table)
        assert sorted(items) == list(range(1, 31))
        assert items[2]["note"] == ""
        assert "note" not in items[3] or items[3]["note"] is None
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        pg.close()


def test_keyless_source_is_refused_before_confirm_and_nothing_is_created(client, moto_endpoint):
    endpoint, port = moto_endpoint
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"d298k_{token}", f"q21_keyless_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (note TEXT, amount NUMERIC(12,2))")  # nosec B608
        cur.executemany(
            f"INSERT INTO {src_table} VALUES (%s, %s)",  # nosec B608
            [(f"n{i}", f"{i}.25") for i in range(1, 6)],
        )
        src, dst = _route(endpoint, port, src_table, token)
        ok, staged = h.mcp_call(client, "start_transfer", {
            "source_connector_id": src, "source_table": src_table,
            "dest_connector_id": dst, "dest_table": dest_table,
            "sync_mode": "full_refresh_overwrite",
        })
        assert not ok or h.blockers_of(staged) or not staged.get("ack_id"), staged
        assert "key" in str(staged).lower()
        assert dest_table not in _table_names(endpoint)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        pg.close()


def test_writer_creates_from_pending_map_rows_when_the_table_is_absent(moto_endpoint, monkeypatch):
    """QA's job: the plan could not confirm the table, so every Map row was
    ``pending_dest_schema``. The writer skipped pending rows while existence
    was unknown and found "no columns were provided to create it" (D298). A
    confirmed-absent table resolves them, and CREATE gets the HASH key."""
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    from connectors.dynamodb_writer import write_mapped_rows

    endpoint, port = moto_endpoint
    table = f"q21_pending_{uuid.uuid4().hex[:8]}"
    pending = [
        {"source": c, "target": c, "source_type": t, "assignment_strategy": "pending_dest_schema"}
        for c, t in (("id", "INTEGER"), ("note", "TEXT"))
    ]
    result = write_mapped_rows(
        host=endpoint, port=port, database="us-east-1", username="test", password="test",
        schema="", connection_string="", ssl=False, table_name=table,
        headers=["id", "note"], data_rows=[["1", "a"], ["2", ""]],
        mappings=pending, column_types={"id": "INTEGER", "note": "TEXT"},
        create_table=True, conflict_columns=["id"], write_mode="overwrite",
    )
    assert result.ok, result.error
    assert _key_schema(endpoint, table) == [{"AttributeName": "id", "KeyType": "HASH"}]
    items = _items(endpoint, table)
    assert sorted(items) == [1, 2] and items[2]["note"] == ""
