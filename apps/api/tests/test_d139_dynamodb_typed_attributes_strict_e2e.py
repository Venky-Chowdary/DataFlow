"""D139: a plain typed table loads into DynamoDB as N / S / BOOL attributes, strict.

QA: every column of a typed SQL table (INT id included) was refused at plan
"13 column(s) collapse fidelity on write" in strict mode. DynamoDB's attribute
types are N (number, up to 38 digits exact), S, BOOL, B, L, M and NULL
(https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html):
integers and exact decimals are N without loss, booleans BOOL, text S.

moto's DynamoDB server; QA's MCP route with validation_mode=strict. The raw
attribute type of every item is read back, not a deserialized value.
"""

from __future__ import annotations

import uuid

import pytest

import services.config  # noqa: F401 - built before the harness blanks DATAFLOW_SEED_DEMO
from tests import r4_route_harness as h
from tests.test_d139_dynamodb_neo4j_destination_mcp_route_e2e import (  # noqa: F401 - fixtures
    client,
    moto_endpoint,
)

pytestmark = pytest.mark.timeout(300)


def test_typed_table_lands_as_native_attributes_in_strict_mode(client, moto_endpoint):
    import boto3

    from services.connector_store import create_connector

    endpoint, port = moto_endpoint
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"d139t_{token}", f"d139t_out_{token}"
    try:
        cur.execute(
            f"CREATE TABLE {src_table} (id INTEGER PRIMARY KEY, big BIGINT, "  # nosec B608
            "amount NUMERIC(20,6), active BOOLEAN, name VARCHAR(40), note TEXT)"
        )
        cur.executemany(
            f"INSERT INTO {src_table} VALUES (%s, %s, %s, %s, %s, %s)",  # nosec B608
            [(1, 9007199254740993, "99999999999.999999", True, "Ada", "x"),
             (2, -5, "0.000001", False, "Zoë 山田", ""),
             (3, None, None, None, None, None)],
        )
        src = create_connector(
            {"name": f"d139t-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d139t-ddb-{token}", "type": "dynamodb", "role": "destination",
             "workspace_id": "", "ssl": False, "host": endpoint, "port": port,
             "database": "us-east-1", "username": "test", "password": "test"}
        )
        job = h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
        })
        assert str(job.get("status")) == "completed", job

        ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1",
                           aws_access_key_id="test", aws_secret_access_key="test")
        items = {int(i["id"]["N"]): i for i in ddb.scan(TableName=dest_table)["Items"]}
        assert sorted(items) == [1, 2, 3]
        one, two, three = items[1], items[2], items[3]
        assert one["big"] == {"N": "9007199254740993"}  # beyond 2^53: exact, not a double
        assert one["amount"] == {"N": "99999999999.999999"}
        assert one["active"] == {"BOOL": True} and two["active"] == {"BOOL": False}
        assert one["name"] == {"S": "Ada"} and two["name"] == {"S": "Zoë 山田"}
        assert two["note"] == {"S": ""}
        # SQL NULL is absent or the NULL type, never a value.
        for col in ("big", "amount", "active", "name", "note"):
            assert col not in three or three[col] == {"NULL": True}, (col, three.get(col))
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        pg.close()
