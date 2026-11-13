"""DynamoDB upsert updates mapped attributes; append never overwrites a key.

Defect: ``dynamodb_writer.write_mapped_rows`` took no write mode and wrote
every dense row as a BatchWriteItem ``PutRequest``. PutItem replaces the whole
item, so an upsert deleted every attribute the mapping does not write (the
client's ``loyalty_tier``) — the contract (G15, PR 416 for MongoDB) is that
unmapped destination fields stay. An append run silently replaced items whose
key was already stored.

Fixed: upsert → UpdateItem ``SET`` of the mapped non-key attributes; append →
PutItem with ``attribute_not_exists(<key>)``, a stored key is a rejected row
naming the reason; overwrite keeps the whole-item PutItem.
"""

from __future__ import annotations

import pytest

moto = pytest.importorskip("moto")

_MAP = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


@pytest.fixture
def ddb(monkeypatch):
    for k, v in {
        "AWS_ACCESS_KEY_ID": "x",
        "AWS_SECRET_ACCESS_KEY": "x",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        import boto3

        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="ws_cust",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        client.put_item(
            TableName="ws_cust",
            Item={"id": {"S": "1"}, "name": {"S": "old"}, "loyalty_tier": {"S": "gold"}},
        )
        client.put_item(TableName="ws_cust", Item={"id": {"S": "2"}, "name": {"S": "keepme"}})
        yield client


def _write(rows, **kw):
    from connectors.dynamodb_writer import write_mapped_rows

    return write_mapped_rows(
        host="",
        port=0,
        database="",
        username="x",
        password="x",
        schema="",
        connection_string="",
        ssl=False,
        table_name="ws_cust",
        headers=["id", "name"],
        data_rows=rows,
        mappings=_MAP,
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        create_table=False,
        conflict_columns=["id"],
        **kw,
    )


def _items(client):
    return {
        it["id"]["S"]: {k: list(v.values())[0] for k, v in it.items()}
        for it in client.scan(TableName="ws_cust")["Items"]
    }


def test_upsert_keeps_unmapped_attributes(ddb):
    r = _write([["1", "new"], ["3", "third"]], write_mode="upsert")
    assert r.ok, r.error
    assert r.rows_written == 2
    items = _items(ddb)
    assert items["1"] == {"id": "1", "name": "new", "loyalty_tier": "gold"}
    assert items["2"] == {"id": "2", "name": "keepme"}
    assert items["3"] == {"id": "3", "name": "third"}


def test_append_never_overwrites_a_stored_key(ddb):
    r = _write([["2", "APPENDED"], ["4", "four"]], write_mode="insert", sync_mode="append")
    assert r.ok, r.error
    items = _items(ddb)
    assert items["2"] == {"id": "2", "name": "keepme"}
    assert items["4"] == {"id": "4", "name": "four"}
    assert r.rows_written == 1
    assert r.rejected_rows == 1
    detail = r.rejected_details[0]
    assert detail["row"] == 1
    assert detail["column"] == "id"
    assert "already" in detail["reason"].lower()
    # Gate-8 is handed only what landed.
    assert [row.get("id") for row in r.meta.get("reconcile_sample") or []] == ["4"]


def test_append_conflict_fails_the_job_under_policy_fail(ddb):
    r = _write([["2", "APPENDED"]], write_mode="insert", error_policy="fail")
    assert not r.ok
    assert "already" in (r.error or "").lower()
    assert _items(ddb)["2"] == {"id": "2", "name": "keepme"}


def test_overwrite_still_replaces_whole_item(ddb):
    r = _write([["1", "new"]], write_mode="overwrite", sync_mode="full_refresh_overwrite")
    assert r.ok, r.error
    assert _items(ddb)["1"] == {"id": "1", "name": "new"}


def test_many_upserts_run_on_bounded_workers(ddb):
    rows = [[str(i), f"n{i}"] for i in range(100, 160)]
    r = _write(rows, write_mode="upsert")
    assert r.ok, r.error
    assert r.rows_written == 60
    items = _items(ddb)
    assert all(items[str(i)]["name"] == f"n{i}" for i in range(100, 160))
    assert items["1"]["loyalty_tier"] == "gold"


def test_adapter_passes_write_mode_to_dynamodb_writer(monkeypatch):
    """The buffered engine path called the writer with no write mode."""
    import connectors.dynamodb_writer as dw
    import src.transfer.adapters as adapters
    from connectors.writer_common import WriteResult
    from src.transfer.models import EndpointConfig

    seen: dict = {}

    def _fake_write(**kw):
        seen.update(kw)
        return WriteResult(
            ok=True, rows_written=1, table_name="ws_cust", target_schema="", checksum="",
            chunks_completed=1,
        )

    monkeypatch.setattr(dw, "write_mapped_rows", _fake_write)
    monkeypatch.setattr(
        adapters,
        "resolve_connector_config",
        lambda endpoint: {"type": "dynamodb", "host": "", "port": 0, "database": ""},
    )
    adapters._write_destination_database(
        EndpointConfig(kind="database", format="dynamodb", table="ws_cust"),
        [{"id": "1", "name": "x"}],
        ["id", "name"],
        {"id": "string", "name": "string"},
        list(_MAP),
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert seen.get("write_mode") == "upsert"
    assert seen.get("conflict_columns") == ["id"]
