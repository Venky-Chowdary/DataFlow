"""DynamoDB → DynamoDB Scan + BatchWriteItem — dest Scan COUNT."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_dynamodb_common import dynamodb_family_name, dynamodb_type_is_copy_safe  # noqa: E402
from services.copy_dynamodb_dynamodb import (  # noqa: E402
    copy_dynamodb_to_dynamodb,
    dynamodb_dynamodb_copy_enabled,
)
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _ddb_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 8000), timeout=1):
            pass
    except OSError:
        pytest.skip("DynamoDB Local 8000 not reachable")


def _ddb_cfg(table: str) -> dict:
    return {
        "type": "dynamodb",
        "format": "dynamodb",
        "host": "127.0.0.1",
        "port": 8000,
        "database": table,
        "table": table,
        "username": "local",
        "password": "local",
        "ssl": False,
    }


def _ddb_client():
    _ddb_or_skip()
    pytest.importorskip("boto3")
    from connectors.aws_common import boto3_client

    return boto3_client("dynamodb", _ddb_cfg("probe"))


def _wait_active(client, table: str) -> None:
    client.get_waiter("table_exists").wait(TableName=table)


def _ensure_table(client, table: str) -> None:
    from botocore.exceptions import ClientError

    try:
        client.describe_table(TableName=table)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
    client.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    _wait_active(client, table)


def _delete_table(client, table: str) -> None:
    from botocore.exceptions import ClientError

    try:
        client.delete_table(TableName=table)
        client.get_waiter("table_not_exists").wait(TableName=table)
    except ClientError:
        return


def _dest_count(table: str) -> int:
    n = destination_row_count(
        "dynamodb", _ddb_cfg(table), schema="", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_items(client, table: str, rows: int) -> None:
    from connectors.dynamodb_writer import _batch_write_with_retry

    _ensure_table(client, table)
    pending: list[dict] = []
    for i in range(1, rows + 1):
        pending.append(
            {
                "PutRequest": {
                    "Item": {
                        "id": {"N": str(i)},
                        "label": {"S": f"r{i}"},
                    }
                }
            }
        )
        if len(pending) == 25:
            _batch_write_with_retry(client, table, pending)
            pending = []
    if pending:
        _batch_write_with_retry(client, table, pending)


def test_dynamodb_family_and_copy_safe_types():
    assert dynamodb_family_name("amazon_dynamodb") == "dynamodb"
    assert dynamodb_family_name("dynamodb") == "dynamodb"
    assert dynamodb_type_is_copy_safe("long") is True
    assert dynamodb_type_is_copy_safe("string") is True
    assert dynamodb_type_is_copy_safe("object") is True
    assert dynamodb_type_is_copy_safe("binary") is True
    assert dynamodb_type_is_copy_safe("javascript") is False


def test_dynamodb_dynamodb_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", "0")
    assert dynamodb_dynamodb_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg("missing"),
            source_table="missing",
            dest_cfg=_ddb_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            dynamodb_ddls=["long"],
            replace_destination=True,
        )


def test_dynamodb_dynamodb_same_table_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", raising=False)
    cfg = _ddb_cfg("same_table")
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_dynamodb_to_dynamodb(
            source_cfg=cfg,
            source_table="same_table",
            dest_cfg=cfg,
            dest_table="same_table",
            pairs=[("id", "id")],
            dynamodb_ddls=["long"],
            replace_destination=True,
        )


def test_dynamodb_dynamodb_public_proxy_declines():
    dest = {
        **_ddb_cfg("nope"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:8000",
        "connection_string": "https://caboose.proxy.rlwy.net:8000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg("missing"),
            source_table="missing",
            dest_cfg=dest,
            dest_table="nope",
            pairs=[("id", "id")],
            dynamodb_ddls=["long"],
            replace_destination=True,
        )


def test_live_dynamodb_dynamodb_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", raising=False)
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdsrc{tag}"
    dest = f"dfddst{tag}"
    try:
        _seed_items(client, src, 800)
        _delete_table(client, dest)
        result = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("dynamodb_read") == "scan"
        assert result.source_snapshot.get("dynamodb_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_count_is_not_itemcount(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", raising=False)
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdicsrc{tag}"
    dest = f"dfdicdst{tag}"
    _seed_items(client, src, 80)
    _delete_table(client, dest)
    from botocore.client import BaseClient

    orig = BaseClient._make_api_call

    def _spoof(self, operation_name, kwarg):
        resp = orig(self, operation_name, kwarg)
        if operation_name == "DescribeTable" and isinstance(resp, dict):
            table = resp.get("Table") or {}
            table["ItemCount"] = 0
            resp["Table"] = table
        return resp

    monkeypatch.setattr(BaseClient, "_make_api_call", _spoof)
    try:
        result = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
    finally:
        monkeypatch.setattr(BaseClient, "_make_api_call", orig)
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_is_not_put_item(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", raising=False)
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdputsrc{tag}"
    dest = f"dfdputdst{tag}"
    _seed_items(client, src, 80)
    _delete_table(client, dest)
    from botocore.client import BaseClient

    orig = BaseClient._make_api_call

    def _no_put(self, operation_name, kwarg):
        if operation_name == "PutItem":
            raise AssertionError("DynamoDB→DynamoDB COPY must not PutItem")
        return orig(self, operation_name, kwarg)

    monkeypatch.setattr(BaseClient, "_make_api_call", _no_put)
    try:
        result = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("dynamodb_read") == "scan"
        assert _dest_count(dest) == 80
    finally:
        monkeypatch.setattr(BaseClient, "_make_api_call", orig)
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_nested_null_empty_preserved():
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdnestsrc{tag}"
    dest = f"dfdnestdst{tag}"
    try:
        _ensure_table(client, src)
        _delete_table(client, dest)
        client.put_item(
            TableName=src,
            Item={
                "id": {"N": "1"},
                "label": {"NULL": True},
                "payload": {"M": {"a": {"N": "1"}, "b": {"L": [{"S": "x"}, {"S": "y"}]}}},
            },
        )
        client.put_item(
            TableName=src,
            Item={"id": {"N": "2"}, "label": {"S": ""}},
        )
        client.put_item(
            TableName=src,
            Item={"id": {"N": "3"}, "label": {"S": "x"}},
        )
        result = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label"), ("payload", "payload")],
            dynamodb_ddls=["long", "string", "object"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        got = {
            int(it["id"]["N"]): it
            for it in client.scan(TableName=dest).get("Items") or []
        }
        assert got[1]["label"] == {"NULL": True}
        assert got[1]["payload"] == {
            "M": {"a": {"N": "1"}, "b": {"L": [{"S": "x"}, {"S": "y"}]}}
        }
        assert got[2]["label"] == {"S": ""}
        assert got[3]["label"] == {"S": "x"}
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_skip_when_dest_count_matches():
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdskipsrc{tag}"
    dest = f"dfdskipdst{tag}"
    try:
        _seed_items(client, src, 800)
        _delete_table(client, dest)
        first = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_occupied_mismatch_declines():
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdoccsrc{tag}"
    dest = f"dfdoccdst{tag}"
    try:
        _seed_items(client, src, 800)
        _seed_items(client, dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied DynamoDB dest"):
            copy_dynamodb_to_dynamodb(
                source_cfg=_ddb_cfg(src),
                source_table=src,
                dest_cfg=_ddb_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                dynamodb_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_overwrite_replaces_dest():
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdovwsr{tag}"
    dest = f"dfdovwds{tag}"
    try:
        _seed_items(client, src, 800)
        _seed_items(client, dest, 1)
        result = copy_dynamodb_to_dynamodb(
            source_cfg=_ddb_cfg(src),
            source_table=src,
            dest_cfg=_ddb_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            dynamodb_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("dynamodb_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)


def test_live_dynamodb_dynamodb_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DYNAMODB_DYNAMODB_COPY", raising=False)
    client = _ddb_client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfdstrsrc{tag}"
    dest = f"dfdstrdst{tag}"
    try:
        _seed_items(client, src, 800)
        _delete_table(client, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ddb-ddb-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict("database", _ddb_cfg(src))
        destination = EndpointConfig.from_dict("database", _ddb_cfg(dest))
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "scan_batch_write_dynamodb_dynamodb"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("DynamoDB" in line or "BatchWriteItem" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _delete_table(client, src)
        _delete_table(client, dest)
