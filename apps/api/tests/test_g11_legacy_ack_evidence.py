"""G11 re-review M4: the write_new acknowledgement is on each run / stream's evidence.

* The Redis and DynamoDB writers dropped ``legacy_field_ack`` (only MongoDB
  and Elasticsearch recorded it); the earlier test checked only the
  endpoint's ``extra``.
* Multi-stream: the stream loop stamped the acknowledgement on the shared
  destination endpoint, so it leaked into later streams; CDC multi-stream
  stamped it on a copy, so it was lost.
* An unknown ``legacy_field_policy`` (a typo) silently read as "no
  acknowledgement".
"""

from __future__ import annotations

import json
import socket
import uuid
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _written_before_g11(monkeypatch):
    """These collections / indexes / prefixes / tables were written by runs
    before the G11 upgrade, so the old-field check applies to them
    (``naming_history.object_history``; final review M-d)."""
    import services.naming_history as naming_history

    monkeypatch.setattr(naming_history, "object_history",
                        lambda *a, **k: naming_history.LEGACY)

ACK = "First_Name kept as stored; this run writes First Name (legacy_field_policy=write_new)"


def _redis_up() -> bool:
    try:
        socket.create_connection(("localhost", 6379), timeout=1).close()
    except OSError:
        return False
    return True


@pytest.mark.skipif(not _redis_up(), reason="no Redis on :6379")
def test_redis_writer_records_the_acknowledgement():
    import redis

    from connectors.redis_writer import write_mapped_rows

    prefix = f"g11a_{uuid.uuid4().hex[:8]}"
    try:
        res = write_mapped_rows(
            host="localhost", port=6379, database="0", username="", password="", schema="",
            connection_string="", ssl=False, table_name=prefix, headers=["id", "name"],
            data_rows=[["1", "Ada"]],
            mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "First Name"}],
            column_types={"id": "INTEGER", "name": "TEXT"}, create_table=True,
            conflict_columns=["id"], legacy_field_ack=[ACK],
        )
        assert res.ok, res.error
        assert ACK in res.warnings, res.warnings
    finally:
        r = redis.Redis(host="localhost", port=6379)
        for key in r.scan_iter(match=f"{prefix}:*"):
            r.delete(key)
        r.close()


def test_dynamodb_writer_records_the_acknowledgement():
    import connectors.dynamodb_writer as writer

    client = MagicMock()
    client.describe_table.return_value = {"Table": {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
        "TableStatus": "ACTIVE"}}
    client.scan.return_value = {"Items": []}
    client.batch_write_item.return_value = {"UnprocessedItems": {}}
    with patch.object(writer, "boto3_client", return_value=client):
        res = writer.write_mapped_rows(
            host="us-east-1", port=0, database="", username="", password="", schema="",
            connection_string="", ssl=False, table_name="people", headers=["id", "name"],
            data_rows=[["1", "Ada"]],
            mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "First Name"}],
            column_types={"id": "INTEGER", "name": "TEXT"}, create_table=False,
            conflict_columns=["id"], legacy_field_ack=[ACK],
        )
    assert res.ok, res.error
    assert ACK in res.warnings, res.warnings


def test_stream_acknowledgement_does_not_leak_to_the_next_stream():
    import services.target_name_legacy as legacy
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="mongodb", host="h", database="db",
                          table="s1", extra={"legacy_field_policy": "write_new"})
    legacy.begin_run(dest)
    maps = [{"source": "a", "target": "address.city"}]
    found = {"s1": {"address_city"}, "s2": set()}
    with patch.object(legacy, "mongo_fields_present",
                      side_effect=lambda cfg, table, names: found[table]), \
            patch("src.transfer.adapters.resolve_connector_config", return_value={"database": "db"}):
        rows, ack1 = legacy.prepare_stream_targets(dest, maps, "upsert")
        legacy.set_run_ack(dest, ack1)
        assert "legacy_field_ack" in dest.extra and "address_city" in ack1[0]
        dest.table = "s2"
        rows, ack2 = legacy.prepare_stream_targets(dest, maps, "upsert")
        legacy.set_run_ack(dest, ack2)
    assert ack2 == [] and "legacy_field_ack" not in dest.extra, dest.extra


def test_cdc_multi_stream_keeps_each_streams_acknowledgement():
    """CDC multi-stream binds a per-stream copy for the check: the
    acknowledgement must come back to the caller, not stay on the copy."""
    import services.target_name_legacy as legacy
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="mongodb", host="h", database="db",
                          table="t", extra={"legacy_field_policy": "write_new"})
    with patch.object(legacy, "mongo_fields_present", return_value={"address_city"}), \
            patch("src.transfer.adapters.resolve_connector_config", return_value={"database": "db"}):
        _rows, ack = legacy.prepare_stream_targets(
            dest, [{"source": "a", "target": "address.city"}], "cdc", stream_name="orders")
    assert ack and "address_city" in ack[0]
    assert "legacy_field_ack" not in (dest.extra or {})  # nothing stamped on the shared endpoint


def test_unknown_policy_is_refused_everywhere():
    from services.preflight_transfer_policy import run_transfer_policy_gates
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=[{"source": "a", "target": "address.city"}],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="mongodb", host="h", table="c",
                                   extra={"legacy_field_policy": "write-new"}),
        sync_mode="upsert",
    )
    assert "Unknown legacy_field_policy 'write-new'" in request_refusal(request)
    gates = {g["id"]: g for g in run_transfer_policy_gates(
        sync_mode="upsert", dest_type="mongodb", source_type="sqlite", source_kind="database",
        mappings=[{"source": "a", "target": "address.city"}], destination_table="c",
        destination_endpoint={"format": "mongodb", "legacy_field_policy": "write-new"},
    )}
    assert gates["g23e_target_legacy_fields"]["status"] == "block"


def test_unknown_policy_is_a_422_at_request_time(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from src.main import app
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    with TestClient(app) as client:
        resp = client.post("/api/v1/transfer/run", data={
            "source_kind": "database", "source_format": "sqlite", "source_database": "/x.db",
            "source_table": "t", "dest_kind": "database", "dest_format": "mongodb",
            "dest_database": "db", "dest_table": "c", "skip_preflight": "true",
            "async_mode": "false",
            "mappings_json": json.dumps([{"source": "a", "target": "address.city"}]),
            "dest_extra_json": json.dumps({"legacy_field_policy": "write-new"}),
        })
    assert resp.status_code == 422, resp.text
    assert "write-new" in resp.text
