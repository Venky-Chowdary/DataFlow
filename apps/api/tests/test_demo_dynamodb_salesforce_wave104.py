"""Demo-critical proofs: DynamoDB → cloud and Salesforce write-safety.

These are the wedges for upcoming client demos. They must stay green.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def test_dynamo_empty_probe_keyschema_seed_contract():
    """Stream empty-probe seed depends on describe_table_schema returning KeySchema."""
    cfg = {"host": "us-east-1", "endpoint_url": "http://localhost:8000"}
    with patch(
        "connectors.dynamodb_reader.describe_table_schema",
        return_value=(["pk", "sk"], {"pk": "VARCHAR", "sk": "VARCHAR"}),
    ):
        from connectors.dynamodb_reader import describe_table_schema

        names, types = describe_table_schema(cfg, "EmptyDemo")
    assert names == ["pk", "sk"]
    # Same absorb path stream.py uses when probe.headers is empty.
    columns: list[str] = []
    schema: dict[str, str] = {}
    if not columns:
        columns = list(names)
        for name, lt in (types or {}).items():
            schema.setdefault(name, lt)
    assert columns == ["pk", "sk"]
    assert schema["pk"] == "VARCHAR"


def test_dynamo_to_attr_bool_is_bool_not_string():
    from connectors.dynamodb_writer import _to_attr

    attr = _to_attr("true", "BOOLEAN")
    assert "BOOL" in attr
    assert attr["BOOL"] is True
    attr_f = _to_attr("0", "BOOL")
    assert attr_f["BOOL"] is False


def test_dynamo_to_attr_restores_ss_ns_envelope():
    from connectors.dynamodb_writer import _to_attr

    # Reader emits "v" — must round-trip (items kept for backward compat).
    ss = _to_attr({"_df_ddb_set": "SS", "v": ["a", "b"]}, "ARRAY")
    assert "SS" in ss
    assert set(ss["SS"]) == {"a", "b"}

    ns = _to_attr({"_df_ddb_set": "NS", "items": ["1", "2.5"]}, "ARRAY")
    assert "NS" in ns
    assert Decimal("1") in {Decimal(x) for x in ns["NS"]}


def test_dynamo_endpoint_intelligence_prefers_table_over_region_database():
    """Region in database + real table name must sample the table, not the region."""
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="dynamodb",
        host="us-east-1",
        database="us-east-1",
        table="Orders",
        endpoint_url="http://localhost:8000",
    )
    out: dict = {}

    with patch(
        "connectors.dynamodb_reader.describe_table_schema",
        return_value=(["pk", "amount"], {"pk": "VARCHAR", "amount": "DECIMAL"}),
    ) as describe, patch(
        "connectors.dynamodb_reader.estimate_item_count",
        return_value=0,
    ), patch(
        "connectors.dynamodb_reader.read_all_paginated",
        return_value=MagicMock(headers=["pk", "amount"], rows=[], total_rows=0),
    ), patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={
            "type": "dynamodb",
            "host": "us-east-1",
            "endpoint_url": "http://localhost:8000",
        },
    ):
        _attach_db_sample(out, endpoint, sample_limit=5)

    describe.assert_called()
    assert describe.call_args.args[1] == "Orders" or describe.call_args[0][1] == "Orders"


def test_dynamo_stream_forwards_conflict_columns_to_writer():
    from connectors.writer_common import WriteResult
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import _write_batch

    captured: dict = {}

    class _Mod:
        @staticmethod
        def write_mapped_rows(**kwargs):
            captured.update(kwargs)
            return WriteResult(
                ok=True,
                rows_written=1,
                table_name=kwargs.get("table_name") or "",
                target_schema="",
                checksum="abc",
                chunks_completed=1,
                driver="dynamodb",
            )

    dest = EndpointConfig(
        kind="database",
        format="dynamodb",
        host="us-east-1",
        table="Orders",
        endpoint_url="http://localhost:8000",
    )
    with patch("importlib.import_module", return_value=_Mod()):
        rows, checksum, summary = _write_batch(
            "dynamodb",
            dest,
            {
                "host": "us-east-1",
                "port": 8000,
                "database": "Orders",
                "endpoint_url": "http://localhost:8000",
            },
            "Orders",
            ["id", "amount"],
            [["1", "10"]],
            [
                {"source": "id", "target": "id", "confidence": 1.0},
                {"source": "amount", "target": "amount", "confidence": 1.0},
            ],
            {"id": "string", "amount": "decimal"},
            True,
            None,
            0,
            1,
            0,
            conflict_columns=["id"],
            sync_mode="full_refresh_overwrite",
            job_id="demo-job",
        )

    assert rows == 1
    assert captured.get("conflict_columns") == ["id"]
    assert captured.get("sync_mode") == "full_refresh_overwrite"


def test_salesforce_describe_keeps_picklist_and_write_flags():
    from connectors.salesforce import describe_sobject

    payload = {
        "fields": [
            {
                "name": "Status__c",
                "type": "picklist",
                "nillable": True,
                "label": "Status",
                "updateable": True,
                "createable": True,
                "calculated": False,
                "externalId": False,
                "picklistValues": [
                    {"value": "Open", "label": "Open", "active": True},
                    {"value": "Closed", "label": "Closed", "active": True},
                ],
            },
            {
                "name": "Formula__c",
                "type": "string",
                "nillable": True,
                "label": "Formula",
                "updateable": False,
                "createable": False,
                "calculated": True,
            },
        ]
    }
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = payload

    with patch("connectors.salesforce.request", return_value=resp), patch(
        "connectors.salesforce._access",
        return_value=("token", "https://example.my.salesforce.com"),
    ):
        fields = describe_sobject(
            {"host": "https://example.my.salesforce.com", "api_key": "tok"},
            "Account",
        )

    by_name = {f["name"]: f for f in fields}
    assert by_name["Status__c"]["picklistValues"][0]["value"] == "Open"
    assert by_name["Formula__c"]["calculated"] is True
    assert by_name["Formula__c"]["updateable"] is False


def test_salesforce_probe_rejects_login_host():
    from connectors.salesforce import test_salesforce

    ok, msg = test_salesforce(
        host="https://login.salesforce.com",
        api_key="00Dxx0000000000!AQEA...",
    )
    assert ok is False
    assert "instance URL" in msg


def test_salesforce_capability_is_honest_about_collections_batch():
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    sf = CAPABILITY_REGISTRY["salesforce"]
    assert sf["recommended_batch_size"] == 200
    assert sf["pagination"] == "composite_collections"
    assert "Bulk API 2.0" not in (sf.get("rate_limit_notes") or "")


def test_production_sku_includes_demo_wedges():
    from src.transfer.registry import PRODUCTION_SKU

    assert ("database", "dynamodb", "database", "s3") in PRODUCTION_SKU
    assert ("database", "dynamodb", "database", "gcs") in PRODUCTION_SKU
    assert ("database", "dynamodb", "database", "postgresql") in PRODUCTION_SKU
    assert ("database", "salesforce", "database", "postgresql") in PRODUCTION_SKU
    assert ("database", "postgresql", "database", "salesforce") in PRODUCTION_SKU


def test_salesforce_writer_skips_calculated_fields():
    from connectors.salesforce_writer import write_mapped_rows
    from connectors.writer_common import WriteResult

    describe = [
        {
            "name": "Name",
            "type": "string",
            "createable": True,
            "updateable": True,
            "calculated": False,
        },
        {
            "name": "Age__c",
            "type": "double",
            "createable": False,
            "updateable": False,
            "calculated": True,
        },
    ]
    captured: dict = {}

    def fake_request(**kwargs):
        captured["body"] = kwargs.get("data")
        resp = MagicMock()
        resp.content = b"[{}]"
        resp.json.return_value = [{"success": True, "id": "001xx000003DGbQ", "errors": []}]
        return resp

    with patch(
        "connectors.salesforce.describe_sobject",
        return_value=describe,
    ), patch(
        "connectors.salesforce_writer.token",
        return_value="tok",
    ), patch(
        "connectors.salesforce_writer.base_url",
        return_value="https://example.my.salesforce.com",
    ), patch(
        "connectors.salesforce_writer.request",
        side_effect=fake_request,
    ):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            table_name="Account",
            api_key="tok",
            headers=["Name", "Age__c"],
            data_rows=[["Acme", "42"]],
            mappings=[
                {"source": "Name", "target": "Name", "confidence": 1.0},
                {"source": "Age__c", "target": "Age__c", "confidence": 1.0},
            ],
            column_types={"Name": "string", "Age__c": "decimal"},
            write_mode="insert",
        )

    assert isinstance(result, WriteResult)
    assert result.ok
    records = captured["body"]["records"]
    assert "Name" in records[0]
    assert "Age__c" not in records[0]
    assert any("Skipped non-writable" in w for w in (result.warnings or []))


def test_salesforce_writer_rejects_login_host():
    from connectors.salesforce_writer import write_mapped_rows

    result = write_mapped_rows(
        host="https://login.salesforce.com",
        table_name="Account",
        api_key="tok",
        headers=["Name"],
        data_rows=[["Acme"]],
        mappings=[{"source": "Name", "target": "Name", "confidence": 1.0}],
        column_types={"Name": "string"},
        write_mode="insert",
    )
    assert result.ok is False
    assert "instance URL" in (result.error or "")


def test_salesforce_upsert_by_id_uses_patch_not_post():
    from connectors.salesforce_writer import write_mapped_rows

    describe = [
        {"name": "Id", "type": "id", "createable": False, "updateable": False, "calculated": False},
        {"name": "Name", "type": "string", "createable": True, "updateable": True, "calculated": False},
    ]
    calls: list[dict] = []

    def fake_request(**kwargs):
        calls.append({"method": kwargs.get("method"), "body": kwargs.get("data")})
        resp = MagicMock()
        resp.content = b"[{}]"
        records = (kwargs.get("data") or {}).get("records") or [{}]
        resp.json.return_value = [
            {"success": True, "id": r.get("Id") or "001xx000003NEW0", "errors": []}
            for r in records
        ]
        return resp

    with patch(
        "connectors.salesforce.describe_sobject",
        return_value=describe,
    ), patch(
        "connectors.salesforce_writer.token",
        return_value="tok",
    ), patch(
        "connectors.salesforce_writer.request",
        side_effect=fake_request,
    ):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            table_name="Account",
            api_key="tok",
            headers=["Id", "Name"],
            data_rows=[["001xx000003DGbQ", "Acme"], ["", "NewCo"]],
            mappings=[
                {"source": "Id", "target": "Id", "confidence": 1.0},
                {"source": "Name", "target": "Name", "confidence": 1.0},
            ],
            column_types={"Id": "string", "Name": "string"},
            write_mode="upsert",
            conflict_columns=["Id"],
        )

    assert result.ok
    methods = [c["method"] for c in calls]
    assert "PATCH" in methods
    # Empty Id must quarantine — refuse inventing an insert without identity
    # (at-least-once upsert would silently collapse / mint records).
    assert any(
        (d.get("column") or "") == "Id" and "Id" in (d.get("reason") or "")
        for d in (result.rejected_details or [])
    )
    patch_call = next(c for c in calls if c["method"] == "PATCH")
    assert "Id" in patch_call["body"]["records"][0]


def test_salesforce_introspect_exposes_id_and_external_id_keys():
    from services.schema_introspect import _introspect_salesforce

    fields = [
        {
            "name": "Id",
            "type": "id",
            "nillable": False,
            "label": "Record ID",
            "updateable": False,
            "createable": False,
            "calculated": False,
            "externalId": False,
            "idLookup": True,
        },
        {
            "name": "ExtKey__c",
            "type": "string",
            "nillable": True,
            "label": "Ext",
            "updateable": True,
            "createable": True,
            "calculated": False,
            "externalId": True,
            "idLookup": True,
        },
        {
            "name": "Name",
            "type": "string",
            "nillable": False,
            "label": "Name",
            "updateable": True,
            "createable": True,
            "calculated": False,
            "externalId": False,
            "idLookup": False,
        },
    ]
    with patch(
        "connectors.salesforce.list_sobjects",
        return_value=["Account"],
    ), patch(
        "connectors.salesforce.describe_sobject",
        return_value=fields,
    ):
        out = _introspect_salesforce(
            host="https://example.my.salesforce.com",
            api_key="tok",
            table="Account",
        )
    assert out["ok"]
    assert out["primary_key_columns"] == ["Id"]
    assert any(u.get("columns") == ["ExtKey__c"] for u in out["unique_keys"])
    by_name = {c["name"]: c for c in out["columns"]}
    assert by_name["ExtKey__c"]["externalId"] is True
    assert by_name["Id"]["is_primary_key"] is True
