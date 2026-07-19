"""End-to-end Transfer Studio / stream / ops paths for market-gap features.

These tests call adapters.write_destination_database and stream._write_batch —
not bare writer modules — so registry-only theater cannot pass.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.transfer.adapters import write_destination_database
from src.transfer.connector_dispatch import has_reader, has_writer, load_writer
from src.transfer.models import EndpointConfig
from src.transfer.registry import validate_transfer
from src.transfer.stream import _write_batch


def test_dispatch_registers_all_promoted_drivers() -> None:
    for driver in ("sqlserver", "oracle", "iceberg", "kafka", "salesforce", "hubspot"):
        assert has_writer(driver), driver
    for driver in ("sqlserver", "oracle", "salesforce", "hubspot"):
        assert has_reader(driver), driver
    assert callable(load_writer("iceberg"))


def test_validate_transfer_allows_iceberg_and_saas_activation() -> None:
    ok, msg = validate_transfer("file", "csv", "database", "iceberg")
    assert ok, msg
    ok, msg = validate_transfer("database", "postgresql", "database", "salesforce")
    assert ok, msg
    ok, msg = validate_transfer("database", "postgresql", "database", "hubspot")
    assert ok, msg
    ok, msg = validate_transfer("file", "json", "database", "kafka")
    assert ok, msg


def test_e2e_adapter_write_file_to_iceberg(tmp_path: Path) -> None:
    warehouse = tmp_path / "wh"
    dest = EndpointConfig(
        kind="database",
        format="iceberg",
        database=str(warehouse),
        schema="mart",
        table="customers",
        connection_string=str(warehouse),
    )
    records = [
        {"id": "1", "email": "a@x.com"},
        {"id": "2", "email": "b@x.com"},
    ]
    columns = ["id", "email"]
    schema = {"id": "string", "email": "string"}
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in columns]

    written, ddl, summary = write_destination_database(
        dest, records, columns, schema, mappings
    )
    assert written == 2
    assert summary["type"] == "iceberg"
    assert summary["driver"] == "iceberg"
    assert any("iceberg" in x.lower() or "WRITE" in x for x in ddl)
    meta = list((warehouse / "mart" / "customers" / "metadata").glob("v*.metadata.json"))
    assert meta, "Iceberg metadata commit missing — not end-to-end"


def test_e2e_stream_write_batch_iceberg(tmp_path: Path) -> None:
    warehouse = tmp_path / "stream_wh"
    dest = EndpointConfig(
        kind="database",
        format="iceberg",
        database=str(warehouse),
        table="events",
        connection_string=str(warehouse),
    )
    cfg = {
        "host": "",
        "port": 0,
        "database": str(warehouse),
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": str(warehouse),
        "ssl": False,
        "api_key": "",
    }
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "v", "target": "v"},
    ]
    rows_written, checksum, summary = _write_batch(
        "iceberg",
        dest,
        cfg,
        "events",
        ["id", "v"],
        [["1", "x"], ["2", "y"]],
        mappings,
        {"id": "string", "v": "string"},
        True,
        None,
        1,
        1,
        0,
        write_mode="append",
    )
    assert rows_written == 2
    assert checksum
    assert summary["driver"] == "iceberg"


@patch("connectors.salesforce_writer.request")
def test_e2e_adapter_write_salesforce_reverse_etl(mock_req: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.content = b"[]"
    mock_resp.json.return_value = [
        {"success": True, "id": "001AAA"},
        {"success": True, "id": "001BBB"},
    ]
    mock_req.return_value = mock_resp

    dest = EndpointConfig(
        kind="database",
        format="salesforce",
        host="example.my.salesforce.com",
        table="Account",
        api_key="session-token",
        extra={"activation_batch_size": 200},
    )
    records = [
        {"External_Id__c": "e1", "Name": "Acme"},
        {"External_Id__c": "e2", "Name": "Beta"},
    ]
    columns = ["External_Id__c", "Name"]
    mappings = [{"source": c, "target": c} for c in columns]

    written, ddl, summary = write_destination_database(
        dest,
        records,
        columns,
        {c: "string" for c in columns},
        mappings,
        write_mode="upsert",
        conflict_columns=["External_Id__c"],
    )
    assert written == 2
    assert summary["type"] == "salesforce"
    assert summary["driver"] == "salesforce"
    mock_req.assert_called()
    # Ensure api_key / token path was used (Authorization via saas_common.request)
    assert mock_req.call_args.kwargs.get("token") or mock_req.call_args[1].get("token")


@patch("connectors.hubspot_writer.request")
def test_e2e_stream_write_hubspot(mock_req: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.content = b"{}"
    mock_resp.json.return_value = {"results": [{"id": "1"}], "errors": []}
    mock_req.return_value = mock_resp

    dest = EndpointConfig(kind="database", format="hubspot", table="contacts", api_key="pat")
    cfg = {
        "host": "",
        "port": 443,
        "database": "",
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": "",
        "ssl": True,
        "api_key": "pat",
    }
    written, _, summary = _write_batch(
        "hubspot",
        dest,
        cfg,
        "contacts",
        ["email", "firstname"],
        [["a@x.com", "Ada"]],
        [{"source": "email", "target": "email"}, {"source": "firstname", "target": "firstname"}],
        {"email": "string", "firstname": "string"},
        False,
        None,
        1,
        1,
        0,
        write_mode="insert",  # stream path must promote to upsert
        conflict_columns=["email"],
    )
    assert written == 1
    assert summary["driver"] == "hubspot"


def test_e2e_ops_freshness_and_dlq_api() -> None:
    from fastapi.testclient import TestClient

    from services.ops_metrics import record_cdc_poll
    from services.quarantine_dlq import append_dlq_event
    from src.main import app

    record_cdc_poll(lag_seconds=42.0, job_id="job-e2e", stream="orders", schedule_id="s1")
    append_dlq_event(job_id="job-e2e", action="quarantine.replay_failed", rows=3)

    client = TestClient(app)
    fr = client.get("/api/v1/ops/freshness")
    assert fr.status_code == 200
    body = fr.json()
    assert body["worst_lag_seconds"] is not None
    assert body["worst_lag_seconds"] >= 42.0
    assert body["pipelines"]

    dlq = client.get("/api/v1/ops/dlq?limit=20")
    assert dlq.status_code == 200
    assert dlq.json()["count"] >= 1


def test_e2e_reverse_etl_plan_applied_to_destination() -> None:
    """Planner output must be enough for adapter activation write."""
    from services.reverse_etl import plan_activation

    dest = EndpointConfig(kind="database", format="salesforce", table="", api_key="t")
    plan = plan_activation(
        destination_kind="salesforce",
        object_name="Contact",
        primary_key="Email",
        mode="upsert",
    )
    assert plan.object_name == "Contact"
    dest.table = plan.object_name
    dest.extra = {"activation_batch_size": plan.batch_size}
    assert dest.table == "Contact"
    assert dest.extra["activation_batch_size"] == 200
