"""Market-gap P0/P1: honesty, SQL Server/Oracle, Iceberg, reverse-ETL, Kafka, ops."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from services.ops_metrics import freshness_summary, record_cdc_poll, snapshot
from services.reverse_etl import plan_activation, supported_activation_kinds
from src.transfer.connector_capabilities import (
    enrich_catalog_entry,
    resolve_driver_type,
    transfer_ready,
    get_capabilities,
)
from src.transfer.connector_registry import CONNECTOR_MODULES, assert_registry_matches_capabilities


def test_registry_matches_capabilities() -> None:
    assert_registry_matches_capabilities()


def test_sqlserver_oracle_resolve_first_class() -> None:
    assert resolve_driver_type("sqlserver") == "sqlserver"
    assert resolve_driver_type("sql_server") == "sqlserver"
    assert resolve_driver_type("mssql") == "sqlserver"
    assert resolve_driver_type("oracle") == "oracle"
    assert "sqlserver" in CONNECTOR_MODULES
    assert "oracle" in CONNECTOR_MODULES


def test_sqlserver_oracle_certified_when_caps_ready() -> None:
    """When DBAPI is present, catalog enrichment marks full transfer."""
    for brand in ("sqlserver", "oracle"):
        caps = get_capabilities(brand, brand)
        if not transfer_ready(caps):
            pytest.skip(f"{brand} DBAPI not installed in this environment")
        row = enrich_catalog_entry(
            {"id": brand, "name": brand, "category": "database", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is True, brand
        assert row["certification_tier"] == "certified", brand


def test_iceberg_and_kafka_are_dest_modules() -> None:
    assert "iceberg" in CONNECTOR_MODULES
    assert "kafka" in CONNECTOR_MODULES
    assert resolve_driver_type("apache_iceberg") == "iceberg"
    assert resolve_driver_type("apache_kafka") == "kafka"
    ice = enrich_catalog_entry(
        {"id": "iceberg", "name": "Iceberg", "category": "lakehouse", "status": "live", "description": ""}
    )
    assert ice["transfer_ready"] is True
    assert ice["certification_tier"] in {"certified", "destination_only"} or "Destination" in ice.get(
        "capability_label", ""
    )


def test_iceberg_writer_append_and_schema_evolve(tmp_path: Path) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    warehouse = tmp_path / "warehouse"
    mappings = [
        {"source": "id", "target": "id", "target_type": "long"},
        {"source": "name", "target": "name", "target_type": "string"},
    ]
    r1 = write_mapped_rows(
        host="",
        database=str(warehouse),
        connection_string="",
        username="",
        password="",
        schema="analytics",
        ssl=False,
        port=0,
        table_name="events",
        headers=["id", "name"],
        data_rows=[["1", "alpha"], ["2", "beta"]],
        mappings=mappings,
        column_types={"id": "long", "name": "string"},
        write_mode="append",
    )
    assert r1.ok, r1.error
    assert r1.rows_written == 2

    mappings2 = mappings + [{"source": "city", "target": "city", "target_type": "string"}]
    r2 = write_mapped_rows(
        host="",
        database=str(warehouse),
        connection_string="",
        username="",
        password="",
        schema="analytics",
        ssl=False,
        port=0,
        table_name="events",
        headers=["id", "name", "city"],
        data_rows=[["3", "gamma", "NYC"]],
        mappings=mappings2,
        column_types={"id": "long", "name": "string", "city": "string"},
        write_mode="append",
    )
    assert r2.ok, r2.error
    meta_dir = warehouse / "analytics" / "events" / "metadata"
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    assert len(versions) >= 2
    latest = json.loads(versions[-1].read_text(encoding="utf-8"))
    field_names = {f["name"] for f in latest["schema"]["fields"]}
    assert "city" in field_names
    assert latest["format-version"] == 2
    assert latest["current-snapshot-id"]


def test_salesforce_hubspot_activation_planners() -> None:
    assert "salesforce" in supported_activation_kinds()
    assert "hubspot" in supported_activation_kinds()
    sf = plan_activation(
        destination_kind="salesforce",
        object_name="Account",
        primary_key="External_Id__c",
    )
    assert sf.batch_size == 200
    assert any("Collections" in n for n in sf.notes)
    hs = plan_activation(
        destination_kind="hubspot",
        object_name="contacts",
        primary_key="email",
    )
    assert hs.batch_size == 100
    assert any("idProperty" in n for n in hs.notes)


def test_salesforce_hubspot_are_write_ready() -> None:
    for brand in ("salesforce", "hubspot"):
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is True, brand
        assert CONNECTOR_MODULES[brand].writer.endswith("_writer")


@patch("connectors.salesforce_writer.request")
def test_salesforce_writer_upsert_quarantines_failures(mock_req: MagicMock) -> None:
    from connectors.salesforce_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"[{}]"
    mock_resp.json.return_value = [
        {"success": True, "id": "001"},
        {"success": False, "errors": [{"message": "DUPLICATE_VALUE"}]},
    ]
    mock_req.return_value = mock_resp

    result = write_mapped_rows(
        host="example.my.salesforce.com",
        api_key="token",
        table_name="Account",
        headers=["External_Id__c", "Name"],
        data_rows=[["ext-1", "Acme"], ["ext-2", "Beta"]],
        mappings=[
            {"source": "External_Id__c", "target": "External_Id__c"},
            {"source": "Name", "target": "Name"},
        ],
        column_types={},
        write_mode="upsert",
        conflict_columns=["External_Id__c"],
        connection_string="",
        username="",
        password="",
        schema="",
        ssl=True,
        port=443,
        database="",
    )
    assert result.ok
    assert result.rows_written == 1
    assert len(result.rejected_details) >= 1


@patch("connectors.hubspot_writer.request")
def test_hubspot_writer_upsert(mock_req: MagicMock) -> None:
    from connectors.hubspot_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"{}"
    mock_resp.json.return_value = {
        "results": [{"id": "1"}, {"id": "2"}],
        "errors": [],
    }
    mock_req.return_value = mock_resp

    result = write_mapped_rows(
        host="",
        api_key="pat-xxx",
        table_name="contacts",
        headers=["email", "firstname"],
        data_rows=[["a@x.com", "Ada"], ["b@x.com", "Bob"]],
        mappings=[
            {"source": "email", "target": "email"},
            {"source": "firstname", "target": "firstname"},
        ],
        column_types={},
        write_mode="upsert",
        conflict_columns=["email"],
        connection_string="",
        username="",
        password="",
        schema="",
        ssl=True,
        port=443,
        database="",
    )
    assert result.ok
    assert result.rows_written == 2


def test_ops_freshness_labeled_lag() -> None:
    record_cdc_poll(lag_seconds=12.5, job_id="j1", stream="orders", schedule_id="sched-a")
    record_cdc_poll(lag_seconds=3.0, job_id="j2", stream="users", schedule_id="sched-b")
    snap = snapshot()
    assert snap["gauges"]["dataflow_cdc_lag_seconds"] == 3.0
    assert any("sched-a" in k for k in snap["pipeline_lag_seconds"])
    summary = freshness_summary(max_lag_warn_seconds=10)
    assert summary["worst_lag_seconds"] is not None
    assert summary["worst_lag_seconds"] >= 12.5
    assert any(p["stale"] for p in summary["pipelines"])
    assert summary["stale_count"] >= 1
    assert summary["slo_status"] in {"warn", "critical"}
    assert any(a["schedule_id"] == "sched-a" for a in summary["alerts"])
    assert summary["critical_threshold_seconds"] >= 10


def test_ops_freshness_critical_alert() -> None:
    record_cdc_poll(lag_seconds=400.0, job_id="j-crit", stream="orders", schedule_id="sched-crit")
    summary = freshness_summary(max_lag_warn_seconds=60.0, max_lag_critical_seconds=120.0)
    assert summary["slo_status"] == "critical"
    assert summary["critical_count"] >= 1
    assert any(a["severity"] == "critical" for a in summary["alerts"])


def test_fiction_no_longer_includes_promoted_engines() -> None:
    from services.connector_capability_registry import get_connector_capability

    for key in ("sqlserver", "oracle", "iceberg", "kafka"):
        cap = get_connector_capability(key)
        # Must not be hard-demoted fiction when modules exist
        assert key in CONNECTOR_MODULES
        # transfer_ready follows driver caps (may be False without DBAPI for sql engines)
        assert "transfer_ready" in cap
