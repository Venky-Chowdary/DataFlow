"""SQL/warehouse introspect must attach sample rows for Validate dry-run."""

from __future__ import annotations

from unittest.mock import patch

from src.transfer.endpoint_intelligence import _attach_sql_sample_rows
from src.transfer.models import EndpointConfig


def test_attach_sql_sample_rows_sets_data_for_dry_run() -> None:
    out: dict = {
        "columns": ["ID", "NAME"],
        "schema": {"ID": "NUMBER", "NAME": "TEXT"},
        "message": "Found existing table",
    }
    ep = EndpointConfig(
        kind="database",
        format="snowflake",
        database="DEMO",
        schema="PUBLIC",
        table="CSVTESTFILE",
        warehouse="COMPUTE_WH",
    )
    cfg = {
        "host": "xy12345",
        "port": 443,
        "database": "DEMO",
        "schema": "PUBLIC",
        "warehouse": "COMPUTE_WH",
        "username": "u",
        "password": "p",
        "connection_string": "",
        "ssl": True,
        "type": "snowflake",
    }
    records = [{"ID": 1, "NAME": "a"}, {"ID": 2, "NAME": "b"}]
    headers = ["ID", "NAME"]
    inferred = {"ID": "NUMBER", "NAME": "TEXT"}

    with patch(
        "src.transfer.adapters.read_source_database",
        return_value=(records, headers, inferred),
    ) as mock_read:
        _attach_sql_sample_rows(out, ep, cfg, "snowflake", "CSVTESTFILE", 50)

    mock_read.assert_called_once()
    assert len(out["data"]) == 2
    assert len(out["sample_data"]) == 2
    assert out["data"][0]["NAME"] == "a"
    assert "sample row" in out["message"].lower()


def test_attach_sql_sample_rows_empty_table_message() -> None:
    out: dict = {"columns": ["ID"], "schema": {"ID": "NUMBER"}, "message": "ok"}
    ep = EndpointConfig(kind="database", format="postgresql", table="t", database="db")
    with patch(
        "src.transfer.adapters.read_source_database",
        return_value=([], ["ID"], {"ID": "INTEGER"}),
    ):
        _attach_sql_sample_rows(out, ep, {"type": "postgresql"}, "postgresql", "t", 10)
    assert out["data"] == []
    assert "empty" in out["message"].lower()
