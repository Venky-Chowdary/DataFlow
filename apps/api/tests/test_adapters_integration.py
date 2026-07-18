"""Adapter integration tests — file→warehouse paths and registry probes."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from transfer.adapters import (  # noqa: E402
    parse_file_content,
    records_to_matrix,
    resolve_connector_config,
    write_destination_database,
)
from transfer.connector_registry import CONNECTOR_MODULES, run_probe
from transfer.models import EndpointConfig
from transfer.registry import validate_transfer


def test_csv_to_snowflake_route_via_registry():
    ok, msg = validate_transfer("file", "csv", "database", "snowflake")
    assert ok, msg


def test_all_registry_drivers_have_run_probe():
    for driver in CONNECTOR_MODULES:
        assert callable(run_probe)


@pytest.mark.parametrize("driver", sorted(CONNECTOR_MODULES.keys()))
def test_run_probe_returns_tuple(driver: str):
    ok, message = run_probe(driver, {
        "host": "invalid.example",
        "port": CONNECTOR_MODULES[driver].probe and 1 or 27017,
        "database": "test",
        "username": "u",
        "password": "p",
    })
    assert isinstance(ok, bool)
    assert isinstance(message, str)


def test_resolve_connector_config_merges_inline():
    ep = EndpointConfig(
        format="snowflake",
        host="acct.snowflakecomputing.com",
        port=443,
        database="WH_DB",
        schema="PUBLIC",
        username="user",
        password="secret",
        warehouse="COMPUTE_WH",
        table="orders",
    )
    cfg = resolve_connector_config(ep)
    assert cfg["host"] == "acct.snowflakecomputing.com"
    assert cfg["warehouse"] == "COMPUTE_WH"
    assert cfg["database"] == "WH_DB"


def test_resolve_connector_config_inline_overrides_saved_connector(monkeypatch):
    """Per-transfer inline values must win over saved connector values."""
    from transfer import adapters

    saved = {
        "host": "saved.snowflakecomputing.com",
        "port": 443,
        "database": "SNOWFLAKE",
        "schema": "PUBLIC",
        "username": "saved_user",
        "password": "saved_pass",
        "warehouse": "SAVED_WH",
        "type": "snowflake",
        "role": "SAVED_ROLE",
    }
    monkeypatch.setattr(
        adapters, "_lookup_saved_connector", lambda connector_id, workspace_id=None: saved
    )

    # User overrides database and warehouse per transfer.
    ep = EndpointConfig(
        format="snowflake",
        connector_id="conn-1",
        database="DATAFLOW",
        warehouse="COMPUTE_WH",
        table="orders",
    )
    cfg = resolve_connector_config(ep)
    assert cfg["database"] == "DATAFLOW"
    assert cfg["warehouse"] == "COMPUTE_WH"
    # Saved credentials fill missing inline fields.
    assert cfg["host"] == "saved.snowflakecomputing.com"
    assert cfg["username"] == "saved_user"
    assert cfg["password"] == "saved_pass"


def test_resolve_connector_config_placeholder_database_uses_saved(monkeypatch):
    """The UI form default 'test_db' should not override the saved connector database."""
    from transfer import adapters

    saved = {
        "host": "saved.snowflakecomputing.com",
        "port": 443,
        "database": "DATAFLOW",
        "schema": "PUBLIC",
        "username": "user",
        "password": "pass",
        "warehouse": "COMPUTE_WH",
        "type": "snowflake",
    }
    monkeypatch.setattr(
        adapters, "_lookup_saved_connector", lambda connector_id, workspace_id=None: saved
    )

    ep = EndpointConfig(
        format="snowflake",
        connector_id="conn-1",
        database="test_db",
        table="orders",
    )
    cfg = resolve_connector_config(ep)
    assert cfg["database"] == "DATAFLOW"


def test_records_to_matrix_csv_like_rows():
    records = [
        {"order_id": "1", "amount": "10.50", "active": "true"},
        {"order_id": "2", "amount": "20.00", "active": "false"},
    ]
    headers, rows = records_to_matrix(records, ["order_id", "amount", "active"])
    assert headers == ["order_id", "amount", "active"]
    assert rows[0] == ["1", "10.50", "true"]


def test_write_destination_snowflake_invokes_writer():
    endpoint = EndpointConfig(
        format="snowflake",
        host="acct.snowflakecomputing.com",
        port=443,
        database="ANALYTICS",
        schema="PUBLIC",
        username="user",
        password="secret",
        warehouse="COMPUTE_WH",
        table="csv_import",
    )
    records = [{"order_id": "1", "amount": "99.99"}]
    columns = ["order_id", "amount"]
    schema = {"order_id": "string", "amount": "decimal"}
    mappings = [
        {"source": "order_id", "target": "order_id", "confidence": 0.95},
        {"source": "amount", "target": "amount", "confidence": 0.95, "transform": "decimal"},
    ]

    mock_result = SimpleNamespace(
        ok=True,
        rows_written=1,
        target_schema="PUBLIC",
        table_name="csv_import",
        checksum="abc",
        driver="snowflake",
        rejected_rows=0,
        warnings=[],
        error=None,
    )

    with patch("connectors.snowflake_writer.write_mapped_rows", return_value=mock_result) as writer:
        count, ddl_log, meta = write_destination_database(
            endpoint, records, columns, schema, mappings,
        )

    assert count == 1
    assert meta["type"] == "snowflake"
    assert meta["table"] == "csv_import"
    assert any("SNOWFLAKE COLUMN" in line for line in ddl_log)
    writer.assert_called_once()
    call_kw = writer.call_args.kwargs
    assert call_kw["warehouse"] == "COMPUTE_WH"
    assert call_kw["schema"] == "PUBLIC"


def test_parse_file_content_csv_bytes():
    csv_bytes = b"order_id,amount\n1,10.50\n2,20.00\n"
    with patch("transfer.adapters.FileParser") as fp:
        fp.parse.return_value = SimpleNamespace(
            success=True,
            error=None,
            data=[{"order_id": "1", "amount": "10.50"}, {"order_id": "2", "amount": "20.00"}],
            columns=["order_id", "amount"],
        )
        fp.infer_schema.return_value = {"order_id": "string", "amount": "decimal"}
        records, cols, schema = parse_file_content(csv_bytes, "orders.csv")
    assert len(records) == 2
    assert cols == ["order_id", "amount"]
    assert schema["amount"] == "decimal"


def test_adapter_imports_all_writer_modules():
    for driver, spec in CONNECTOR_MODULES.items():
        mod = importlib.import_module(spec.writer)
        assert hasattr(mod, spec.writer_fn), f"{driver} missing {spec.writer_fn}"
