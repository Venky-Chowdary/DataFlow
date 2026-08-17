"""Wizard volume is COUNT(*), never the 100-row preview window."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.transfer.endpoint_intelligence import (
    apply_measured_row_estimate,
    _attach_sql_sample_rows,
)
from src.transfer.models import EndpointConfig


def test_apply_measured_row_estimate_uses_count_not_sample():
    out: dict = {}
    apply_measured_row_estimate(out, 30_432, sample_len=100)
    assert out["row_estimate"] == 30_432
    assert out["sample_row_count"] == 100
    assert not out.get("row_estimate_uncertain")


def test_apply_measured_row_estimate_never_publishes_sample_as_population():
    out: dict = {"row_estimate": 0}
    apply_measured_row_estimate(out, None, sample_len=100)
    assert out["row_estimate"] == 0
    assert out["row_estimate_uncertain"] is True
    assert out["sample_row_count"] == 100


def test_sql_sample_stamps_reader_total_rows_not_len_records():
    endpoint = EndpointConfig(
        kind="database",
        format="mysql",
        host="db.example",
        port=3306,
        database="venky",
        table="orders",
        extra={"introspect_purpose": "source"},
    )
    records = [{"id": i} for i in range(100)]
    stamp_holder: dict = {}

    def _fake_read(ep, *, limit, raise_on_truncate, stamp_total=None):
        if stamp_total is not None:
            stamp_total["total_rows"] = 30_432
            stamp_total["sample_rows"] = len(records)
            stamp_holder.update(stamp_total)
        return records, ["id"], {"id": "integer"}

    out: dict = {"row_estimate": 0, "columns": ["id"]}
    with (
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value={"type": "mysql", "host": "db.example", "port": 3306, "database": "venky"},
        ),
        patch(
            "src.transfer.adapters.read_source_database",
            side_effect=_fake_read,
        ),
    ):
        _attach_sql_sample_rows(out, endpoint, {"type": "mysql"}, "mysql", "orders", 100)

    assert out["row_estimate"] == 30_432
    assert out["sample_row_count"] == 100
    assert len(out["sample_data"]) == 100
    assert stamp_holder["total_rows"] == 30_432


def test_destination_sample_skipped_when_columns_already_known():
    endpoint = EndpointConfig(
        kind="database",
        format="snowflake",
        host="acct",
        table="AUDIT",
        extra={"introspect_purpose": "destination"},
    )
    out = {"columns": ["ID", "NAME"], "schema": {"ID": "NUMBER"}, "row_estimate": 0}
    with patch("src.transfer.adapters.read_source_database") as read:
        _attach_sql_sample_rows(out, endpoint, {"type": "snowflake"}, "snowflake", "AUDIT", 100)
    read.assert_not_called()
    assert out["row_estimate"] == 0


def test_pack_source_read_stamps_batch_total():
    from src.transfer.adapters import _pack_source_read

    batch = SimpleNamespace(total_rows=30_432)
    stamp: dict = {}
    records = [{"id": 1}] * 100
    _pack_source_read(records, ["id"], {"id": "int"}, batch=batch, stamp_total=stamp)
    assert stamp["total_rows"] == 30_432
    assert stamp["sample_rows"] == 100


def test_snowflake_reader_count_uses_same_cursor_not_nested_login():
    import inspect

    from connectors.snowflake_reader import read_table_batch

    source = inspect.getsource(read_table_batch)
    assert "total = count_table_rows(" not in source
    assert "SELECT COUNT(*) FROM" in source
    assert "skip_population_count" in source


def test_dest_snowflake_skips_list_probe_when_table_set():
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="snowflake",
        host="acct",
        table="AUDIT",
        extra={"introspect_purpose": "destination"},
    )
    with (
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value={"type": "snowflake", "host": "acct", "port": 443, "database": "DB"},
        ),
        patch("connectors.snowflake.test_snowflake") as probe,
        patch(
            "src.transfer.endpoint_intelligence._attach_db_sample",
            side_effect=lambda out, ep, sample_limit=100: out.update(
                {
                    "columns": ["ID"],
                    "schema": {"ID": "NUMBER"},
                    "table_exists": True,
                    "message": "Found existing table `AUDIT`",
                }
            ),
        ),
    ):
        out = introspect_endpoint(endpoint)

    probe.assert_not_called()
    assert out["connected"] is True
    assert out["table_exists"] is True
    assert out["columns"] == ["ID"]


def test_dest_connect_error_is_not_create_new():
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="snowflake",
        host="acct",
        table="AUDIT",
        extra={"introspect_purpose": "destination"},
    )
    out: dict = {"connected": False, "objects": [], "columns": [], "schema": {}, "message": ""}
    with (
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value={"type": "snowflake", "host": "acct", "port": 443, "database": "DB"},
        ),
        patch(
            "src.transfer.endpoint_intelligence._introspect_table_schema_rich",
            return_value=({}, {}, {"probe_error": "250001 Incorrect username or password"}),
        ),
    ):
        _attach_db_sample(out, endpoint)

    assert out.get("table_exists") is None
    assert out.get("auto_create") in (None, [])
    assert "250001" in str(out.get("message") or "")


def test_dest_missing_table_still_create_on_write():
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="snowflake",
        host="acct",
        table="NEW_AUDIT",
        extra={"introspect_purpose": "destination"},
    )
    out: dict = {"connected": False, "objects": [], "columns": [], "schema": {}, "message": ""}
    with (
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value={"type": "snowflake", "host": "acct", "port": 443, "database": "DB"},
        ),
        patch(
            "src.transfer.endpoint_intelligence._introspect_table_schema_rich",
            return_value=({}, {}, {}),
        ),
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is False
    assert any("CREATE TABLE" in str(item) for item in (out.get("auto_create") or []))
    assert "created automatically" in str(out.get("message") or "")
