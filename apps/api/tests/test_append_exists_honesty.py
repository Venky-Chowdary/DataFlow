"""Append into existing tables must not false create-new or fail Gate-8 blindly."""

from __future__ import annotations

from unittest.mock import patch

from src.transfer.endpoint_intelligence import _attach_db_sample
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import _sort_key_for_columns, run_reconciliation


def test_unlisted_but_introspectable_table_is_not_create_new():
    """Schema-scoped SHOW TABLES miss must not short-circuit into create-new."""
    out: dict = {
        "kind": "database",
        "format": "postgresql",
        "connected": True,
        # airports missing from the limited public list (wrong schema / LIMIT).
        "objects": [{"name": "users", "type": "table"}],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "PostgreSQL connected",
        "table_exists": False,
    }
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="railway",
        schema="public",
        table="airports",
        extra={"introspect_purpose": "destination"},
    )
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "postgresql", "database": "railway", "schema": "public", "ssl": False},
    ), patch(
        "src.transfer.endpoint_intelligence._introspect_table_schema",
        return_value={
            "city": "TEXT",
            "code": "TEXT",
            "country": "TEXT",
            "lat": "NUMERIC",
            "lon": "NUMERIC",
            "name": "TEXT",
        },
    ), patch(
        "src.transfer.endpoint_intelligence._attach_sql_sample_rows",
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is True
    assert out["columns"] == ["city", "code", "country", "lat", "lon", "name"]
    assert out["auto_create"] == []
    assert any(o.get("name") == "airports" for o in out["objects"])
    assert "not found" not in (out.get("message") or "").lower()


def test_sort_key_prefers_code_over_first_column():
    assert _sort_key_for_columns(["city", "code", "name"]) == "code"
    assert _sort_key_for_columns(["city", "name", "id"]) == "id"


def test_streaming_append_passes_with_reconcile_sample():
    """Streaming path records=[] must still Gate-8-pass append via stashed sample."""
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="railway",
        schema="public",
        table="airports",
    )
    source_sample = [
        {"city": "Amsterdam", "code": "AMS", "country": "NL", "lat": "52.31", "lon": "4.76", "name": "Schiphol"},
        {"city": "Paris", "code": "CDG", "country": "FR", "lat": "49.01", "lon": "2.55", "name": "CDG"},
    ]
    dest_sample = [
        {"city": "Amsterdam", "code": "AMS", "country": "NL", "lat": "52.31", "lon": "4.76", "name": "Schiphol"},
        {"city": "Paris", "code": "CDG", "country": "FR", "lat": "49.01", "lon": "2.55", "name": "CDG"},
    ]
    mappings = [{"source": c, "target": c} for c in source_sample[0]]

    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "postgresql", "database": "railway", "schema": "public", "ssl": False},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(60, "whole-table-checksum-differs"),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        return_value=dest_sample,
    ) as read_sample:
        report = run_reconciliation(
            endpoint=endpoint,
            records=[],  # streaming
            columns=list(source_sample[0].keys()),
            rows_written=30,
            writer_checksum="batch-checksum",
            dest_summary={
                "schema": "public",
                "table": "airports",
                "source_row_count": 30,
                "reconcile_sample": source_sample,
            },
            mappings=mappings,
            validation_mode="strict",
        )

    assert report["passed"] is True, report
    assert "sample" in report["message"].lower()
    assert read_sample.called
    kwargs = read_sample.call_args.kwargs
    assert kwargs.get("sort_key") == "code"
    assert kwargs.get("key_values")  # keyed IN (...) read for append honesty


def test_streaming_append_still_fails_without_sample_proof():
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="railway",
        table="airports",
    )
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "postgresql", "ssl": False},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(60, "diff"),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        return_value=[],
    ):
        report = run_reconciliation(
            endpoint=endpoint,
            records=[],
            columns=["code", "city"],
            rows_written=30,
            writer_checksum="x",
            dest_summary={"table": "airports", "source_row_count": 30},
            mappings=[{"source": "code", "target": "code"}, {"source": "city", "target": "city"}],
            validation_mode="strict",
        )
    assert report["passed"] is False
    assert "sample" in report["message"].lower()
