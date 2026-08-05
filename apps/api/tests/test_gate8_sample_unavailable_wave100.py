"""Wave 100 Q1: destination sample read failures must fail Gate-8, not pass."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from services.reconciliation import TargetSampleUnavailable, read_target_sample
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation


def test_read_target_sample_raises_when_sqlite_path_missing():
    with pytest.raises(TargetSampleUnavailable, match="sqlite path missing"):
        read_target_sample(
            "sqlite",
            {"database": "", "connection_string": "", "host": ""},
            schema="",
            table_name="t",
            columns=["id"],
            limit=10,
        )


def test_read_target_sample_raises_on_postgres_connection_failure():
    with patch(
        "connectors.postgresql_conn.get_connection",
        side_effect=RuntimeError("connection refused"),
    ):
        with pytest.raises(TargetSampleUnavailable, match="connection refused"):
            read_target_sample(
                "postgresql",
                {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "db",
                    "username": "u",
                    "password": "p",
                    "ssl": False,
                },
                schema="public",
                table_name="t",
                columns=["id"],
                limit=10,
            )


def test_reconcile_step_fails_gate8_when_sample_unavailable():
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="db",
        table="t",
    )
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "postgresql", "database": "db", "schema": "public"},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(1, "abc"),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        side_effect=TargetSampleUnavailable("boom"),
    ):
        report = run_reconciliation(
            endpoint=endpoint,
            records=[{"id": "1"}],
            columns=["id"],
            rows_written=1,
            writer_checksum="abc",
            dest_summary={"schema": "public", "table": "t", "source_row_count": 1},
            mappings=[{"source": "id", "target": "id"}],
            validation_mode="strict",
        )
    assert report["passed"] is False
    assert "sample compare unavailable" in (report.get("message") or "").lower()


def test_reconcile_step_fails_delete_proof_when_sample_unavailable():
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        database="db",
        table="t",
    )
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "postgresql", "database": "db", "schema": "public"},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(0, ""),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        side_effect=TargetSampleUnavailable("boom"),
    ):
        report = run_reconciliation(
            endpoint=endpoint,
            records=[],
            columns=["id"],
            rows_written=0,
            writer_checksum="",
            dest_summary={
                "schema": "public",
                "table": "t",
                "source_row_count": 0,
                "reconcile_deletes": ["1", "2"],
            },
            mappings=[{"source": "id", "target": "id"}],
            validation_mode="strict",
        )
    assert report["passed"] is False
    assert "delete proof unavailable" in (report.get("message") or "").lower()
