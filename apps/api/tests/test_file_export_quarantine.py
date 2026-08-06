"""File export must surface quarantine — never silent-drop mapped rows."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import FileExportMapBlocked, write_destination_file
from src.transfer.models import EndpointConfig
from services.migration_risk_contract import create_migration_risk_contract


def test_file_export_continue_contract_returns_rejected_details():
    endpoint = EndpointConfig(kind="file_export", format="csv")
    contract = create_migration_risk_contract(
        column="price",
        source_type="TEXT",
        destination_type="DECIMAL",
        approved_by="admin@dataflow.app",
        reason="file export cast holdout",
        execution_policy="CAST_AND_CONTINUE",
    )
    _content, _name, summary = write_destination_file(
        endpoint,
        [
            {"id": "1", "price": "10.00"},
            {"id": "2", "price": "not-a-number"},
        ],
        ["id", "price"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {
                "source": "price",
                "target": "price",
                "transform": "decimal",
                "target_type": "decimal",
                "risk_contract": contract.to_dict(),
            },
        ],
        column_types={"id": "string", "price": "decimal"},
        validation_mode="balanced",
    )
    assert int(summary.get("rows") or 0) == 1
    assert int(summary.get("rejected_rows") or 0) >= 1
    assert summary.get("rejected_details")


def test_file_export_never_serializes_df_missing_sentinel():
    """STOP_COLUMN / coerce omit must not leak __DF_MISSING__ into CSV bytes."""
    from services.value_serializer import DF_MISSING_SENTINEL

    endpoint = EndpointConfig(kind="file_export", format="csv")
    contract = create_migration_risk_contract(
        column="note",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="omit bad note",
        execution_policy="STOP_COLUMN",
    )
    content, _name, summary = write_destination_file(
        endpoint,
        [
            {"id": "1", "note": "not-an-int"},
            {"id": "2", "note": "10"},
        ],
        ["id", "note"],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {
                "source": "note",
                "target": "note",
                "transform": "integer",
                "target_type": "INTEGER",
                "risk_contract": contract.to_dict(),
                "risk_acknowledged": True,
            },
        ],
        column_types={"id": "string", "note": "string"},
        validation_mode="balanced",
    )
    text = content.decode("utf-8") if isinstance(content, (bytes, bytearray)) else str(content)
    assert DF_MISSING_SENTINEL not in text
    assert "__DF_MISSING__" not in text
    assert int(summary.get("rows") or 0) >= 1


def test_file_export_fail_job_raises_with_quarantine_payload():
    endpoint = EndpointConfig(kind="file_export", format="csv")
    contract = create_migration_risk_contract(
        column="price",
        source_type="TEXT",
        destination_type="DECIMAL",
        approved_by="admin@dataflow.app",
        reason="file export FAIL_JOB",
        execution_policy="FAIL_JOB",
    )
    with pytest.raises(FileExportMapBlocked) as excinfo:
        write_destination_file(
            endpoint,
            [{"id": "1", "price": "nope"}],
            ["id", "price"],
            mappings=[
                {"source": "id", "target": "id", "transform": "none"},
                {
                    "source": "price",
                    "target": "price",
                    "transform": "decimal",
                    "target_type": "decimal",
                    "risk_contract": contract.to_dict(),
                },
            ],
            column_types={"id": "string", "price": "decimal"},
            validation_mode="strict",
        )
    assert excinfo.value.rejected_details
    assert int(excinfo.value.rejected_rows) >= 1
