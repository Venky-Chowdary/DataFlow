"""Gate-8 fingerprint remap must use write-path dest_kind / PK / policy."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import map_rows_for_fingerprint
from services.migration_risk_contract import create_migration_risk_contract
from services.scd2_engine import prepare_scd2_mapped_rows
from src.transfer.models import EndpointConfig


def test_map_rows_for_fingerprint_surfaces_fail_job_rejects():
    contract = create_migration_risk_contract(
        column="amount",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="fingerprint remap FAIL_JOB parity",
        execution_policy="FAIL_JOB",
    )
    mapped, rejected = map_rows_for_fingerprint(
        headers=["id", "amount"],
        data_rows=[["1", "nope"], ["2", "10"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {
                "source": "amount",
                "target": "amount",
                "transform": "to_integer",
                "target_type": "integer",
                "risk_contract": contract.to_dict(),
            },
        ],
        target_cols=["id", "amount"],
        column_types={"id": "string", "amount": "integer"},
        dest_types={"id": "string", "amount": "integer"},
        error_policy="quarantine",
        dest_kind="sqlite",
        destination_pk_columns=["id"],
    )
    assert mapped  # good row still mapped
    assert rejected
    assert any(
        str(d.get("execution_policy") or "").upper() == "FAIL_JOB" for d in rejected
    )


def test_prepare_scd2_fail_job_does_not_require_destination_write(tmp_path: Path):
    """Preflight validate must abort before opening a history transaction."""
    db = tmp_path / "scd2_pre.db"
    endpoint = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(db),
        connection_string=f"sqlite:///{db}",
        table="dim",
    )
    contract = create_migration_risk_contract(
        column="price",
        source_type="TEXT",
        destination_type="DECIMAL",
        approved_by="admin@dataflow.app",
        reason="SCD2 prepare preflight FAIL_JOB",
        execution_policy="FAIL_JOB",
    )
    prepared = prepare_scd2_mapped_rows(
        endpoint,
        [{"id": "1", "price": "nope"}],
        ["id", "price"],
        {"id": "string", "price": "decimal"},
        [
            {"source": "id", "target": "id"},
            {
                "source": "price",
                "target": "price",
                "transform": "decimal",
                "target_type": "decimal",
                "risk_contract": contract.to_dict(),
            },
        ],
        ["id"],
        validation_mode="balanced",
    )
    assert prepared.get("ok") is False
    assert prepared.get("mapped_rows") == []
    assert prepared.get("rejected_details")
    # Destination table must not have been created by prepare-only path.
    assert not db.exists() or db.stat().st_size == 0
