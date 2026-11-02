"""FAIL_JOB under quarantine job policy must abort primary SQL writes."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sqlite_writer import write_mapped_rows
from services.migration_risk_contract import create_migration_risk_contract


def test_sqlite_fail_job_aborts_under_quarantine_policy():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        contract = create_migration_risk_contract(
            column="price",
            source_type="TEXT",
            destination_type="DECIMAL",
            approved_by="admin@dataflow.app",
            reason="writer FAIL_JOB abort proof",
            execution_policy="FAIL_JOB",
        )
        result = write_mapped_rows(
            host=str(db_path),
            port=0,
            database=str(db_path),
            schema="",
            username="",
            password="",
            connection_string=f"sqlite:///{db_path}",
            ssl=False,
            table_name="products",
            headers=["id", "price"],
            data_rows=[["1", "nope"]],
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
            error_policy="quarantine",
            conflict_columns=["id"],
            create_table=True,
        )
        assert result.ok is False, result.error
        err = (result.error or "").lower()
        assert (
            "abort" in err
            or "fail_job" in err
            or "rejected" in err
            or "risk contract" in err
        ), result.error
        assert result.rejected_details
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass
