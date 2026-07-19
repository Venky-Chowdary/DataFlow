"""Guards against the Validate accuracy bugs we hit in Studio (Mongo → Snowflake)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.contract_file_store import FileContractStore
from services.data_contract import ColumnRule, DataContract
from services.data_integrity import run_integrity_audit
from services.transform_engine import infer_transform_for_mapping
from src.transfer.registry import validate_transfer


def test_validate_transfer_never_returns_supported_token():
    ok, msg = validate_transfer("database", "mongodb", "database", "snowflake")
    assert ok is True
    assert msg.lower() != "supported"
    assert msg.lower().startswith("live route:")


def test_status_to_boolean_date_flag_does_not_infer_date_transform():
    """posted_date_estimated in the target name must not force a date parse on status."""
    assert (
        infer_transform_for_mapping("status", "posted_date_estimated", "VARCHAR", "BOOLEAN")
        == "boolean"
    )
    # Without target type, source semantics still win over target name heuristics.
    inferred = infer_transform_for_mapping("status", "posted_date_estimated", "VARCHAR", None)
    assert inferred not in {"date", "datetime"}


def test_balanced_integrity_does_not_emit_strict_85_confidence_messages():
    report = run_integrity_audit(
        source_columns=["status", "agent", "ip"],
        target_columns=["posted_date_estimated", "department", "dedup_hash"],
        mappings=[
            {"source": "status", "target": "posted_date_estimated", "confidence": 0.59, "target_type": "BOOLEAN"},
            {"source": "agent", "target": "department", "confidence": 0.59, "target_type": "VARCHAR"},
            {"source": "ip", "target": "dedup_hash", "confidence": 0.55, "target_type": "VARCHAR"},
        ],
        source_schemas=[
            {"name": "status", "inferred_type": "VARCHAR"},
            {"name": "agent", "inferred_type": "VARCHAR"},
            {"name": "ip", "inferred_type": "VARCHAR"},
        ],
        target_schemas=[
            {"name": "posted_date_estimated", "inferred_type": "BOOLEAN"},
            {"name": "department", "inferred_type": "VARCHAR"},
            {"name": "dedup_hash", "inferred_type": "VARCHAR"},
        ],
        sample_rows=[{"status": "active", "agent": "ops", "ip": "1.1.1.1"}],
        validation_mode="balanced",
        destination_db_type="snowflake",
    )
    joined = " | ".join(str(i) for i in report["issues"])
    assert "85%" not in joined
    # Real blocker remains the lossy / wrong map — not a silent pass.
    assert report["blocks_transfer"] is True
    assert any("posted_date_estimated" in str(i) for i in report["issues"])
    conf = next(c for c in report["checks"] if c["check"] == "mapping_confidence")
    assert conf["passed"] is True


def test_file_contract_store_roundtrip(tmp_path):
    store = FileContractStore(path=tmp_path / "contracts.json")
    contract = DataContract(
        name="mongo → snowflake · jobs",
        columns=[
            ColumnRule(
                source_name="status",
                target_name="status",
                source_type="VARCHAR",
                target_type="VARCHAR",
            )
        ],
        mappings=[{"source": "status", "target": "status", "confidence": 0.99}],
        metadata={"validation_mode": "balanced"},
    )
    store.save_contract(contract)
    listed = store.list_contracts()
    assert any(c.id == contract.id for c in listed)
    loaded = store.get_contract(contract.id)
    assert loaded is not None
    assert loaded.name == contract.name
    assert loaded.status.value == "draft"
