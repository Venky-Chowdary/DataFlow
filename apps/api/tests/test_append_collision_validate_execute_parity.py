"""Validate must run the append key-collision probe Execute runs.

A second append of the same file into a table that already stores those keys is
a deterministic write abort. Execute proved that (G6 blocked on the stored key)
while the persisted-plan Validate reported "Target DDL compatible · APPROVE ·
Execute-ready", because the plan transport never handed the destination
connection to the probe. Green Validate → refused Run is the failure these tests
pin: the transport must carry the connection, and a probe that cannot run must
say so instead of claiming compatibility.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.models import GateId, GateStatus
from services.destination_key_collision_probe import DestinationCollisionResult
from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
from services.transfer_plan_store import create_plan


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json"
    )
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _plan_id() -> str:
    plan = create_plan(
        {
            "name": "excel-pg-append",
            "source": {"kind": "file", "format": "xlsx"},
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": "dst",
                "table": "pii_people_wide",
            },
            "source_columns": ["username", "email"],
            "source_schema": {"username": "VARCHAR", "email": "VARCHAR"},
            "target_columns": ["username", "email"],
            "target_schema": {"username": "VARCHAR", "email": "VARCHAR"},
            "sample_rows": [{"username": "ada", "email": "ada@b.co"}],
            "policies": {
                "validation_mode": "strict",
                "sync_mode": "full_refresh_append",
            },
        }
    )
    sync_plan_mappings(
        plan.id,
        [
            {"source": "username", "target": "username", "confidence": 0.99},
            {"source": "email", "target": "email", "confidence": 0.99},
        ],
    )
    return plan.id


def test_plan_validate_hands_the_destination_connection_to_the_probe():
    """The kwarg the probe needs — without it every append re-validated green."""
    captured: dict = {}

    def fake_run_file_preflight(**kw):
        captured.update(kw)
        return {
            "passed": True,
            "passed_count": 16,
            "total_gates": 16,
            "gates": [],
            "blockers": [],
        }

    probe_cfg = {
        "type": "postgresql",
        "host": "127.0.0.1",
        "port": 5433,
        "database": "dataflow",
    }
    with patch("services.transfer_plan_service._preflight") as mock_pf, patch(
        "services.transfer_plan_service.read_source_database",
        side_effect=Exception("skip"),
    ):
        mock_pf.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "db_type": "postgresql",
                "column_types": {"username": "VARCHAR", "email": "VARCHAR"},
                "primary_key_columns": ["username"],
                "_probe_cfg": probe_cfg,
                "message": "ok",
            },
            fake_run_file_preflight,
            lambda **_k: [],
        )
        run_plan_preflight(_plan_id())

    assert captured["destination_config"] == probe_cfg
    assert captured["destination_pk_columns"] == ["username"]
    assert captured["destination_table"] == "pii_people_wide"


def _g6(collision, *, sync_mode: str = "full_refresh_append"):
    from preflight.gates import gate_g6_target_ddl
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        SourceConfig,
        TransferPlan,
    )
    from services.preflight_service import FilePreflightContext

    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            db_type="excel",
            columns=[ColumnSchema(name="username", inferred_type="VARCHAR")],
            connected=True,
            row_count_estimate=5,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=True,
            can_create_table=True,
            can_write=True,
            target_columns=[ColumnSchema(name="username", inferred_type="TEXT")],
        ),
        mappings=[
            ColumnMapping(source="username", target="username", confidence=0.99),
        ],
        sync_mode=sync_mode,
        validation_mode="strict",
        ddl_compatible=True,
        destination_pk_columns=["username"],
    )
    ctx = FilePreflightContext(
        plan=plan,
        sample_rows=[{"username": "ada"}],
        destination_collision=collision,
    )
    return gate_g6_target_ddl(ctx)


def test_probe_that_could_not_run_is_not_reported_as_compatible():
    """skipped_no_destination is unknown, and unknown must not read as proven."""
    result = _g6(
        DestinationCollisionResult(
            status="skipped_no_destination",
            message="Destination connection or table unavailable for collision probe",
            key_column="username",
        )
    )
    assert result.gate_id == GateId.G6_TARGET_DDL
    assert result.status == GateStatus.WARN
    assert "not probed" in result.message
    assert "username" in result.message
    assert "Target DDL compatible" not in result.message
    assert result.details["collision_probe"]["status"] == "skipped_no_destination"


def test_probe_that_ran_clean_records_the_evidence():
    result = _g6(
        DestinationCollisionResult(
            status="ran",
            message="Probed 5 key value(s)",
            key_column="username",
            values_probed=5,
        )
    )
    assert result.status == GateStatus.PASS
    assert "no collision" in result.message
    assert result.details["collision_probe"]["values_probed"] == 5


def test_stored_key_still_blocks():
    result = _g6(
        DestinationCollisionResult(
            status="ran",
            message="Probed 5 key value(s)",
            key_column="username",
            values_probed=5,
            findings=[{"column": "username", "value": "ada"}],
        )
    )
    assert result.status == GateStatus.BLOCK
    assert "Append would duplicate" in result.message
