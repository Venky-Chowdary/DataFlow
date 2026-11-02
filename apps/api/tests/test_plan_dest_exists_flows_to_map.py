"""A persisted plan must carry the destination-existence verdict into Map.

Snowflake→MySQL with a brand-new destination table printed
``VARCHAR(16777216) → VARCHAR(16777216) loses fidelity`` because the probe's
"table absent" verdict never reached the mapping pipeline: the plan stored no
``table_exists`` and Map mapped against ``None`` (unknown), leaving every
``target_type`` empty.  Absent-but-proven must become create-new; unproven must
stay unknown and fail closed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.shape_contract import (
    FIDELITY_DEST_TYPE_UNREAD,
    SHAPE_CREATE_NEW,
    SHAPE_UNKNOWN,
)
from services.transfer_plan_service import run_plan_mapping
from services.transfer_plan_store import create_plan

_SOURCE_COLUMNS = ["employee_id", "age"]
_SOURCE_SCHEMA = {"employee_id": "VARCHAR(16777216)", "age": "BIGINT"}


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _plan(dest_extra: dict) -> str:
    return create_plan({
        "name": "sf-mysql-new-table",
        "source": {"kind": "database", "format": "snowflake", "connector_id": "src"},
        "destination": {
            "kind": "database",
            "format": "mysql",
            "connector_id": "dst",
            "table": "Newdata",
            "extra": dest_extra,
        },
        "source_columns": _SOURCE_COLUMNS,
        "source_schema": _SOURCE_SCHEMA,
        "target_columns": [],
        "target_schema": {},
        "policies": {"sync_mode": "full_refresh_append"},
    }).id


def test_proven_absent_destination_maps_as_create_new():
    result = run_plan_mapping(_plan({"table_exists": False}), use_llm=False)

    assert result["shape_contract"]["shape"] == SHAPE_CREATE_NEW
    for mapping in result["mappings"]:
        target_type = str(mapping.get("target_type") or "").strip()
        assert target_type, f"{mapping['source']} has no destination type"
        # MySQL cannot hold Snowflake's 16MB VARCHAR declaration verbatim.
        assert "16777216" not in target_type
        assert mapping.get("assignment_strategy") != "pending_dest_schema"
        assert mapping.get("fidelity") != FIDELITY_DEST_TYPE_UNREAD


def test_unproven_destination_stays_unknown_and_fails_closed():
    result = run_plan_mapping(_plan({}), use_llm=False)

    assert result["shape_contract"]["shape"] == SHAPE_UNKNOWN
    for mapping in result["mappings"]:
        assert mapping.get("assignment_strategy") == "pending_dest_schema"
        assert not str(mapping.get("target_type") or "").strip()
        assert mapping.get("fidelity") == FIDELITY_DEST_TYPE_UNREAD
        assert "loses fidelity" not in str(mapping.get("fidelity_reason") or "").lower()
