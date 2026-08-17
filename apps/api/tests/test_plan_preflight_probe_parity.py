"""Plan Validate must wire source probe + policy kwargs like Studio / Execute."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch


def test_plan_preflight_refetches_thin_sample_cache() -> None:
    """25-row plan snapshot must not win over PREFLIGHT_SAMPLE_LIMIT refetch."""
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    captured: dict[str, Any] = {}
    thin = [{"id": str(i)} for i in range(25)]
    fat = [{"id": str(i)} for i in range(PREFLIGHT_SAMPLE_LIMIT)]

    def _capture_pf(**kwargs):
        captured["sample_len"] = len(kwargs.get("sample_rows") or [])
        return {
            "passed": True,
            "passed_count": 1,
            "total_gates": 1,
            "readiness_score": 100,
            "gates": [],
            "blockers": [],
            "warnings": [],
        }

    plan = MagicMock()
    plan.source = {
        "kind": "database",
        "format": "mysql",
        "connector_id": "src-mysql",
        "table": "users",
    }
    plan.destination = {
        "kind": "database",
        "format": "postgresql",
        "connector_id": "dst-pg",
        "table": "users",
    }
    plan.source_columns = ["id"]
    plan.source_schema = {"id": "VARCHAR"}
    plan.target_columns = ["id"]
    plan.target_schema = {}
    plan.row_count_estimate = 1000
    plan.sample_rows = thin
    plan.policies = {
        "sync_mode": "full_refresh_append",
        "schema_policy": "manual_review",
        "validation_mode": "strict",
        "stream_contracts": [],
    }
    rev = MagicMock()
    rev.mappings = [{"source": "id", "target": "id", "confidence": 0.99}]
    rev.source_columns = []
    rev.source_schema = {}
    rev.source_schema_hash = ""
    rev.target_schema_hash = ""
    rev.version = 1
    rev.mapping_hash = "h"
    plan.active_revision.return_value = rev

    with (
        patch("services.transfer_plan_service.get_plan", return_value=plan),
        patch(
            "services.transfer_plan_service._preflight",
            return_value=(
                lambda pf, gates, **kw: {**pf, "gates": list(gates)},
                lambda _m: 0.85,
                lambda **kw: {
                    "connected": True,
                    "table_exists": True,
                    "db_type": "postgresql",
                    "column_types": {"id": "text"},
                },
                _capture_pf,
                lambda **kw: [],
            ),
        ),
        patch(
            "services.transfer_plan_service.read_source_database",
            return_value=(fat, ["id"], {"id": "VARCHAR"}),
        ),
        patch("services.transfer_plan_service.add_preflight_run"),
        patch("services.transfer_plan_service.append_audit_event"),
    ):
        from services.transfer_plan_service import run_plan_preflight

        run_plan_preflight("plan-thin")

    assert captured["sample_len"] == PREFLIGHT_SAMPLE_LIMIT


def test_plan_preflight_passes_source_probe_and_policy_kwargs() -> None:
    captured: dict[str, Any] = {}

    def _capture_pf(**kwargs):
        captured["pf"] = kwargs
        return {
            "passed": True,
            "passed_count": 1,
            "total_gates": 1,
            "readiness_score": 100,
            "gates": [],
            "blockers": [],
            "warnings": [],
        }

    def _capture_policy(**kwargs):
        captured["policy"] = kwargs
        return []

    plan = MagicMock()
    plan.source = {
        "kind": "database",
        "format": "mysql",
        "connector_id": "src-mysql",
        "table": "users",
    }
    plan.destination = {
        "kind": "database",
        "format": "postgresql",
        "connector_id": "dst-pg",
        "table": "users",
        "schema": "railway",
    }
    plan.source_columns = ["id", "email"]
    plan.source_schema = {"id": "VARCHAR", "email": "VARCHAR"}
    plan.target_columns = ["id", "email"]
    plan.target_schema = {}
    plan.row_count_estimate = 100
    plan.sample_rows = [{"id": "1", "email": "a@b.com"}]
    plan.policies = {
        "sync_mode": "full_refresh_append",
        "schema_policy": "manual_review",
        "validation_mode": "strict",
        "stream_contracts": [
            {"name": "users", "selected": True, "primary_key": ["id"]}
        ],
        "write_via_staging": False,
    }
    rev = MagicMock()
    rev.mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "email", "target": "email", "confidence": 0.9},
    ]
    rev.source_columns = []
    rev.source_schema = {}
    rev.source_schema_hash = ""
    rev.target_schema_hash = ""
    rev.version = 1
    rev.mapping_hash = "h"
    plan.active_revision.return_value = rev

    dest_meta = {
        "connected": True,
        "table_exists": True,
        "can_create_table": True,
        "can_write": True,
        "column_types": {"id": "text", "email": "text"},
        "column_nullability": {},
        "primary_key_columns": ["id"],
        "unique_keys": [],
        "foreign_keys": [],
        "db_type": "postgresql",
        "privilege_probe": {},
    }

    with (
        patch("services.transfer_plan_service.get_plan", return_value=plan),
        patch(
            "services.transfer_plan_service._preflight",
            return_value=(
                lambda pf, gates, **kw: {**pf, "gates": list(gates)},
                lambda _m: 0.85,
                lambda **kw: dest_meta,
                _capture_pf,
                _capture_policy,
            ),
        ),
        patch("services.transfer_plan_service.add_preflight_run"),
        patch("services.transfer_plan_service.append_audit_event"),
    ):
        from services.transfer_plan_service import run_plan_preflight

        run_plan_preflight("plan-1")

    pf = captured["pf"]
    assert pf["source_connector_id"] == "src-mysql"
    assert pf["source_table"] == "users"
    assert pf["stream_contracts"][0]["primary_key"] == ["id"]
    assert pf["contract_primary_key"] == "id"
    assert pf["destination_table"] == "users"

    pol = captured["policy"]
    assert pol["source_kind"] == "database"
    assert pol["source_type"] == "mysql"
    assert pol["dest_type"] == "postgresql"
