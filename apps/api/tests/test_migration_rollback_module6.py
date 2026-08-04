"""Module 6 — Migration Rollback Workflow (fail-closed, honest).

Enterprise promise: every migration has an explicit rollback plan.
Never invent one-click undo of committed production rows.
"""

from __future__ import annotations

import pytest

from services.migration_rollback import (
    EXECUTABLE_STRATEGIES,
    ROLLBACK_STRATEGIES,
    RollbackContractError,
    RollbackRefuseError,
    execute_rollback,
    plan_rollback,
    verify_rollback_signature,
)
from services.recovery_honesty import honesty_dict


def test_rollback_strategies_are_enumerated():
    assert "DOCUMENT_ONLY" in ROLLBACK_STRATEGIES
    assert "DISCARD_STAGING" in ROLLBACK_STRATEGIES
    assert "REQUIRE_WAREHOUSE_RESTORE" in ROLLBACK_STRATEGIES
    assert "DISCARD_STAGING" in EXECUTABLE_STRATEGIES
    assert "DOCUMENT_ONLY" not in EXECUTABLE_STRATEGIES


def test_plan_rollback_defaults_document_only_for_primary_write():
    plan = plan_rollback(
        job_id="job-1",
        sync_mode="incremental",
        destination_table="orders",
        staging_table=None,
        rows_written=100,
        promote_blocked=False,
    )
    assert plan.strategy == "DOCUMENT_ONLY"
    assert plan.executable is False
    assert plan.population_undo_claimed is False
    assert plan.guarantees
    assert plan.non_guarantees
    assert verify_rollback_signature(plan.to_dict())


def test_plan_rollback_selects_discard_staging_when_stage_present():
    plan = plan_rollback(
        job_id="job-2",
        sync_mode="full_refresh_overwrite",
        destination_table="orders",
        staging_table="orders_df_staging",
        rows_written=50,
        promote_blocked=True,
    )
    assert plan.strategy == "DISCARD_STAGING"
    assert plan.executable is True
    assert plan.staging_table == "orders_df_staging"
    assert plan.population_undo_claimed is False
    assert "primary" in " ".join(plan.non_guarantees).lower() or "production" in " ".join(
        plan.non_guarantees
    ).lower()


def test_execute_document_only_refuses():
    plan = plan_rollback(
        job_id="job-3",
        sync_mode="incremental",
        destination_table="orders",
        staging_table=None,
        rows_written=10,
    )
    with pytest.raises(RollbackRefuseError):
        execute_rollback(
            plan.to_dict(),
            approved_by="ops@example.com",
            reason="want undo",
            drop_staging_fn=lambda *_a, **_k: True,
        )


def test_execute_discard_staging_calls_drop_and_audits():
    plan = plan_rollback(
        job_id="job-4",
        sync_mode="full_refresh_overwrite",
        destination_table="orders",
        staging_table="orders_df_staging",
        rows_written=10,
        promote_blocked=True,
    )
    calls: list[str] = []

    def _drop(table: str) -> bool:
        calls.append(table)
        return True

    result = execute_rollback(
        plan.to_dict(),
        approved_by="ops@example.com",
        reason="Discard blocked staging after Validate failure",
        drop_staging_fn=_drop,
    )
    assert result["ok"] is True
    assert result["strategy"] == "DISCARD_STAGING"
    assert calls == ["orders_df_staging"]
    assert result["population_undo_claimed"] is False
    assert result["audit"]["approved_by"] == "ops@example.com"


def test_execute_discard_staging_fails_closed_when_drop_fails():
    plan = plan_rollback(
        job_id="job-5",
        sync_mode="full_refresh_overwrite",
        destination_table="orders",
        staging_table="orders_df_staging",
        rows_written=10,
        promote_blocked=True,
    )
    with pytest.raises(RollbackRefuseError):
        execute_rollback(
            plan.to_dict(),
            approved_by="ops@example.com",
            reason="try discard",
            drop_staging_fn=lambda *_a, **_k: False,
        )


def test_tampered_plan_rejected():
    plan = plan_rollback(
        job_id="job-6",
        sync_mode="incremental",
        destination_table="orders",
        staging_table=None,
        rows_written=1,
    )
    raw = plan.to_dict()
    raw["strategy"] = "DISCARD_STAGING"
    with pytest.raises(RollbackContractError):
        execute_rollback(
            raw,
            approved_by="evil",
            reason="tamper",
            drop_staging_fn=lambda *_a, **_k: True,
        )


def test_recovery_honesty_surfaces_staging_discard_capability():
    h = honesty_dict()
    assert h["transfer_undo_claimed"] is False
    caps = h["capabilities"]
    assert caps["transfer_undo"]["available"] is False
    assert caps["staging_discard"]["available"] is True
    assert "DISCARD_STAGING" in caps["staging_discard"]["note"] or "staging" in caps[
        "staging_discard"
    ]["note"].lower()
