"""Module 14 — Execution Engine Contract tests."""

from __future__ import annotations

import pytest

from services.execution_engine_contract import (
    ExecutionContractError,
    ResumeKind,
    assert_resume_allowed,
    decide_resume,
    execution_contract_dict,
    is_idempotent_sync,
    job_has_durable_progress,
    kafka_offset_commit_must_fail_closed,
    noop_checkpoint_posture,
    resolve_reclaim_resume,
)


def test_refuse_insert_resume_without_checkpoint_after_writes():
    """Append/insert refuse only when rows were committed but checkpoint lost."""
    d = decide_resume(
        resume_requested=True,
        checkpoint_has_progress=False,
        sync_mode="insert",
        rows_committed=50,
    )
    assert d["kind"] == ResumeKind.REFUSED.value
    assert d["allowed"] is False
    with pytest.raises(ExecutionContractError):
        assert_resume_allowed(
            resume_requested=True,
            checkpoint_has_progress=False,
            sync_mode="append",
            rows_committed=1,
        )


def test_zero_writes_allows_from_zero_even_for_append():
    """Orphan/claim false-resume with 0 rows must not refuse Excel→PG append."""
    d = assert_resume_allowed(
        resume_requested=True,
        checkpoint_has_progress=False,
        sync_mode="full_refresh_append",
        rows_committed=0,
    )
    assert d["kind"] == ResumeKind.FROM_ZERO_NO_WRITES.value
    assert d["allowed"] is True


def test_upsert_may_restart_from_zero():
    d = assert_resume_allowed(
        resume_requested=True,
        checkpoint_has_progress=False,
        sync_mode="upsert",
        rows_committed=10,
    )
    assert d["kind"] == ResumeKind.FROM_ZERO_IDEMPOTENT.value
    assert d["delivery"] == "at_least_once"


def test_checkpoint_resume_allowed():
    d = assert_resume_allowed(
        resume_requested=True,
        checkpoint_has_progress=True,
        sync_mode="insert",
    )
    assert d["kind"] == ResumeKind.CHECKPOINT.value


def test_reclaim_resume_only_when_durable_progress():
    assert resolve_reclaim_resume({"status": "pending"}) is False
    assert resolve_reclaim_resume({"status": "running", "checkpoint": {}}) is False
    assert resolve_reclaim_resume({"status": "running", "records_processed": 0}) is False
    assert resolve_reclaim_resume({"status": "running", "records_processed": 12}) is True
    assert job_has_durable_progress(
        {"checkpoint": {"rows_processed": 5, "chunk_index": 0}}
    )
    assert not job_has_durable_progress({"checkpoint": {"rows_processed": 0}})
    # Cursor / file_offset alone are durable — reclaim must resume, not wipe.
    assert resolve_reclaim_resume(
        {"status": "running", "checkpoint": {"file_offset": 4096, "rows_processed": 0}}
    )
    assert resolve_reclaim_resume(
        {"status": "paused", "checkpoint": {"cursor_value": "2024-01-01", "offset": 0}}
    )


def test_engine_checkpoint_progress_matches_reclaim_tokens():
    """Parity: engine must not wipe file_offset/cursor on Module 14 reclaim."""
    from services.checkpoint_service import Checkpoint
    from src.transfer.engine import _checkpoint_has_progress

    bare = Checkpoint(job_id="j1")
    assert _checkpoint_has_progress(bare) is False
    with_file = Checkpoint(job_id="j1", file_offset=2048)
    assert _checkpoint_has_progress(with_file) is True
    with_cursor = Checkpoint(job_id="j1", cursor_value="ts-1")
    assert _checkpoint_has_progress(with_cursor) is True


def test_idempotent_sync_helpers():
    assert is_idempotent_sync("upsert")
    assert is_idempotent_sync("overwrite")
    assert not is_idempotent_sync("insert")
    assert not is_idempotent_sync("append")


def test_kafka_offset_failure_is_fail_closed_error():
    err = kafka_offset_commit_must_fail_closed(RuntimeError("broker down"))
    assert isinstance(err, ExecutionContractError)
    assert "fail closed" in str(err).lower()
    assert "at-least-once" in str(err).lower()


def test_noop_checkpoint_never_claims_resume():
    posture = noop_checkpoint_posture()
    assert posture["durable"] is False
    assert posture["resume_supported"] is False


def test_contract_never_claims_exactly_once():
    blob = execution_contract_dict()
    assert blob["delivery_default"] == "at_least_once"
    assert blob["never_claim_exactly_once"] is True
    assert blob["never_silent_drop"] is True
    assert blob["capabilities"]["exactly_once"]["available"] is True
    assert blob["capabilities"]["exactly_once"]["platform_claimed"] is False
    assert "refuse_insert_resume_without_checkpoint_after_writes" in blob[
        "duplicate_prevention"
    ]
    assert "allow_from_zero_when_rows_committed_zero" in blob["duplicate_prevention"]
    assert "exactly_once" in blob["selectable_delivery"]


def test_assert_delivery_allows_exactly_once_token():
    from services.execution_engine_contract import (
        DeliveryGuaranteeError,
        assert_delivery_guarantee_allowed,
    )

    assert assert_delivery_guarantee_allowed("at_least_once") == "at_least_once"
    assert assert_delivery_guarantee_allowed("exactly_once") == "exactly_once"
    try:
        assert_delivery_guarantee_allowed("at_most_once")
        raised = False
    except DeliveryGuaranteeError:
        raised = True
    assert raised is True


def test_recovery_honesty_embeds_execution_contract():
    from services.recovery_honesty import honesty_dict

    h = honesty_dict()
    assert "execution_engine_contract" in h
    assert h["execution_engine_contract"]["delivery_default"] == "at_least_once"
