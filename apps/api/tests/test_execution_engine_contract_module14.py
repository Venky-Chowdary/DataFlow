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
    kafka_offset_commit_must_fail_closed,
    noop_checkpoint_posture,
)


def test_refuse_insert_resume_without_checkpoint():
    d = decide_resume(
        resume_requested=True,
        checkpoint_has_progress=False,
        sync_mode="insert",
    )
    assert d["kind"] == ResumeKind.REFUSED.value
    assert d["allowed"] is False
    with pytest.raises(ExecutionContractError):
        assert_resume_allowed(
            resume_requested=True,
            checkpoint_has_progress=False,
            sync_mode="append",
        )


def test_upsert_may_restart_from_zero():
    d = assert_resume_allowed(
        resume_requested=True,
        checkpoint_has_progress=False,
        sync_mode="upsert",
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
    assert blob["capabilities"]["exactly_once"]["available"] is False
    assert "refuse_insert_resume_without_checkpoint" in blob["duplicate_prevention"]
    assert "exactly_once" not in blob["selectable_delivery"]


def test_assert_delivery_refuses_exactly_once():
    from services.execution_engine_contract import (
        DeliveryGuaranteeError,
        assert_delivery_guarantee_allowed,
    )

    assert assert_delivery_guarantee_allowed("at_least_once") == "at_least_once"
    try:
        assert_delivery_guarantee_allowed("exactly_once")
        raised = False
    except DeliveryGuaranteeError:
        raised = True
    assert raised is True


def test_recovery_honesty_embeds_execution_contract():
    from services.recovery_honesty import honesty_dict

    h = honesty_dict()
    assert "execution_engine_contract" in h
    assert h["execution_engine_contract"]["delivery_default"] == "at_least_once"
