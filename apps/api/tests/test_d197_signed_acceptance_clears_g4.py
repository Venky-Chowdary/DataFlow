"""D197: a signed acceptance the plan minted must be able to clear G4.

QA signed ``risk_acceptance`` on the Mongo plan. ``_sign_required_risk_contracts``
signed a contract on ``_id`` (fidelity ``mutate`` from ``trim_id``), but
``trim_id`` is a safe normalize, so G4 did not route the mapping through the
contract branch; its confidence branch (0.70 < 0.85) accepted only
``user_override`` and never read the contract. The approval could not clear the
block it was signed for. A verified contract naming the pair is an attributed
approval at least as strong as ``user_override``; a tampered one is not.
"""

from __future__ import annotations

from typing import Any

import pytest
from preflight.risk_contract import make_clearing_risk_contract, mapping_review_cleared

from services.mapping_pipeline import assert_mappings_executable


def _held_id(**extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source": "_id",
        "target": "_id",
        "source_type": "OBJECTID",
        "target_type": "VARCHAR(24)",
        "transform": "trim_id",
        "fidelity": "mutate",
        "requires_review": True,
        "confidence": 0.7,
    }
    row.update(extra)
    return row


def _signed(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["risk_contract"] = make_clearing_risk_contract(
        column="_id",
        source_type="OBJECTID",
        destination_type="VARCHAR(24)",
        execution_policy="QUARANTINE_ROW",
        approved_by="qa@example.com",
        reason="ObjectId lands as 24 hex characters",
    )
    return out


def _g4(row: dict[str, Any]):
    from preflight.gates import gate_g4_mapping_confidence
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    fields = {k: row[k] for k in (
        "source", "target", "confidence", "requires_review", "fidelity", "transform",
    )}
    if row.get("risk_contract"):
        fields["risk_contract"] = row["risk_contract"]
    if row.get("user_override"):
        fields["user_override"] = True
    plan = TransferPlan(
        source=SourceConfig(
            kind="database", connected=True, parseable=True,
            columns=[ColumnSchema(name="_id", inferred_type="OBJECTID")],
        ),
        destination=DestinationConfig(
            kind="database", db_type="postgresql", connected=True,
            table_exists=False, can_write=True, target_columns=[],
        ),
        mappings=[ColumnMapping(**fields)],
        confidence_threshold=0.85,
        validation_mode="strict",
    )
    return gate_g4_mapping_confidence(PreflightContext(plan=plan, sample_rows=[]))


def test_unapproved_low_confidence_mapping_still_blocks():
    gate = _g4(_held_id())
    assert gate.status.value == "block", gate.message
    assert mapping_review_cleared(_held_id()) is False
    with pytest.raises(ValueError):
        assert_mappings_executable([_held_id()])


def test_signed_contract_clears_g4_and_execute_alike():
    row = _signed(_held_id())
    gate = _g4(row)
    assert gate.status.value != "block", gate.message
    assert mapping_review_cleared(row) is True
    assert_mappings_executable([row])


def test_tampered_contract_does_not_clear():
    row = _signed(_held_id())
    row["risk_contract"] = {**row["risk_contract"], "reason": "edited after signing"}
    assert _g4(row).status.value == "block"
    assert mapping_review_cleared(row) is False


def test_user_override_still_clears_as_before():
    row = _held_id(user_override=True)
    assert _g4(row).status.value != "block"
    assert mapping_review_cleared(row) is True
