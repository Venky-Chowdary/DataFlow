"""Validate (G4) and Execute must agree on when a review demand is cleared.

Regression: a lossy TIMESTAMP → date mapping carrying a signed continue-policy
Migration Risk Contract cleared G4, so Validate went green, but Execute still
refused it as "require review before Execute" — a green Validate that fails at
Run is a parity break, not an operational outcome.
"""

from __future__ import annotations

from typing import Any

import pytest
from preflight.risk_contract import (
    make_clearing_risk_contract,
    mapping_review_cleared,
)
from services.mapping_pipeline import assert_mappings_executable


def _lossy_mapping(**extra: Any) -> dict[str, Any]:
    mapping: dict[str, Any] = {
        "source": "updated_at",
        "target": "updated_at",
        "source_type": "TIMESTAMP",
        "target_type": "date",
        "transform": "datetime",
        "fidelity": "lossy_cast",
        "type_narrowing": True,
        "requires_review": True,
        "confidence": 0.7,
    }
    mapping.update(extra)
    return mapping


def _signed(mapping: dict[str, Any]) -> dict[str, Any]:
    out = dict(mapping)
    out["risk_contract"] = make_clearing_risk_contract(
        column=str(mapping["source"]),
        source_type=str(mapping.get("source_type") or "TEXT"),
        destination_type=str(mapping.get("target_type") or "TEXT"),
        execution_policy="QUARANTINE_ROW",
        approved_by="admin@dataflow.app",
        reason="precision loss accepted for pilot",
    )
    return out


def test_signed_contract_clears_review_at_execute() -> None:
    mapping = _signed(_lossy_mapping())
    assert mapping_review_cleared(mapping) is True
    assert_mappings_executable([mapping])


def test_unsigned_lossy_mapping_still_blocks_execute() -> None:
    mapping = _lossy_mapping()
    assert mapping_review_cleared(mapping) is False
    with pytest.raises(ValueError):
        assert_mappings_executable([mapping])


def test_boolean_ack_alone_never_clears_review() -> None:
    mapping = _lossy_mapping(risk_acknowledged=True)
    assert mapping_review_cleared(mapping) is False
    with pytest.raises(ValueError):
        assert_mappings_executable([mapping])


def test_user_override_cannot_clear_a_lossy_mapping() -> None:
    """G4 refuses bare override on lossy — Execute must refuse it too."""
    mapping = _lossy_mapping(user_override=True)
    assert mapping_review_cleared(mapping) is False
    with pytest.raises(ValueError):
        assert_mappings_executable([mapping])


def test_user_override_clears_an_ambiguous_non_lossy_mapping() -> None:
    mapping = {
        "source": "amount",
        "target": "amount_usd",
        "source_type": "DECIMAL(18,2)",
        "target_type": "DECIMAL(18,2)",
        "fidelity": "preserve",
        "requires_review": True,
        "score_gap": 0.04,
        "confidence": 0.72,
        "user_override": True,
    }
    assert mapping_review_cleared(mapping) is True
    assert_mappings_executable([mapping])


def test_structural_mapping_needs_contract_not_override() -> None:
    struct = {
        "source": "address",
        "target": "address_city",
        "struct_derived": True,
        "struct_policy": "flatten_top_level_keys",
        "requires_review": True,
        "user_override": True,
        "fidelity": "preserve",
        "confidence": 0.6,
    }
    assert mapping_review_cleared(struct) is False
    signed = _signed(struct)
    assert mapping_review_cleared(signed) is True


def test_g4_and_execute_agree_on_the_same_mapping() -> None:
    """The gate and the Execute assertion must reach the same verdict."""
    from preflight.gates import gate_g4_mapping_confidence
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    mapping = _signed(_lossy_mapping())
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="updated_at", inferred_type="TIMESTAMP")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            table_exists=True,
            can_write=True,
            target_columns=[ColumnSchema(name="updated_at", inferred_type="date")],
        ),
        mappings=[ColumnMapping(**{
            "source": "updated_at",
            "target": "updated_at",
            "confidence": 0.7,
            "requires_review": True,
            "fidelity": "lossy_cast",
            "type_narrowing": True,
            "transform": "datetime",
            "risk_contract": mapping["risk_contract"],
        })],
        confidence_threshold=0.75,
        validation_mode="strict",
    )
    gate = gate_g4_mapping_confidence(PreflightContext(plan=plan, sample_rows=[]))
    assert gate.status.value != "block", gate.message
    assert_mappings_executable([mapping])
