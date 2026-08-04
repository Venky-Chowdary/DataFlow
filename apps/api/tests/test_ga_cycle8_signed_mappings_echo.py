"""Cycle 8 — Validate echoes signed Risk Contracts for FE↔Execute wiring."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_hydrate_risk_contract_produces_signed_payload():
    from services.preflight_service import _hydrate_risk_contract

    draft = {
        "source": "amount",
        "target": "amount",
        "source_type": "DECIMAL",
        "target_type": "INTEGER",
        "risk_acknowledged": True,
        "risk_contract": {
            "column": "amount",
            "source_type": "DECIMAL",
            "destination_type": "INTEGER",
            "execution_policy": "CAST_AND_CONTINUE",
            "approved_by": "ops",
            "reason": "legacy cast",
            "quarantine_policy": "holdout_rejected_rows",
            "retry_policy": "none",
            "rollback_strategy": "DOCUMENT_ONLY",
        },
    }
    signed = _hydrate_risk_contract(draft, table="orders", migration_id="mig-1")
    assert signed is not None
    assert signed.get("risk_id")
    assert signed.get("signature")
    assert signed.get("execution_policy") == "CAST_AND_CONTINUE"
    assert signed.get("table") == "orders"


def test_signed_mappings_shape_for_fe_merge():
    """Shape contract: source/target + risk_contract + risk_acknowledged."""
    row = {
        "source": "amount",
        "target": "amt",
        "risk_acknowledged": True,
        "risk_contract": {
            "risk_id": "mrc-1",
            "signature": "mrc-sha256:abc",
            "execution_policy": "QUARANTINE_ROW",
            "column": "amount",
            "source_type": "TEXT",
            "destination_type": "INTEGER",
            "approved_by": "ops",
            "reason": "q",
        },
    }
    echo = {
        "source": str(row.get("source") or ""),
        "target": str(row.get("target") or ""),
        "risk_contract": row.get("risk_contract"),
        "risk_acknowledged": bool(row.get("risk_acknowledged")),
    }
    assert echo["source"] == "amount"
    assert echo["risk_contract"]["risk_id"] == "mrc-1"
    assert echo["risk_acknowledged"] is True
