"""GA Module B — tampered Risk Contract signatures must not re-sign / clear."""

from __future__ import annotations

from services.migration_risk_contract import create_migration_risk_contract
from services.preflight_service import _hydrate_risk_contract


def test_hydrate_accepts_valid_signature():
    c = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="ops@example.com",
        reason="cast",
        execution_policy="CAST_AND_CONTINUE",
    )
    out = _hydrate_risk_contract({"source": "amt", "risk_contract": c.to_dict()})
    assert out is not None
    assert out["risk_id"] == c.risk_id


def test_hydrate_refuses_tampered_signature():
    c = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="ops@example.com",
        reason="cast",
        execution_policy="CAST_AND_CONTINUE",
    )
    raw = c.to_dict()
    raw["execution_policy"] = "FAIL_JOB"  # body changed, signature stale
    out = _hydrate_risk_contract({"source": "amt", "risk_contract": raw})
    assert out is None


def test_hydrate_signs_unsigned_draft():
    draft = {
        "column": "amt",
        "source_type": "TEXT",
        "destination_type": "INTEGER",
        "approved_by": "ops@example.com",
        "reason": "cast",
        "execution_policy": "CAST_AND_CONTINUE",
    }
    out = _hydrate_risk_contract({"source": "amt", "risk_contract": draft})
    assert out is not None
    assert str(out.get("signature") or "").startswith("mrc-sha256:")
