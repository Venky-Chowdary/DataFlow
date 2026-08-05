"""GA Module J — property / fuzz invariants for hardening contracts."""

from __future__ import annotations

import random

import pytest

from services.conversion_contract import classify_conversion, invents_unproven_capacity
from services.migration_risk_contract import (
    create_migration_risk_contract,
    verify_risk_contract,
)
from services.reconciliation import reconcile
from services.signed_proof_pack import (
    build_signed_proof_pack,
    verify_signed_proof_pack,
)

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402


@given(
    col=st.text(min_size=1, max_size=32, alphabet=st.characters(whitelist_categories=("L", "N"))),
    policy=st.sampled_from(
        ["CAST_AND_CONTINUE", "QUARANTINE_ROW", "SKIP_ROW", "FAIL_JOB", "STOP_COLUMN"]
    ),
)
@settings(max_examples=40, deadline=None)
def test_property_risk_contract_signature_roundtrip(col: str, policy: str):
    c = create_migration_risk_contract(
        column=col,
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="prop@example.com",
        reason="property test",
        execution_policy=policy,
    )
    assert verify_risk_contract(c) is True
    raw = c.to_dict()
    raw["reason"] = raw["reason"] + "x"
    assert verify_risk_contract(raw) is False


@given(
    src=st.sampled_from(["abc", "xyz", "111"]),
    tgt=st.sampled_from(["abc", "xyz", "222"]),
    compared=st.integers(min_value=0, max_value=20),
)
@settings(max_examples=40, deadline=None)
def test_property_checksum_mismatch_never_passes(src: str, tgt: str, compared: int):
    if src == tgt:
        return
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum=src,
        target_checksum=tgt,
        strict_checksum=False,
        sample_compare={"passed": True, "compared": max(compared, 1), "mismatches": []},
    )
    assert r.passed is False
    assert r.checksum_match is False


def test_fuzz_proof_pack_tamper_fails_verify():
    pack = build_signed_proof_pack(
        job_id="fuzz-1",
        reconciliation={
            "passed": True,
            "coverage": "full_checksum",
            "source_checksum": "a",
            "target_checksum": "a",
        },
        accepted_risks=[
            create_migration_risk_contract(
                column="c",
                source_type="T",
                destination_type="I",
                approved_by="ops",
                reason="fuzz",
                execution_policy="CAST_AND_CONTINUE",
            ).to_dict()
        ],
    )
    assert verify_signed_proof_pack(pack)["ok"] is True
    # Mutate a random top-level field (excluding signature envelope).
    keys = [k for k in pack if k not in ("content_sha256", "signature")]
    key = random.choice(keys)
    pack[key] = {"tampered": True} if not isinstance(pack[key], dict) else {
        **pack[key],
        "_fuzz": random.random(),
    }
    assert verify_signed_proof_pack(pack)["ok"] is False


def test_invent_never_classified_lossless_without_contract():
    assert invents_unproven_capacity("DECIMAL", "DECIMAL(38,10)", dest_db="snowflake")
    c = classify_conversion(
        "DECIMAL",
        "DECIMAL(38,10)",
        dest_db="snowflake",
        risk_acknowledged=False,
    )
    assert c["conversion_class"] != "lossless"
    assert c["requires_risk_contract"] is True
