"""Parity at Scale phase 3 and the signed proof pack (review defects 4 and 7).

A scan upgrades Gate-8's claim to a full population proof only where Gate-8
was merely coverage-limited: it passed, no fidelity veto applies and it
claimed no more than a sample. A failed Gate-8, coerced cells, a failed
ladder layer or another scope stay as Gate-8 decided, whatever the scan says.
"""

from __future__ import annotations

from typing import Any

import pytest

PINNED_MATCH = {
    "outcome": "match", "complete": True, "certified": True,
    "consistency": {"kind": "pinned", "source": {"kind": "pinned"}, "destination": {"kind": "pinned"}},
}


def _recon(**over: Any) -> dict[str, Any]:
    recon = {"passed": True, "source_rows": 900_000, "target_rows": 900_000, "phase": "post_write",
             "coverage_limit": {"rows": 900_000, "limit": 250_000},
             "verification_verdict_qualifier": "coverage_limited",
             "parity_at_scale": dict(PINNED_MATCH)}
    recon.update(over)
    return recon


def _classify(recon: dict[str, Any]) -> dict[str, Any]:
    from services.signed_proof_pack import classify_post_write_assurance

    return classify_post_write_assurance(recon)


def _completeness(recon: dict[str, Any]) -> list[str]:
    from services.signed_proof_pack import proof_pack_evidence_completeness_errors

    return proof_pack_evidence_completeness_errors(
        job_success=True, ddl_hash="d" * 64, mapping_hash="m" * 64,
        connector_versions={"source": "postgresql 16.15", "destination": "postgresql 16.15"},
        reconciliation=recon, claim_migration_proven=True)


def test_a_coverage_limited_gate8_is_upgraded_by_a_pinned_matching_scan() -> None:
    claim = _classify(_recon())
    assert claim["migration_proven"] is True
    assert claim["source_checksum_provenance"] == "engine_segment_digest"
    assert not [e for e in _completeness(_recon()) if "checksums" in e]


@pytest.mark.parametrize("over, why", [
    ({"passed": False, "phase": "post_write_failed"}, "Gate-8 failed"),
    ({"coerced_null_rows": 4}, "coerced cells"),
    ({"verification_ladder": {"passed": False, "layers": {"L4": {"passed": False}}}}, "failed ladder L4"),
    ({"coverage_limit": None, "verification_verdict_qualifier": None}, "not coverage-limited"),
    ({"coverage": "writer_ack", "source_checksum": "abc"}, "writer acknowledgement only"),
])
def test_the_scan_never_overrides_gate8(over: dict[str, Any], why: str) -> None:
    recon = _recon(**over)
    claim = _classify(recon)
    assert claim["migration_proven"] is False, why
    assert claim.get("source_checksum_provenance") != "engine_segment_digest", why
    assert any("checksums" in e for e in _completeness(recon)), why


def test_a_scan_with_differences_adds_nothing_and_never_calls_quarantine_a_failure() -> None:
    recon = _recon(parity_at_scale={**PINNED_MATCH, "outcome": "differences_found"})
    claim = _classify(recon)
    assert claim["migration_proven"] is False and claim["population_proof"] is False
    base = _classify(_recon(parity_at_scale=None))
    assert claim["claim_level"] == base["claim_level"]  # Gate-8's own claim stands


def test_a_superseded_or_unfinished_scan_proves_nothing() -> None:
    for outcome in ("running", "error"):
        claim = _classify(_recon(parity_at_scale={"outcome": outcome, "scan_id": "psd1-new",
                                                  "superseded_scan_id": "psd1-old"}))
        assert claim["migration_proven"] is False, outcome
