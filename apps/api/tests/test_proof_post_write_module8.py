"""Module 8 — Proof must never claim migration correctness without post-write evidence."""

from __future__ import annotations

import pytest

from services.preflight_proof_bundle import build_preflight_proof_bundle
from services.signed_proof_pack import (
    ProofClaimError,
    assert_pack_may_claim_migration_proven,
    build_signed_proof_pack,
    classify_post_write_assurance,
    export_proof_pack_for_job,
    verify_signed_proof_pack,
)


def test_pre_write_simulation_is_not_migration_proven():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "preview": True,
            "post_write_pending": True,
            "phase": "pre_write_simulation",
        }
    )
    assert a["migration_proven"] is False
    assert a["post_write_verified"] is False
    assert a["claim_level"] == "pre_write_only"


def test_writer_ack_is_not_migration_proven():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "source_checksum": "abc",
            "target_checksum": "",
        }
    )
    assert a["migration_proven"] is False
    assert a["claim_level"] == "writer_ack"


def test_sample_post_write_is_assured_not_population():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_sample_verified",
            "coverage": "sample",
            "checksum_match": False,
            "population_proof": False,
            "source_checksum": "a",
            "target_checksum": "b",
            "sample_compare": {"passed": True, "compared": 5},
        }
    )
    assert a["migration_proven"] is False  # sample ≠ population claim
    assert a["post_write_verified"] is True
    assert a["claim_level"] == "sample"
    assert a["population_proof"] is False


def test_full_checksum_post_write_is_strongest_claim():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
        }
    )
    assert a["post_write_verified"] is True
    assert a["claim_level"] == "full_checksum"
    # Still not RI / population orphan proof — migration row-fidelity claim only.
    assert a["migration_proven"] is True
    assert a["population_proof"] is False
    assert a["referential_integrity_proven"] is False


def test_full_checksum_with_dest_ri_scan_is_ri_proven():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "physical_state": {
                "referential_integrity": {
                    "verified": True,
                    "asked": True,
                    "relations": [
                        {
                            "columns": ["parent_id"],
                            "referred_table": "parent",
                            "status": "scanned",
                            "available": True,
                            "orphan_count": 0,
                        }
                    ],
                    "orphan_rows": 0,
                }
            },
        }
    )
    assert a["migration_proven"] is True
    assert a["referential_integrity_proven"] is True
    assert "referential integrity proven" in a["note"].lower()


def test_full_checksum_without_relationships_is_not_ri_proven():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "physical_state": {
                "referential_integrity": {
                    "verified": False,
                    "asked": False,
                    "reason": "source declares no foreign keys",
                    "relations": [],
                }
            },
        }
    )
    assert a["migration_proven"] is True
    assert a["referential_integrity_proven"] is False


def test_signed_pack_stamps_assurance_and_refuses_fake_proven():
    pack = build_signed_proof_pack(
        job_id="j1",
        reconciliation={"passed": True, "preview": True, "post_write_pending": True},
        actor="ops",
    )
    assert pack["assurance"]["migration_proven"] is False
    assert pack["assurance"]["claim_level"] == "pre_write_only"
    assert verify_signed_proof_pack(pack)["ok"] is True
    with pytest.raises(ProofClaimError):
        assert_pack_may_claim_migration_proven(pack)


def test_signed_pack_with_full_checksum_may_claim_row_fidelity():
    pack = build_signed_proof_pack(
        job_id="j2",
        reconciliation={
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "x",
            "target_checksum": "x",
            "source_rows": 10,
            "target_rows": 10,
        },
        actor="ops",
        ddl_hash="ddl-abc",
        mapping_hash="map-abc",
        connector_versions={"source": "postgresql@14.0", "destination": "snowflake@7.0"},
    )
    assert pack["assurance"]["migration_proven"] is True
    assert_pack_may_claim_migration_proven(pack)


def test_export_without_recon_is_incomplete_not_proven():
    pack = export_proof_pack_for_job({"_id": "j3", "reconciliation": None}, actor="ops")
    assert pack["assurance"]["claim_level"] == "none"
    assert pack["assurance"]["migration_proven"] is False


def test_preflight_bundle_approve_never_means_migration_proven():
    """Execute-ready ≠ migration proven — preview must stamp honesty."""
    bundle = build_preflight_proof_bundle(
        columns=["id"],
        sample_rows=[{"id": 1}],
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_records=[{"id": 1}],
        target_records=[],  # pre-write
        validation_mode="strict",
    )
    assert bundle["migration_proven"] is False
    assert bundle["post_write_proof"] is False
    # May still be execute-ready when no blockers.
    assert bundle["transfer_decision"]["decision"] in {"approve", "review", "block"}
    assert "post-write" in (bundle.get("evidence_summary") or "").lower() or bundle[
        "reconciliation"
    ].get("preview")


def test_append_delta_is_not_migration_proven():
    a = classify_post_write_assurance(
        {
            "passed": True,
            "phase": "post_write_row_count",
            "coverage": "row_count",
            "assurance_level": "row_count",
            "checksum_scope": "whole_table_not_comparable",
            "source_checksum": "aaa",
            "target_checksum": "bbb",
            "checksum_match": False,
            "message": "Append delta verified (200 row(s) appended: 100 → 300).",
        }
    )
    assert a["migration_proven"] is False
    assert a["claim_level"] == "row_count"
    assert a["checksum_match"] is False
    assert a["post_write_verified"] is True

