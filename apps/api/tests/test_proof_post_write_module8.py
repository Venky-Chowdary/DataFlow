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


def test_source_reread_without_identity_alignment_is_not_migration_proven():
    pack = build_signed_proof_pack(
        job_id="reread-unaligned",
        reconciliation={
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "source_checksum_provenance": "independent_source_reread",
            "identity_hash_aligned": False,
        },
        ddl_hash="ddl-abc",
        mapping_hash="map-abc",
        connector_versions={
            "source": "openpyxl 3.1.5",
            "destination": "psycopg2 2.9.9 / server 16.2",
        },
        job_success=True,
    )
    assert pack["assurance"]["migration_proven"] is False
    reasons = " ".join(pack.get("proof_incomplete_reasons") or [])
    assert "identity/hash alignment" in reasons


def test_source_reread_with_identity_and_versions_is_migration_proven():
    """Named bar: independent re-read + identity alignment + release strings."""
    pack = build_signed_proof_pack(
        job_id="reread-aligned",
        reconciliation={
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "a" * 64,
            "target_checksum": "a" * 64,
            "source_rows": 10,
            "target_rows": 10,
            "source_checksum_provenance": "independent_source_reread",
            "source_independently_reread": True,
            "identity_hash_aligned": True,
        },
        ddl_hash="ddl-abc",
        mapping_hash="map-abc",
        connector_versions={
            "source": "openpyxl 3.1.5",
            "destination": "postgresql 16.2 / psycopg2 2.9.9",
        },
        job_success=True,
    )
    assert pack["assurance"]["migration_proven"] is True
    assert pack["connector_versions_honesty"] == "provided"
    assert_pack_may_claim_migration_proven(pack)


def test_proof_pack_records_elapsed_and_phase_split():
    """Elapsed time is part of the signed evidence, with the phase split beside it."""
    pack = build_signed_proof_pack(
        job_id="timed",
        reconciliation={"passed": True, "coverage": "row_count"},
        timing={
            "elapsed_seconds": 1.25,
            "records_per_second": 8.0,
            "phase_profile": {
                "phases": [
                    {"phase": "transform_write", "seconds": 0.8, "rows": 10},
                    {"phase": "checksum", "seconds": 0.4, "rows": 10},
                ],
                "busy_seconds": 1.2,
                "elapsed_seconds": 1.25,
            },
        },
    )
    assert pack["timing"]["elapsed_seconds"] == 1.25
    assert pack["timing"]["phase_profile"]["phases"][1]["phase"] == "checksum"
    assert verify_signed_proof_pack(pack)["ok"] is True


def test_format_only_versions_cannot_keep_migration_proven():
    pack = build_signed_proof_pack(
        job_id="fmt-proven",
        reconciliation={
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
        },
        ddl_hash="ddl-abc",
        mapping_hash="map-abc",
        connector_versions={"source": "postgresql", "destination": "snowflake"},
        job_success=True,
    )
    assert pack["assurance"]["migration_proven"] is False
    assert pack["connector_versions_honesty"] == "format_or_kind_only"
    reasons = " ".join(pack.get("proof_incomplete_reasons") or [])
    assert "captured release" in reasons


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

