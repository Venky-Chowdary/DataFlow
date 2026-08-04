"""Cycle 7 Enterprise GA — create-new invent, string width, hollow proof."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_create_new_without_map_stamp_does_not_invent_boolean_from_samples():
    """DDL must keep source/carrier proposal — samples must not invent BOOLEAN."""
    from connectors.writer_common import resolve_target_columns

    cols, types = resolve_target_columns(
        [{"source": "flag", "target": "flag"}],
        {"flag": "VARCHAR"},
        sample_values_by_source={"flag": ["true", "false", "1", "0"]},
        table_exists=False,
    )
    by = dict(zip(cols, types))
    assert by["flag"].upper() in {"VARCHAR", "STRING", "TEXT"}
    assert by["flag"].upper() != "BOOLEAN"


def test_create_new_without_map_stamp_does_not_invent_integer_from_samples():
    from connectors.writer_common import resolve_target_columns

    cols, types = resolve_target_columns(
        [{"source": "code", "target": "code"}],
        {"code": "VARCHAR"},
        sample_values_by_source={"code": ["1", "2", "3"]},
        table_exists=False,
    )
    by = dict(zip(cols, types))
    assert "INT" not in by["code"].upper().replace("VARCHAR", "")


def test_bare_string_to_varchar_n_invents_capacity():
    from services.conversion_contract import invents_unproven_capacity

    assert invents_unproven_capacity("TEXT", "VARCHAR(100)", dest_db="postgresql")
    assert invents_unproven_capacity("STRING", "VARCHAR(255)", dest_db="bigquery")
    assert invents_unproven_capacity("VARCHAR", "VARCHAR(50)", dest_db="mysql")
    # Proven width → same width is not invent of unproven capacity.
    assert not invents_unproven_capacity("VARCHAR(100)", "VARCHAR(100)", dest_db="postgresql")


def test_hollow_proven_pack_strips_migration_proven_without_hashes():
    from services.signed_proof_pack import (
        assert_pack_may_claim_migration_proven,
        build_signed_proof_pack,
        ProofClaimError,
    )
    import pytest

    pack = build_signed_proof_pack(
        job_id="hollow-1",
        reconciliation={
            "passed": True,
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
        },
        actor="ops",
        # Intentionally omit ddl_hash / mapping_hash / connector_versions
    )
    assert pack["assurance"]["migration_proven"] is False
    assert pack["assurance"]["claim_level"] == "incomplete_proof_evidence"
    reasons = " ".join(pack.get("proof_incomplete_reasons") or [])
    assert "ddl_hash" in reasons or "mapping_hash" in reasons
    assert "connector_versions" in reasons
    with pytest.raises(ProofClaimError):
        assert_pack_may_claim_migration_proven(pack)


def test_complete_evidence_keeps_migration_proven():
    from services.signed_proof_pack import (
        assert_pack_may_claim_migration_proven,
        build_signed_proof_pack,
    )

    pack = build_signed_proof_pack(
        job_id="solid-1",
        reconciliation={
            "passed": True,
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
        },
        actor="ops",
        ddl_hash="ddl-1",
        mapping_hash="map-1",
        connector_versions={"source": "pg@1", "destination": "sf@2"},
    )
    assert pack["assurance"]["migration_proven"] is True
    assert pack["connector_versions_honesty"] == "provided"
    assert_pack_may_claim_migration_proven(pack)


def test_format_only_connector_versions_honesty_stamp():
    from services.signed_proof_pack import build_signed_proof_pack

    pack = build_signed_proof_pack(
        job_id="fmt-1",
        reconciliation={"passed": False},
        connector_versions={"source": "postgresql", "destination": "snowflake"},
    )
    assert pack["connector_versions_honesty"] == "format_or_kind_only"
