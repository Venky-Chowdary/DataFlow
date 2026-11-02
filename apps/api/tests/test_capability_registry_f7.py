"""Phase F7 — capability registry coverage + Decision Artifact consumption."""

from __future__ import annotations

from services.connector_capability_registry import (
    CAPABILITY_REGISTRY,
    capability_profile_hash,
    export_live_capability_matrix,
    get_connector_capability,
)
from services.decision_kernel.execute_gate import build_artifact_from_mappings
from src.transfer.connector_capabilities import (
    TRANSFER_READY_CATALOG_IDS,
    resolve_driver_type,
)


def test_every_transfer_ready_unique_driver_has_static_registry_entry():
    drivers = sorted({resolve_driver_type(cid) for cid in TRANSFER_READY_CATALOG_IDS})
    missing = [d for d in drivers if d not in CAPABILITY_REGISTRY]
    assert not missing, f"F7: add CAPABILITY_REGISTRY rows for {missing}"


def test_capability_profile_hash_stable_and_hex():
    h1 = capability_profile_hash("postgresql")
    h2 = capability_profile_hash("postgresql")
    assert h1 == h2
    assert len(h1) == 64
    assert all(c in "0123456789abcdef" for c in h1)
    # Alias must resolve to same normalized profile as postgres driver.
    assert capability_profile_hash("postgres") == h1 or get_connector_capability(
        "postgres"
    ).get("normalized_key") == "postgresql"


def test_export_matrix_has_no_missing_static():
    matrix = export_live_capability_matrix()
    assert matrix["unique_driver_count"] >= 25
    assert matrix["missing_static_registry"] == []
    assert len(matrix["engines"]) == matrix["unique_driver_count"]


def test_decision_artifact_stamps_capability_hashes():
    art = build_artifact_from_mappings(
        [
            {
                "source": "id",
                "target": "id",
                "source_type": "BIGINT",
                "target_type": "BIGINT",
                "confidence": 0.99,
            }
        ],
        source_db="postgresql",
        dest_db="snowflake",
        artifact_id="da_f7",
        created_at="1970-01-01T00:00:00+00:00",
    )
    assert art.capability_source_hash == capability_profile_hash("postgresql")
    assert art.capability_dest_hash == capability_profile_hash("snowflake")
    assert art.capability_source_hash != art.capability_dest_hash
