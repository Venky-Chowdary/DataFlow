"""Execute must check a Validate stamp against the Map the operator approved.

Regression: an untouched Map came back as "Decision Artifact DDL identity
diverged from current Map" because Execute re-derives its own mapping set
(``_auto_map`` → enrich → auto-propagate → additive stamps) and hashed *that*
against a stamp taken over the operator's Map rows.
"""

from __future__ import annotations

from services.decision_kernel import build_artifact_from_mappings
from src.transfer.engine import _enforce_decision_artifact, _operator_contract_maps


class _Req:
    def __init__(
        self,
        mappings: list[dict],
        *,
        artifact: dict | None = None,
        approved_hash: str = "",
    ) -> None:
        self.mappings = mappings
        self.decision_artifact = artifact or {}
        self.approved_ddl_identity_hash = ""
        self.approved_decision_artifact_hash = approved_hash


OPERATOR_MAPS = [
    {"source": "country", "target": "country", "source_type": "TEXT"},
    {"source": "total", "target": "total", "source_type": "DECIMAL(7,3)"},
]
# What Execute re-derives for the same Map (create-compatible-new stamps).
ENRICHED_MAPS = [
    {**OPERATOR_MAPS[0], "target_type": "LONGTEXT"},
    {**OPERATOR_MAPS[1], "target": "total_text", "target_type": "LONGTEXT"},
]


def _validate_artifact() -> dict:
    return build_artifact_from_mappings(
        OPERATOR_MAPS, dest_db="mysql", route_id="validate:mysql"
    ).to_dict()


def test_stamped_run_is_checked_against_the_operator_map() -> None:
    art = _validate_artifact()
    req = _Req(OPERATOR_MAPS, artifact=art, approved_hash=art["content_hash"])

    assert _operator_contract_maps(req, ENRICHED_MAPS) == OPERATOR_MAPS

    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}},
        _operator_contract_maps(req, ENRICHED_MAPS),
        dest_db="mysql",
        approved_decision_artifact_hash=art["content_hash"],
        decision_artifact=art,
        sync_mode="full_refresh_append",
    )
    assert err is None
    assert accepted is not None


def test_enriched_maps_still_diverge_from_the_stamp() -> None:
    """The gate itself is unchanged — only the input it is given."""
    art = _validate_artifact()
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}},
        ENRICHED_MAPS,
        dest_db="mysql",
        approved_decision_artifact_hash=art["content_hash"],
        decision_artifact=art,
        sync_mode="full_refresh_append",
    )
    assert err is not None
    assert "diverged" in err
    assert accepted is None


def test_unstamped_run_keeps_using_the_derived_maps() -> None:
    req = _Req(OPERATOR_MAPS)
    assert _operator_contract_maps(req, ENRICHED_MAPS) == ENRICHED_MAPS


def test_repreflight_dest_exists_artifact_is_not_adopted_when_operator_stamped() -> None:
    """Schedule beat: operator hash + Execute dest-exists overlay must not refuse.

    Adopting proof_bundle.decision_artifact as supplied compared dest-exists
    DDL to the operator Map and failed every later hourly beat.
    """
    from services.schema_fingerprint import fingerprint_schema

    art = build_artifact_from_mappings(
        OPERATOR_MAPS,
        dest_db="mysql",
        route_id="validate:mysql",
        sync_mode="full_refresh_append",
    ).to_dict()
    dest_types = {"country": "TEXT", "total": "DECIMAL(7,3)"}
    live = fingerprint_schema(list(dest_types.keys()), dest_types)
    overlay = build_artifact_from_mappings(
        ENRICHED_MAPS,
        dest_db="mysql",
        dest_fingerprint=live,
        route_id="execute:mysql",
    ).to_dict()
    req = _Req(OPERATOR_MAPS, approved_hash=art["content_hash"])
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": overlay}},
        _operator_contract_maps(req, ENRICHED_MAPS),
        dest_db="mysql",
        approved_decision_artifact_hash=art["content_hash"],
        decision_artifact=None,
        sync_mode="full_refresh_append",
        destination_column_types=dest_types,
        destination_table_exists=True,
    )
    assert err is None
    assert accepted is not None


def test_real_map_edit_after_validate_is_still_refused() -> None:
    """Fail-closed is preserved: editing the Map must invalidate the stamp."""
    art = _validate_artifact()
    edited = [
        OPERATOR_MAPS[0],
        {"source": "total", "target": "total", "source_type": "DECIMAL(7,3)",
         "target_type": "VARCHAR(8)"},
    ]
    req = _Req(edited, artifact=art, approved_hash=art["content_hash"])
    err, accepted = _enforce_decision_artifact(
        {"proof_bundle": {"decision_artifact": art}},
        _operator_contract_maps(req, ENRICHED_MAPS),
        dest_db="mysql",
        approved_decision_artifact_hash=art["content_hash"],
        decision_artifact=art,
        sync_mode="full_refresh_append",
    )
    assert err is not None
    assert accepted is None
