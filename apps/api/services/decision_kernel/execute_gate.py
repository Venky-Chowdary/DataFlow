"""Execute Decision Artifact gate (Phase C11).

Refuse write when an operator-supplied artifact hash does not match the
kernel-stamped artifact for the current Map/DDL. Programmatic
``skip_preflight`` callers may omit a prior Validate stamp — the gate builds
an inline artifact (same pattern as DDL identity inline stamp).
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from services.decision_kernel.ddl import approved_mapping_ddl_fingerprint
from services.decision_kernel.models import (
    AssignmentStrategy,
    CanonicalType,
    ColumnSpec,
    ConversionDecision,
    DecisionArtifact,
    DdlPlan,
    MappingDecision,
    ProofPlan,
    build_decision_artifact,
    decision_artifact_from_dict,
)
from services.decision_kernel.conversion import ConversionClass, classify_mapping
from services.decision_kernel.risk import risk_level_for_conversion
from services.mapping_constraints import is_intentional_omit, write_mappings
from services.shape_contract import (
    SHAPE_CREATE_NEW,
    SHAPE_PENDING,
    SHAPE_UNKNOWN,
    classify_dest_exists_shape,
)


def _norm_col(name: str) -> str:
    return re.sub(r"[\s-]+", "_", str(name or "").strip().lower())


def _canonical_from_type_stamp(stamp: str) -> CanonicalType:
    from services.decision_kernel.types import normalize_logical_type
    from services.type_system import integer_bit_width

    logical = normalize_logical_type(stamp) or "string"
    width = integer_bit_width(stamp)
    return CanonicalType(logical=logical, native=str(stamp or ""), bit_width=width)


def build_artifact_from_mappings(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
    source_db: str = "",
    tenant_id: str = "",
    route_id: str = "",
    source_fingerprint: str = "",
    dest_fingerprint: str = "",
    sync_mode: str = "full_refresh_overwrite",
    error_policy: str = "quarantine",
    artifact_id: str | None = None,
    created_at: str | None = None,
) -> DecisionArtifact:
    """Build a DecisionArtifact from Map rows (kernel invent + ConversionClass)."""
    from services.connector_capability_registry import capability_profile_hash
    from services.decision_kernel.types import materialize_dest_ddl

    maps = [m for m in (mappings or []) if isinstance(m, dict)]
    src_cols: list[ColumnSpec] = []
    dst_cols: list[ColumnSpec] = []
    map_decisions: list[MappingDecision] = []
    column_ddl: dict[str, str] = {}

    for m in maps:
        if is_intentional_omit(m):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or "").strip()
        if not src:
            continue
        src_type = str(
            m.get("source_type") or m.get("inferred_type") or m.get("type") or ""
        )
        tgt_type = str(m.get("target_type") or m.get("dest_type") or "")
        if src_type:
            src_cols.append(ColumnSpec(name=src, canonical=_canonical_from_type_stamp(src_type)))
        wire = ""
        if tgt and tgt_type:
            wire = str(
                materialize_dest_ddl(dest_db, tgt_type, source_type=src_type) or tgt_type
            )
            column_ddl[tgt] = wire
            dst_cols.append(
                ColumnSpec(name=tgt, canonical=_canonical_from_type_stamp(wire or tgt_type))
            )
        conv = classify_mapping(
            m,
            declared_source_type=src_type,
            declared_target_type=tgt_type,
            destination_db_type=dest_db,
        )
        try:
            cclass = ConversionClass(str(conv["conversion_class"]))
        except ValueError:
            cclass = ConversionClass.NEEDS_MANUAL_MAPPING
        create_new = bool(m.get("create_new") or m.get("createNew"))
        strategy = AssignmentStrategy.OPERATOR_SUPPLIED
        if create_new:
            strategy = AssignmentStrategy.CREATE_NEW
        elif not tgt:
            strategy = AssignmentStrategy.UNASSIGNED
        map_decisions.append(
            MappingDecision(
                source=src,
                target=tgt or None,
                confidence=float(m.get("confidence") or 0.0),
                assignment_strategy=strategy,
                conversion=ConversionDecision(
                    conversion_class=cclass,
                    risk_level=risk_level_for_conversion(conv),
                    lossy=bool(conv.get("lossy")),
                    recommended_action=(
                        "mint_risk_contract"
                        if conv.get("requires_risk_contract")
                        else "proceed"
                    ),
                    reason=str(conv.get("reason") or ""),
                ),
                create_new=create_new,
                omitted=False,
            )
        )

    ddl_hash = approved_mapping_ddl_fingerprint(maps, dest_db=dest_db)
    src_engine = (source_db or "").strip().lower()
    dst_engine = (dest_db or "").strip().lower()
    return build_decision_artifact(
        tenant_id=tenant_id or "anonymous",
        route_id=route_id or f"execute:{dst_engine or 'unknown'}",
        # Empty is honest (file inference / unprobed). Do not substitute
        # the literal "map" — that hid source DDL drift after Validate.
        source_fingerprint=(source_fingerprint or "").strip(),
        # Empty is honest (create-new / overwrite / unprobed). Do not
        # substitute the engine name — that hid dest-exists DDL drift.
        dest_fingerprint=(dest_fingerprint or "").strip(),
        source_columns=src_cols,
        dest_columns=dst_cols,
        mappings=map_decisions,
        ddl=DdlPlan(
            ddl_identity_hash=ddl_hash,
            column_ddl=column_ddl,
            dialect=dst_engine,
        ),
        proof=ProofPlan(),
        sync_mode=sync_mode,
        error_policy=error_policy,
        # Phase F7 — Decision Artifact consumes capability profile hashes (SSOT).
        capability_source_hash=capability_profile_hash(src_engine) if src_engine else "",
        capability_dest_hash=capability_profile_hash(dst_engine) if dst_engine else "",
        artifact_id=artifact_id,
        created_at=created_at,
    )


def create_new_validate_holds_after_dest_exists(
    *,
    mappings: list[dict[str, Any]] | None,
    dest_db: str,
    approved_content_hash: str,
    live_dest_fingerprint: str,
    source_fingerprint: str = "",
    source_db: str = "",
    sync_mode: str = "",
    error_policy: str = "quarantine",
    destination_table_exists: bool | None = None,
    dest_column_names: list[str] | None = None,
    source_column_names: list[str] | None = None,
    column_nullability: dict[str, bool] | None = None,
    column_defaults: dict[str, str] | None = None,
    identity_columns: list[str] | None = None,
    generated_columns: list[str] | None = None,
    tenant_id: str = "",
    route_id: str = "",
) -> tuple[bool, str]:
    """True when a create-new Validate stamp remains the Map contract after dest exists.

    Dest appearing after the operator's first write is not dest schema drift and
    must not invent create-new. G13 (unaccounted source) and G14 (dest-only
    NOT NULL) still refuse. A real Map/DDL edit still refuses.
    """
    approved = (approved_content_hash or "").strip().lower()
    dest_fp = (live_dest_fingerprint or "").strip()
    dests = [str(c) for c in (dest_column_names or []) if str(c).strip()]
    if not approved or not dest_fp or destination_table_exists is not True or not dests:
        return False, ""
    maps = [m for m in (mappings or []) if isinstance(m, dict)]
    dest_engine = (dest_db or "").strip().lower()
    create_new = build_artifact_from_mappings(
        maps,
        dest_db=dest_engine,
        source_db=(source_db or "").strip().lower(),
        tenant_id=tenant_id or "anonymous",
        route_id=route_id or f"validate:{dest_engine or 'unknown'}",
        source_fingerprint=(source_fingerprint or "").strip(),
        dest_fingerprint="",
        sync_mode=sync_mode,
        error_policy=error_policy,
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    if create_new.content_hash.lower() != approved:
        return False, ""
    dest_l = {_norm_col(c) for c in dests}
    for m in write_mappings(maps):
        tgt = str(m.get("target") or "").strip()
        if tgt and _norm_col(tgt) not in dest_l:
            return (
                False,
                "Decision Artifact dest schema drifted since Validate — "
                "mapped column missing after dest exists. Re-run Validate before Execute.",
            )
    sources = [str(c) for c in (source_column_names or []) if str(c).strip()] or [
        str(m.get("source") or "").strip()
        for m in maps
        if str(m.get("source") or "").strip()
    ]
    verdict = classify_dest_exists_shape(
        destination_table_exists=True,
        source_columns=sources,
        dest_columns=dests,
        mappings=maps,
        column_nullability=column_nullability,
        column_defaults=column_defaults,
        identity_columns=identity_columns,
        generated_columns=generated_columns,
    )
    shape = str(verdict.get("shape") or "")
    if shape in {SHAPE_CREATE_NEW, SHAPE_UNKNOWN, SHAPE_PENDING}:
        return (
            False,
            "Destination existence unproven — not treating as create-new. "
            "Reload destination schema before Execute.",
        )
    unfilled = list(verdict.get("unfilled_required") or [])
    if unfilled:
        return (
            False,
            "Decision Artifact dest schema drifted since Validate — dest-only "
            f"NOT NULL without a mapping: {', '.join(unfilled[:6])}. Re-run Validate.",
        )
    unaccounted = list(verdict.get("unaccounted_sources") or [])
    if unaccounted:
        return (
            False,
            "Decision Artifact dest schema drifted since Validate — extra source "
            f"column(s) unaccounted: {', '.join(unaccounted[:6])}. Re-run Validate.",
        )
    false_friends = list(verdict.get("false_friend_sources") or [])
    if false_friends:
        return (
            False,
            "Decision Artifact dest schema drifted since Validate — false-friend "
            f"column(s): {', '.join(false_friends[:6])}. Re-run Validate.",
        )
    return True, ""


def enforce_decision_artifact(
    *,
    mappings: list[dict[str, Any]] | None,
    dest_db: str = "",
    approved_content_hash: str = "",
    artifact_payload: Mapping[str, Any] | None = None,
    skip_preflight: bool = False,
    sync_mode: str = "full_refresh_overwrite",
    error_policy: str = "quarantine",
    tenant_id: str = "",
    route_id: str = "",
    dest_fingerprint: str = "",
    source_fingerprint: str = "",
    source_db: str = "",
    destination_table_exists: bool | None = None,
    dest_column_names: list[str] | None = None,
    source_column_names: list[str] | None = None,
    column_nullability: dict[str, bool] | None = None,
    column_defaults: dict[str, str] | None = None,
    identity_columns: list[str] | None = None,
    generated_columns: list[str] | None = None,
) -> tuple[str | None, DecisionArtifact | None]:
    """Fail closed when Execute artifact authority is missing or drifts.

    Returns ``(error_message, artifact)``. ``artifact`` is set when the gate
    allows the write (inline stamp or verified match).

    A create-new Validate stamp (empty dest fingerprint) remains valid after
    dest exists when the Map contract still holds — dest appearing after the
    first write is not dest schema drift.
    """
    has_maps = bool(mappings)
    approved = (approved_content_hash or "").strip().lower()

    def _dest_exists_hold(stamp: str) -> tuple[bool, str]:
        return create_new_validate_holds_after_dest_exists(
            mappings=mappings,
            dest_db=dest_db,
            approved_content_hash=stamp,
            live_dest_fingerprint=dest_fingerprint,
            source_fingerprint=source_fingerprint,
            source_db=source_db,
            sync_mode=sync_mode,
            error_policy=error_policy,
            destination_table_exists=destination_table_exists,
            dest_column_names=dest_column_names,
            source_column_names=source_column_names,
            column_nullability=column_nullability,
            column_defaults=column_defaults,
            identity_columns=identity_columns,
            generated_columns=generated_columns,
            tenant_id=tenant_id,
            route_id=route_id,
        )

    if artifact_payload:
        try:
            supplied = decision_artifact_from_dict(artifact_payload)
        except ValueError as exc:
            return (f"Decision Artifact refused: {exc}", None)
        dest_fp = (dest_fingerprint or "").strip()
        src_fp = (source_fingerprint or "").strip()
        current = build_artifact_from_mappings(
            mappings,
            dest_db=dest_db,
            tenant_id=tenant_id or supplied.tenant_id,
            route_id=route_id or supplied.route_id,
            source_fingerprint=src_fp,
            dest_fingerprint=dest_fp,
            sync_mode=sync_mode or supplied.sync_mode,
            error_policy=error_policy or supplied.error_policy,
            # Deterministic compare: rebuild without volatile ids
            artifact_id="da_compare",
            created_at="1970-01-01T00:00:00+00:00",
        )
        # Compare decision body (ddl + mappings conversion), not volatile ids.
        # Prefer explicit content_hash on the supplied artifact.
        if supplied.content_hash:
            # Recompute expected from supplied body (tamper check already in from_dict)
            # and require ddl identity + mapping conversion alignment with live maps.
            if supplied.ddl.ddl_identity_hash != current.ddl.ddl_identity_hash:
                return (
                    "Decision Artifact DDL identity diverged from current Map — "
                    "re-run Validate before Execute.",
                    None,
                )
            if dest_fp and supplied.dest_fingerprint != dest_fp:
                if not supplied.dest_fingerprint:
                    holds, hold_err = _dest_exists_hold(supplied.content_hash)
                    if holds:
                        return (None, supplied)
                    if hold_err:
                        return (hold_err, None)
                return (
                    "Decision Artifact dest schema drifted since Validate — "
                    "re-run Validate before Execute.",
                    None,
                )
            if src_fp and supplied.source_fingerprint != src_fp:
                return (
                    "Decision Artifact source schema drifted since Validate — "
                    "re-run Validate before Execute.",
                    None,
                )
            if approved and approved != supplied.content_hash.lower():
                return (
                    "Decision Artifact content_hash does not match approved hash — "
                    "refuse write.",
                    None,
                )
            return (None, supplied)
        return (
            "Decision Artifact missing content_hash — refuse write.",
            None,
        )

    if not has_maps:
        return (None, None)

    current = build_artifact_from_mappings(
        mappings,
        dest_db=dest_db,
        tenant_id=tenant_id,
        route_id=route_id,
        source_fingerprint=(source_fingerprint or "").strip(),
        dest_fingerprint=(dest_fingerprint or "").strip(),
        sync_mode=sync_mode,
        error_policy=error_policy,
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )

    if approved:
        if approved != current.content_hash.lower():
            holds, hold_err = _dest_exists_hold(approved)
            if holds:
                return (None, current)
            if hold_err:
                return (hold_err, None)
            return (
                "Decision Artifact content_hash mismatch — Map/DDL drifted "
                "since Validate. Re-run Validate before Execute.",
                None,
            )
        return (None, current)

    if skip_preflight:
        # Inline stamp — same honesty as DDL identity for API/CLI/scheduler.
        if not current.content_hash:
            return (
                "Decision Artifact inline stamp produced an empty content_hash — "
                "refuse write.",
                None,
            )
        return (None, current)

    return (
        "Decision Artifact requires Validate preflight before Execute — "
        "refuse write without content_hash (re-run Validate).",
        None,
    )


__all__ = [
    "build_artifact_from_mappings",
    "create_new_validate_holds_after_dest_exists",
    "enforce_decision_artifact",
]
