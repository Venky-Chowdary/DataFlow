from __future__ import annotations

import time
from typing import Callable

from preflight.models import (
    GateId,
    GateResult,
    GateStatus,
    PreflightContext,
    PreflightResult,
    TransferPlan,
)

GateFn = Callable[[PreflightContext], GateResult]

LOSSY_COERCIONS = {
    ("VARCHAR", "INTEGER"),
    ("VARCHAR", "TIMESTAMP"),
    ("TEXT", "DATE"),
    ("FLOAT", "INTEGER"),
}


def gate_g1_source(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    src = ctx.plan.source
    if src.error:
        return _block(GateId.G1_SOURCE, f"Source error: {src.error}", start)
    if not src.connected and src.kind != "file":
        return _block(GateId.G1_SOURCE, "Source not connected", start)
    if src.kind == "file" and not src.parseable:
        return _block(GateId.G1_SOURCE, "File not parseable or corrupt", start)
    if not src.columns:
        return _block(GateId.G1_SOURCE, "No columns detected in source", start)
    return _pass(GateId.G1_SOURCE, f"Source readable — {len(src.columns)} columns", start)


def gate_g2_destination(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    dest = ctx.plan.destination
    if dest.error:
        return _block(GateId.G2_DESTINATION, f"Destination error: {dest.error}", start)
    if not dest.connected:
        return _block(GateId.G2_DESTINATION, "Destination not reachable", start)
    if not dest.can_write:
        return _block(GateId.G2_DESTINATION, "Insufficient write permissions", start)
    return _pass(GateId.G2_DESTINATION, "Destination reachable with write access", start)


def gate_g3_schema_contract(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    dest_by_name = {c.name.lower(): c for c in ctx.plan.destination.target_columns}
    issues: list[str] = []

    try:
        from services.type_system import is_lossy_coercion
    except ImportError:
        is_lossy_coercion = None

    for m in ctx.plan.mappings:
        target = dest_by_name.get(m.target.lower())
        if not target:
            continue
        source_col = next((c for c in ctx.plan.source.columns if c.name == m.source), None)
        if not source_col:
            continue
        pair = (source_col.inferred_type.upper(), target.inferred_type.upper())
        lossy = pair in LOSSY_COERCIONS
        if not lossy and is_lossy_coercion:
            lossy = is_lossy_coercion(source_col.inferred_type, target.inferred_type)
        if lossy:
            issues.append(
                f"Lossy coercion: {m.source} ({source_col.inferred_type}) → "
                f"{m.target} ({target.inferred_type})"
            )

    if issues:
        return _block(
            GateId.G3_SCHEMA_CONTRACT,
            f"{len(issues)} type coercion issue(s)",
            start,
            {"issues": issues},
        )
    return _pass(GateId.G3_SCHEMA_CONTRACT, "Schema contract valid", start)


def gate_g4_mapping_confidence(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    threshold = ctx.plan.confidence_threshold
    mapped_targets = {m.target.lower() for m in ctx.plan.mappings}
    unmapped_required = [
        r for r in ctx.plan.required_targets if r.lower() not in mapped_targets
    ]
    if unmapped_required:
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"Required fields unmapped: {', '.join(unmapped_required)}",
            start,
            {"unmapped": unmapped_required},
        )

    low_confidence = [
        m
        for m in ctx.plan.mappings
        if m.confidence < threshold and not m.user_override
    ]
    if low_confidence:
        names = [f"{m.source}→{m.target} ({m.confidence:.2f})" for m in low_confidence]
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"{len(low_confidence)} mapping(s) below threshold {threshold}",
            start,
            {"low_confidence": names},
        )

    ambiguous = [
        m
        for m in ctx.plan.mappings
        if m.requires_review and not m.user_override
    ]
    if ambiguous:
        names = [
            f"{m.source}→{m.target} (gap {m.score_gap:.2f})"
            for m in ambiguous
        ]
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"{len(ambiguous)} ambiguous mapping(s) require review",
            start,
            {"ambiguous_mappings": names},
        )
    return _pass(
        GateId.G4_MAPPING_CONFIDENCE,
        f"All {len(ctx.plan.mappings)} mappings meet confidence threshold",
        start,
    )


def gate_g5_dry_run(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    passed, errors = ctx.run_dry_run()
    if not passed:
        return _block(
            GateId.G5_DRY_RUN,
            f"Dry-run failed — {len(errors)} error(s)",
            start,
            {"errors": errors[:20]},
        )
    return _pass(GateId.G5_DRY_RUN, "Sample transform dry-run passed", start)


def gate_g6_target_ddl(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    if not ctx.plan.destination.connected:
        return GateResult(
            gate_id=GateId.G6_TARGET_DDL,
            status=GateStatus.SKIP,
            message="Skipped — verify destination connectivity first (G2)",
            details={"reason": ctx.plan.destination.error or "not_connected"},
            duration_ms=(time.perf_counter() - start) * 1000,
        )
    if not ctx.plan.ddl_compatible:
        return _block(
            GateId.G6_TARGET_DDL,
            "Target DDL incompatible",
            start,
            {"issues": ctx.plan.ddl_issues},
        )

    pk_candidates = [m.target for m in ctx.plan.mappings if m.target.lower().endswith("_id")]
    for col_group in [pk_candidates] if pk_candidates else []:
        dupes = ctx.probe_unique_constraint(col_group)
        if dupes:
            return _block(
                GateId.G6_TARGET_DDL,
                f"UNIQUE constraint would fail — {len(dupes)} duplicate group(s)",
                start,
                {"sample_duplicates": dupes[:5]},
            )
    return _pass(GateId.G6_TARGET_DDL, "Target DDL compatible", start)


def gate_g7_capacity(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    needed = ctx.plan.estimated_bytes
    available = ctx.plan.available_staging_bytes
    if available > 0 and needed > available:
        return _block(
            GateId.G7_CAPACITY,
            f"Insufficient staging capacity: need {needed}, have {available}",
            start,
        )
    return _pass(GateId.G7_CAPACITY, "Capacity sufficient", start)


def gate_g8_reconciliation(ctx: PreflightContext) -> GateResult:
    """Post-transfer gate — skipped during preflight, run after transfer completes."""
    return GateResult(
        gate_id=GateId.G8_RECONCILIATION,
        status=GateStatus.SKIP,
        message="Post-transfer reconciliation — runs after transfer",
    )


PREFLIGHT_GATES: list[tuple[GateId, GateFn]] = [
    (GateId.G1_SOURCE, gate_g1_source),
    (GateId.G2_DESTINATION, gate_g2_destination),
    (GateId.G3_SCHEMA_CONTRACT, gate_g3_schema_contract),
    (GateId.G4_MAPPING_CONFIDENCE, gate_g4_mapping_confidence),
    (GateId.G5_DRY_RUN, gate_g5_dry_run),
    (GateId.G6_TARGET_DDL, gate_g6_target_ddl),
    (GateId.G7_CAPACITY, gate_g7_capacity),
    (GateId.G8_RECONCILIATION, gate_g8_reconciliation),
]


def _pass(gate_id: GateId, message: str, start: float, details: dict | None = None) -> GateResult:
    return GateResult(
        gate_id=gate_id,
        status=GateStatus.PASS,
        message=message,
        details=details or {},
        duration_ms=(time.perf_counter() - start) * 1000,
    )


def _block(gate_id: GateId, message: str, start: float, details: dict | None = None) -> GateResult:
    return GateResult(
        gate_id=gate_id,
        status=GateStatus.BLOCK,
        message=message,
        details=details or {},
        duration_ms=(time.perf_counter() - start) * 1000,
    )
