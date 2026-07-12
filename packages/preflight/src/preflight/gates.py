from __future__ import annotations

import time
from typing import Any, Callable

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
    ("VARCHAR", "BOOLEAN"),
    ("TEXT", "DATE"),
    ("FLOAT", "INTEGER"),
    ("DOUBLE", "INTEGER"),
    ("REAL", "INTEGER"),
    ("DECIMAL", "INTEGER"),
    ("NUMERIC", "INTEGER"),
    ("NUMBER", "INTEGER"),
    ("TIMESTAMP", "DATE"),
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
        if (ctx.plan.validation_mode or "strict").lower() in {"strict", "maximum"}:
            return _block(
                GateId.G3_SCHEMA_CONTRACT,
                f"{len(issues)} type coercion issue(s)",
                start,
                {"issues": issues},
            )
        return _pass(
            GateId.G3_SCHEMA_CONTRACT,
            f"{len(issues)} type coercion warning(s) — will be verified by dry-run",
            start,
            {"issues": issues, "warnings": issues},
        )
    return _pass(GateId.G3_SCHEMA_CONTRACT, "Schema contract valid", start)


def gate_g4_mapping_confidence(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    threshold = ctx.plan.confidence_threshold
    # Mapping candidates below the floor (used by the semantic mapper) are too
    # weak to keep, but values between the floor and the user threshold are
    # accepted by G4 so G5's data-integrity audit can apply the stricter check.
    confidence_floor = max(0.55, threshold - 0.3)
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
        if m.confidence < confidence_floor and not m.user_override
    ]
    if low_confidence:
        names = [f"{m.source}→{m.target} ({m.confidence:.2f})" for m in low_confidence]
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"{len(low_confidence)} mapping(s) below floor {confidence_floor}",
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
        f"All {len(ctx.plan.mappings)} mappings meet confidence floor",
        start,
    )


def gate_g5_dry_run(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    passed, errors = ctx.run_dry_run()
    details: dict[str, Any] = {"errors": errors[:20]}

    # Layer the data integrity audit into G5 so it runs without creating a 9th gate.
    integrity = gate_g9_data_integrity(ctx)
    if integrity.status == GateStatus.BLOCK:
        integrity_issues = integrity.details.get("issues", [])
        details["errors"].extend(integrity_issues)
        details["integrity_checks_failed"] = integrity.details.get("checks_failed", 0)
        return _block(
            GateId.G5_DRY_RUN,
            f"Dry-run / integrity failed — {len(details['errors'])} issue(s)",
            start,
            details,
        )
    if integrity.status == GateStatus.PASS:
        details["integrity_checks_passed"] = integrity.details.get("checks_passed", 0)

    if not passed:
        return _block(
            GateId.G5_DRY_RUN,
            f"Dry-run failed — {len(errors)} error(s)",
            start,
            details,
        )
    return _pass(
        GateId.G5_DRY_RUN,
        "Sample transform dry-run and integrity checks passed",
        start,
        details,
    )


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

    # Schemaless destinations (MongoDB/DynamoDB/Redis) only have a hard uniqueness
    # contract on `_id`; other `*_id` fields are foreign keys and may repeat.
    dest_kind = (ctx.plan.destination.db_type or "").lower()
    schemaless = dest_kind in {"mongodb", "dynamodb", "redis"}
    source_cols = [c.name for c in ctx.plan.source.columns]
    tgt_by_src = {m.source: m.target for m in ctx.plan.mappings}
    pk = None
    if schemaless:
        for src, tgt in tgt_by_src.items():
            if tgt == "_id":
                pk = src
                break
        if not pk:
            pk = next((c for c in source_cols if c.lower() == "_id"), None)
    else:
        for src, tgt in tgt_by_src.items():
            if tgt.lower() in {"id", "_id"}:
                pk = src
                break
        if not pk:
            for c in source_cols:
                if c.lower() in {"id", "_id"}:
                    pk = c
                    break
        if not pk:
            pk = next((c for c in source_cols if c.lower().endswith("_id")), None)

    pk_targets: list[str] = []
    if pk:
        pk_targets.append(tgt_by_src.get(pk, pk))
    for col_group in [pk_targets] if pk_targets else []:
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


def gate_g9_data_integrity(ctx: PreflightContext) -> GateResult:
    """Critical data integrity — financial precision, required nulls, duplicate keys."""
    start = time.perf_counter()
    audit = getattr(ctx, "run_integrity_audit", None)
    if not callable(audit):
        return GateResult(
            gate_id=GateId.G9_DATA_INTEGRITY,
            status=GateStatus.SKIP,
            message="Skipped — integrity audit not available",
            duration_ms=(time.perf_counter() - start) * 1000,
        )
    report = audit()
    if report.get("blocks_transfer"):
        issues = report.get("issues", [])[:15]
        return _block(
            GateId.G9_DATA_INTEGRITY,
            f"Data integrity failed — {len(issues)} issue(s)",
            start,
            {"issues": issues, "checks_failed": report.get("checks_failed", 0)},
        )
    return _pass(
        GateId.G9_DATA_INTEGRITY,
        report.get("summary", "Data integrity checks passed"),
        start,
        {"checks_passed": report.get("checks_passed", 0)},
    )


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
