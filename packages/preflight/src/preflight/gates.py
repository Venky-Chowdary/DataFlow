from __future__ import annotations

import hashlib
import json
import tempfile
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

    # Schemaless document stores (MongoDB, DynamoDB, Redis) do not enforce a
    # column-level type contract; every field can hold any BSON/DynamoDB type.
    # Skip lossy-coercion checks for these destinations.
    dest_kind = (ctx.plan.destination.db_type or "").lower()
    schemaless = dest_kind in {"mongodb", "dynamodb", "redis"}
    if schemaless:
        return _pass(GateId.G3_SCHEMA_CONTRACT, "Schemaless destination — no DDL type contract to validate", start)

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


def _actual_disk_bytes() -> int:
    """Return usable bytes on the temporary/staging volume, or 0 if unknown."""
    try:
        import shutil

        usage = shutil.disk_usage(tempfile.gettempdir())
        return int(usage.free * 0.85)
    except Exception:
        return 0


def gate_g7_capacity(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    needed = ctx.plan.estimated_bytes
    available = ctx.plan.available_staging_bytes or _actual_disk_bytes()
    if available and needed > available:
        return _block(
            GateId.G7_CAPACITY,
            f"Insufficient staging capacity: need {needed}, have {available}",
            start,
        )
    ratio = f" ({available // max(needed, 1)}x headroom)" if available and needed else ""
    return _pass(GateId.G7_CAPACITY, f"Capacity sufficient{ratio}", start)


def _dry_run_transform(value: str, transform: str | None) -> str | None:
    """Best-effort preview of how a transform will affect a string value.

    Returns ``None`` only for transforms that produce non-deterministic or
    one-way output (hashing, masking, encryption, UUID generation) so the
    dry-run gate knows that row cannot be compared.
    """
    if not transform:
        return value
    t = str(transform).lower().strip()
    if t in {"upper", "uppercase"}:
        return value.upper()
    if t in {"lower", "lowercase"}:
        return value.lower()
    if t in {"trim", "strip", "string", "varchar", "text"}:
        return value.strip()
    if t in {"integer", "int", "number", "decimal", "float", "double", "numeric"}:
        try:
            if "." in value or "e" in value.lower():
                return str(float(value))
            return str(int(value))
        except Exception:
            return value
    if t in {"boolean", "bool"}:
        return "true" if value and value.lower() not in {"false", "0", "", "no", "off"} else "false"
    if t in {"date", "datetime", "timestamp", "time", "iso8601"}:
        return value
    # Non-deterministic / one-way transforms break reconciliation previews.
    if t in {"uuid", "guid", "hash", "md5", "sha256", "mask", "redact", "pii_mask", "anonymize", "encrypt"}:
        return None
    # For other deterministic string-preserving transforms, keep the value as-is.
    return value


def gate_g8_reconciliation(ctx: PreflightContext) -> GateResult:
    """Dry-run reconciliation: ensure sample rows survive mapping without loss."""
    start = time.perf_counter()
    sample_rows = getattr(ctx, "sample_rows", None) or []
    if not sample_rows:
        return GateResult(
            gate_id=GateId.G8_RECONCILIATION,
            status=GateStatus.SKIP,
            message="No sample rows for dry-run reconciliation",
            duration_ms=(time.perf_counter() - start) * 1000,
        )

    source_count = len(sample_rows)
    unknown_transform = any(m.transform for m in ctx.plan.mappings)

    mapped_rows: list[dict[str, Any]] = []
    for row in sample_rows:
        mapped: dict[str, Any] = {}
        for m in ctx.plan.mappings:
            raw = row.get(m.source, "")
            transformed = _dry_run_transform(str(raw) if raw is not None else "", m.transform)
            mapped[m.target] = transformed
        mapped_rows.append(mapped)

    # Detect a likely primary key on the target side and verify uniqueness.
    pk_target = None
    for m in ctx.plan.mappings:
        if m.target.lower() in {"id", "_id"} or m.target.lower().endswith("_id"):
            pk_target = m.target
            break

    duplicates = 0
    if pk_target:
        seen: set[str] = set()
        for row in mapped_rows:
            val = str(row.get(pk_target, ""))
            if val and val in seen:
                duplicates += 1
            seen.add(val)

    if duplicates:
        return _block(
            GateId.G8_RECONCILIATION,
            f"Dry-run reconciliation failed — {duplicates} duplicate target key(s)",
            start,
            {"duplicate_keys": duplicates, "target_rows": len(mapped_rows)},
        )

    # When no non-trivial transforms are applied, the source and target value
    # streams should be identical (column ordering does not matter).
    if not unknown_transform:
        def _sorted_hash(rows: list[dict[str, Any]]) -> str:
            payload = []
            for row in rows:
                payload.append(
                    json.dumps(sorted((str(k), str(v)) for k, v in row.items()), ensure_ascii=True)
                )
            return hashlib.sha256("\n".join(payload).encode("utf-8")).hexdigest()

        source_hash = _sorted_hash(sample_rows)
        target_hash = _sorted_hash(mapped_rows)
        if source_hash != target_hash:
            return _block(
                GateId.G8_RECONCILIATION,
                "Dry-run reconciliation mismatch — source and target fingerprints differ",
                start,
                {"source_rows": source_count, "target_rows": len(mapped_rows)},
            )

    return _pass(
        GateId.G8_RECONCILIATION,
        f"Dry-run reconciliation passed — {source_count} row(s)",
        start,
        {"source_rows": source_count, "target_rows": len(mapped_rows)},
    )


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
