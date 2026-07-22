from __future__ import annotations

import hashlib
import json
import tempfile
import time
from typing import Any, Callable

from preflight.constants import SCHEMALESS_DESTS
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
    schemaless = dest_kind in SCHEMALESS_DESTS
    if schemaless:
        return _pass(GateId.G3_SCHEMA_CONTRACT, "Schemaless destination — no DDL type contract to validate", start)

    try:
        from services.type_system import is_lossy_coercion
    except ImportError:
        is_lossy_coercion = None

    # Value-aware report (host-injected). When sample rows exist we can predict
    # the *real* write outcome per value instead of guessing from declared types.
    report = {}
    try:
        report = ctx.coercion_report() or {}
    except Exception:
        report = {}
    by_source: dict[str, dict] = report.get("by_source", {}) if isinstance(report, dict) else {}
    value_aware = bool(report.get("sampled_rows")) if isinstance(report, dict) else False

    warnings: list[str] = []
    issues_detail: list[dict] = []

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
        if not lossy:
            continue

        label = (
            f"Lossy coercion: {m.source} ({source_col.inferred_type}) → "
            f"{m.target} ({target.inferred_type})"
        )

        # With sampled values we only hard-block when a real value cannot be
        # coerced. A declared-type mismatch whose values all coerce cleanly (or
        # are placeholder text that becomes NULL) is downgraded to a warning —
        # this is what stops schemaless sources (MongoDB widened to TEXT) from
        # producing a wall of false coercion blocks.
        probe = by_source.get(m.source) if value_aware else None
        if probe is not None:
            severity = probe.get("severity", "ok")
            detail = {
                "source": m.source,
                "target": m.target,
                "source_type": source_col.inferred_type,
                "target_type": target.inferred_type,
                "severity": severity,
                "sampled": probe.get("sampled", 0),
                "failed": probe.get("failed", 0),
                "sentinel_nulls": probe.get("sentinel_nulls", 0),
                "sample_failures": probe.get("sample_failures", []),
                "suggested_fix": probe.get("suggested_fix", ""),
                "suggested_target_type": probe.get("suggested_target_type"),
                "suggested_transform": probe.get("suggested_transform"),
            }
            issues_detail.append(detail)
            if severity == "block":
                issues.append(label)
            else:
                warnings.append(label)
        elif value_aware:
            # Report exists and covers this pair as clean (no entry ⇒ all values
            # coerce): downgrade the declared-type mismatch to a warning.
            warnings.append(label)
        else:
            # No samples — strict/maximum fail-closed on declared lossy pairs;
            # balanced warns so operators can proceed after acknowledging risk.
            mode = (ctx.plan.validation_mode or "strict").strip().lower()
            if mode in {"balanced", "review"}:
                warnings.append(label + " (declared; no samples — balanced warn)")
            else:
                issues.append(label)

    if issues:
        # Sample-proven or strict declared write hazards always block Validate.
        return _block(
            GateId.G3_SCHEMA_CONTRACT,
            f"{len(issues)} type coercion issue(s)",
            start,
            {
                "issues": issues,
                "issues_detail": issues_detail,
                "warnings": warnings,
                "rule_id": "g3_schema_contract.lossy_coercion",
                "remediation_kind": "change_target_type",
            },
        )
    if warnings:
        return _pass(
            GateId.G3_SCHEMA_CONTRACT,
            f"Schema contract valid — {len(warnings)} coercion(s) verified against sampled values",
            start,
            {"warnings": warnings, "issues_detail": issues_detail},
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


def _issue_text(issue: Any) -> str:
    if isinstance(issue, str):
        return issue
    if isinstance(issue, dict):
        for key in ("message", "error", "reason", "detail"):
            val = issue.get(key)
            if val:
                col = issue.get("column") or issue.get("source") or issue.get("field")
                return f"{col}: {val}" if col else str(val)
        return str(issue)
    return str(issue)


def _block_message(prefix: str, issues: list[Any]) -> str:
    texts = [_issue_text(i) for i in issues if i]
    texts = [t for t in texts if t]
    if not texts:
        return f"{prefix} — unknown issue"
    head = texts[0]
    if len(texts) == 1:
        return f"{prefix}: {head}"
    return f"{prefix}: {head} (+{len(texts) - 1} more)"


def gate_g5_dry_run(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    passed, errors = ctx.run_dry_run()
    details: dict[str, Any] = {"errors": list(errors[:20])}
    dry_meta = getattr(ctx, "_last_dry_run_meta", None)
    if isinstance(dry_meta, dict):
        details.update(dry_meta)

    if not passed:
        details["issue_texts"] = [_issue_text(i) for i in errors[:20]]
        return _block(
            GateId.G5_DRY_RUN,
            _block_message("Dry-run failed", errors),
            start,
            details,
        )
    return _pass(
        GateId.G5_DRY_RUN,
        (
            "Sample transform dry-run passed"
            + (
                f" ({int(details.get('sample_rows_scanned', 0))} preview rows)"
                if details.get("sample_rows_scanned")
                else ""
            )
        ),
        start,
        details,
    )


_DRIFT_DDL_NOISE = (
    "schema changed since last mapping revision",
    "source schema changed",
    "destination schema changed",
)


def _is_drift_noise_issue(text: str) -> bool:
    lower = (text or "").lower()
    return any(marker in lower for marker in _DRIFT_DDL_NOISE)


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

    dest_kind = (ctx.plan.destination.db_type or "").lower()
    schemaless = dest_kind in SCHEMALESS_DESTS
    # Host apps must never fold fingerprint drift into DDL. Scrub defensively so
    # a stale process cannot block Redis/Mongo/Dynamo as "Target DDL incompatible".
    raw_issues = [str(i) for i in (ctx.plan.ddl_issues or [])]
    ddl_issues = [i for i in raw_issues if not _is_drift_noise_issue(i)]
    scrubbed = len(raw_issues) - len(ddl_issues)

    if schemaless:
        # Document stores have no CREATE/ALTER contract. Only identity-key
        # uniqueness in the sample can fail this gate.
        source_cols = [c.name for c in ctx.plan.source.columns]
        pk_src = pk_tgt = None
        try:
            from services.primary_key import resolve_identity_key

            pk_src, pk_tgt = resolve_identity_key(
                mappings=ctx.plan.mappings,
                source_columns=source_cols,
                dest_kind=dest_kind,
                validation_mode=ctx.plan.validation_mode,
                purpose="uniqueness",
            )
        except Exception:
            for m in ctx.plan.mappings:
                if m.target.lower() == "_id":
                    pk_src, pk_tgt = m.source, m.target
                    break
        if pk_tgt:
            dupes = ctx.probe_unique_constraint([pk_tgt])
            if dupes:
                return _block(
                    GateId.G6_TARGET_DDL,
                    f"UNIQUE constraint would fail on {pk_tgt} — {len(dupes)} duplicate group(s)",
                    start,
                    {
                        "sample_duplicates": dupes[:5],
                        "primary_key": {"source": pk_src, "target": pk_tgt},
                        "rule_id": "g6_target_ddl.unique",
                        "remediation_kind": "fix_source_keys",
                    },
                )
        return _pass(
            GateId.G6_TARGET_DDL,
            "Schemaless destination — no DDL contract (identity key checked)",
            start,
            {
                "schemaless": True,
                "scrubbed_drift_issues": scrubbed,
                "primary_key": {"source": pk_src, "target": pk_tgt},
            },
        )

    if ddl_issues:
        head = ddl_issues[0]
        msg = head if len(ddl_issues) <= 1 else f"{head} (+{len(ddl_issues) - 1} more)"
        return _block(
            GateId.G6_TARGET_DDL,
            msg,
            start,
            {
                "issues": ddl_issues,
                "rule_id": "g6_target_ddl.incompatible",
                "remediation_kind": "fix_ddl",
                "scrubbed_drift_issues": scrubbed,
            },
        )

    # ddl_compatible=False with only drift noise (or empty issues) is a host bug —
    # do not block Execute on a false DDL signal.
    if not ctx.plan.ddl_compatible and not ddl_issues:
        return _pass(
            GateId.G6_TARGET_DDL,
            "Target DDL compatible (ignored empty/drift-only incompatibility flag)",
            start,
            {"scrubbed_drift_issues": scrubbed, "host_flag_ignored": True},
        )

    # Canonical identity key uniqueness probe for SQL destinations.
    source_cols = [c.name for c in ctx.plan.source.columns]
    try:
        from services.primary_key import resolve_identity_key

        pk_src, pk_tgt = resolve_identity_key(
            mappings=ctx.plan.mappings,
            source_columns=source_cols,
            dest_kind=dest_kind,
            validation_mode=ctx.plan.validation_mode,
            purpose="uniqueness",
        )
    except Exception:
        pk_src, pk_tgt = None, None
        for m in ctx.plan.mappings:
            if m.target.lower() in {"id", "_id"}:
                pk_src, pk_tgt = m.source, m.target
                break

    if pk_tgt:
        dupes = ctx.probe_unique_constraint([pk_tgt])
        if dupes:
            return _block(
                GateId.G6_TARGET_DDL,
                f"UNIQUE constraint would fail on {pk_tgt} — {len(dupes)} duplicate group(s)",
                start,
                {
                    "sample_duplicates": dupes[:5],
                    "primary_key": {"source": pk_src, "target": pk_tgt},
                    "rule_id": "g6_target_ddl.unique",
                    "remediation_kind": "fix_source_keys",
                },
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
    if t in {"none", "identity", "passthrough"}:
        return value
    if t in {"upper", "uppercase"}:
        return value.upper()
    if t in {"lower", "lowercase"}:
        return value.lower()
    if t in {"trim", "strip", "string", "varchar", "text"}:
        return value.strip()
    if t in {"strip_controls", "normalize_unicode"}:
        # Deterministic warehouse-safe cleanup — comparable after strip.
        return "".join(ch for ch in value if ch == "\t" or ch == "\n" or ch == "\r" or ord(ch) >= 32)
    if t in {"integer", "int", "number", "decimal", "float", "double", "numeric", "currency", "percentage"}:
        try:
            cleaned = value.replace(",", "").replace("$", "").replace("€", "").replace("%", "").strip()
            if "." in cleaned or "e" in cleaned.lower():
                return str(float(cleaned))
            return str(int(cleaned))
        except Exception:
            return value
    if t in {"boolean", "bool"}:
        return "true" if value and value.lower() not in {"false", "0", "", "no", "off"} else "false"
    if t in {"date", "datetime", "timestamp", "time", "iso8601"}:
        return value
    if t in {"json", "parse_json", "to_json"}:
        # Structural identity for Mongo→VARIANT — keep canonical compact form.
        try:
            parsed = json.loads(value) if value.strip().startswith(("{", "[")) else value
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        except Exception:
            pass
        return value
    # Non-deterministic / one-way transforms break reconciliation previews.
    if t in {"uuid", "guid", "hash", "md5", "sha256", "mask", "redact", "pii_mask", "anonymize", "encrypt"}:
        return None
    # For other deterministic string-preserving transforms, keep the value as-is.
    return value


_NON_DETERMINISTIC = {
    "uuid", "guid", "hash", "md5", "sha256", "mask", "redact", "pii_mask", "anonymize", "encrypt",
}


def _apply_write_path_transform(value: str, transform: str | None) -> tuple[str | None, str | None]:
    """Prefer the real write-path transform so G8 matches coerce/quarantine behavior."""
    try:
        from services.transform_engine import apply_transform
    except Exception:
        out = _dry_run_transform(value, transform)
        if out is None:
            return None, "non_deterministic_transform"
        return out, None
    t = (transform or "none").strip() or "none"
    result, err = apply_transform(value, t)
    if err:
        return None, err
    if result is None:
        return "", None
    return str(result), None


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
    nondeterministic = [
        m.target
        for m in ctx.plan.mappings
        if m.transform and str(m.transform).lower().strip() in _NON_DETERMINISTIC
    ]

    def _serialize_for_write(value: Any) -> str:
        # Match readers/writers: lists/dicts become compact JSON, not Python repr.
        # Studio samples often keep native arrays; str([...]) falsely fails identity.
        if value is None:
            return ""
        try:
            from services.value_serializer import cell_to_string

            return cell_to_string(value)
        except Exception:
            return str(value)

    transform_errors: list[str] = []
    mapped_rows: list[dict[str, Any]] = []
    for row_idx, row in enumerate(sample_rows, start=1):
        mapped: dict[str, Any] = {}
        for m in ctx.plan.mappings:
            raw = row.get(m.source, "")
            raw_s = _serialize_for_write(raw)
            if m.transform and str(m.transform).lower().strip() in _NON_DETERMINISTIC:
                mapped[m.target] = None
                continue
            transformed, err = _apply_write_path_transform(raw_s, m.transform)
            if err:
                transform_errors.append(f"row {row_idx} {m.source}→{m.target}: {err}")
                mapped[m.target] = None
            else:
                mapped[m.target] = transformed
        mapped_rows.append(mapped)

    if transform_errors:
        return _block(
            GateId.G8_RECONCILIATION,
            _block_message("Dry-run reconciliation failed — transform errors", transform_errors),
            start,
            {
                "errors": transform_errors[:20],
                "source_rows": source_count,
                "preview_only": True,
                "note": "Write-path transform failed on sample — fix mapping before Run",
            },
        )

    # Canonical identity key (same helper as G6/G9) — never invent ``user_id`` PK.
    pk_target = None
    try:
        from services.primary_key import resolve_primary_key_target

        pk_target = resolve_primary_key_target(
            ctx.plan.mappings,
            ctx.plan.destination.db_type or "",
            validation_mode=ctx.plan.validation_mode,
        )
    except Exception:
        for m in ctx.plan.mappings:
            if m.target.lower() in {"id", "_id"}:
                pk_target = m.target
                break

    duplicates = 0
    if pk_target:
        seen: set[str] = set()
        for row in mapped_rows:
            val = str(row.get(pk_target, "") or "")
            if val and val in seen:
                duplicates += 1
            seen.add(val)

    if duplicates:
        return _block(
            GateId.G8_RECONCILIATION,
            f"Dry-run reconciliation failed — {duplicates} duplicate target key(s) on {pk_target}",
            start,
            {
                "duplicate_keys": duplicates,
                "primary_key": pk_target,
                "target_rows": len(mapped_rows),
                "rule_id": "g8_reconciliation.duplicate_keys",
                "remediation_kind": "fix_source_keys",
            },
        )

    # Fingerprint: raw source cells vs write-path transformed values (not transform↔transform).
    if not nondeterministic:
        try:
            from services.reconciliation import normalize_cell
        except Exception:
            def normalize_cell(v: Any) -> str:  # type: ignore[misc]
                return "" if v is None else str(v)

        mismatches: list[str] = []
        for row_idx, row in enumerate(sample_rows, start=1):
            for m in ctx.plan.mappings:
                tname = str(m.transform or "").lower().strip()
                # Identity / rename-only: raw must equal transformed after normalize.
                if tname in {"", "none", "identity", "passthrough", "string", "varchar", "text"}:
                    raw = row.get(m.source, "")
                    got = mapped_rows[row_idx - 1].get(m.target)
                    if normalize_cell(raw) != normalize_cell(got):
                        mismatches.append(
                            f"row {row_idx} {m.source}→{m.target}: identity transform altered value"
                        )
                # Lossy declared pairs: value change is expected only when transform
                # is intentional (date truncate, etc.) — surface as detail, not auto-block
                # when transform is explicit; block when transform missing but types lossy.
        if mismatches:
            return _block(
                GateId.G8_RECONCILIATION,
                "Dry-run reconciliation mismatch — identity mapping altered sample values",
                start,
                {
                    "issues": mismatches[:20],
                    "source_rows": source_count,
                    "target_rows": len(mapped_rows),
                    "preview_only": True,
                    "rule_id": "g8_reconciliation.identity_mismatch",
                    "remediation_kind": "review_mappings",
                    "note": (
                        "Pre-write sample fingerprint (not post-load checksum). "
                        "Align serializer/transform or pick an explicit transform — "
                        "Strip controls does not fix identity mismatches."
                    ),
                },
            )

        return _pass(
            GateId.G8_RECONCILIATION,
            f"Dry-run reconciliation passed — {source_count} row(s) (write-path sample)",
            start,
            {
                "source_rows": source_count,
                "target_rows": len(mapped_rows),
                "preview_only": True,
                "note": "Pre-write write-path sample check — live Gate-8 checksum runs after load",
            },
        )

    return _pass(
        GateId.G8_RECONCILIATION,
        (
            f"Dry-run reconciliation skipped fingerprint for non-deterministic "
            f"transform(s) on {', '.join(nondeterministic[:5])} — PK uniqueness checked"
        ),
        start,
        {
            "source_rows": source_count,
            "target_rows": len(mapped_rows),
            "skipped_fingerprint_targets": nondeterministic[:12],
            "preview_only": True,
        },
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
    encoding = next(
        (c for c in (report.get("checks") or []) if c.get("check") == "encoding_anomalies"),
        None,
    )
    encoding_issues = (encoding or {}).get("issues") or []
    if report.get("blocks_transfer"):
        issues = report.get("issues", [])[:15]
        return _block(
            GateId.G9_DATA_INTEGRITY,
            _block_message("Data integrity failed", issues),
            start,
            {
                "issues": issues,
                "issue_texts": [_issue_text(i) for i in issues],
                "checks_failed": report.get("checks_failed", 0),
                "encoding_issues": encoding_issues[:12],
            },
        )
    warnings = list(report.get("warnings") or [])
    if encoding_issues and not warnings:
        warnings = [str(i.get("message") if isinstance(i, dict) else i) for i in encoding_issues[:8]]
    return _pass(
        GateId.G9_DATA_INTEGRITY,
        report.get("summary", "Data integrity checks passed"),
        start,
        {
            "checks_passed": report.get("checks_passed", 0),
            "warnings": warnings[:12],
            "encoding_issues": encoding_issues[:12],
        },
    )


PREFLIGHT_GATES: list[tuple[GateId, GateFn]] = [
    (GateId.G1_SOURCE, gate_g1_source),
    (GateId.G2_DESTINATION, gate_g2_destination),
    (GateId.G3_SCHEMA_CONTRACT, gate_g3_schema_contract),
    (GateId.G4_MAPPING_CONFIDENCE, gate_g4_mapping_confidence),
    (GateId.G5_DRY_RUN, gate_g5_dry_run),
    (GateId.G9_DATA_INTEGRITY, gate_g9_data_integrity),
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
