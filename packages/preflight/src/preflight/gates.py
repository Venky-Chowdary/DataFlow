from __future__ import annotations

import json
import logging
import tempfile
import time
from typing import Any, Callable

from preflight.constants import SCHEMALESS_DESTS
from preflight.models import (
    GateId,
    GateResult,
    GateStatus,
    PreflightContext,
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


def evidence_scope(
    *,
    kind: str,
    sample_rows: int | None = None,
    available_rows: int | None = None,
    columns: int | None = None,
    coverage: str = "sample",
    note: str = "",
) -> dict[str, Any]:
    """Structured evidence scope for Validate gate cards (sample vs full vs pending)."""
    scope: dict[str, Any] = {
        "kind": kind,
        "coverage": coverage,  # full_schema | sample | full_selected | pending | n/a
    }
    if sample_rows is not None:
        scope["sample_rows"] = int(sample_rows)
    if available_rows is not None:
        scope["available_rows"] = int(available_rows)
    if columns is not None:
        scope["columns"] = int(columns)
    if note:
        scope["note"] = note
    return scope


def _with_scope(details: dict[str, Any] | None, scope: dict[str, Any]) -> dict[str, Any]:
    out = dict(details or {})
    out["evidence_scope"] = scope
    return out


def gate_g1_source(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    src = ctx.plan.source
    cols = len(src.columns or [])
    est = int(getattr(src, "row_count_estimate", 0) or 0)
    scope = evidence_scope(
        kind="source_connectivity",
        columns=cols or None,
        available_rows=est or None,
        coverage="n/a",
        note="Connectivity / parse — not a full-table data scan",
    )
    if src.error:
        return _block(
            GateId.G1_SOURCE,
            f"Source error: {src.error}",
            start,
            _with_scope({}, scope),
        )
    if not src.connected and src.kind != "file":
        return _block(GateId.G1_SOURCE, "Source not connected", start, _with_scope({}, scope))
    if src.kind == "file" and not src.parseable:
        return _block(
            GateId.G1_SOURCE,
            "File not parseable or corrupt",
            start,
            _with_scope({}, scope),
        )
    if not src.columns:
        return _block(
            GateId.G1_SOURCE,
            "No columns detected in source",
            start,
            _with_scope({}, scope),
        )
    return _pass(
        GateId.G1_SOURCE,
        f"Source readable — {len(src.columns)} columns",
        start,
        _with_scope({"columns": cols, "row_count_estimate": est}, scope),
    )


def gate_g2_destination(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    dest = ctx.plan.destination
    probe = dict(dest.privilege_probe or {}) if isinstance(getattr(dest, "privilege_probe", None), dict) else {}
    details: dict = {
        "table_exists": dest.table_exists,
        "can_create_table": dest.can_create_table,
        "can_write": dest.can_write,
    }
    if probe:
        details["privilege_probe"] = probe

    if dest.error:
        return _block(
            GateId.G2_DESTINATION,
            f"Destination error: {dest.error}",
            start,
            _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
        )
    if not dest.connected:
        return _block(
            GateId.G2_DESTINATION,
            "Destination not reachable",
            start,
            _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
        )

    status = str(probe.get("status") or "").strip()
    # Create-new cannot trust connectivity-only fallback — fail closed until the
    # privilege catalog is readable or the target table already exists.
    # table_exists=None (unknown) must not be treated as create-new.
    if status == "unavailable" and dest.table_exists is False:
        detail = str(probe.get("detail") or "privilege catalog unavailable").strip()
        return _block(
            GateId.G2_DESTINATION,
            "Privilege catalog unavailable for create-new destination — cannot prove "
            f"CREATE ({detail}). Re-validate when grants are readable, or target an "
            "existing table.",
            start,
            _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
        )
    if status == "unavailable" and dest.table_exists is None:
        detail = str(probe.get("detail") or "privilege catalog unavailable").strip()
        return _block(
            GateId.G2_DESTINATION,
            "Destination table existence unknown and privilege catalog unavailable — "
            f"cannot prove CREATE or INSERT ({detail}). Re-check table/schema and grants.",
            start,
            _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
        )

    if not dest.can_write:
        # Prefer probe.detail (engine-specific privilege) over generic SQL wording.
        if probe.get("detail") and probe.get("status") == "denied":
            return _block(
                GateId.G2_DESTINATION,
                str(probe["detail"]),
                start,
                _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
            )
        if dest.table_exists is False and not dest.can_create_table:
            return _block(
                GateId.G2_DESTINATION,
                "Insufficient privileges to CREATE the destination table "
                "(connected, but schema CREATE / CREATE privilege denied)",
                start,
                _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
            )
        if dest.table_exists is None:
            return _block(
                GateId.G2_DESTINATION,
                "Destination table existence unknown — cannot prove CREATE or INSERT "
                "privileges. Re-check table/schema name and credentials.",
                start,
                _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
            )
        return _block(
            GateId.G2_DESTINATION,
            "Insufficient write permissions "
            "(connected, but INSERT privilege denied on the destination table)",
            start,
            _with_scope(details, evidence_scope(kind="destination_connectivity", coverage="n/a")),
        )

    create_note = ""
    if dest.table_exists is False and dest.can_create_table:
        create_note = "; CREATE table allowed"
    elif dest.table_exists is True:
        create_note = "; target table exists"
    elif dest.table_exists is None:
        create_note = "; table existence unknown"

    method = str(probe.get("method") or "").strip()
    if status == "unavailable" and probe.get("detail"):
        msg = (
            f"Destination reachable with write access{create_note} "
            f"(privilege catalog unavailable — {probe['detail']}; "
            "append/upsert to existing table only)"
        )
    elif method:
        msg = f"Destination writable via {method}{create_note}"
    else:
        msg = f"Destination reachable with write access{create_note}"
    return _pass(
        GateId.G2_DESTINATION,
        msg,
        start,
        _with_scope(
            details,
            evidence_scope(
                kind="destination_connectivity",
                coverage="n/a",
                note="Privilege / connectivity probe — not a data scan",
            ),
        ),
    )


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
        # Honesty: no DDL type contract ≠ proven type safety. SKIP (not green PASS).
        return GateResult(
            gate_id=GateId.G3_SCHEMA_CONTRACT,
            status=GateStatus.SKIP,
            message="Schemaless destination — no DDL type contract to validate",
            details=_with_scope(
                {"schemaless": True, "dest_kind": dest_kind},
                evidence_scope(
                    kind="schema_contract",
                    coverage="n/a",
                    note="Document / key-value store — no column type DDL contract",
                ),
            ),
            duration_ms=(time.perf_counter() - start) * 1000,
        )

    try:
        from services.type_system import (
            decimal_scale_would_truncate,
            is_lossy_coercion,
            normalize_logical_type,
            vector_dim_mismatch,
            vector_dim_unknown_for_native,
        )
    except ImportError:
        is_lossy_coercion = None
        decimal_scale_would_truncate = None
        normalize_logical_type = None
        vector_dim_mismatch = None
        vector_dim_unknown_for_native = None

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
        # Fractional scale that exceeds destination DECIMAL caps is silent truncation
        # unless the mapping target is already a lossless text sink.
        if (
            not lossy
            and decimal_scale_would_truncate
            and normalize_logical_type
            and decimal_scale_would_truncate(source_col.inferred_type, dest_kind)
            and normalize_logical_type(target.inferred_type) not in {"string", "text", "json"}
        ):
            lossy = True
            pair = (source_col.inferred_type.upper(), f"{target.inferred_type.upper()} [scale truncates]")
        # Embedding width drift / unknown native VECTOR dims — never invent 1536.
        if (
            not lossy
            and vector_dim_mismatch
            and vector_dim_mismatch(source_col.inferred_type, target.inferred_type)
        ):
            lossy = True
            pair = (source_col.inferred_type.upper(), f"{target.inferred_type.upper()} [vector dim mismatch]")
        if (
            not lossy
            and vector_dim_unknown_for_native
            and normalize_logical_type
            and vector_dim_unknown_for_native(source_col.inferred_type, dest_kind)
            and normalize_logical_type(target.inferred_type) == "vector"
        ):
            lossy = True
            pair = (source_col.inferred_type.upper(), f"{target.inferred_type.upper()} [vector dim unknown]")
        if not lossy:
            continue

        label = (
            f"Lossy coercion: {m.source} ({source_col.inferred_type}) → "
            f"{m.target} ({target.inferred_type})"
        )
        # Surface scale / vector annotations so operators see the real risk.
        if pair and len(pair) == 2 and "[" in str(pair[1]):
            note = str(pair[1]).split("[", 1)[-1].rstrip("]")
            if note:
                label = f"{label} — {note}"

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

    sample_n = int(report.get("sampled_rows") or 0) if isinstance(report, dict) else 0
    g3_scope = evidence_scope(
        kind="schema_contract",
        sample_rows=sample_n or None,
        columns=len(ctx.plan.mappings),
        coverage="sample" if sample_n else "full_schema",
        note=(
            f"{sample_n} preview rows · value-aware coercion"
            if sample_n
            else "Declared types only — no sample values"
        ),
    )

    if issues:
        # Sample-proven or strict declared write hazards always block Validate.
        return _block(
            GateId.G3_SCHEMA_CONTRACT,
            f"{len(issues)} type coercion issue(s)",
            start,
            _with_scope(
                {
                    "issues": issues,
                    "issues_detail": issues_detail,
                    "warnings": warnings,
                    "rule_id": "g3_schema_contract.lossy_coercion",
                    "remediation_kind": "change_target_type",
                },
                g3_scope,
            ),
        )
    if warnings:
        # Sentinel-null / wire-normalize risks are not "verified clean".
        has_null_loss = any(
            int(d.get("sentinel_nulls") or 0) > 0 for d in issues_detail
        )
        msg = (
            f"Schema contract — {len(warnings)} coercion risk(s) on sample "
            f"({'values may become NULL' if has_null_loss else 'review recommended'})"
        )
        return _pass(
            GateId.G3_SCHEMA_CONTRACT,
            msg,
            start,
            _with_scope({"warnings": warnings, "issues_detail": issues_detail}, g3_scope),
        )
    return _pass(
        GateId.G3_SCHEMA_CONTRACT,
        "Schema contract valid",
        start,
        _with_scope({}, g3_scope),
    )


def gate_g4_mapping_confidence(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    threshold = ctx.plan.confidence_threshold
    # Mapping candidates below the floor (used by the semantic mapper) are too
    # weak to keep, but values between the floor and the user threshold are
    # accepted by G4 so G5's data-integrity audit can apply the stricter check.
    confidence_floor = max(0.55, threshold - 0.3)
    g4_scope = evidence_scope(
        kind="mapping_confidence",
        columns=len(ctx.plan.mappings),
        coverage="full_schema",
        note="All mapped columns · confidence classes (not a row scan)",
    )
    mapped_targets = {m.target.lower() for m in ctx.plan.mappings}
    unmapped_required = [
        r for r in ctx.plan.required_targets if r.lower() not in mapped_targets
    ]
    if unmapped_required:
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"Required fields unmapped: {', '.join(unmapped_required)}",
            start,
            _with_scope({"unmapped": unmapped_required}, g4_scope),
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
            _with_scope({"low_confidence": names}, g4_scope),
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
            _with_scope({"ambiguous_mappings": names}, g4_scope),
        )
    return _pass(
        GateId.G4_MAPPING_CONFIDENCE,
        f"All {len(ctx.plan.mappings)} mappings meet confidence floor",
        start,
        _with_scope({}, g4_scope),
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

    scanned = int(details.get("sample_rows_scanned") or 0)
    available = int(details.get("sample_rows_available") or scanned or 0)
    g5_scope = evidence_scope(
        kind="transform_dry_run",
        sample_rows=scanned or None,
        available_rows=available or None,
        columns=len(ctx.plan.mappings),
        coverage="sample",
        note="Preview sample only — not a full-table transform proof",
    )
    details = _with_scope(details, g5_scope)

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

    def _scope(
        details: dict | None = None,
        *,
        coverage: str = "declared_ddl",
        note: str = "DDL / identity checks",
    ) -> dict[str, Any]:
        return _with_scope(
            details or {},
            evidence_scope(kind="target_ddl", coverage=coverage, note=note),
        )

    if not ctx.plan.destination.connected:
        return _block(
            GateId.G6_TARGET_DDL,
            "Gate-6 cannot prove target DDL — destination not connected (complete G2 first)",
            start,
            _scope(
                {"reason": ctx.plan.destination.error or "not_connected"},
                coverage="none",
                note="Refuse Execute unlock without destination connectivity for DDL proof",
            ),
        )

    dest_kind = (ctx.plan.destination.db_type or "").lower()
    schemaless = dest_kind in SCHEMALESS_DESTS
    # Host apps must never fold fingerprint drift into DDL. Scrub defensively so
    # a stale process cannot block Redis/Mongo/Dynamo as "Target DDL incompatible".
    raw_issues = [str(i) for i in (ctx.plan.ddl_issues or [])]
    ddl_issues = [i for i in raw_issues if not _is_drift_noise_issue(i)]
    scrubbed = len(raw_issues) - len(ddl_issues)

    require_unique = True
    try:
        from services.primary_key import sync_requires_unique_identity

        require_unique = sync_requires_unique_identity(
            getattr(ctx.plan, "sync_mode", "") or "",
            dest_kind=dest_kind,
        )
    except Exception:
        require_unique = True

    # Append/overwrite: strip inferred-PK uniqueness noise from host DDL issues so
    # operators who already chose Full refresh · Append are not told to "switch sync mode".
    if not require_unique:
        before = len(ddl_issues)
        ddl_issues = [
            i
            for i in ddl_issues
            if "duplicate" not in i.lower()
            and "primary key candidate" not in i.lower()
            and "unique constraint" not in i.lower()
        ]
        scrubbed += before - len(ddl_issues)

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
                destination_pk_columns=getattr(ctx.plan, "destination_pk_columns", None) or None,
                contract_primary_key=getattr(ctx.plan, "contract_primary_key", None) or None,
            )
        except Exception:
            for m in ctx.plan.mappings:
                if m.target.lower() == "_id":
                    pk_src, pk_tgt = m.source, m.target
                    break
        if pk_tgt:
            # Append/overwrite: sample uniqueness is not a DDL contract unless dest has PK.
            try:
                from services.primary_key import sync_requires_unique_identity

                if not sync_requires_unique_identity(
                    getattr(ctx.plan, "sync_mode", "") or "",
                    dest_kind=dest_kind,
                ):
                    return _pass(
                        GateId.G6_TARGET_DDL,
                        "Schemaless destination — uniqueness not required for this sync mode",
                        start,
                        _scope(
                            {"schemaless": True, "sync_mode": getattr(ctx.plan, "sync_mode", "")},
                            coverage="n/a",
                            note="Schemaless · uniqueness not required for sync mode",
                        ),
                    )
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
            dupes = ctx.probe_unique_constraint([pk_tgt])
            if dupes:
                return _block(
                    GateId.G6_TARGET_DDL,
                    f"UNIQUE constraint would fail on {pk_tgt} — {len(dupes)} duplicate group(s)",
                    start,
                    _scope(
                        {
                            "sample_duplicates": dupes[:5],
                            "primary_key": {"source": pk_src, "target": pk_tgt},
                            "rule_id": "g6_target_ddl.unique",
                            "remediation_kind": "fix_source_keys",
                        },
                        coverage="sample",
                        note="Sample uniqueness probe on identity key",
                    ),
                )
        else:
            # Key-addressed / upsert destinations must resolve an identity key.
            try:
                from services.primary_key import sync_requires_unique_identity

                if sync_requires_unique_identity(
                    getattr(ctx.plan, "sync_mode", "") or "",
                    dest_kind=dest_kind,
                ):
                    return _block(
                        GateId.G6_TARGET_DDL,
                        "Identity key required — set Primary key on Map "
                        "(code/id/_id/iso) before Run to a key-addressed destination",
                        start,
                        _scope(
                            {
                                "schemaless": True,
                                "rule_id": "g6_target_ddl.missing_identity",
                                "remediation_kind": "set_primary_key",
                                "dest_kind": dest_kind,
                            },
                            coverage="n/a",
                            note="Identity key required for key-addressed destination",
                        ),
                    )
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return _pass(
            GateId.G6_TARGET_DDL,
            "Schemaless destination — no DDL contract (identity key checked)",
            start,
            _scope(
                {
                    "schemaless": True,
                    "scrubbed_drift_issues": scrubbed,
                    "primary_key": {"source": pk_src, "target": pk_tgt},
                },
                coverage="n/a",
                note="Schemaless destination — no CREATE/ALTER contract",
            ),
        )

    if ddl_issues:
        head = ddl_issues[0]
        msg = head if len(ddl_issues) <= 1 else f"{head} (+{len(ddl_issues) - 1} more)"
        return _block(
            GateId.G6_TARGET_DDL,
            msg,
            start,
            _scope(
                {
                    "issues": ddl_issues,
                    "rule_id": "g6_target_ddl.incompatible",
                    "remediation_kind": "fix_ddl",
                    "scrubbed_drift_issues": scrubbed,
                },
                note="Declared DDL compatibility issues",
            ),
        )

    # ddl_compatible=False with only drift noise (or empty issues) is a host bug —
    # do not block Execute on a false DDL signal.
    if not ctx.plan.ddl_compatible and not ddl_issues:
        return _pass(
            GateId.G6_TARGET_DDL,
            "Target DDL compatible (ignored empty/drift-only incompatibility flag)",
            start,
            _scope(
                {"scrubbed_drift_issues": scrubbed, "host_flag_ignored": True},
                note="Drift-only host flag ignored",
            ),
        )

    # Canonical identity key uniqueness probe for SQL destinations.
    # Append/overwrite: skip unless the destination introspected a real PK
    # (INSERT would then fail — fail closed with a clear gate).
    if not require_unique and not (getattr(ctx.plan, "destination_pk_columns", None) or []):
        return _pass(
            GateId.G6_TARGET_DDL,
            "Target DDL compatible (uniqueness not required for this sync mode)",
            start,
            _scope(
                {"sync_mode": getattr(ctx.plan, "sync_mode", ""), "scrubbed_drift_issues": scrubbed},
                note="Uniqueness not required for this sync mode",
            ),
        )

    source_cols = [c.name for c in ctx.plan.source.columns]
    try:
        from services.primary_key import resolve_identity_key

        pk_src, pk_tgt = resolve_identity_key(
            mappings=ctx.plan.mappings,
            source_columns=source_cols,
            dest_kind=dest_kind,
            validation_mode=ctx.plan.validation_mode,
            purpose="uniqueness",
            destination_pk_columns=getattr(ctx.plan, "destination_pk_columns", None) or None,
            contract_primary_key=getattr(ctx.plan, "contract_primary_key", None) or None,
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
                _scope(
                    {
                        "sample_duplicates": dupes[:5],
                        "primary_key": {"source": pk_src, "target": pk_tgt},
                        "rule_id": "g6_target_ddl.unique",
                        "remediation_kind": "fix_source_keys",
                    },
                    coverage="sample",
                    note="Sample uniqueness probe on identity key",
                ),
            )
    return _pass(
        GateId.G6_TARGET_DDL,
        "Target DDL compatible",
        start,
        _scope({"scrubbed_drift_issues": scrubbed}),
    )


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
    g7_scope = evidence_scope(
        kind="capacity",
        coverage="estimated",
        note=(
            f"Need {needed:,} bytes · staging available {available:,}"
            if available
            else f"Need {needed:,} bytes · staging capacity unknown"
        ),
    )
    if available and needed > available:
        return _block(
            GateId.G7_CAPACITY,
            f"Insufficient staging capacity: need {needed}, have {available}",
            start,
            _with_scope({"needed": needed, "available": available}, g7_scope),
        )
    if needed > 0 and not available:
        # Unknown capacity used to PASS — that is not proof of headroom.
        return _block(
            GateId.G7_CAPACITY,
            f"Staging capacity unknown — need {needed:,} bytes; cannot prove headroom",
            start,
            _with_scope({"needed": needed, "available": 0, "unknown": True}, g7_scope),
        )
    rows_est = int(getattr(ctx.plan.source, "row_count_estimate", 0) or 0)
    if needed <= 0 and rows_est > 0:
        return _block(
            GateId.G7_CAPACITY,
            f"Staging byte estimate missing for non-empty source ({rows_est:,} rows)",
            start,
            _with_scope(
                {"needed": 0, "available": available, "row_count_estimate": rows_est},
                evidence_scope(
                    kind="capacity",
                    coverage="none",
                    note="Host must supply estimated_bytes when source row_count_estimate > 0",
                ),
            ),
        )
    ratio = f" ({available // max(needed, 1)}x headroom)" if available and needed else ""
    empty_note = " · empty transfer" if needed <= 0 else ""
    return _pass(
        GateId.G7_CAPACITY,
        f"Capacity sufficient{ratio}{empty_note}",
        start,
        _with_scope({"needed": needed, "available": available}, g7_scope),
    )


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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
    # None values must stay None through the write-path transform so the G8 identity
    # check treats SQL/Dynamo NULL and the literal empty string as distinct values.
    if value is None:
        return None, None
    result, err = apply_transform(value, t)
    if err:
        return None, err
    if result is None:
        # apply_transform maps explicit NULL sentinels → None. Keep the sentinel
        # on the G8 wire so identity fingerprint does not collapse NULL into "".
        lowered = str(value or "").strip().lower()
        if lowered in {"__df_sql_null__", "__df_ddb_null__"}:
            return str(value).strip(), None
        return "", None
    return str(result), None


def gate_g8_reconciliation(ctx: PreflightContext) -> GateResult:
    """Dry-run reconciliation: ensure sample rows survive mapping without loss."""
    start = time.perf_counter()
    dest_kind = (ctx.plan.destination.db_type or "").lower()
    sample_rows = getattr(ctx, "sample_rows", None) or []
    if not sample_rows:
        # Fail closed: SKIP used to unlock Execute with zero reconcile proof.
        return _block(
            GateId.G8_RECONCILIATION,
            "Gate-8 cannot prove reconciliation without sample rows — "
            "load a source sample before Execute",
            start,
            _with_scope(
                {
                    "preview_only": True,
                    "source_rows": 0,
                    "note": (
                        "Pre-write Gate-8 simulation requires Validate sample rows; "
                        "refusing Execute unlock without evidence"
                    ),
                },
                evidence_scope(
                    kind="reconciliation",
                    coverage="none",
                    note="No Validate sample rows — Gate-8 blocked until samples load",
                ),
            ),
        )

    source_count = len(sample_rows)
    nondeterministic = [
        m.target
        for m in ctx.plan.mappings
        if m.transform and str(m.transform).lower().strip() in _NON_DETERMINISTIC
    ]

    def _serialize_for_write(value: Any) -> str | None:
        # Match readers/writers: lists/dicts become compact JSON, not Python repr.
        # Studio samples often keep native arrays; str([...]) falsely fails identity.
        # Preserve None as None so apply_transform can produce SQL NULL, not "".
        if value is None:
            return None
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
    # Append/overwrite do not require uniqueness — skip duplicate-key hard block.
    require_unique = True
    try:
        from services.primary_key import sync_requires_unique_identity

        require_unique = sync_requires_unique_identity(
            getattr(ctx.plan, "sync_mode", "") or "",
            dest_kind=dest_kind,
        )
    except Exception:
        require_unique = True

    pk_target = None
    if require_unique:
        try:
            from services.primary_key import resolve_primary_key_target

            pk_target = resolve_primary_key_target(
                ctx.plan.mappings,
                ctx.plan.destination.db_type or "",
                validation_mode=ctx.plan.validation_mode,
                destination_pk_columns=getattr(ctx.plan, "destination_pk_columns", None) or None,
                contract_primary_key=getattr(ctx.plan, "contract_primary_key", None) or None,
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
        dest_eng = dest_kind
        for row_idx, row in enumerate(sample_rows, start=1):
            for m in ctx.plan.mappings:
                tname = str(m.transform or "").lower().strip()
                # Identity / rename-only: raw must equal transformed after write-path bind.
                if tname in {"", "none", "identity", "passthrough", "string", "varchar", "text"}:
                    raw = row.get(m.source, "")
                    got = mapped_rows[row_idx - 1].get(m.target)
                    ddl = ""
                    try:
                        tgt_col = next(
                            (c for c in ctx.plan.destination.target_columns if c.name == m.target),
                            None,
                        )
                        ddl = (tgt_col.type if tgt_col else "") or ""
                    except Exception:
                        ddl = ""
                    try:
                        from services.reconciliation import fingerprint_for_reconcile

                        left = fingerprint_for_reconcile(
                            raw, ddl_type=ddl or "VARCHAR", engine=dest_eng, transform=None
                        )
                        right = fingerprint_for_reconcile(
                            got, ddl_type=ddl or "VARCHAR", engine=dest_eng, transform=None
                        )
                        same = left == right
                    except Exception:
                        same = normalize_cell(raw) == normalize_cell(got)
                    if not same:
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
            _with_scope(
                {
                    "source_rows": source_count,
                    "target_rows": len(mapped_rows),
                    "preview_only": True,
                    "note": "Pre-write write-path sample check — live Gate-8 checksum runs after load",
                },
                evidence_scope(
                    kind="pre_write_reconciliation",
                    sample_rows=source_count,
                    coverage="sample",
                    note="Pre-write simulation — post-write checksum pending",
                ),
            ),
        )

    return _pass(
        GateId.G8_RECONCILIATION,
        (
            f"Dry-run reconciliation skipped fingerprint for non-deterministic "
            f"transform(s) on {', '.join(nondeterministic[:5])} — PK uniqueness checked"
        ),
        start,
        _with_scope(
            {
                "source_rows": source_count,
                "target_rows": len(mapped_rows),
                "skipped_fingerprint_targets": nondeterministic[:12],
                "preview_only": True,
            },
            evidence_scope(
                kind="pre_write_reconciliation",
                sample_rows=source_count,
                coverage="sample",
                note="Fingerprint skipped for non-deterministic transforms",
            ),
        ),
    )


def gate_g9_data_integrity(ctx: PreflightContext) -> GateResult:
    """Critical data integrity — financial precision, required nulls, duplicate keys."""
    start = time.perf_counter()
    audit = getattr(ctx, "run_integrity_audit", None)
    if not callable(audit):
        return _block(
            GateId.G9_DATA_INTEGRITY,
            "Gate-9 cannot prove data integrity — integrity audit not available",
            start,
            _with_scope(
                {
                    "note": (
                        "Refuse Execute unlock without an integrity audit adapter; "
                        "same fail-closed class as Gate-8 without samples"
                    ),
                },
                evidence_scope(
                    kind="data_integrity",
                    coverage="none",
                    note="Integrity audit adapter missing — Gate-9 blocked",
                ),
            ),
        )
    report = audit()
    sample_rows = getattr(ctx, "sample_rows", None) or []
    checks_ran = int(report.get("checks_passed") or 0) + int(report.get("checks_failed") or 0)
    summary = str(report.get("summary") or "")
    unproven = bool(report.get("unproven")) or (
        checks_ran == 0
        and (
            "not configured" in summary.lower()
            or "no source sample" in summary.lower()
        )
    )
    if unproven:
        return _block(
            GateId.G9_DATA_INTEGRITY,
            "Gate-9 cannot prove data integrity — audit unproven "
            f"({summary or 'no checks ran'})",
            start,
            _with_scope(
                {
                    "summary": summary,
                    "checks_passed": report.get("checks_passed", 0),
                    "checks_failed": report.get("checks_failed", 0),
                    "unproven": True,
                },
                evidence_scope(
                    kind="data_integrity",
                    coverage="none",
                    note="Integrity audit returned no checks — refuse Execute unlock",
                ),
            ),
        )
    probe = report.get("source_uniqueness_probe") or {}
    probe_ran = bool(probe.get("ran")) or bool(getattr(ctx, "source_duplicate_probe_ran", False))
    g9_scope = evidence_scope(
        kind="data_integrity",
        sample_rows=len(sample_rows) or None,
        columns=len(ctx.plan.mappings),
        coverage="full_selected" if probe_ran else "sample",
        note=(
            f"Source uniqueness probe on identity key "
            f"{probe.get('primary_key') or getattr(ctx, 'source_duplicate_probe_pk', '') or 'pk'} "
            f"(GROUP BY / aggregate over selected transfer) · other integrity checks use Validate sample"
            if probe_ran
            else "Integrity checks on Validate sample — full-table uniqueness when source probe is unavailable"
        ),
    )
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
            _with_scope(
                {
                    "issues": issues,
                    "issue_texts": [_issue_text(i) for i in issues],
                    "checks_failed": report.get("checks_failed", 0),
                    "encoding_issues": encoding_issues[:12],
                    "source_uniqueness_probe": probe,
                },
                g9_scope,
            ),
        )
    warnings = list(report.get("warnings") or [])
    if encoding_issues and not warnings:
        warnings = [str(i.get("message") if isinstance(i, dict) else i) for i in encoding_issues[:8]]
    return _pass(
        GateId.G9_DATA_INTEGRITY,
        report.get("summary", "Data integrity checks passed"),
        start,
        _with_scope(
            {
                "checks_passed": report.get("checks_passed", 0),
                "warnings": warnings[:12],
                "encoding_issues": encoding_issues[:12],
                "source_uniqueness_probe": probe,
            },
            g9_scope,
        ),
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
    (GateId.G9_DATA_INTEGRITY, gate_g9_data_integrity),
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
