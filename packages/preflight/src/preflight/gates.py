from __future__ import annotations

import json
import logging
import tempfile
import time
from typing import Any, Callable

from preflight.constants import is_schemaless_dest
from preflight.models import (
    ColumnSchema,
    GateId,
    GateResult,
    GateStatus,
    PreflightContext,
)
from preflight.risk_contract import is_safe_normalize_mapping, mapping_risk_cleared

GateFn = Callable[[PreflightContext], GateResult]


def _risk_cleared(m: Any) -> bool:
    """Continue-policy Risk Contract only — boolean risk_acknowledged never clears."""
    return mapping_risk_cleared(m)

# Offline fallback when apps.api type_system cannot be imported (package-only).
# Hosted Validate must use is_lossy_coercion / is_precision_collapse_coercion.
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
    # IEEE / fixed-point — kept even when host type_system is unavailable.
    ("FLOAT", "DECIMAL"),
    ("DOUBLE", "DECIMAL"),
    ("REAL", "DECIMAL"),
    ("FLOAT", "NUMERIC"),
    ("DOUBLE", "NUMERIC"),
    ("DECIMAL", "FLOAT"),
    ("DECIMAL", "DOUBLE"),
    ("NUMERIC", "FLOAT"),
    ("INTEGER", "FLOAT"),
    ("INTEGER", "DOUBLE"),
    ("BIGINT", "FLOAT"),
    ("BIGINT", "DOUBLE"),
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

    # INSERT grant alone must not green-light create-new. Unknown/false create with
    # a missing table is a hard block — otherwise Validate APPROVE invents DDL.
    if dest.table_exists is False and not dest.can_create_table:
        return _block(
            GateId.G2_DESTINATION,
            "Destination table is missing and CREATE is not proven "
            "(INSERT may be allowed on other objects, but create-new DDL is denied/unknown)",
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
    # column-level type contract — but declared BSON/AttributeValue affinity
    # still matters (ObjectId↛NUMBER). Never invent green PASS from SKIP alone.
    dest_kind = (ctx.plan.destination.db_type or "").lower()
    schemaless = is_schemaless_dest(dest_kind)
    if schemaless:
        affinity_issues: list[str] = []
        affinity_warnings: list[str] = []
        affinity_detail: list[dict] = []
        try:
            from services.type_system import assess_bson_affinity
        except ImportError:
            assess_bson_affinity = None  # type: ignore[assignment]

        if assess_bson_affinity is not None:
            src_by_name = {c.name.lower(): c for c in ctx.plan.source.columns}
            for m in ctx.plan.mappings:
                if _is_intentional_omit_mapping(m) or not m.target:
                    continue
                source_col = src_by_name.get((m.source or "").lower())
                if not source_col:
                    continue
                src_type = source_col.inferred_type or ""
                dest_col = dest_by_name.get((m.target or "").lower())
                tgt_type = (
                    getattr(m, "target_type", None)
                    or (dest_col.inferred_type if dest_col else None)
                    or src_type
                )
                risks = assess_bson_affinity(
                    src_type,
                    str(tgt_type or src_type),
                    destination_db_type=dest_kind,
                )
                for risk in risks:
                    label = f"{m.source} → {m.target}: {risk.get('message') or risk.get('kind')}"
                    detail = {
                        "source": m.source,
                        "target": m.target,
                        "source_type": src_type,
                        "target_type": tgt_type,
                        "kind": risk.get("kind"),
                        "severity": risk.get("severity"),
                        "message": risk.get("message"),
                        "risk_acknowledged": _risk_cleared(m),
                    }
                    affinity_detail.append(detail)
                    if risk.get("severity") == "block":
                        if _risk_cleared(m):
                            affinity_warnings.append(label + " (risk contract)")
                        else:
                            affinity_issues.append(
                                label + " — sign Migration Risk Contract or remap"
                            )
                    else:
                        affinity_warnings.append(label)

        if affinity_issues:
            return GateResult(
                gate_id=GateId.G3_SCHEMA_CONTRACT,
                status=GateStatus.BLOCK,
                message=(
                    f"Schemaless BSON affinity blocked {len(affinity_issues)} mapping(s) "
                    f"on {dest_kind}"
                ),
                details=_with_scope(
                    {
                        "schemaless": True,
                        "dest_kind": dest_kind,
                        "issues": affinity_issues,
                        "warnings": affinity_warnings,
                        "issues_detail": affinity_detail,
                        "bson_affinity": True,
                    },
                    evidence_scope(
                        kind="schema_contract",
                        coverage="declared_types",
                        note="Schemaless dest — BSON/AttributeValue affinity (no DDL)",
                    ),
                ),
                duration_ms=(time.perf_counter() - start) * 1000,
            )

        # Honesty: affinity cleared (or no types) ≠ proven DDL type safety.
        note = (
            f"Schemaless destination — BSON affinity checked ({len(affinity_detail)} risk(s)); "
            "no DDL type contract"
            if affinity_detail or affinity_warnings
            else "Schemaless destination — no DDL type contract to validate"
        )
        return GateResult(
            gate_id=GateId.G3_SCHEMA_CONTRACT,
            status=GateStatus.SKIP,
            message=note,
            details=_with_scope(
                {
                    "schemaless": True,
                    "dest_kind": dest_kind,
                    "warnings": affinity_warnings,
                    "issues_detail": affinity_detail,
                    "bson_affinity": bool(affinity_detail or affinity_warnings),
                },
                evidence_scope(
                    kind="schema_contract",
                    coverage="n/a" if not affinity_detail else "declared_types",
                    note="Document / key-value store — BSON affinity when types known",
                ),
            ),
            duration_ms=(time.perf_counter() - start) * 1000,
        )

    try:
        from services.type_system import (
            decimal_precision_would_truncate,
            decimal_scale_would_truncate,
            is_lossy_coercion,
            is_nested_document_collapse,
            is_nested_shape_collapse,
            is_precision_collapse_coercion,
            normalize_logical_type,
            vector_dim_mismatch,
            vector_dim_unknown_for_native,
        )
    except ImportError:
        is_lossy_coercion = None
        is_nested_document_collapse = None
        is_nested_shape_collapse = None
        is_precision_collapse_coercion = None
        decimal_scale_would_truncate = None
        decimal_precision_would_truncate = None
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
    examined = 0
    skipped_missing_dest = 0

    for m in ctx.plan.mappings:
        if _is_intentional_omit_mapping(m) or not m.target:
            continue
        target = dest_by_name.get(m.target.lower())
        source_col = next((c for c in ctx.plan.source.columns if c.name == m.source), None)
        if not source_col:
            continue
        # Create-new / empty dest / ADD column: still enforce stamped target_type.
        # Never PASS by skipping every mapping when dest columns are empty.
        if not target:
            stamped = (getattr(m, "target_type", None) or "").strip()
            if not stamped:
                skipped_missing_dest += 1
                label = f"{m.source} → {m.target}"
                issues.append(
                    f"{label} — destination column missing and no stamped "
                    "target_type; cannot prove type contract"
                )
                continue
            target = ColumnSchema(name=m.target, inferred_type=stamped)
        examined += 1
        pair = (source_col.inferred_type.upper(), target.inferred_type.upper())
        # Prefer type_system SSOT when available; LOSSY_COERCIONS is offline fallback only.
        if is_lossy_coercion:
            lossy = bool(
                is_lossy_coercion(
                    source_col.inferred_type,
                    target.inferred_type,
                    dest_db=dest_kind,
                    dest_table_exists=getattr(
                        ctx.plan.destination, "table_exists", None
                    ),
                )
            )
        else:
            lossy = pair in LOSSY_COERCIONS
        # Declared type loss (not probe-only domain wraps) — samples cannot soft-pass.
        declared_lossy = bool(lossy)
        # Fractional scale that exceeds destination DECIMAL caps is silent truncation
        # unless the mapping target is already a lossless text sink.
        platform_decimal_trunc = False
        if (
            normalize_logical_type
            and normalize_logical_type(target.inferred_type) not in {"string", "text", "json"}
        ):
            if decimal_scale_would_truncate and decimal_scale_would_truncate(
                source_col.inferred_type, dest_kind
            ):
                platform_decimal_trunc = True
                lossy = True
                pair = (
                    source_col.inferred_type.upper(),
                    f"{target.inferred_type.upper()} [scale truncates]",
                )
            if (
                decimal_precision_would_truncate
                and decimal_precision_would_truncate(
                    source_col.inferred_type, dest_kind
                )
            ):
                platform_decimal_trunc = True
                lossy = True
                pair = (
                    source_col.inferred_type.upper(),
                    f"{target.inferred_type.upper()} [precision clamps]",
                )
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
        # Both sides may normalize to the same logical family (datetime) while
        # still collapsing TZ polarity / IEEE precision — treat as lossy.
        _dest_exists = getattr(ctx.plan.destination, "table_exists", None)
        if (
            not lossy
            and is_precision_collapse_coercion
            and is_precision_collapse_coercion(
                source_col.inferred_type,
                target.inferred_type,
                dest_db=str(
                    getattr(getattr(ctx.plan, "destination", None), "db_type", "")
                    or getattr(getattr(ctx.plan, "destination", None), "kind", "")
                    or ""
                ),
                dest_table_exists=_dest_exists,
            )
        ):
            lossy = True
        # Nested STRUCT/MAP field contract or nested→document collapse.
        nested_collapse = bool(
            is_nested_shape_collapse
            and is_nested_shape_collapse(
                source_col.inferred_type,
                target.inferred_type,
                dest_db=dest_kind,
            )
        )
        if not lossy and nested_collapse:
            lossy = True
        # ObjectId → unbounded TEXT keeps the hex value but the destination no
        # longer enforces the ObjectId domain — Accept risk, never silent green.
        objectid_text_domain = False
        try:
            from services.specialty_fit import objectid_text_domain_polarity

            objectid_text_domain = objectid_text_domain_polarity(
                source_col.inferred_type, target.inferred_type
            )
        except ImportError:
            objectid_text_domain = False
        if objectid_text_domain:
            lossy = True
        # Coercion probe may block wire values even when declared types look
        # safe (naive DATETIME→TIMESTAMPTZ). Never skip those columns.
        probe_early = by_source.get(m.source) if value_aware else None
        if not lossy and probe_early:
            sev = str(probe_early.get("severity") or "").lower()
            if sev == "block" or bool(probe_early.get("has_blocking_failures")):
                lossy = True
            elif int(probe_early.get("json_scalar_wraps") or 0) > 0:
                # Bare scalar→JSON string is a domain change even when declared
                # types are not lossy (e.g. INTEGER→VARIANT). Examine wrap path.
                lossy = True
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

        # IEEE→fixed, datetime→date, timestamptz→NTZ, DECIMAL(p,s) narrow,
        # platform DECIMAL caps: never sample soft-pass.
        fidelity_collapse = bool(
            (
                is_precision_collapse_coercion
                and is_precision_collapse_coercion(
                    source_col.inferred_type,
                    target.inferred_type,
                    dest_db=str(
                        getattr(getattr(ctx.plan, "destination", None), "db_type", "")
                        or getattr(getattr(ctx.plan, "destination", None), "kind", "")
                        or ""
                    ),
                    dest_table_exists=_dest_exists,
                )
            )
            or platform_decimal_trunc
        )
        if fidelity_collapse:
            if "float" in source_col.inferred_type.lower() and "decimal" in (
                target.inferred_type.lower()
            ):
                label = f"{label} — float→decimal (IEEE precision risk; not soft-passed by samples)"
            elif "decimal" in source_col.inferred_type.lower() and "float" in (
                target.inferred_type.lower()
            ):
                label = (
                    f"{label} — decimal→float (IEEE magnitude/scale loss; "
                    "not soft-passed by samples)"
                )
            elif normalize_logical_type and normalize_logical_type(
                target.inferred_type
            ) == "date":
                label = f"{label} — datetime→date (time-of-day truncation; not soft-passed by samples)"
            elif "ntz" in target.inferred_type.lower() or "without time zone" in (
                target.inferred_type.lower()
            ):
                label = f"{label} — timestamptz→ntz (timezone polarity drop; not soft-passed by samples)"
            elif "decimal" in source_col.inferred_type.lower() and "decimal" in (
                target.inferred_type.lower()
            ):
                label = (
                    f"{label} — DECIMAL(p,s) narrowing / platform cap "
                    "(scale or integer-digit capacity shrinks; not soft-passed by samples)"
                )
            elif "unsigned" in source_col.inferred_type.lower() or "uint" in (
                source_col.inferred_type.lower()
            ):
                label = (
                    f"{label} — UNSIGNED→signed integer overflow risk "
                    "(not soft-passed by samples; widen to BIGINT/DECIMAL)"
                )
            elif platform_decimal_trunc:
                label = (
                    f"{label} — destination DECIMAL platform cap "
                    "(not soft-passed by samples)"
                )
            else:
                # Specialty polarity (ObjectId→TEXT, …) must not be mislabeled
                # as VARCHAR width narrowing — OBJECTID logical family is string.
                try:
                    from services.type_system import (
                        specialty_carrier_base,
                        specialty_carrier_would_collapse,
                        string_width_would_narrow,
                    )

                    if specialty_carrier_would_collapse(
                        source_col.inferred_type, target.inferred_type
                    ):
                        spec = specialty_carrier_base(source_col.inferred_type) or "specialty"
                        label = (
                            f"{label} — {spec} specialty polarity collapse "
                            "(prefer VARCHAR(24)/BINARY(12) for ObjectId; "
                            "bare TEXT/VARCHAR drops carrier domain)"
                        )
                    elif (
                        normalize_logical_type
                        and normalize_logical_type(source_col.inferred_type)
                        in {"string", "text"}
                        and normalize_logical_type(target.inferred_type)
                        in {"string", "text"}
                        and string_width_would_narrow(
                            source_col.inferred_type, target.inferred_type
                        )
                    ):
                        label = (
                            f"{label} — VARCHAR/CHAR width narrowing "
                            "(declared capacity shrinks; not soft-passed by samples)"
                        )
                except ImportError:
                    if normalize_logical_type and normalize_logical_type(
                        source_col.inferred_type
                    ) in {"string", "text"} and normalize_logical_type(
                        target.inferred_type
                    ) in {"string", "text"}:
                        label = (
                            f"{label} — VARCHAR/CHAR width narrowing "
                            "(declared capacity shrinks; not soft-passed by samples)"
                        )

        document_collapse = bool(
            is_nested_document_collapse
            and is_nested_document_collapse(
                source_col.inferred_type,
                target.inferred_type,
                # Without the dialect the helper fails closed, so ARRAY/MAP into
                # the destination's own document wire (Snowflake VARIANT, MySQL
                # JSON, PG JSONB) was reported as field-DDL loss and blocked
                # every schemaless source that had no struct_policy set.
                dest_db=dest_kind,
            )
        )
        field_shape_loss = bool(nested_collapse and not document_collapse)
        if document_collapse:
            label = (
                f"{label} — nested→document (STRUCT/MAP/ARRAY field DDL not preserved; "
                "Airbyte-style JSON/VARIANT path)"
            )
        elif field_shape_loss:
            label = (
                f"{label} — nested field/element contract mismatch "
                "(STRUCT/MAP/ARRAY shape)"
            )

        # Explicit Map struct_policy acknowledges document serialization.
        policy = (getattr(m, "struct_policy", None) or "").strip().lower()
        intentional_json = policy in {
            "store_as_json",
            "flatten_top_level_keys",
            "flatten_deep",
            "explode_rows",
        }

        # With sampled values we only hard-block when a real value cannot be
        # coerced. A declared-type mismatch whose values all coerce cleanly (or
        # are placeholder text that becomes NULL) is downgraded to a warning —
        # this is what stops schemaless sources (MongoDB widened to TEXT) from
        # producing a wall of false coercion blocks.
        # Exception: fidelity_collapse / nested field mismatch always block.
        # nested→document blocks unless struct_policy acknowledges the path.
        #
        # Create-new UUID→STRING/TEXT or exact CHAR/VARCHAR(36) wire: domain not
        # enforced at destination — Accept risk (never silent-green UUID→UUID).
        uuid_string_create_new = False
        uuid_exact_wire_domain = False
        if (
            not field_shape_loss
            and (
                getattr(m, "create_new", False)
                or ctx.plan.destination.table_exists is False
            )
            and normalize_logical_type
            and normalize_logical_type(source_col.inferred_type) == "uuid"
        ):
            try:
                from services.type_system import (
                    uuid_exact_wire_carrier,
                    uuid_would_collapse,
                )

                if uuid_exact_wire_carrier(target.inferred_type) and (
                    normalize_logical_type(target.inferred_type) != "uuid"
                ):
                    uuid_exact_wire_domain = True
                elif fidelity_collapse and uuid_would_collapse(
                    source_col.inferred_type, target.inferred_type
                ):
                    uuid_string_create_new = True
            except ImportError:
                uuid_string_create_new = False
                uuid_exact_wire_domain = False

        probe = by_source.get(m.source) if value_aware else None
        # ObjectId→bare TEXT/VARCHAR: hex wire is value-lossless; domain polarity
        # still needs Accept risk (not a silent hard-block on existing PG TEXT).
        objectid_text_polarity = False
        if not field_shape_loss:
            try:
                from services.type_system import (
                    specialty_carrier_base,
                    specialty_carrier_would_collapse,
                )

                objectid_source = (
                    specialty_carrier_base(source_col.inferred_type) == "OBJECTID"
                )
                objectid_text_polarity = objectid_source and (
                    (
                        fidelity_collapse
                        and specialty_carrier_would_collapse(
                            source_col.inferred_type, target.inferred_type
                        )
                    )
                    or objectid_text_domain
                )
            except ImportError:
                objectid_text_polarity = False

        if uuid_string_create_new or uuid_exact_wire_domain:
            # Domain polarity needs Accept risk — never soft-pass create-new
            # UUID→STRING/CHAR(36) while Map CTA stays silent.
            risk_ack = _risk_cleared(m)
            uuid_label = (
                f"{label} — create-new stores UUID as {target.inferred_type} "
                "(UUID domain is not enforced at destination)"
            )
            detail = {
                "source": m.source,
                "target": m.target,
                "column": m.source,
                "source_type": source_col.inferred_type,
                "target_type": target.inferred_type,
                "severity": "warn" if risk_ack else "block",
                "fidelity_collapse": True,
                "uuid_string_create_new": True,
                "uuid_exact_wire_domain": uuid_exact_wire_domain,
                "risk_acknowledged": risk_ack,
                "reason": uuid_label,
                "message": uuid_label,
                "sampled": (probe or {}).get("sampled", 0) if probe else 0,
                "failed": (probe or {}).get("failed", 0) if probe else 0,
                "sentinel_nulls": (probe or {}).get("sentinel_nulls", 0) if probe else 0,
                "sample_failures": (probe or {}).get("sample_failures", []) if probe else [],
                "suggested_fix": (
                    "Sign a Migration Risk Contract (continue policy) for UUID "
                    "string-wire polarity, or choose a destination with native UUID."
                ),
                "suggested_target_type": (probe or {}).get("suggested_target_type") if probe else None,
                "suggested_transform": (probe or {}).get("suggested_transform") if probe else None,
            }
            issues_detail.append(detail)
            if risk_ack:
                warnings.append(uuid_label + " (risk contract)")
            else:
                issues.append(
                    uuid_label + " — sign Migration Risk Contract or remap to native UUID"
                )
        elif objectid_text_polarity:
            risk_ack = _risk_cleared(m)
            oid_label = (
                f"{label} — ObjectId→TEXT keeps hex values; ObjectId domain "
                "is not enforced at destination"
            )
            detail = {
                "source": m.source,
                "target": m.target,
                "column": m.source,
                "source_type": source_col.inferred_type,
                "target_type": target.inferred_type,
                "severity": "warn" if risk_ack else "block",
                "fidelity_collapse": True,
                "objectid_text_polarity": True,
                "risk_acknowledged": risk_ack,
                "reason": oid_label,
                "message": oid_label,
                "sampled": (probe or {}).get("sampled", 0) if probe else 0,
                "failed": (probe or {}).get("failed", 0) if probe else 0,
                "sentinel_nulls": (probe or {}).get("sentinel_nulls", 0) if probe else 0,
                "sample_failures": (probe or {}).get("sample_failures", []) if probe else [],
                "suggested_fix": (
                    "Sign a Migration Risk Contract for ObjectId→TEXT, or remap to "
                    "VARCHAR(24) / BINARY(12)."
                ),
                "suggested_target_type": "VARCHAR(24)",
                "suggested_transform": None,
            }
            issues_detail.append(detail)
            if risk_ack:
                warnings.append(oid_label + " (risk contract)")
            else:
                issues.append(
                    oid_label + " — sign Migration Risk Contract or remap to VARCHAR(24)"
                )
        elif fidelity_collapse or field_shape_loss:
            # Declared fidelity + nested shape collapses honor verified continue-
            # policy Risk Contract (same SSOT as G8 holdouts / write quarantine).
            # Without a contract, nested shape and declared collapses hard-block.
            risk_ack = _risk_cleared(m)
            collapse_warn = bool(risk_ack)
            if collapse_warn:
                warnings.append(
                    label
                    + (
                        " (risk contract — nested shape accepted; remap preferred)"
                        if field_shape_loss
                        else " (risk contract)"
                    )
                )
            else:
                issues.append(
                    label
                    if field_shape_loss or not fidelity_collapse
                    else (label + " — sign Migration Risk Contract or remap")
                )
            detail = {
                "source": m.source,
                "target": m.target,
                "column": m.source,
                "source_type": source_col.inferred_type,
                "target_type": target.inferred_type,
                "severity": "warn" if collapse_warn else "block",
                "fidelity_collapse": fidelity_collapse,
                "nested_shape_collapse": field_shape_loss,
                "risk_acknowledged": risk_ack,
                # Holdout only when samples/transforms fail — clean lossy casts still write.
                "contracted_holdout": False,
                "reason": label,
                "message": label,
                "sampled": 0,
                "failed": 0,
                "sentinel_nulls": 0,
                "sample_failures": [],
                "suggested_fix": (
                    "Accept risk on Map, or remap to a fidelity-preserving type."
                    if fidelity_collapse or field_shape_loss
                    else ""
                ),
                "suggested_target_type": None,
                "suggested_transform": None,
            }
            if probe is not None:
                detail.update({
                    "sampled": probe.get("sampled", 0),
                    "failed": probe.get("failed", 0),
                    "sentinel_nulls": probe.get("sentinel_nulls", 0),
                    "sample_failures": probe.get("sample_failures", []),
                    "suggested_fix": probe.get("suggested_fix", "") or detail["suggested_fix"],
                    "suggested_target_type": probe.get("suggested_target_type"),
                    "suggested_transform": probe.get("suggested_transform"),
                })
            else:
                # Schema-contract specialty / width collapses: guide remap.
                try:
                    from services.type_system import (
                        specialty_carrier_would_collapse,
                        create_new_mapping_target_type,
                    )

                    if specialty_carrier_would_collapse(
                        source_col.inferred_type, target.inferred_type
                    ):
                        suggested = create_new_mapping_target_type(
                            source_col.inferred_type,
                            getattr(ctx.plan.destination, "db_type", "") or "",
                        )
                        detail["suggested_target_type"] = suggested
                        detail["suggested_fix"] = (
                            f"Remap target type to {suggested} (or create-new "
                            "with that DDL) — bare TEXT/VARCHAR drops specialty domain."
                        )
                except ImportError:
                    pass
            issues_detail.append(detail)
        elif document_collapse and not intentional_json:
            # Nested→document: struct_policy OR signed continue-policy Risk Contract.
            risk_ack = _risk_cleared(m)
            if risk_ack:
                warnings.append(
                    label + " (risk contract — nested document accepted; prefer struct_policy)"
                )
                if probe is not None:
                    issues_detail.append({
                        "source": m.source,
                        "target": m.target,
                        "source_type": source_col.inferred_type,
                        "target_type": target.inferred_type,
                        "severity": "warn",
                        "nested_document_collapse": True,
                        "risk_acknowledged": True,
                        "contracted_holdout": int(probe.get("failed") or 0) > 0,
                        "sampled": probe.get("sampled", 0),
                        "failed": probe.get("failed", 0),
                        "sentinel_nulls": probe.get("sentinel_nulls", 0),
                        "sample_failures": probe.get("sample_failures", []),
                        "suggested_fix": (
                            "Set struct_policy=store_as_json or map to native STRUCT/OBJECT"
                        ),
                        "suggested_target_type": probe.get("suggested_target_type"),
                        "suggested_transform": probe.get("suggested_transform"),
                    })
            else:
                issues.append(
                    label + " — set Map struct_policy to store_as_json (or flatten) to proceed"
                )
                if probe is not None:
                    issues_detail.append({
                        "source": m.source,
                        "target": m.target,
                        "source_type": source_col.inferred_type,
                        "target_type": target.inferred_type,
                        "severity": "block",
                        "nested_document_collapse": True,
                        "sampled": probe.get("sampled", 0),
                        "failed": probe.get("failed", 0),
                        "sentinel_nulls": probe.get("sentinel_nulls", 0),
                        "sample_failures": probe.get("sample_failures", []),
                        "suggested_fix": "Set struct_policy=store_as_json or map to native STRUCT/OBJECT",
                        "suggested_target_type": probe.get("suggested_target_type"),
                        "suggested_transform": probe.get("suggested_transform"),
                    })
        elif document_collapse and intentional_json:
            warnings.append(label + f" — acknowledged via struct_policy={policy}")
        elif probe is not None:
            severity = probe.get("severity", "ok")
            risk_ack = _risk_cleared(m)
            # Head-sample "ok" must never soft-pass declared lossy without a
            # verified continue-policy Migration Risk Contract.
            force_block = bool(declared_lossy and not risk_ack)
            # Signed continue policy matches write/G8: sample cast failures hold
            # out (warn) — do not re-lock Validate after the operator signed.
            if risk_ack and severity == "block":
                severity = "warn"
            json_wraps = int(probe.get("json_scalar_wraps") or 0)
            detail = {
                "source": m.source,
                "target": m.target,
                "source_type": source_col.inferred_type,
                "target_type": target.inferred_type,
                "severity": "block" if force_block or severity == "block" else severity,
                "sampled": probe.get("sampled", 0),
                "failed": probe.get("failed", 0),
                "sentinel_nulls": probe.get("sentinel_nulls", 0),
                "json_scalar_wraps": json_wraps,
                "sample_failures": probe.get("sample_failures", []),
                "suggested_fix": probe.get("suggested_fix", "")
                or (
                    "Sign a Migration Risk Contract, widen the destination type, or remap"
                    if force_block
                    else (
                        "Sign a Migration Risk Contract if wrapping bare scalars as "
                        "JSON strings is intentional, or emit real JSON objects/arrays upstream."
                        if json_wraps
                        else ""
                    )
                ),
                "suggested_target_type": probe.get("suggested_target_type"),
                "suggested_transform": probe.get("suggested_transform"),
                "risk_acknowledged": risk_ack,
                "contracted_holdout": bool(
                    risk_ack and int(probe.get("failed") or 0) > 0
                ),
                "declared_lossy": True,
            }
            issues_detail.append(detail)
            if force_block:
                issues.append(
                    label
                    + " — declared lossy; sign Migration Risk Contract or remap "
                    "(samples cannot soft-pass)"
                )
            elif severity == "block":
                issues.append(label)
            elif json_wraps:
                wrap_label = (
                    label
                    + " — bare scalar(s) wrapped as JSON string (domain change)"
                )
                if risk_ack:
                    warnings.append(wrap_label + " (risk contract)")
                else:
                    issues.append(
                        wrap_label + " — sign Migration Risk Contract if intentional"
                    )
                    detail["severity"] = "block"
                    detail["suggested_fix"] = (
                        "Accept risk on Map if wrapping bare scalars as JSON "
                        "strings is intentional, or emit real JSON objects/arrays upstream."
                    )
            else:
                warnings.append(label)
        elif value_aware:
            # Probe omits pure text sinks (no cast risk) — declared mismatch
            # stays a warn. Typed sinks with samples but no by_source row are
            # unproven: never invent "all values coerce" from absence.
            tgt_l = (
                normalize_logical_type(target.inferred_type)
                if normalize_logical_type
                else str(target.inferred_type or "").lower()
            )
            if tgt_l in {"string", "text", "json", "variant", "document"}:
                warnings.append(label)
            else:
                unproven = label + " — unproven (no per-column coercion probe)"
                # Typed sinks stay fail-closed in every validation mode.
                issues.append(unproven)
                issues_detail.append({
                    "source": m.source,
                    "target": m.target,
                    "source_type": source_col.inferred_type,
                    "target_type": target.inferred_type,
                    "severity": "block",
                    "probe_unproven": True,
                    "suggested_fix": (
                        "Re-run Validate so every mapped column receives a "
                        "coercion probe, or remap to a proven-compatible type."
                    ),
                })
        else:
            # No samples — declared lossy blocks unless verified Risk Contract.
            risk_ack = _risk_cleared(m)
            mode = (ctx.plan.validation_mode or "strict").strip().lower()
            if lossy and risk_ack:
                warnings.append(
                    label + " — declared lossy; risk contract (no samples)"
                )
            elif lossy and not risk_ack:
                issues.append(
                    label + " — declared lossy; sign Migration Risk Contract or remap"
                )
            elif mode in {"balanced", "review"}:
                warnings.append(label + " (declared; no samples — balanced warn)")
            else:
                # Non-lossy declared mismatch without samples — still fail-closed in strict.
                issues.append(label + " — no samples to prove coercion")

    # Destination NOT NULL contract (Airbyte required-field class): existing
    # typed columns that refuse NULL must not receive nullable sources / empty samples.
    sample_rows = list(getattr(ctx, "sample_rows", None) or [])
    for m in ctx.plan.mappings:
        if _is_intentional_omit_mapping(m) or not m.target:
            continue
        target = dest_by_name.get(str(m.target).lower())
        if not target or target.nullable:
            continue
        source_col = next((c for c in ctx.plan.source.columns if c.name == m.source), None)
        src_nullable = True if source_col is None else bool(source_col.nullable)
        # STOP_COLUMN / CAST+COERCE invent NULL into the primary table — refuse on
        # NOT NULL destinations even when samples look clean (Validate≠write gap).
        if _risk_cleared(m) and _continue_policy_disposition(m) == "null_cell":
            label = (
                f"NOT NULL contract: {m.source} → {m.target} "
                f"({target.inferred_type}) rejects NULL invent — "
                "STOP_COLUMN / coerce continue-policy cannot bind NULL into a "
                "required column; remap, use QUARANTINE_ROW, or widen nullability"
            )
            issues.append(label)
            issues_detail.append({
                "source": m.source,
                "target": m.target,
                "source_type": source_col.inferred_type if source_col else "",
                "target_type": target.inferred_type,
                "severity": "block",
                "not_null_contract": True,
                "null_invent_policy_blocked": True,
                "suggested_fix": (
                    "Use QUARANTINE_ROW / SKIP_ROW, or remap so the destination "
                    "column is nullable / has a DEFAULT"
                ),
            })
            continue
        null_samples = 0
        for row in sample_rows[:200]:
            if not isinstance(row, dict):
                continue
            val = row.get(m.source)
            if val is None or str(val).strip() == "":
                null_samples += 1
        if null_samples:
            label = (
                f"NOT NULL contract: {m.source} → {m.target} "
                f"({target.inferred_type}) rejects NULL — "
                f"{null_samples} empty/null sample value(s)"
            )
            issues.append(label)
            issues_detail.append({
                "source": m.source,
                "target": m.target,
                "source_type": source_col.inferred_type if source_col else "",
                "target_type": target.inferred_type,
                "severity": "block",
                "not_null_contract": True,
                "null_samples": null_samples,
            })
        elif src_nullable and sample_rows:
            # Mongo/file introspect defaults nullable=True — clean samples must not
            # false-block every NOT NULL destination column.
            warnings.append(
                f"NOT NULL contract: {m.source} → {m.target} — source marked nullable; "
                f"{len(sample_rows[:200])} sample row(s) have no nulls"
            )
        elif src_nullable and not sample_rows:
            warnings.append(
                f"NOT NULL contract: {m.source} → {m.target} — source marked nullable "
                "(no samples to prove non-null)"
            )

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

    # Fail-closed: never invent PASS when every mapping was skipped (empty dest,
    # missing stamps, or omit-only plans with unexamined create-new rows).
    active = [
        m
        for m in ctx.plan.mappings
        if not _is_intentional_omit_mapping(m) and getattr(m, "target", None)
    ]
    if active and examined == 0:
        return _block(
            GateId.G3_SCHEMA_CONTRACT,
            (
                f"Schema contract unproven — {len(active)} mapping(s) could not be "
                f"examined ({skipped_missing_dest} missing dest/target_type)"
            ),
            start,
            _with_scope(
                {
                    "issues": issues
                    or [
                        "No destination columns and no stamped target_type — "
                        "cannot prove schema contract"
                    ],
                    "issues_detail": issues_detail,
                    "warnings": warnings,
                    "examined": examined,
                    "skipped_missing_dest": skipped_missing_dest,
                    "rule_id": "g3_schema_contract.unexamined",
                    "remediation_kind": "confirm_destination_schema",
                },
                g3_scope,
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


def _is_intentional_omit_mapping(m: Any) -> bool:
    if getattr(m, "intentional_omit", False):
        return True
    raw = str(getattr(m, "transform", None) or "").strip().lower()
    return raw in {"omit", "intentional_omit", "drop", "exclude"}


def _is_lossy_mapping(m: Any) -> bool:
    fidelity = str(getattr(m, "fidelity", None) or "").strip().lower()
    if fidelity == "lossy_cast":
        return True
    return bool(getattr(m, "type_narrowing", False))


def _requires_risk_ack(m: Any) -> bool:
    """Lossy casts, type narrowing, and value-mutating transforms need a contract.

    Safe normalize (trim / trim_id / email / phone / case) is Map-Ready — not a
    Migration Risk Contract path. Must stay aligned with Map ``isSafeNormalizeMapping``.
    """
    if is_safe_normalize_mapping(m):
        return False
    if _is_lossy_mapping(m):
        return True
    fidelity = str(getattr(m, "fidelity", None) or "").strip().lower()
    return fidelity == "mutate"


def _is_structural_review_mapping(m: Any) -> bool:
    """STRUCT flatten / specialty identity cannot clear via bare user_override."""
    if getattr(m, "struct_derived", False):
        return True
    policy = str(getattr(m, "struct_policy", None) or "").strip().lower()
    if policy in {"flatten_top_level_keys", "flatten_deep", "explode_rows"}:
        return True
    xf = str(getattr(m, "transform", None) or "").strip().lower()
    return xf in {"identity_specialty", "specialty"}


def gate_g4_mapping_confidence(ctx: PreflightContext) -> GateResult:
    start = time.perf_counter()
    threshold = ctx.plan.confidence_threshold
    # Client fail-closed: Map threshold is the G4 floor. Soft band (threshold−0.3)
    # previously let low-confidence exact-name remaps pass into Execute.
    confidence_floor = max(0.55, float(threshold or 0.85))
    active = [m for m in ctx.plan.mappings if not _is_intentional_omit_mapping(m)]
    g4_scope = evidence_scope(
        kind="mapping_confidence",
        columns=len(active),
        coverage="full_schema",
        note="All mapped columns · confidence classes (not a row scan)",
    )
    mapped_targets = {m.target.lower() for m in active if m.target}
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

    # Lossy / mutate / narrowing cannot clear via bare user_override or boolean ack.
    risk_unacked = [
        m for m in active if _requires_risk_ack(m) and not _risk_cleared(m)
    ]
    if risk_unacked:
        names = [f"{m.source}→{m.target}" for m in risk_unacked]
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"{len(risk_unacked)} mapping(s) require a signed Migration Risk Contract "
            "with a continue execution policy (lossy/narrowing/mutate)",
            start,
            _with_scope({"risk_unacknowledged": names}, g4_scope),
        )

    # STRUCT flatten / specialty — boolean ack alone is insufficient.
    structural_unacked = [
        m
        for m in active
        if (
            (m.requires_review or _is_structural_review_mapping(m))
            and _is_structural_review_mapping(m)
            and not _risk_cleared(m)
        )
    ]
    if structural_unacked:
        names = [f"{m.source}→{m.target}" for m in structural_unacked]
        return _block(
            GateId.G4_MAPPING_CONFIDENCE,
            f"{len(structural_unacked)} STRUCT/specialty mapping(s) require a "
            "signed Migration Risk Contract",
            start,
            _with_scope({"structural_unacknowledged": names}, g4_scope),
        )

    low_confidence = [
        m
        for m in active
        if m.confidence < confidence_floor
        and not m.user_override
        and not _requires_risk_ack(m)
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
        for m in active
        if m.requires_review
        and not m.user_override
        and not _requires_risk_ack(m)
        and not _is_structural_review_mapping(m)
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
        f"All {len(active)} mappings meet confidence floor",
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
    # Parity with G8: continue-policy Risk Contracts hold out cast failures —
    # they must not keep Sample dry-run blocked after Accept · cast & continue.
    from preflight.risk_contract import partition_transform_dry_run_errors

    hard_errors, contracted = partition_transform_dry_run_errors(
        list(errors or []),
        list(ctx.plan.mappings or []),
    )
    details: dict[str, Any] = {
        "errors": list(hard_errors[:20]),
        "contracted_holdouts": list(contracted[:20]),
        "contracted_holdout_count": len(contracted),
    }
    if hard_errors:
        details["kind"] = "transform_errors"
    dry_meta = getattr(ctx, "_last_dry_run_meta", None)
    if isinstance(dry_meta, dict):
        details.update({k: v for k, v in dry_meta.items() if k not in details})

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

    if hard_errors:
        details["issue_texts"] = [_issue_text(i) for i in hard_errors[:20]]
        return _block(
            GateId.G5_DRY_RUN,
            _block_message("Dry-run failed", hard_errors),
            start,
            details,
        )
    # Contracted-only failures: gate does not hard-block Execute, but must not
    # claim a clean "passed" — auditors reject "dry-run passed" beside holdouts.
    if contracted:
        details["note"] = (
            "Continue-policy Risk Contract holdouts on sample — hard transforms "
            "cleared; cast failures quarantine/hold out at write (not silent invent). "
            "This is not a clean transform pass."
        )
        details["transform_status"] = "completed_with_contracted_holdouts"
    elif not passed and not errors:
        # run_dry_run returned False with empty errors (rare adapter failure).
        return _block(
            GateId.G5_DRY_RUN,
            "Dry-run failed — no sample transform proof",
            start,
            details,
        )
    if contracted and not hard_errors:
        rows_bit = (
            f" ({int(details.get('sample_rows_scanned', 0))} preview rows)"
            if details.get("sample_rows_scanned")
            else ""
        )
        return _pass(
            GateId.G5_DRY_RUN,
            (
                f"Sample transform completed with {len(contracted)} contracted "
                f"holdout(s){rows_bit} — not a clean pass; holdouts quarantine at write"
            ),
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
    schemaless = is_schemaless_dest(dest_kind)
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
                if m.target and str(m.target).lower() == "_id":
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
                from services.primary_key import missing_identity_blocks

                if missing_identity_blocks(
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

    # Append into a table that already enforces this key: the write aborts on the
    # first stored key, so the verdict belongs here, not in a duplicate-key error
    # after Execute has started.
    collision = getattr(ctx, "destination_collision", None)
    if collision is not None and getattr(collision, "findings", None):
        found = list(collision.findings)
        key = getattr(collision, "key_column", "") or "identity key"
        if getattr(collision, "idempotent_apply", False):
            # Resume: the overlap is the interrupted batch being re-delivered and
            # the writer resolves it on the enforced key. Blocking here leaves a
            # half-loaded destination with no forward path.
            return _pass(
                GateId.G6_TARGET_DDL,
                (
                    f"Resume re-delivery overlaps {len(found)} existing destination "
                    f"key(s) on {key} — applied idempotently on the enforced key "
                    "(at-least-once read, key-resolved write)."
                ),
                start,
                _scope(
                    {
                        "sample_collisions": found[:5],
                        "primary_key": {"target": key},
                        "sync_mode": getattr(ctx.plan, "sync_mode", ""),
                        "rule_id": "g6_target_ddl.append_key_collision_resume",
                        "probe_status": getattr(collision, "status", ""),
                        "values_probed": getattr(collision, "values_probed", 0),
                        "delivery": "at_least_once_idempotent_apply",
                        "delta_scope": getattr(collision, "delta_scope", {}) or {},
                    },
                    coverage="sample",
                    note="Destination key collision probe on resumed append batch",
                ),
            )
        delta_scope = getattr(collision, "delta_scope", {}) or {}
        if delta_scope:
            # The collision is inside the delta this cursor will re-read, so the
            # operator needs to know the key returns with a newer cursor value —
            # an append cannot store it twice.
            cause = (
                f"The rows after watermark {delta_scope.get('watermark')} on "
                f"{delta_scope.get('cursor_column')} carry {len(found)} key(s) the "
                f"destination already stores on {key}, so an append aborts. "
                "Switch this sync to upsert/merge (key-resolved), which is how an "
                "updated row is meant to land."
            )
        else:
            cause = (
                f"Append would duplicate {len(found)} existing destination key(s) on "
                f"{key} — the destination enforces uniqueness, so the insert aborts. "
                "Switch this sync to upsert/merge (key-resolved) or overwrite."
            )
        return _block(
            GateId.G6_TARGET_DDL,
            cause,
            start,
            _scope(
                {
                    "sample_collisions": found[:5],
                    "primary_key": {"target": key},
                    "sync_mode": getattr(ctx.plan, "sync_mode", ""),
                    "rule_id": "g6_target_ddl.append_key_collision",
                    "remediation_kind": "change_sync_mode",
                    "probe_status": getattr(collision, "status", ""),
                    "values_probed": getattr(collision, "values_probed", 0),
                    "delta_scope": delta_scope,
                },
                coverage="sample",
                note="Destination key collision probe on append batch",
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
            if m.target and str(m.target).lower() in {"id", "_id"}:
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
    if t in {"hash", "md5", "sha256", "mask", "redact", "pii_mask", "anonymize", "encrypt"}:
        return None
    # For other deterministic string-preserving transforms, keep the value as-is.
    return value


_NON_DETERMINISTIC = {
    # Deterministic UUID *parse* stays comparable — only generators/one-way.
    "hash", "md5", "sha256", "mask", "redact", "pii_mask", "anonymize", "encrypt",
}


def _continue_policy_disposition(mapping: Any) -> str:
    """Map signed continue-policy to G8 dry-run behavior.

    ``holdout`` — omit row (QUARANTINE_ROW / SKIP_ROW / default CAST quarantine).
    ``null_cell`` — keep row with NULL cell (STOP_COLUMN / CAST+COERCE).
    """
    raw = None
    if isinstance(mapping, dict):
        raw = mapping.get("risk_contract") or mapping.get("riskContract")
    else:
        raw = getattr(mapping, "risk_contract", None)
    if not isinstance(raw, dict):
        return "holdout"
    pol = str(raw.get("execution_policy") or "").strip().upper()
    qp = str(
        raw.get("quarantine_policy") or raw.get("quarantinePolicy") or ""
    ).strip().upper()
    if pol == "STOP_COLUMN":
        return "null_cell"
    if pol in {"CAST_AND_CONTINUE", "TRANSFORM_AND_CONTINUE"} and qp in {
        "NULL",
        "COERCE",
        "COERCE_NULL",
        "NULL_CELL",
    }:
        return "null_cell"
    return "holdout"


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
    dest_by_name = {
        str(c.name or "").lower(): c
        for c in (getattr(ctx.plan.destination, "target_columns", None) or [])
        if getattr(c, "name", None)
    }

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
    contracted_holdouts: list[str] = []
    contracted_null_cells: list[str] = []
    mapped_rows: list[dict[str, Any]] = []
    # Parallel to mapped_rows: source rows that survive quarantine holdouts.
    # Fingerprint MUST use this list — never sample_rows[i] vs mapped_rows[i]
    # after holdouts shrink the write set (production IndexError / Validate 500).
    kept_sample_rows: list[tuple[int, dict[str, Any]]] = []
    for row_idx, row in enumerate(sample_rows, start=1):
        mapped: dict[str, Any] = {}
        row_holdout = False
        for m in ctx.plan.mappings:
            if _is_intentional_omit_mapping(m) or not m.target:
                continue
            raw = row.get(m.source, "")
            raw_s = _serialize_for_write(raw)
            if m.transform and str(m.transform).lower().strip() in _NON_DETERMINISTIC:
                mapped[m.target] = None
                continue
            transformed, err = _apply_write_path_transform(raw_s, m.transform)
            if err:
                line = f"row {row_idx} {m.source}→{m.target}: {err}"
                # Continue-policy Risk Contract matches write disposition:
                # quarantine/skip → omit row; STOP_COLUMN/coerce → NULL cell
                # (blocked when destination is NOT NULL — same as G3).
                if _risk_cleared(m):
                    disposition = _continue_policy_disposition(m)
                    dest_col = dest_by_name.get(str(m.target or "").lower()) if dest_by_name else None
                    if (
                        disposition == "null_cell"
                        and dest_col is not None
                        and not bool(getattr(dest_col, "nullable", True))
                    ):
                        transform_errors.append(
                            line
                            + " — NOT NULL destination refuses STOP_COLUMN/coerce NULL invent"
                        )
                        mapped[m.target] = None
                    elif disposition == "null_cell":
                        contracted_null_cells.append(line)
                        mapped[m.target] = None
                    else:
                        contracted_holdouts.append(line)
                        row_holdout = True
                else:
                    transform_errors.append(line)
                    mapped[m.target] = None
            else:
                mapped[m.target] = transformed
        if not row_holdout:
            mapped_rows.append(mapped)
            kept_sample_rows.append((row_idx, row))

    if transform_errors:
        return _block(
            GateId.G8_RECONCILIATION,
            _block_message("Dry-run reconciliation failed — transform errors", transform_errors),
            start,
            {
                "errors": transform_errors[:20],
                "contracted_holdouts": contracted_holdouts[:20],
                "source_rows": source_count,
                "preview_only": True,
                "note": (
                    "Write-path transform failed on sample without a continue-policy "
                    "Risk Contract — remap, clean cells, or Accept · cast & continue / "
                    "quarantine on Map"
                ),
                "kind": "transform_errors",
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
                if m.target and str(m.target).lower() in {"id", "_id"}:
                    pk_target = m.target
                    break

    duplicates = 0
    if pk_target:
        # Match destination CI / CITEXT / UNIQUE(lower(col)) equality.
        dest_ddl = ""
        try:
            for c in getattr(ctx.plan.destination, "target_columns", None) or []:
                if getattr(c, "name", None) == pk_target:
                    dest_ddl = str(getattr(c, "inferred_type", "") or "")
                    break
        except Exception:
            dest_ddl = ""
        unique_keys = getattr(ctx.plan, "destination_unique_keys", None) or []
        nulls_collide = False
        try:
            from services.type_system import (
                unique_equality_key,
                unique_key_forces_casefold,
                unique_key_nulls_collide,
                unique_key_row_in_scope,
            )

            casefold = unique_key_forces_casefold(
                pk_target, ddl_type=dest_ddl, unique_keys=unique_keys
            )
            nulls_collide = unique_key_nulls_collide(
                pk_target, unique_keys=unique_keys
            )
        except Exception:
            unique_equality_key = lambda v, _d=None, force_casefold=False, null_sentinel=None: (  # noqa: E731
                (null_sentinel or "") if v is None or not str(v).strip() else str(v).strip()
            )
            unique_key_row_in_scope = lambda _row, _col, unique_keys=None: True  # noqa: E731
            casefold = False

        null_sentinel = "\x00NULL\x00" if nulls_collide else None
        seen: set[str] = set()
        for row in mapped_rows:
            if not unique_key_row_in_scope(row, pk_target, unique_keys=unique_keys):
                continue
            raw = str(row.get(pk_target, "") or "")
            val = unique_equality_key(
                raw if raw else None,
                dest_ddl,
                force_casefold=casefold,
                null_sentinel=null_sentinel,
                dest_kind=dest_kind,
            )
            if val and val in seen:
                duplicates += 1
            if val:
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

    # G9 parity: composite / dest UNIQUE constraints (hybrid Snowflake, PG composites).
    # Append mode skips identity uniqueness above — still must catch enforced UNIQUE.
    try:
        from services.data_integrity import _check_destination_unique_constraints

        target_types: dict[str, str] = {}
        for c in getattr(ctx.plan.destination, "target_columns", None) or []:
            name = getattr(c, "name", None)
            if name:
                target_types[str(name)] = str(
                    getattr(c, "inferred_type", None)
                    or getattr(c, "type", None)
                    or ""
                )
        # Mapped rows already use target names — identity source=target for probe.
        mapping_dicts = [
            {"source": m.target, "target": m.target} for m in ctx.plan.mappings
        ]
        composite_issues = _check_destination_unique_constraints(
            mapping_dicts,
            mapped_rows,
            destination_pk_columns=getattr(ctx.plan, "destination_pk_columns", None)
            or [],
            destination_unique_keys=getattr(ctx.plan, "destination_unique_keys", None)
            or [],
            target_types=target_types,
            dest_kind=dest_kind,
        )
        if composite_issues:
            return _block(
                GateId.G8_RECONCILIATION,
                _block_message(
                    "Dry-run reconciliation failed — destination UNIQUE/PK duplicates",
                    composite_issues,
                ),
                start,
                {
                    "issues": composite_issues[:15],
                    "target_rows": len(mapped_rows),
                    "rule_id": "g8_reconciliation.destination_unique",
                    "remediation_kind": "fix_source_keys",
                },
            )
    except Exception as exc:
        # Never silent-pass uniqueness proof when the destination declared UNIQUE/PK.
        if getattr(ctx.plan, "destination_unique_keys", None) or getattr(
            ctx.plan, "destination_pk_columns", None
        ):
            return _block(
                GateId.G8_RECONCILIATION,
                "Dry-run reconciliation could not prove destination UNIQUE/PK constraints",
                start,
                {
                    "issues": [f"UNIQUE probe error: {exc}"],
                    "target_rows": len(mapped_rows),
                    "rule_id": "g8_reconciliation.destination_unique_probe",
                    "remediation_kind": "retry_validate",
                    "note": str(exc)[:400],
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
        # Zip kept sources with mapped_rows — quarantine holdouts must not re-index
        # into a shorter write set (IndexError → API 500 on Validate preflight).
        if len(kept_sample_rows) != len(mapped_rows):
            return _block(
                GateId.G8_RECONCILIATION,
                "Dry-run reconciliation invariant failed — holdout/write-set length mismatch",
                start,
                {
                    "source_rows": source_count,
                    "kept_rows": len(kept_sample_rows),
                    "target_rows": len(mapped_rows),
                    "preview_only": True,
                    "rule_id": "g8_reconciliation.holdout_alignment",
                    "remediation_kind": "retry_validate",
                },
            )
        for (row_idx, row), mapped in zip(kept_sample_rows, mapped_rows):
            for m in ctx.plan.mappings:
                if _is_intentional_omit_mapping(m) or not m.target:
                    continue
                tname = str(m.transform or "").lower().strip()
                # Identity / rename-only: serialized source wire must equal mapped
                # cell after destination bind. Use cell_to_string for arrays/objects
                # (same as mapped_rows) — never Python repr. Identity transforms must
                # not strip whitespace (that false-failed Mongo long-text samples).
                if tname in {"", "none", "identity", "passthrough", "string", "varchar", "text"}:
                    raw = row.get(m.source, "")
                    got = mapped.get(m.target)
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

                        wire = _serialize_for_write(raw)
                        left = fingerprint_for_reconcile(
                            wire, ddl_type=ddl or "VARCHAR", engine=dest_eng, transform=None
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
                    "contracted_holdouts": contracted_holdouts[:20],
                    "contracted_holdout_count": len(contracted_holdouts),
                    "contracted_null_cells": contracted_null_cells[:20],
                    "contracted_null_cell_count": len(contracted_null_cells),
                    "note": (
                        "Pre-write write-path sample check — live Gate-8 checksum runs after load"
                        + (
                            f"; {len(contracted_holdouts)} row(s) held out under "
                            "quarantine/skip continue-policy"
                            if contracted_holdouts
                            else ""
                        )
                        + (
                            f"; {len(contracted_null_cells)} cell(s) NULL-invent under "
                            "STOP_COLUMN/coerce continue-policy"
                            if contracted_null_cells
                            else ""
                        )
                    ),
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
    """Critical data integrity — financial precision, required nulls, duplicate keys.

    Coverage honesty: sample-only passes must never claim population / full-table
    uniqueness. When ``source_uniqueness_probe.ran`` is true, coverage is
    ``full_selected`` for that probe only — other checks remain sample-scoped.
    """
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
    probe_status = str(
        probe.get("status")
        or getattr(ctx, "source_duplicate_probe_status", "")
        or ""
    ).strip().lower()
    # Never invent full_selected from a skip/error — only explicit ran.
    probe_ran = bool(probe.get("ran")) and probe_status in ("", "ran")
    if not probe_ran:
        probe_ran = bool(getattr(ctx, "source_duplicate_probe_ran", False)) and probe_status in (
            "",
            "ran",
        )
    coverage = "full_selected" if probe_ran else "sample"
    if probe_ran:
        pk_label = (
            probe.get("primary_key")
            or getattr(ctx, "source_duplicate_probe_pk", "")
            or "pk"
        )
        g9_note = (
            f"Source uniqueness probe on identity key {pk_label} "
            f"(GROUP BY / aggregate over selected transfer) · "
            f"other integrity checks use Validate sample — not a full population proof"
        )
    elif probe_status in ("error", "skipped_unsupported", "skipped_no_source"):
        detail = str(probe.get("message") or probe_status)
        g9_note = (
            f"Source uniqueness probe unavailable ({detail}) — "
            "population uniqueness not proven; uniqueness-required syncs fail closed"
        )
    else:
        g9_note = (
            "Integrity checks on Validate sample only — source uniqueness probe "
            "did not run; full-table / population uniqueness is not proven"
        )
    g9_scope = evidence_scope(
        kind="data_integrity",
        sample_rows=len(sample_rows) or None,
        columns=len(ctx.plan.mappings),
        coverage=coverage,
        note=g9_note,
    )
    encoding = next(
        (c for c in (report.get("checks") or []) if c.get("check") == "encoding_anomalies"),
        None,
    )
    encoding_issues = (encoding or {}).get("issues") or []
    if report.get("blocks_transfer"):
        issues = report.get("issues", [])[:15]
        checks = list(report.get("checks") or [])
        transform_fail = next(
            (
                c
                for c in checks
                if c.get("check") == "transform_dry_run" and not c.get("passed")
            ),
            None,
        )
        g9_details: dict[str, Any] = {
            "issues": issues,
            "issue_texts": [_issue_text(i) for i in issues],
            "checks_failed": report.get("checks_failed", 0),
            "encoding_issues": encoding_issues[:12],
            "source_uniqueness_probe": probe,
        }
        # Stamp transform_errors so root-cause does not invent "fidelity collapse"
        # for empty-url / cast holdouts (parity with G5/G8).
        if transform_fail is not None and not any(
            c.get("fidelity_collapse") for c in checks if isinstance(c, dict)
        ):
            g9_details["kind"] = str(transform_fail.get("kind") or "transform_errors")
            if transform_fail.get("note"):
                g9_details["note"] = transform_fail.get("note")
            if transform_fail.get("contracted_holdouts"):
                g9_details["contracted_holdouts"] = transform_fail.get(
                    "contracted_holdouts"
                )
        return _block(
            GateId.G9_DATA_INTEGRITY,
            _block_message("Data integrity failed", issues),
            start,
            _with_scope(g9_details, g9_scope),
        )
    warnings = list(report.get("warnings") or [])
    if encoding_issues and not warnings:
        warnings = [str(i.get("message") if isinstance(i, dict) else i) for i in encoding_issues[:8]]
    base_summary = str(report.get("summary") or "Data integrity checks passed")
    # Operator-facing message must distinguish sample vs population coverage.
    if coverage == "sample":
        if "sample" not in base_summary.lower():
            pass_msg = f"{base_summary} (Validate sample only — population uniqueness not proven)"
        else:
            pass_msg = base_summary
    else:
        if "full" not in base_summary.lower() and "population" not in base_summary.lower():
            pass_msg = (
                f"{base_summary} (full-selected uniqueness probe · other checks on sample)"
            )
        else:
            pass_msg = base_summary
    return _pass(
        GateId.G9_DATA_INTEGRITY,
        pass_msg,
        start,
        _with_scope(
            {
                "checks_passed": report.get("checks_passed", 0),
                "warnings": warnings[:12],
                "encoding_issues": encoding_issues[:12],
                "source_uniqueness_probe": probe,
                "coverage": coverage,
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
