"""Multi-agent mapping pipeline — plan Part 0A agent architecture."""

from __future__ import annotations

import logging
import re

from services.data_profiler import UNTYPED_TEXT_LOGICALS as _UNTYPED_TEXT_LOGICALS
from services.semantic_mapper import map_columns
from services.transform_engine import infer_transform_for_mapping
from services.decision_kernel import (
    create_new_mapping_target_type,
    ddl_type,
    normalize_logical_type,
)
from services.type_system import ddl_carrier_type

logger = logging.getLogger("datawrap.mapping")

CONFIDENCE_FLOOR = 0.72
# Untyped VARCHAR with no samples — refuse inflated confidence (thin SaaS / failed introspect).
_UNTYPED_VARCHAR_CONF_CAP = 0.78


# When the destination schema is generic or unknown, create-new columns should
# inherit the typed transform's logical type instead of staying as VARCHAR/text.
_TYPED_TRANSFORM_TARGET_TYPE: dict[str, str] = {
    "integer": "INTEGER",
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "DATETIME",
    "time": "TIME",
    "json": "JSON",
    "binary": "BINARY",
    "uuid": "UUID",
    "currency": "DECIMAL",
    "percentage": "DECIMAL",
}


def _canonicalize_schema_rows(schemas: list[dict] | None) -> list[dict] | None:
    """Prefer native_type / parametric carriers over collapsed VARCHAR labels.

    Also drops NULL wire sentinels from the sample evidence: an all-NULL
    ``DECIMAL(7,3)`` column arrived carrying ``__DF_SQL_NULL__`` strings, which
    read as non-numeric text and made Map invent a lossy ``<col>_text``
    LONGTEXT destination instead of honouring the declared numeric type.
    """
    from services.value_serializer import evidence_samples

    if not schemas:
        return schemas
    out: list[dict] = []
    for s in schemas:
        raw = s.get("native_type") or s.get("inferred_type") or "VARCHAR"
        carrier = ddl_carrier_type(str(raw))
        out.append({
            **s,
            "inferred_type": carrier,
            "samples": evidence_samples(s.get("samples")),
        })
    return out


def _stamp_create_new_type_risks(
    mappings: list[dict],
    *,
    destination_db_type: str = "",
    dest_table_exists: bool | None = None,
) -> list[dict]:
    """Annotate create-new mappings with cross-dialect precision/width risk.

    Competitors often hide create-new type loss until write time. We surface it
    on the mapping so Map / Validate / Pilot all see the same risk chip.
    """
    from services.create_new_risk_stamp import apply_create_new_risk_stamps

    return apply_create_new_risk_stamps(
        mappings, destination_db_type, dest_table_exists=dest_table_exists
    )


def _demote_untyped_varchar_confidence(
    mappings: list[dict],
    *,
    source_schemas: list[dict] | None,
    source_types_authoritative: bool = False,
) -> list[dict]:
    """Cap confidence when Map only has bare VARCHAR and zero samples.

    A catalog-declared type is evidence in its own right: an all-NULL
    ``TEXT`` column matched by name onto a text destination column was capped
    to 0.78 and blocked Execute at the confidence floor, while the identical
    column on a create-new route mapped at 0.95. The cap is for sources whose
    VARCHAR is a placeholder (thin SaaS / failed introspect), so it only
    applies when the declared type is unproven or the target is not text.
    """
    by_name = {s["name"]: s for s in (source_schemas or [])}
    text_logicals = {"string", "text", "varchar", "unknown"}
    refined: list[dict] = []
    for m in mappings:
        out = dict(m)
        src = by_name.get(m.get("source") or "", {})
        src_type = str(out.get("source_type") or src.get("inferred_type") or "VARCHAR")
        samples = src.get("samples") or []
        logical = normalize_logical_type(src_type)
        target_logical = normalize_logical_type(str(out.get("target_type") or ""))
        catalog_typed = source_types_authoritative and logical != "unknown"
        if catalog_typed and target_logical in text_logicals:
            refined.append(out)
            continue
        if logical in text_logicals and not samples:
            conf = min(float(out.get("confidence") or 0), _UNTYPED_VARCHAR_CONF_CAP)
            out["confidence"] = round(conf, 3)
            out["requires_review"] = True
            reason = str(out.get("reasoning") or "")
            note = "weak type evidence (VARCHAR, no samples)"
            if note not in reason.lower():
                out["reasoning"] = f"{reason} · {note}".strip(" ·")
        refined.append(out)
    return refined


_VALUE_REWRITING_TRANSFORMS = frozenset({"trim", "trim_id", "upper", "lower", "uuid"})


def _passthrough_identity_transform(
    transform: str,
    *,
    strategy: str,
    create_new: bool,
    user_override: bool,
    src_type: str,
    tgt_type: str,
) -> str:
    """Drop name-triggered value rewrites on identity create-new mappings.

    ``infer_transform_for_mapping`` stamps ``trim_id`` for any target whose name
    ends in ``id``, so copying a table into a table DataFlow itself creates
    rewrote every ``_id`` / ``uid`` / ``userId`` value. That is a mutation the
    operator never asked for: it marks the mapping fidelity ``mutate``, drops
    confidence to 0.70 and blocks G4 — a create-new identity column is a
    byte-exact copy. Type-driven transforms (decimal, date, json) are untouched;
    an operator who wants Trim still chooses it explicitly.
    """
    if user_override or transform not in _VALUE_REWRITING_TRANSFORMS:
        return transform
    if not (create_new or strategy in {"identity_passthrough", "create_compatible_new"}):
        return transform
    try:
        same_family = normalize_logical_type(src_type) == normalize_logical_type(tgt_type)
    except Exception:
        return transform
    return "none" if same_family else transform


def classify_format(source_columns: list[str], file_format: str | None = None) -> dict:
    from services.domain_profiles import detect_data_domain
    from services.semantic_analyzer import analyze_column

    domain_profile = detect_data_domain(source_columns)

    semantic_hits = 0
    for col in source_columns[:12]:
        analyzed = analyze_column(col, "VARCHAR", [])
        if analyzed.get("detection_source") != "unknown":
            semantic_hits += 1

    hints = [c.upper() for c in source_columns[:8]]
    payment_tokens = {
        "AMT", "PAY_AMT", "PAY_AMOUNT", "PAYMENT_AMOUNT",
        "PAYMENT", "PAY", "PMT", "PYMT",
        "TXN_AMT", "TRANSACTION_AMOUNT",
        "TXN_DT", "TRANSACTION_DATE", "PAY_DATE", "PAYMENT_DATE",
        "PAY_DT", "VALUE_DATE", "DTPMT",
        "CUST_ID", "ACCT_NO", "REF_NO", "CCY", "CURRENCY", "CURRENCY_CODE",
        "PAYMENT_ID", "MERCHANT_ID", "PAYER_ID", "BENEFICIARY_ACCOUNT",
    }
    overlap = len(set(hints) & payment_tokens)

    if overlap >= 2:
        fmt = "payment_feed"
        confidence = min(0.95, 0.75 + overlap * 0.05)
    elif domain_profile["domain"] != "general" and domain_profile["confidence"] >= 0.4:
        fmt = f"{domain_profile['domain']}_feed"
        confidence = domain_profile["confidence"]
    elif semantic_hits >= max(2, len(source_columns[:8]) // 2):
        fmt = "semantic_tabular"
        confidence = min(0.92, 0.7 + semantic_hits * 0.04)
    elif file_format:
        fmt = file_format
        confidence = 0.78
    else:
        fmt = "generic_tabular"
        confidence = 0.72

    return {
        "format": fmt,
        "confidence": confidence,
        "agent": "FormatClassifierAgent",
        "semantic_hits": semantic_hits,
        "domain": domain_profile,
    }


def enrich_columns(source_columns: list[str], source_schemas: list[dict] | None = None) -> dict[str, str]:
    from services.semantic_analyzer import analyze_column

    enrichments: dict[str, str] = {}
    schema_by_name = {s["name"]: s for s in (source_schemas or [])}

    for col in source_columns:
        schema = schema_by_name.get(col, {"name": col, "inferred_type": "VARCHAR", "samples": []})
        analyzed = analyze_column(schema["name"], schema.get("inferred_type", "VARCHAR"), schema.get("samples", []))
        enrichments[col] = analyzed["description"]
    return enrichments


def generate_transforms(
    mappings: list[dict],
    *,
    schema_by_name: dict | None = None,
    target_by_name: dict | None = None,
) -> list[dict]:
    """TransformCodegenAgent — logical transform hints aligned with dry-run engine."""
    schema_by_name = schema_by_name or {}
    target_by_name = target_by_name or {}
    transforms: list[dict] = []
    for m in mappings:
        src_type = schema_by_name.get(m["source"], {}).get("inferred_type", "VARCHAR")
        tgt_type = target_by_name.get(m["target"], {}).get("inferred_type")
        logical = m.get("transform") or infer_transform_for_mapping(
            m["source"], m["target"], src_type, tgt_type
        )
        transforms.append(
            {
                "source": m["source"],
                "target": m["target"],
                "transform": logical,
                "agent": "TransformCodegenAgent",
            }
        )
    return transforms


def _normalize_col_token(name: str) -> str:
    # Preserve leading/trailing underscores so columns like `id` and `_id`
    # are not entailed to the same target column.
    return re.sub(r"[\s-]+", "_", name.strip().lower()).strip()


def _column_entailed(candidate: str, target: str) -> bool:
    """True when candidate target plausibly maps to a known destination column."""
    c = _normalize_col_token(candidate)
    t = _normalize_col_token(target)
    if c == t:
        return True
    c_parts = [p for p in c.split("_") if p]
    t_parts = [p for p in t.split("_") if p]
    if not c_parts or not t_parts:
        return False
    if len(c_parts) == 1 and len(t_parts) > 1:
        return False
    if len(t_parts) == 1 and len(c_parts) > 1:
        return t_parts[0] in c_parts
    return set(c_parts) == set(t_parts)


def _is_create_new_mapping(m: dict) -> bool:
    if m.get("create_new"):
        return True
    strategy = str(m.get("assignment_strategy") or "")
    return strategy in {"create_compatible_new", "identity_passthrough"}


def _repair_unparseable_numeric_targets(
    mappings: list[dict],
    *,
    source_schemas: list[dict] | None,
    target_schemas: list[dict] | None,
    destination_db_type: str = "",
) -> list[dict]:
    """Rewrite hex/ObjectId → NUMBER/INTEGER mappings to create-new VARCHAR.

    Studio was proposing ``_id`` → ``id`` NUMBER(38,0) when Mongo introspect
    lacked samples; Validate then correctly blocked. Even with a typed dest
    ``id``, non-numeric samples must never stay on that column.
    """
    from services.schema_inference import samples_fit_logical_type

    src_by = {s["name"]: s for s in (source_schemas or [])}
    tgt_by = {s["name"]: s for s in (target_schemas or [])}
    taken = {str(m.get("target") or "").lower() for m in mappings}
    for t in tgt_by:
        taken.add(t.lower())
    out: list[dict] = []
    for m in mappings:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        samples = [str(x) for x in (src_by.get(src, {}).get("samples") or [])[:8] if str(x).strip()]
        tgt_type = str(
            m.get("target_type")
            or tgt_by.get(tgt, {}).get("inferred_type")
            or ""
        )
        logical = normalize_logical_type(tgt_type)
        if (
            samples
            and len(samples) >= 2
            and logical in {"integer", "decimal"}
            and not samples_fit_logical_type(samples, tgt_type or "INTEGER", field_name=src)
        ):
            dest_db = (destination_db_type or "").strip().lower()
            dest_native = ddl_type(dest_db, "VARCHAR") if dest_db else "VARCHAR"
            candidate = src.strip() or tgt
            if candidate.lower() in taken and candidate.lower() == tgt.lower():
                # Keep source name when inventing beside an incompatible dest.
                base = candidate
                candidate = f"{base}_text" if f"{base}_text".lower() not in taken else f"src_{base}"
            elif candidate.lower() in taken:
                base = candidate
                candidate = f"{base}_text" if f"{base}_text".lower() not in taken else f"src_{base}"
            taken.add(candidate.lower())
            repaired = {
                **m,
                "target": candidate,
                "target_type": dest_native,
                "source_type": src_by.get(src, {}).get("inferred_type") or m.get("source_type") or "VARCHAR",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
                "transform": "none",
                "requires_review": True,
                "confidence": min(float(m.get("confidence") or 0.92), 0.92),
                "reasoning": (
                    f"{m.get('reasoning', '')} · samples are not numeric — "
                    f"CREATE/ADD '{candidate}' as {dest_native} instead of "
                    f"lossy {tgt} ({tgt_type or logical})"
                ).strip(" ·"),
            }
            out.append(repaired)
            continue
        out.append(m)
    return out


def entailment_prune(mappings: list[dict], target_columns: list[str]) -> tuple[list[dict], list[str]]:
    """Drop mappings whose target is not entailed by any known target column.

    Create-new / ADD COLUMN proposals are kept — they intentionally name a
    column that does not exist yet (e.g. ObjectId → `_id_text` beside DECIMAL `id`).
    """
    if not target_columns:
        return mappings, []
    kept: list[dict] = []
    pruned: list[str] = []
    for m in mappings:
        if _is_create_new_mapping(m):
            kept.append(m)
            continue
        tgt = m["target"]
        if any(_column_entailed(tgt, known) for known in target_columns):
            kept.append(m)
        else:
            pruned.append(m["source"])
    return kept, pruned


def validate_mappings(mappings: list[dict], *, confidence_threshold: float = 0.85) -> dict:
    """ValidationCriticAgent — flag low-confidence or duplicate targets."""
    issues: list[str] = []
    seen_targets: set[str] = set()
    for m in mappings:
        if m["confidence"] < confidence_threshold:
            issues.append(f"Low confidence: {m['source']} → {m['target']} ({m['confidence']:.0%})")
        if m.get("requires_review"):
            gap = float(m.get("score_gap", 0.0))
            issues.append(
                f"Ambiguous mapping: {m['source']} → {m['target']} "
                f"(winner gap {gap:.0%}; review required)"
            )
        tgt = m["target"].lower()
        if tgt in seen_targets:
            issues.append(f"Duplicate target column: {m['target']}")
        seen_targets.add(tgt)
    return {
        "passed": len(issues) == 0,
        "issues": issues,
        "agent": "ValidationCriticAgent",
        "requires_reflexion": any(("Low confidence" in i or "Ambiguous mapping" in i) for i in issues),
    }


def assert_mappings_executable(mappings: list[dict] | None) -> None:
    """Hard-block Execute when any mapping still requires operator review.

    ``user_override`` / ``approved`` clear the block after an explicit Studio
    confirmation. ``skip_preflight`` must never bypass this gate.

    Enterprise GA: boolean ``risk_acknowledged`` alone never unlocks a lossy
    write — lossy mappings must carry a verified continue-policy Risk Contract.

    Unsigned Map drafts are signed here (same hydrate Validate uses) so a green
    Validate cannot fail Execute solely because the FE still held drafts.
    When ``mappings`` is a list, it is updated in place with signed contracts.
    """
    from services.migration_risk_contract import (
        hydrate_mappings_risk_contracts,
        lossy_mappings_missing_risk_contracts,
    )

    hydrated = hydrate_mappings_risk_contracts(list(mappings or []))
    if isinstance(mappings, list):
        mappings[:] = hydrated
    work = hydrated

    pending: list[str] = []
    for m in work:
        if not m.get("requires_review"):
            continue
        if m.get("user_override") or m.get("approved") or m.get("operator_approved"):
            continue
        src = str(m.get("source") or "?")
        tgt = str(m.get("target") or "?")
        pending.append(f"{src} → {tgt}")
    if pending:
        sample = "; ".join(pending[:8])
        more = f" (+{len(pending) - 8} more)" if len(pending) > 8 else ""
        raise ValueError(
            f"{len(pending)} mapping(s) require review before Execute: {sample}{more}"
        )

    missing_contracts = lossy_mappings_missing_risk_contracts(work)
    if missing_contracts:
        sample = "; ".join(missing_contracts[:8])
        more = (
            f" (+{len(missing_contracts) - 8} more)"
            if len(missing_contracts) > 8
            else ""
        )
        raise ValueError(
            f"{len(missing_contracts)} lossy mapping(s) lack a verified Migration "
            f"Risk Contract (execution policy): {sample}{more}"
        )


def run_mapping_pipeline(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    file_format: str | None = None,
    confidence_threshold: float = 0.85,
    use_llm: bool = True,
    source_samples: dict[str, list[str]] | None = None,
    validation_mode: str = "strict",
    destination_db_type: str = "",
    source_db_type: str = "",
    schema_policy: str = "manual_review",
    sync_mode: str = "",
    destination_table_exists: bool | None = None,
    source_types_authoritative: bool = False,
    prior_mappings: list[dict] | None = None,
) -> dict:
    from services.semantic_analyzer import analyze_schema

    classification = classify_format(source_columns, file_format)
    # Capture what the schema actually declared before canonicalization collapses
    # VARCHAR(500) to VARCHAR. Without this the fidelity verdict cannot see a
    # 500→40 truncation, which is precisely the loss no transform announces.
    declared_source_types = {
        str(s.get("name") or ""): str(s.get("native_type") or s.get("inferred_type") or "")
        for s in (source_schemas or [])
    }
    declared_target_types = {
        str(s.get("name") or ""): str(s.get("native_type") or s.get("inferred_type") or "")
        for s in (target_schemas or [])
    }
    source_schemas = _canonicalize_schema_rows(source_schemas)
    target_schemas = _canonicalize_schema_rows(target_schemas)
    enrichments = enrich_columns(source_columns, source_schemas)

    if source_schemas is None and source_columns:
        source_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in source_columns]
    if target_schemas is None and target_columns:
        target_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in target_columns]

    # Carriers that actually exist in the destination. Identity targets derived
    # below for a create-new table are proposals, not live columns, and must
    # never grant bind-existing authority to the Decision Kernel.
    introspected_target_schemas: list[dict] | None = target_schemas

    if source_samples and source_columns:
        from services.data_profiler import merge_profiler_schema, profile_dataset
        from services.value_serializer import evidence_samples

        source_samples = {
            col: evidence_samples(vals) for col, vals in source_samples.items()
        }

        max_len = max((len(v) for v in source_samples.values()), default=0)
        profile_rows: list[dict] = []
        for i in range(min(max_len, 500)):
            profile_rows.append({
                col: (vals[i] if i < len(vals) else None)
                for col, vals in source_samples.items()
            })
        if profile_rows:
            profiled = profile_dataset(source_columns, profile_rows)
            merged_schema = merge_profiler_schema(
                {s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])},
                profiled.get("schema", {}),
                # A database source declares its own types; sampling must not
                # re-guess them and lose precision or the numeric class.
                authoritative_existing=source_types_authoritative,
            )
            # profile_dataset returns columns as name→profile dict — attach for
            # Map strip (null%/min/max/observed DECIMAL scale), not type invent only.
            col_profiles = profiled.get("columns") or {}
            # One source-type SSOT: when profiling upgrades an untyped text
            # carrier (CSV/Parquet strings) to a proven numeric/temporal type,
            # the fidelity verdict must judge that same type. Otherwise Map
            # invents NUMBER from the profile and then calls its own invent a
            # lossy VARCHAR→NUMBER cast, blocking every file→warehouse route.
            for name, upgraded in merged_schema.items():
                declared = declared_source_types.get(name, "")
                if not upgraded or not declared:
                    continue
                if normalize_logical_type(declared) in _UNTYPED_TEXT_LOGICALS and (
                    normalize_logical_type(upgraded)
                    != normalize_logical_type(declared)
                ):
                    declared_source_types[name] = str(upgraded)
            source_schemas = [
                {
                    **s,
                    "inferred_type": merged_schema.get(s["name"], s.get("inferred_type", "VARCHAR")),
                    "samples": source_samples.get(s["name"], s.get("samples", []))[:8],
                    "null_rate": (col_profiles.get(s["name"]) or {}).get("null_rate"),
                    "distinct_ratio": (col_profiles.get(s["name"]) or {}).get("distinct_ratio"),
                    "statistics": (col_profiles.get(s["name"]) or {}).get("statistics") or {},
                }
                for s in (source_schemas or [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in source_columns])
            ]

    semantic_analysis = analyze_schema(source_schemas or [])

    base_mappings = map_columns(
        source_columns,
        target_columns,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        destination_db_type=destination_db_type,
        destination_table_exists=destination_table_exists,
    )

    # If the destination schema is unknown AND confirmed create-new, derive
    # targets from identity mapping. Never invent targets when the table exists
    # (or existence is unknown) but columns failed to load.
    pending_schema = any(
        (m.get("assignment_strategy") == "pending_dest_schema") for m in (base_mappings or [])
    )
    if (
        not target_columns
        and not target_schemas
        and base_mappings
        and not pending_schema
        and destination_table_exists is False
    ):
        target_columns = [m["target"] for m in base_mappings]
        introspected_target_schemas = None
        target_schemas = [
            {
                "name": m["target"],
                "inferred_type": m.get("target_type", "VARCHAR"),
                "samples": [],
            }
            for m in base_mappings
        ]

    pruned, dropped = entailment_prune(base_mappings, target_columns)
    unmapped_after_prune = [s for s in source_columns if s not in {m["source"] for m in pruned}]

    from services.llm_mapping import refine_mappings_with_llm

    llm_samples = source_samples
    if not llm_samples and source_schemas:
        llm_samples = {
            s["name"]: [str(x) for x in (s.get("samples") or [])[:5]]
            for s in source_schemas
            if s.get("samples")
        }
    pruned, llm_meta = refine_mappings_with_llm(
        pruned,
        source_columns,
        target_columns,
        source_samples=llm_samples,
        enabled=use_llm,
    )

    # Module 13 — re-apply operator-locked priors before constraints / proof.
    override_report: dict = {}
    if prior_mappings:
        from services.mapping_engine_contract import merge_mappings_preserve_overrides

        pruned, override_report = merge_mappings_preserve_overrides(
            prior_mappings,
            pruned,
        )

    from services.mapping_constraints import (
        enforce_destination_constraints,
        mapping_plan_summary,
    )

    pruned, constraint_dropped, invented_blocked = enforce_destination_constraints(
        pruned,
        target_columns,
        confidence_floor=max(0.55, confidence_threshold - 0.35),
    )
    dropped = list(dict.fromkeys([*dropped, *constraint_dropped]))

    schema_by_name = {s["name"]: s for s in (source_schemas or [])}
    target_by_name = {s["name"]: s for s in (target_schemas or [])}

    enriched_mappings = []
    for m in pruned:
        enrichment = enrichments.get(m["source"], "")
        reasoning = m["reasoning"]
        if enrichment and enrichment not in reasoning.lower():
            reasoning = f"{reasoning} · enriched: {enrichment}"
        src_type = schema_by_name.get(m["source"], {}).get("inferred_type", "VARCHAR")
        src_type = ddl_carrier_type(str(src_type))
        tgt_type = target_by_name.get(m["target"], {}).get("inferred_type")
        # Provenance, not just a value: a stamp read out of the destination
        # catalog records what exists today, while an operator stamp records an
        # approved ceiling. Writers must be able to tell them apart — otherwise
        # today's narrow carrier freezes the column and every drifted row
        # quarantines under backfill instead of widening the destination.
        catalog_stamp = bool(tgt_type) and not (
            m.get("user_override") or m.get("userOverride")
        )
        strategy = str(m.get("assignment_strategy") or "")
        # Partial Studio: never invent dest types from source — Map/Validate must
        # stay pending until live schema loads (false-green preserve cliff).
        pending_dest = strategy == "pending_dest_schema"
        # Create-new / missing dest type: auto-widen unsigned widths.
        # Preserve DECIMAL(p,s) / VECTOR(n) / TIMESTAMPTZ carriers — never strip params.
        col_samples = [
            str(x) for x in (schema_by_name.get(m["source"], {}).get("samples") or [])[:8]
        ] or None

        if pending_dest:
            tgt_type = ""
        elif not tgt_type:
            src_l = str(src_type).lower()
            intentional_create = (
                strategy in {"identity_passthrough", "create_compatible_new"}
                or destination_table_exists is False
                or bool(m.get("create_new"))
            )
            if intentional_create and "unsigned" in src_l and (
                "bigint" in src_l or normalize_logical_type(src_type) == "decimal"
            ):
                # Still sample-observe when possible — bare DECIMAL → (38,15) cliff.
                tgt_type = create_new_mapping_target_type(
                    "DECIMAL",
                    destination_db_type or "",
                    samples=col_samples,
                    source_db=source_db_type,
                ) if destination_db_type or col_samples else "DECIMAL"
            elif intentional_create and "unsigned" in src_l:
                # INT/MEDIUMINT/SMALLINT UNSIGNED → BIGINT create-new (signed INT overflows).
                tgt_type = "BIGINT"
            elif destination_db_type and intentional_create:
                # Intentional create-new / ADD COLUMN — Decision Kernel invent.
                # Distinct from pending_dest_schema (Studio names-only refuse).
                tgt_type = create_new_mapping_target_type(
                    src_type,
                    destination_db_type,
                    samples=col_samples,
                    source_db=source_db_type,
                )
            elif destination_db_type and destination_table_exists is True:
                # Existing table, column missing from Studio, not create-new —
                # refuse invent (partial Studio honesty). Backfill + create_new
                # paths stamp above / via stamp_additive_mapping_types.
                tgt_type = ""
            elif destination_db_type and destination_table_exists is None:
                # Existence unproven — refuse invent. inventing create-new widths
                # before the destination catalog loads is a false-green cliff.
                tgt_type = ""
            elif destination_db_type:
                # Exhaustive: create-new / exists=False handled above; refuse.
                tgt_type = ""
            else:
                # No dest dialect — still stamp observed DECIMAL(p,s) for Map honesty.
                if col_samples and normalize_logical_type(src_type) in {"decimal", "float"}:
                    tgt_type = create_new_mapping_target_type(
                        src_type, "", samples=col_samples
                    )
                else:
                    tgt_type = src_type
        else:
            tgt_type = ddl_carrier_type(str(tgt_type))
            # Create-new already stamped bare DECIMAL/FLOAT — upgrade from samples.
            if (
                strategy in {"identity_passthrough", "create_compatible_new"}
                or destination_table_exists is False
            ) and col_samples:
                from services.type_system import parse_numeric_precision_scale

                bare = normalize_logical_type(tgt_type) in {"decimal", "float"}
                p, _ = parse_numeric_precision_scale(str(tgt_type))
                if bare and (p is None or normalize_logical_type(tgt_type) == "float"):
                    upgraded = create_new_mapping_target_type(
                        src_type or tgt_type,
                        destination_db_type or "",
                        samples=col_samples,
                        source_db=source_db_type,
                    )
                    if upgraded:
                        tgt_type = upgraded
        # LLM-invented transforms are held as suggested_transform until Map accept —
        # do not let deterministic infer silently re-apply the invent.
        if m.get("llm_invented_transform") and not m.get("user_override"):
            transform = m.get("transform") or "none"
        else:
            transform = infer_transform_for_mapping(
                m["source"],
                m["target"],
                src_type,
                tgt_type or src_type,
                source_samples=col_samples,
                destination_db_type=destination_db_type,
            )
            transform = _passthrough_identity_transform(
                transform,
                strategy=strategy,
                create_new=bool(m.get("create_new")),
                user_override=bool(m.get("user_override")),
                src_type=src_type,
                tgt_type=tgt_type or src_type,
            )
        # New/generic destinations: typed transforms must stamp *physical* DDL
        # for the destination (DATETIME(6)/CHAR(36)/JSONB) — never bare logical
        # tokens (DATETIME/UUID/JSON) that later false-block Validate.
        if (
            not pending_dest
            and tgt_type
            and normalize_logical_type(tgt_type) in {"string", "text", "varchar", "unknown"}
        ):
            typed_target = _TYPED_TRANSFORM_TARGET_TYPE.get(transform)
            if typed_target:
                if typed_target == "DECIMAL" and normalize_logical_type(src_type) == "decimal":
                    tgt_type = (
                        ddl_type(destination_db_type, src_type)
                        if destination_db_type
                        else src_type
                    )
                elif destination_db_type:
                    seed = (
                        src_type
                        if normalize_logical_type(src_type)
                        == normalize_logical_type(typed_target)
                        else typed_target
                    )
                    tgt_type = create_new_mapping_target_type(
                        seed, destination_db_type, source_db=source_db_type
                    )
                else:
                    tgt_type = typed_target

        enriched_mappings.append(
            {
                **m,
                "transform": transform,
                "source_type": src_type,
                "target_type": tgt_type or "",
                **(
                    {"target_type_origin": "destination_catalog"}
                    if catalog_stamp
                    else {}
                ),
                "reasoning": reasoning,
                "agent": "MappingReasonerAgent",
                "format_class": classification["format"],
                **(
                    {
                        "requires_review": True,
                        "fidelity": "cast",
                        "fidelity_reason": (
                            "Destination type pending Studio schema — refuse "
                            "source_type invent for Map/Validate."
                        ),
                    }
                    if pending_dest or (not tgt_type and strategy != "identity_passthrough")
                    else {}
                ),
            }
        )

    from services.sample_validator import refine_mappings_with_samples

    enriched_mappings = refine_mappings_with_samples(
        enriched_mappings,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
    )
    enriched_mappings = _repair_unparseable_numeric_targets(
        enriched_mappings,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        destination_db_type=destination_db_type,
    )

    from services.mapping_quality import (
        detect_cross_field_issues,
        refine_mappings_with_quality,
    )

    enriched_mappings = refine_mappings_with_quality(
        enriched_mappings,
        source_schemas=source_schemas,
        destination_db_type=destination_db_type or "",
    )
    enriched_mappings = _demote_untyped_varchar_confidence(
        enriched_mappings,
        source_schemas=source_schemas,
        source_types_authoritative=source_types_authoritative,
    )
    quality_issues = detect_cross_field_issues(enriched_mappings, source_schemas=source_schemas)

    sample_quality_report: dict = {}
    if source_samples and source_columns:
        from services.sample_quality import analyze_dataset_quality

        max_len = max((len(v) for v in source_samples.values()), default=0)
        quality_rows = [
            {
                col: (vals[i] if i < len(vals) else None)
                for col, vals in source_samples.items()
            }
            for i in range(min(max_len, 500))
        ]
        if quality_rows:
            sample_quality_report = analyze_dataset_quality(
                source_columns,
                quality_rows,
                schema={s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])},
            )
            for issue in sample_quality_report.get("issues", [])[:20]:
                if issue not in quality_issues:
                    quality_issues.append(issue)

    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    coercion_issues = validate_mapping_coercions(
        enriched_mappings,
        source_types={s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])},
        target_types={s["name"]: s.get("inferred_type", "VARCHAR") for s in (target_schemas or [])},
        schema_policy=schema_policy,
        validation_mode=validation_mode,
        dest_db_type=destination_db_type,
        dest_table_exists=destination_table_exists,
    )
    if coercion_issues:
        quality_issues = [*quality_issues, *[c["message"] for c in coercion_issues if c.get("severity") == "block"]]

    from services.transform_resolver import attach_transforms_to_mappings

    column_type_map = {s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])}
    dest_type_map = {s["name"]: s.get("inferred_type", "VARCHAR") for s in (target_schemas or [])}
    enriched_mappings = attach_transforms_to_mappings(
        enriched_mappings,
        column_types=column_type_map,
        dest_types=dest_type_map,
    )

    from services.mapping_proof import stamp_mapping_fidelity

    # Transforms are final here, so the verdict computed now is the one every
    # surface renders. Stamping it on the mapping keeps Map, column review, the
    # proof drawer, and the Pilot plan from each inventing their own risk chip.
    enriched_mappings = stamp_mapping_fidelity(
        enriched_mappings,
        source_types=declared_source_types,
        target_types=declared_target_types,
        destination_db_type=destination_db_type or "",
        dest_table_exists=destination_table_exists,
    )
    # Snapshot before the risk/Kernel stamps: both may replace a projected
    # carrier with the destination's physical DDL, which invalidates the verdict
    # just computed.
    pre_stamp_targets = [str(m.get("target_type") or "") for m in enriched_mappings]
    enriched_mappings = _stamp_create_new_type_risks(
        enriched_mappings,
        destination_db_type=destination_db_type or "",
        dest_table_exists=destination_table_exists,
    )
    # Additive create-new gaps: Kernel stamp before pending honesty pass.
    try:
        from services.decision_kernel import stamp_additive_mapping_types

        samples_by_src = {
            str(s.get("name") or ""): list(s.get("samples") or [])[:32]
            for s in (source_schemas or [])
            if s.get("name")
        }
        live_types = {
            str(s.get("name") or ""): str(s.get("inferred_type") or "")
            for s in (introspected_target_schemas or [])
            if s.get("name") and str(s.get("inferred_type") or "").strip()
        }
        enriched_mappings, _ = stamp_additive_mapping_types(
            enriched_mappings,
            dest_db=destination_db_type or "",
            live_dest_types=live_types,
            source_types=declared_source_types,
            samples_by_source=samples_by_src,
            backfill_new_fields=False,
            dest_table_exists=destination_table_exists,
        )
        # These stamps replace projected carriers with the destination's own
        # physical DDL (identity ``TIMESTAMPTZ`` → SQL Server ``DATETIMEOFFSET``).
        # A verdict stamped against the pre-stamp spelling compares a source
        # dialect token to a foreign dialect and reads offset-pinned → session
        # relative as a collapse, so the mapping kept ``lossy_cast`` and Execute
        # demanded a Risk Contract for a lossless write. Recompute on the final
        # types — the verdict must always describe the type path that will run.
        if [str(m.get("target_type") or "") for m in enriched_mappings] != (
            pre_stamp_targets
        ):
            enriched_mappings = stamp_mapping_fidelity(
                enriched_mappings,
                source_types=declared_source_types,
                target_types=declared_target_types,
                destination_db_type=destination_db_type or "",
                dest_table_exists=destination_table_exists,
            )
    except Exception as stamp_exc:
        # Fail-closed honesty: leave create-new target_type blank so Map/Validate
        # cannot invent preserve@0.99 after Kernel stamp failed.
        logger.warning(
            "additive Kernel stamp failed on Map: %s", stamp_exc, exc_info=stamp_exc
        )
        fixed: list[dict] = []
        for m in enriched_mappings:
            row = dict(m)
            strat = str(row.get("assignment_strategy") or "")
            if (
                bool(row.get("create_new"))
                or strat in {"create_compatible_new", "identity_passthrough"}
            ) and not str(row.get("target_type") or "").strip():
                row["requires_review"] = True
                row["confidence"] = min(float(row.get("confidence") or 0), 0.55)
            fixed.append(row)
        enriched_mappings = fixed
    # Reassert pending-Studio honesty after sample/quality boosts — never leave
    # invented preserve @ 0.99 when dest schema was never loaded.
    fixed_pending: list[dict] = []
    for m in enriched_mappings:
        row = dict(m)
        if str(row.get("assignment_strategy") or "") == "pending_dest_schema":
            row["target_type"] = ""
            row["create_new"] = False
            row["requires_review"] = True
            row["fidelity"] = "cast"
            row["fidelity_reason"] = (
                "Destination type pending Studio schema — refuse source_type invent."
            )
            try:
                row["confidence"] = min(float(row.get("confidence") or 0.55), 0.55)
            except (TypeError, ValueError):
                row["confidence"] = 0.55
            row.pop("mapping_class", None)
        fixed_pending.append(row)
    enriched_mappings = fixed_pending

    from services.structural_array import (
        array_strategy_gate_issues,
        parent_key_hint_from_schemas,
        stamp_mapping_array_strategies,
    )

    # Sample-aware Array<Primitive|Object> strategies — JSON default; normalize/hybrid
    # require explicit operator child_table_spec (never silent fan-out).
    enriched_mappings = stamp_mapping_array_strategies(
        enriched_mappings,
        source_samples=source_samples,
        dest_db=destination_db_type or "",
        parent_key_hint=parent_key_hint_from_schemas(
            source_schemas, source_columns
        ),
    )

    transforms = generate_transforms(
        enriched_mappings,
        schema_by_name=schema_by_name,
        target_by_name=target_by_name,
    )
    validation = validate_mappings(enriched_mappings, confidence_threshold=confidence_threshold)
    array_gate = array_strategy_gate_issues(enriched_mappings)
    if array_gate:
        quality_issues = [*quality_issues, *array_gate]
    if quality_issues or coerce_blocks_transfer(coercion_issues):
        validation = {
            **validation,
            "passed": False,
            "issues": [*validation.get("issues", []), *quality_issues],
            "requires_reflexion": True,
        }
    elif sample_quality_report.get("blocks_transfer"):
        validation = {
            **validation,
            "passed": False,
            "issues": [*validation.get("issues", []), *sample_quality_report.get("issues", [])[:10]],
            "requires_reflexion": True,
        }

    from services.data_integrity import run_integrity_audit

    integrity = run_integrity_audit(
        source_columns=source_columns,
        target_columns=target_columns,
        mappings=enriched_mappings,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        source_samples=source_samples,
        validation_mode=validation_mode,
        destination_db_type=destination_db_type,
        dest_table_exists=destination_table_exists,
    )
    if integrity.get("blocks_transfer"):
        validation = {
            **validation,
            "passed": False,
            "issues": [*validation.get("issues", []), *integrity.get("issues", [])[:15]],
            "requires_reflexion": True,
        }

    plan_summary = mapping_plan_summary(
        source_columns=source_columns,
        target_columns=target_columns,
        mappings=enriched_mappings,
        dropped_sources=dropped,
        invented_targets=invented_blocked,
    )
    from services.confidence_calibration import summarize_mapping_confidence

    plan_summary["confidence_calibration"] = summarize_mapping_confidence(enriched_mappings)
    if unmapped_after_prune:
        plan_summary["entailment_unmapped"] = unmapped_after_prune

    from services.mapping_proof import build_mapping_proof

    mapping_proof = build_mapping_proof(
        enriched_mappings,
        target_columns=target_columns,
        destination_db_type=destination_db_type,
        sync_mode=sync_mode,
        destination_table_exists=destination_table_exists,
    )
    plan_summary["dest_mode"] = mapping_proof.get("dest_mode")
    plan_summary["mapping_proof_summary"] = mapping_proof.get("summary")

    agents_used = [
        "FormatClassifierAgent",
        "ColumnEnrichmentAgent",
        "MappingReasonerAgent",
        "SampleValidatorAgent",
        "MappingQualityAgent",
        "SampleQualityAgent",
        "DataIntegrityAgent",
        "TransformCodegenAgent",
        "ValidationCriticAgent",
        "MappingProofAgent",
    ]
    if llm_meta.get("llm_used"):
        agents_used.insert(3, "LLMMappingAgent")
    if dropped:
        agents_used.insert(3, "EntailmentPruner")

    from services.semantic_mapper import ml_baseline_status

    from services.mapping_engine_contract import stamp_mappings_evidence

    enriched_mappings = stamp_mappings_evidence(enriched_mappings)

    engine = {
        "automapper": "bm25_hungarian_semantic",
        "ml_baseline": ml_baseline_status(),
        "llm": {
            "used": bool(llm_meta.get("llm_used")),
            "strategy": llm_meta.get("strategy") or "deterministic_only",
            "policy": llm_meta.get("llm_policy"),
            "provider": llm_meta.get("llm_provider"),
            "error": llm_meta.get("llm_error"),
        },
        "mapping_engine_contract": override_report or {
            "contract_version": "mapping_engine_contract.v1",
            "note": "Evidence stamped; no prior operator mappings supplied.",
        },
        "deterministic_guarantees": [
            "optimal_bipartite_hungarian",
            "type_compat_penalty",
            "sample_consistency_probe",
            "create_new_type_risk_stamp",
            "g1_g9_preflight",
            "operator_locked_mapping_preserve",
        ],
    }

    from services.shape_contract import classify_dest_exists_shape

    shape_contract = classify_dest_exists_shape(
        destination_table_exists=destination_table_exists,
        source_columns=list(source_columns or []),
        dest_columns=list(target_columns or []),
        mappings=list(enriched_mappings),
    )

    return {
        "mappings": enriched_mappings,
        "shape_contract": shape_contract,
        "transforms": transforms,
        "validation": validation,
        "classification": classification,
        "semantic_analysis": semantic_analysis,
        "pruned_sources": dropped,
        "plan_summary": plan_summary,
        "mapping_proof": mapping_proof,
        "quality_issues": quality_issues,
        "coercion_issues": coercion_issues,
        "sample_quality": sample_quality_report,
        "integrity": integrity,
        "agents_used": agents_used,
        "llm": llm_meta,
        "engine": engine,
        "mapping_override_report": override_report,
    }
