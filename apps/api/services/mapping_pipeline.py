"""Multi-agent mapping pipeline — plan Part 0A agent architecture."""

from __future__ import annotations

import re

from services.semantic_mapper import map_columns
from services.transform_engine import infer_transform, infer_transform_for_mapping

CONFIDENCE_FLOOR = 0.72


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
    return re.sub(r"[_\s-]+", "_", name.lower()).strip("_")


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


def entailment_prune(mappings: list[dict], target_columns: list[str]) -> tuple[list[dict], list[str]]:
    """Drop mappings whose target is not entailed by any known target column."""
    if not target_columns:
        return mappings, []
    kept: list[dict] = []
    pruned: list[str] = []
    for m in mappings:
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
    schema_policy: str = "manual_review",
) -> dict:
    from services.semantic_analyzer import analyze_schema

    classification = classify_format(source_columns, file_format)
    enrichments = enrich_columns(source_columns, source_schemas)

    if source_schemas is None and source_columns:
        source_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in source_columns]
    if target_schemas is None and target_columns:
        target_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in target_columns]

    if source_samples and source_columns:
        from services.data_profiler import merge_profiler_schema, profile_dataset

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
            )
            source_schemas = [
                {
                    **s,
                    "inferred_type": merged_schema.get(s["name"], s.get("inferred_type", "VARCHAR")),
                    "samples": source_samples.get(s["name"], s.get("samples", []))[:8],
                }
                for s in (source_schemas or [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in source_columns])
            ]

    semantic_analysis = analyze_schema(source_schemas or [])

    base_mappings = map_columns(
        source_columns,
        target_columns,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
    )

    # If the destination schema is unknown, derive it from the identity mapping.
    # This lets the type-coercion and transform resolvers produce correct target
    # types and DDL when the user has not created a destination table yet.
    if not target_columns and not target_schemas and base_mappings:
        target_columns = [m["target"] for m in base_mappings]
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

    from services.mapping_constraints import enforce_destination_constraints, mapping_plan_summary

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
        tgt_type = target_by_name.get(m["target"], {}).get("inferred_type")
        enriched_mappings.append(
            {
                **m,
                "transform": infer_transform_for_mapping(m["source"], m["target"], src_type, tgt_type),
                "source_type": src_type,
                "target_type": tgt_type,
                "reasoning": reasoning,
                "agent": "MappingReasonerAgent",
                "format_class": classification["format"],
            }
        )

    from services.sample_validator import refine_mappings_with_samples

    enriched_mappings = refine_mappings_with_samples(
        enriched_mappings,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
    )

    from services.mapping_quality import detect_cross_field_issues, refine_mappings_with_quality

    enriched_mappings = refine_mappings_with_quality(
        enriched_mappings,
        source_schemas=source_schemas,
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

    from services.type_coercion_validator import coerce_blocks_transfer, validate_mapping_coercions

    coercion_issues = validate_mapping_coercions(
        enriched_mappings,
        source_types={s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])},
        target_types={s["name"]: s.get("inferred_type", "VARCHAR") for s in (target_schemas or [])},
        schema_policy=schema_policy,
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

    transforms = generate_transforms(
        enriched_mappings,
        schema_by_name=schema_by_name,
        target_by_name=target_by_name,
    )
    validation = validate_mappings(enriched_mappings, confidence_threshold=confidence_threshold)
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
    ]
    if llm_meta.get("llm_used"):
        agents_used.insert(3, "LLMMappingAgent")
    if dropped:
        agents_used.insert(3, "EntailmentPruner")

    return {
        "mappings": enriched_mappings,
        "transforms": transforms,
        "validation": validation,
        "classification": classification,
        "semantic_analysis": semantic_analysis,
        "pruned_sources": dropped,
        "plan_summary": plan_summary,
        "quality_issues": quality_issues,
        "coercion_issues": coercion_issues,
        "sample_quality": sample_quality_report,
        "integrity": integrity,
        "agents_used": agents_used,
        "llm": llm_meta,
    }
