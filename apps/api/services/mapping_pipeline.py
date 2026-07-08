"""Multi-agent mapping pipeline — plan Part 0A agent architecture."""

from __future__ import annotations

from services.semantic_mapper import map_columns
from services.transform_engine import infer_transform

CONFIDENCE_FLOOR = 0.72


def classify_format(source_columns: list[str], file_format: str | None = None) -> dict:
    from services.semantic_analyzer import analyze_column

    semantic_hits = 0
    for col in source_columns[:12]:
        analyzed = analyze_column(col, "VARCHAR", [])
        if analyzed.get("semantic_type") not in ("unknown", "text", ""):
            semantic_hits += 1

    hints = [c.upper() for c in source_columns[:8]]
    payment_tokens = {"AMT", "CUST_ID", "TXN_DT", "ACCT_NO", "CCY", "REF_NO", "PAY_AMT"}
    overlap = len(set(hints) & payment_tokens)

    if overlap >= 2:
        fmt = "payment_feed"
        confidence = min(0.95, 0.75 + overlap * 0.05)
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


def generate_transforms(mappings: list[dict]) -> list[dict]:
    """TransformCodegenAgent — emit SQL-safe transform hints per mapping."""
    transforms: list[dict] = []
    for m in mappings:
        src = m["source"].upper()
        tgt = m["target"].lower()
        expr = f"source.{m['source']}"
        if "amount" in tgt or src in {"AMT", "PAY_AMT"}:
            expr = f"CAST(NULLIF(TRIM(source.{m['source']}), '') AS NUMERIC(18,4))"
        elif "date" in tgt or "dt" in src.lower():
            expr = f"TRY_TO_DATE(source.{m['source']})"
        elif "id" in tgt:
            expr = f"TRIM(CAST(source.{m['source']} AS VARCHAR))"
        transforms.append(
            {
                "source": m["source"],
                "target": m["target"],
                "expression": expr,
                "agent": "TransformCodegenAgent",
            }
        )
    return transforms


def entailment_prune(mappings: list[dict], target_columns: list[str]) -> tuple[list[dict], list[str]]:
    """Drop mappings whose target is not entailed by any known target column."""
    if not target_columns:
        return mappings, []
    targets_lower = {t.lower() for t in target_columns}
    kept: list[dict] = []
    pruned: list[str] = []
    for m in mappings:
        tgt = m["target"].lower()
        if tgt in targets_lower or any(tgt in t or t in tgt for t in targets_lower):
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
) -> dict:
    from services.semantic_analyzer import analyze_schema

    classification = classify_format(source_columns, file_format)
    enrichments = enrich_columns(source_columns, source_schemas)

    if source_schemas is None and source_columns:
        source_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in source_columns]
    if target_schemas is None and target_columns:
        target_schemas = [{"name": c, "inferred_type": "VARCHAR", "samples": []} for c in target_columns]

    semantic_analysis = analyze_schema(source_schemas or [])

    base_mappings = map_columns(
        source_columns,
        target_columns,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
    )
    pruned, dropped = entailment_prune(base_mappings, target_columns)
    if not pruned:
        pruned = base_mappings

    enriched_mappings = []
    for m in pruned:
        enrichment = enrichments.get(m["source"], "")
        reasoning = m["reasoning"]
        if enrichment and enrichment not in reasoning.lower():
            reasoning = f"{reasoning} · enriched: {enrichment}"
        enriched_mappings.append(
            {
                **m,
                "transform": infer_transform(m["source"], m["target"], "VARCHAR"),
                "reasoning": reasoning,
                "agent": "MappingReasonerAgent",
                "format_class": classification["format"],
            }
        )

    transforms = generate_transforms(enriched_mappings)
    validation = validate_mappings(enriched_mappings, confidence_threshold=confidence_threshold)

    agents_used = [
        "FormatClassifierAgent",
        "ColumnEnrichmentAgent",
        "MappingReasonerAgent",
        "TransformCodegenAgent",
        "ValidationCriticAgent",
    ]
    if dropped:
        agents_used.insert(3, "EntailmentPruner")

    return {
        "mappings": enriched_mappings,
        "transforms": transforms,
        "validation": validation,
        "classification": classification,
        "semantic_analysis": semantic_analysis,
        "pruned_sources": dropped,
        "agents_used": agents_used,
    }
