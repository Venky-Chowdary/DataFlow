"""Sample-driven mapping validation — boost or penalize confidence from parse rates."""

from __future__ import annotations

from services.transform_engine import apply_transform, infer_transform_for_mapping


def _parse_rate(samples: list[str], transform: str) -> tuple[float, list[str]]:
    if not samples:
        return 1.0, []
    ok = 0
    evaluated = 0
    issues: list[str] = []
    nullable_transforms = {"trim", "trim_id", "none", "identity", "upper", "lower", "hash_pii"}
    for raw in samples[:12]:
        if raw is None or str(raw).strip() == "":
            if transform in nullable_transforms:
                ok += 1
            evaluated += 1
            continue
        evaluated += 1
        _, err = apply_transform(str(raw), transform)
        if err:
            if len(issues) < 3:
                issues.append(err)
        else:
            ok += 1
    if evaluated == 0:
        return 1.0, []
    return ok / evaluated, issues


def refine_mapping_confidence(
    mapping: dict,
    *,
    samples: list[str] | None = None,
    source_type: str = "VARCHAR",
    target_type: str | None = None,
) -> dict:
    """Adjust confidence using sample parse success for inferred transform."""
    out = dict(mapping)
    samples = samples or []
    transform = mapping.get("transform") or infer_transform_for_mapping(
        mapping["source"],
        mapping["target"],
        source_type,
        target_type,
        source_samples=samples,
    )
    rate, issues = _parse_rate(samples, transform)
    conf = float(mapping.get("confidence", 0.0))

    if rate >= 0.95 and len(samples) >= 2:
        is_identity = mapping.get("assignment_strategy") == "identity_passthrough" or mapping.get("create_new")
        # Existing-dest sample proof may reach 0.99; create-new stays capped.
        conf = min(0.93 if is_identity else 0.99, conf + 0.04)
        reason = mapping.get("reasoning", "")
        if "sample-validated" not in reason.lower():
            n = len(samples)
            out["reasoning"] = (
                f"{reason} · sample-validated ({int(rate * 100)}%, n={n})"
            ).strip(" ·")
    elif rate < 0.5 and len(samples) >= 2:
        conf = max(0.55, conf - 0.12)
        out["requires_review"] = True
        out["reasoning"] = (
            f"{mapping.get('reasoning', '')} · sample parse failures ({len(issues)})"
        ).strip(" ·")

    out["confidence"] = round(conf, 3)
    out["sample_parse_rate"] = round(rate, 3)
    out["transform"] = transform
    if issues:
        out["sample_issues"] = issues[:3]
    return out


def refine_mappings_with_samples(
    mappings: list[dict],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
) -> list[dict]:
    src_by_name = {s["name"]: s for s in (source_schemas or [])}
    tgt_by_name = {s["name"]: s for s in (target_schemas or [])}
    refined: list[dict] = []
    for m in mappings:
        src = src_by_name.get(m["source"], {})
        tgt = tgt_by_name.get(m["target"], {})
        refined.append(
            refine_mapping_confidence(
                m,
                samples=list(src.get("samples") or []),
                source_type=src.get("inferred_type", "VARCHAR"),
                target_type=tgt.get("inferred_type"),
            )
        )
    return refined
