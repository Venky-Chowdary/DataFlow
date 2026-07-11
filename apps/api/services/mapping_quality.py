"""Cross-field mapping quality analysis — statistical and semantic consistency."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
PHONE_RE = re.compile(r"^\+?[0-9][0-9\s().-]{6,18}[0-9]$")


def _non_empty(samples: list[str]) -> list[str]:
    return [s.strip() for s in samples if s is not None and str(s).strip()]


def _null_rate(samples: list[str]) -> float:
    if not samples:
        return 0.0
    empty = sum(1 for s in samples if s is None or str(s).strip() == "")
    return empty / len(samples)


def _unique_ratio(values: list[str]) -> float:
    if not values:
        return 0.0
    return len(set(values)) / len(values)


def _pattern_rate(values: list[str], pattern: re.Pattern[str]) -> float:
    if not values:
        return 0.0
    return sum(1 for v in values if pattern.match(v)) / len(values)


def analyze_column_profile(name: str, samples: list[str]) -> dict[str, Any]:
    """Infer column profile from sample values for mapping quality scoring."""
    vals = _non_empty([str(x) for x in samples[:24]])
    profile: dict[str, Any] = {
        "name": name,
        "sample_count": len(samples),
        "non_empty_count": len(vals),
        "null_rate": round(_null_rate(samples), 3),
        "unique_ratio": round(_unique_ratio(vals), 3),
        "likely_identifier": False,
        "likely_email": False,
        "likely_phone": False,
        "likely_uuid": False,
        "likely_numeric": False,
    }
    if len(vals) >= 2:
        profile["likely_identifier"] = profile["unique_ratio"] >= 0.95
    if len(vals) >= 2:
        profile["likely_email"] = _pattern_rate(vals, EMAIL_RE) >= 0.8
        profile["likely_phone"] = _pattern_rate(vals, PHONE_RE) >= 0.7
        profile["likely_uuid"] = _pattern_rate(vals, UUID_RE) >= 0.8
        numeric = 0
        for v in vals:
            try:
                float(v.replace(",", ""))
                numeric += 1
            except ValueError:
                pass
        profile["likely_numeric"] = numeric / len(vals) >= 0.85
    return profile


def score_mapping_pair(
    mapping: dict,
    *,
    source_profile: dict[str, Any] | None = None,
    target_name: str = "",
) -> tuple[float, list[str]]:
    """
    Adjust mapping confidence using cross-field heuristics.
    Returns (delta, notes).
    """
    delta = 0.0
    notes: list[str] = []
    src = mapping.get("source", "")
    tgt = (target_name or mapping.get("target", "")).lower()
    src_lower = src.lower()
    profile = source_profile or {}

    # Semantic role alignment boosts
    if profile.get("likely_email") and ("email" in tgt or "mail" in tgt):
        delta += 0.05
        notes.append("email pattern aligned")
    elif profile.get("likely_email") and "email" not in tgt:
        delta -= 0.08
        notes.append("email-like source mapped to non-email target")

    if profile.get("likely_uuid") and ("uuid" in tgt or tgt.endswith("_id") or tgt == "id"):
        delta += 0.04
        notes.append("uuid pattern aligned")
    elif profile.get("likely_uuid") and not (tgt.endswith("_id") or tgt == "id"):
        delta -= 0.06
        notes.append("uuid-like values on non-id target")

    if profile.get("likely_identifier") and (tgt.endswith("_id") or tgt == "id" or "key" in tgt):
        delta += 0.04
        notes.append("high-cardinality identifier aligned")

    if profile.get("likely_numeric") and any(k in tgt for k in ("amount", "qty", "count", "total", "price")):
        delta += 0.03
        notes.append("numeric samples on numeric target")

    if profile.get("likely_phone") and ("phone" in tgt or "mobile" in tgt or "tel" in tgt):
        delta += 0.04
        notes.append("phone pattern aligned")

    # Name token overlap without full semantic match
    src_tokens = set(re.split(r"[_\s-]+", src_lower))
    tgt_tokens = set(re.split(r"[_\s-]+", tgt))
    overlap = src_tokens & tgt_tokens - {"", "id", "code", "num", "no", "number"}
    if overlap and float(mapping.get("confidence", 0)) < 0.8:
        delta += min(0.03 * len(overlap), 0.06)

    return delta, notes


def refine_mappings_with_quality(
    mappings: list[dict],
    *,
    source_schemas: list[dict] | None = None,
) -> list[dict]:
    """Apply cross-field quality scoring to each mapping."""
    src_by_name = {s["name"]: s for s in (source_schemas or [])}
    refined: list[dict] = []
    for m in mappings:
        src = src_by_name.get(m["source"], {})
        samples = [str(x) for x in (src.get("samples") or [])]
        profile = analyze_column_profile(m["source"], samples)
        delta, notes = score_mapping_pair(m, source_profile=profile)
        out = dict(m)
        conf = min(0.99, max(0.0, float(m.get("confidence", 0.0)) + delta))
        out["confidence"] = round(conf, 3)
        if notes:
            reason = m.get("reasoning", "")
            tag = f"quality: {', '.join(notes[:2])}"
            if tag.lower() not in reason.lower():
                out["reasoning"] = f"{reason} · {tag}".strip(" ·")
        if delta < -0.05:
            out["requires_review"] = True
        out["column_profile"] = {
            k: profile[k]
            for k in ("null_rate", "unique_ratio", "likely_identifier", "likely_email", "likely_uuid")
        }
        refined.append(out)
    return refined


def detect_cross_field_issues(
    mappings: list[dict],
    source_schemas: list[dict] | None = None,
) -> list[str]:
    """Flag inconsistent mapping sets (e.g. two sources → same id target with different profiles)."""
    issues: list[str] = []
    src_by_name = {s["name"]: s for s in (source_schemas or [])}
    by_target: dict[str, list[dict]] = {}
    for m in mappings:
        by_target.setdefault(m["target"].lower(), []).append(m)

    for tgt, group in by_target.items():
        if len(group) < 2:
            continue
        id_like = []
        for m in group:
            prof = analyze_column_profile(
                m["source"],
                [str(x) for x in (src_by_name.get(m["source"], {}).get("samples") or [])],
            )
            if prof.get("likely_identifier"):
                id_like.append(m["source"])
        if len(id_like) >= 2:
            issues.append(
                f"Multiple identifier-like sources mapped to '{tgt}': {', '.join(id_like)}"
            )

    emails = [m for m in mappings if "email" in m["target"].lower()]
    if len(emails) > 1:
        issues.append(f"Multiple sources mapped to email fields: {', '.join(m['source'] for m in emails)}")

    return issues
