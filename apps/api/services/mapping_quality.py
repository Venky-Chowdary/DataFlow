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
DATE_RE = re.compile(r"^(?:\d{4}-\d{2}-\d{2}|\d{8}|\d{4}/\d{2}/\d{2})(?:[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$")
BOOL_VALUES = {"true", "false", "yes", "no", "y", "n", "1", "0", "t", "f"}


def _non_empty(samples: list[str]) -> list[str]:
    return [s.strip() for s in samples if s is not None and str(s).strip()]


def _null_rate(samples: list[str]) -> float:
    if not samples:
        return 0.0
    empty = sum(1 for s in samples if s is None or str(s).strip() == "")
    return empty / len(samples)


def _contains_term(name: str, terms: set[str]) -> bool:
    lowered = name.lower()
    return any(term in lowered for term in terms)


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
        "likely_date": False,
        "likely_boolean": False,
        "semantic_pattern_score": 0.0,
    }
    if len(vals) >= 2:
        email_ratio = _pattern_rate(vals, EMAIL_RE)
        phone_ratio = _pattern_rate(vals, PHONE_RE)
        uuid_ratio = _pattern_rate(vals, UUID_RE)
        date_ratio = _pattern_rate(vals, DATE_RE)
        bool_hits = sum(1 for v in vals if v.lower() in BOOL_VALUES)
        numeric = 0
        for v in vals:
            try:
                float(v.replace(",", ""))
                numeric += 1
            except ValueError:
                pass

        numeric_ratio = numeric / len(vals)
        bool_ratio = bool_hits / len(vals)
        best_ratio = max(email_ratio, phone_ratio, uuid_ratio, date_ratio, numeric_ratio, bool_ratio)
        profile["semantic_pattern_score"] = round(best_ratio, 3)

        profile["likely_identifier"] = profile["unique_ratio"] >= 0.95 or uuid_ratio >= 0.5
        profile["likely_email"] = email_ratio >= 0.5 or _contains_term(name, {"email", "mail"})
        profile["likely_phone"] = phone_ratio >= 0.5 or _contains_term(name, {"phone", "mobile", "tel"})
        profile["likely_uuid"] = uuid_ratio >= 0.5 or _contains_term(name, {"uuid", "guid", "identifier"})
        profile["likely_numeric"] = numeric_ratio >= 0.75 or _contains_term(name, {"amount", "qty", "total", "balance", "price"})
        profile["likely_date"] = date_ratio >= 0.5 or _contains_term(name, {"date", "time", "dt", "timestamp", "created", "updated"})
        profile["likely_boolean"] = bool_ratio >= 0.7 or _contains_term(name, {"flag", "is_", "has_", "active", "status"})

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

    if profile.get("likely_date") and any(k in tgt for k in ("date", "time", "dt", "timestamp", "created", "updated")):
        delta += 0.04
        notes.append("date-like source aligned to temporal target")
    elif profile.get("likely_date") and not any(k in tgt for k in ("date", "time", "dt", "timestamp", "created", "updated")):
        delta -= 0.03
        notes.append("date-like source mapped to non-temporal target")

    if profile.get("likely_boolean") and any(k in tgt for k in ("flag", "is_", "active", "status")):
        delta += 0.03
        notes.append("boolean pattern aligned")

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
    profile_cache: dict[str, dict[str, Any]] = {}
    refined: list[dict] = []
    for m in mappings:
        src_name = m["source"]
        if src_name not in profile_cache:
            src = src_by_name.get(src_name, {})
            samples = [str(x) for x in (src.get("samples") or [])]
            profile_cache[src_name] = analyze_column_profile(src_name, samples)
        profile = profile_cache[src_name]

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
            for k in (
                "null_rate",
                "unique_ratio",
                "likely_identifier",
                "likely_email",
                "likely_uuid",
                "likely_date",
                "likely_boolean",
                "semantic_pattern_score",
            )
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
    profile_cache: dict[str, dict[str, Any]] = {}
    by_target: dict[str, list[dict]] = {}
    for m in mappings:
        by_target.setdefault(m["target"].lower(), []).append(m)

    for tgt, group in by_target.items():
        if len(group) < 2:
            continue
        id_like = []
        for m in group:
            src_name = m["source"]
            if src_name not in profile_cache:
                src = src_by_name.get(src_name, {})
                profile_cache[src_name] = analyze_column_profile(
                    src_name,
                    [str(x) for x in (src.get("samples") or [])],
                )
            prof = profile_cache[src_name]
            if prof.get("likely_identifier"):
                id_like.append(src_name)
        if len(id_like) >= 2:
            issues.append(
                f"Multiple identifier-like sources mapped to '{tgt}': {', '.join(id_like)}"
            )

    emails = [m for m in mappings if "email" in m["target"].lower()]
    if len(emails) > 1:
        issues.append(f"Multiple sources mapped to email fields: {', '.join(m['source'] for m in emails)}")

    return issues
