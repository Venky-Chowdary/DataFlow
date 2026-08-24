"""Cross-field mapping quality analysis — statistical and semantic consistency."""

from __future__ import annotations

import re
from typing import Any

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
PHONE_RE = re.compile(r"^\+?[0-9][0-9\s().-]{6,18}[0-9]$")
DATE_RE = re.compile(r"^(?:\d{4}-\d{2}-\d{2}|\d{8}|\d{4}/\d{2}/\d{2})(?:[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$")
BOOL_VALUES = {"true", "false", "yes", "no", "y", "n", "1", "0", "t", "f"}

# Create-new identity is "ready to CREATE", not proven against an existing dest.
IDENTITY_PASSTHROUGH_CONF_CAP = 0.93

# Evidence-class anchors — calibrated bands so scores do not cluster on one formula.
# Create-new stays capped; match-existing varies by evidence strength.
CONFIDENCE_CLASS_ANCHORS: dict[str, float] = {
    "exact_name_type": 0.99,
    "safe_type_promotion": 0.97,
    "structural_json": 0.91,
    "semantic_inference": 0.78,
    "custom_transform_sparse": 0.66,
    "create_new_projected": IDENTITY_PASSTHROUGH_CONF_CAP,
    "weak_or_conflicted": 0.55,
}

CONFIDENCE_CLASS_LABELS: dict[str, str] = {
    "exact_name_type": "Exact name + compatible type",
    "safe_type_promotion": "Deterministic type promotion",
    "structural_json": "Structured data → JSON/VARIANT",
    "semantic_inference": "Semantic name inference",
    "custom_transform_sparse": "Custom transform · sparse sample",
    "create_new_projected": "Projected CREATE · not dest-proven",
    "weak_or_conflicted": "Weak or conflicted evidence",
}

# Pairs are in the *logical* vocabulary ``_logical_type`` returns
# (``normalize_logical_type``): BIGINT/SMALLINT collapse to ``integer``,
# DOUBLE/REAL to ``float``, NUMBER to ``decimal``, and TIMESTAMP/DATETIME to
# ``datetime``. Physical spellings here are unreachable — ``("date",
# "timestamp")`` never matched, so every DATE → TIMESTAMP widening (the shape
# of a re-run against a destination DataFlow itself created) was classed
# "weak or conflicted" and held for review.
_SAFE_PROMOTIONS = frozenset({
    ("integer", "decimal"),
    ("integer", "float"),
    # float→decimal is IEEE→fixed-point invent — not a safe promotion (lossy).
    ("date", "datetime"),
    ("boolean", "integer"),
    ("string", "text"),
    ("text", "string"),
})

_IDENTITY_TRANSFORMS = frozenset({"", "none", "identity", "cast", "auto", "passthrough"})

# Typed parse guards quarantine unparseable cells; within one family they do not
# change a parseable value.
_PARSE_GUARD_FAMILIES: tuple[frozenset[str], ...] = (
    frozenset({"decimal", "numeric", "number", "integer", "int", "bigint", "float", "double"}),
    frozenset({"date", "datetime", "timestamp", "time"}),
    frozenset({"json", "array", "struct", "map"}),
    frozenset({"string", "text", "varchar"}),
)


def _is_passthrough_transform(transform: str, src_logical: str, tgt_logical: str) -> bool:
    """True when the transform does not change the value.

    ``generate_transforms`` labels every mapping with its logical type, so an
    untouched INTEGER→INTEGER column arrives carrying ``transform="integer"``.
    Read literally that looks like a custom transform, which pushed exact
    name+type matches into ``semantic_inference`` and stamped
    ``requires_review`` on them — making ``exact_name_type`` unreachable in
    practice and blocking G4 on transfers between *identical* schemas. A
    transform that merely names the logical type both sides already share is a
    passthrough cast, not a value change.
    """
    xf = (transform or "").strip().lower()
    if xf in _IDENTITY_TRANSFORMS:
        return True
    if bool(src_logical) and xf == src_logical == tgt_logical:
        return True
    # The guard is also a passthrough when it names the *family* both sides
    # share: DOUBLE→DOUBLE arrives labelled ``decimal`` because the numeric
    # parse guard is family-wide, and reading that as a custom transform
    # demoted identical-schema columns to semantic_inference (0.78) — below the
    # G4 floor, on a column whose fidelity verdict is ``preserve``.
    for family in _PARSE_GUARD_FAMILIES:
        if xf in family and src_logical in family and tgt_logical in family:
            return True
    return False


_STRUCTURAL_LOGICALS = frozenset({"json", "array", "struct", "map"})

_TEMPORAL_NAME_TERMS = frozenset({"date", "time", "dt", "timestamp", "created", "updated"})
_TEMPORAL_TYPE_TOKENS = (
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    "TIMESTAMPTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
)
_STRING_TYPE_TOKENS = (
    "VARCHAR",
    "CHAR",
    "TEXT",
    "STRING",
    "CLOB",
    "NVARCHAR",
    "NCHAR",
)


def _non_empty(samples: list[str]) -> list[str]:
    return [s.strip() for s in samples if s is not None and str(s).strip()]


def _null_rate(samples: list[str]) -> float:
    if not samples:
        return 0.0
    empty = sum(1 for s in samples if s is None or str(s).strip() == "")
    return empty / len(samples)


def _contains_term(name: str, terms: set[str] | frozenset[str]) -> bool:
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


def _is_temporal_type(type_str: str) -> bool:
    t = str(type_str or "").upper()
    return any(tok in t for tok in _TEMPORAL_TYPE_TOKENS)


def _is_string_type(type_str: str) -> bool:
    t = str(type_str or "").upper()
    if not t:
        return False
    return any(tok in t for tok in _STRING_TYPE_TOKENS)


def _target_is_temporal(target_name: str, target_type: str) -> bool:
    """True when dest type family is temporal OR name tokens clearly say so."""
    if _is_temporal_type(target_type):
        return True
    return _contains_term(target_name, _TEMPORAL_NAME_TERMS)


def analyze_column_profile(name: str, samples: list[str]) -> dict[str, Any]:
    """Infer column profile from sample values for mapping quality scoring."""
    vals = _non_empty([str(x) for x in samples[:24]])
    profile: dict[str, Any] = {
        "name": name,
        "sample_count": len(samples),
        "non_empty_count": len(vals),
        "null_rate": round(_null_rate(samples), 3),
        "unique_ratio": round(_unique_ratio(vals), 3),
        "samples": vals[:12],
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
        # Name alone is not enough for "status" — that is usually a string enum.
        profile["likely_boolean"] = bool_ratio >= 0.7 or (
            bool_ratio >= 0.5 and _contains_term(name, {"flag", "is_", "has_", "active", "enabled", "verified"})
        )

        # Sample-aware DECIMAL(p,s) / IEEE kind for Map profiling strip.
        if numeric_ratio >= 0.5 or profile["likely_numeric"]:
            try:
                from services.decimal_observe import observe_numeric_samples

                obs = observe_numeric_samples(vals)
                if obs.get("kind") not in {None, "empty"}:
                    profile["observed_precision"] = obs.get("precision")
                    profile["observed_scale"] = obs.get("scale")
                    profile["numeric_kind"] = obs.get("kind")
                    profile["suggested_carrier"] = obs.get("carrier")
                    profile["ieee_signals"] = obs.get("ieee_signals") or []
            except Exception:
                pass
            nums: list[float] = []
            for v in vals:
                try:
                    nums.append(float(v.replace(",", "").replace("$", "").replace("£", "").replace("€", "")))
                except ValueError:
                    continue
            if nums:
                profile["min"] = min(nums)
                profile["max"] = max(nums)

    return profile


def merge_column_profile(
    base: dict[str, Any],
    *,
    schema_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge analyzer profile with data_profiler statistics for Map strip SSOT."""
    out = dict(base or {})
    src = schema_row or {}
    if src.get("null_rate") is not None:
        out["null_rate"] = src.get("null_rate")
    if src.get("distinct_ratio") is not None:
        out["unique_ratio"] = src.get("distinct_ratio")
    stats = src.get("statistics") if isinstance(src.get("statistics"), dict) else {}
    for key in (
        "min",
        "max",
        "mean",
        "observed_precision",
        "observed_scale",
        "numeric_kind",
        "suggested_carrier",
        "ieee_signals",
    ):
        if stats.get(key) is not None and out.get(key) is None:
            out[key] = stats.get(key)
    return out


def column_profile_for_map(profile: dict[str, Any] | None) -> dict[str, Any]:
    """Compact column_profile stamped onto mappings for Map / Validate."""
    p = profile or {}
    keys = (
        "null_rate",
        "unique_ratio",
        "min",
        "max",
        "observed_precision",
        "observed_scale",
        "numeric_kind",
        "suggested_carrier",
        "ieee_signals",
        "likely_identifier",
        "likely_email",
        "likely_uuid",
        "likely_date",
        "likely_boolean",
        "semantic_pattern_score",
    )
    return {k: p[k] for k in keys if k in p and p[k] is not None}


def _logical_type(type_str: str) -> str:
    try:
        from services.type_system import normalize_logical_type

        return str(normalize_logical_type(type_str) or "string").lower()
    except Exception:
        t = str(type_str or "").upper()
        if any(x in t for x in ("INT", "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE")):
            return "integer" if "INT" in t or t == "NUMBER" else "decimal"
        if any(x in t for x in ("JSON", "JSONB", "VARIANT", "ARRAY")):
            return "json"
        if any(x in t for x in ("DATE", "TIME", "TIMESTAMP")):
            # Same vocabulary as ``normalize_logical_type`` above.
            return "datetime" if "TIME" in t else "date"
        if "BOOL" in t:
            return "boolean"
        return "string"


def _is_dialect_sanctioned_carrier(
    source_type: str, target_type: str, dest_db: str
) -> bool:
    """True when the destination column is exactly the carrier our DDL would create.

    A cross-family pair is normally weak evidence, but ``DECIMAL(12,2) → TEXT``
    on SQLite is the exact-digit carrier :func:`materialize_dest_ddl` itself
    picks — re-running a migration into a table DataFlow created must not score
    its own DDL as an incompatible-type conflict.
    """
    if not dest_db or not source_type or not target_type:
        return False
    from services.type_system import materialize_dest_ddl

    try:
        expected = str(materialize_dest_ddl(dest_db, source_type) or "").strip()
    except Exception:
        return False
    if not expected:
        return False
    return _logical_type(expected) == _logical_type(target_type)


def classify_mapping_confidence(
    mapping: dict,
    *,
    source_profile: dict[str, Any] | None = None,
    destination_db_type: str = "",
) -> dict[str, Any]:
    """Return evidence class + calibrated axes (not a single opaque %).

    Axes are independent signals so operators can see *why* the score is high
    or low — mapping fit, transform safety, semantic classification, sample DQ.
    """
    profile = source_profile or {}
    is_identity = bool(
        mapping.get("assignment_strategy") == "identity_passthrough" or mapping.get("create_new")
    )
    src = str(mapping.get("source") or "")
    tgt = str(mapping.get("target") or "")
    src_l = src.lower()
    tgt_l = tgt.lower()
    src_logical = _logical_type(
        str(mapping.get("source_type") or mapping.get("inferred_type") or "")
    )
    tgt_logical = _logical_type(
        str(mapping.get("target_type") or mapping.get("dest_type") or mapping.get("source_type") or "")
    )
    transform = str(mapping.get("transform") or "").strip().lower()
    has_custom_xf = not _is_passthrough_transform(transform, src_logical, tgt_logical)
    samples = [str(x) for x in (profile.get("samples") or []) if str(x).strip()]
    if not samples and mapping.get("samples"):
        samples = [str(x) for x in mapping["samples"] if str(x).strip()]
    sparse = len(samples) < 3
    name_exact = src_l == tgt_l or src_l.replace("_", "") == tgt_l.replace("_", "")
    type_same = src_logical == tgt_logical
    safe_promo = (src_logical, tgt_logical) in _SAFE_PROMOTIONS or (
        _is_dialect_sanctioned_carrier(
            str(mapping.get("source_type") or mapping.get("inferred_type") or ""),
            str(mapping.get("target_type") or mapping.get("dest_type") or ""),
            destination_db_type,
        )
    )
    structural = src_logical in _STRUCTURAL_LOGICALS and tgt_logical in _STRUCTURAL_LOGICALS
    pattern = float(profile.get("semantic_pattern_score") or 0.0)
    null_rate = float(profile.get("null_rate") or 0.0)

    if is_identity:
        cls = "create_new_projected"
    elif has_custom_xf and sparse:
        cls = "custom_transform_sparse"
    elif structural and not has_custom_xf:
        cls = "structural_json"
    elif name_exact and type_same and not has_custom_xf:
        cls = "exact_name_type"
    elif name_exact and (type_same or safe_promo) and not has_custom_xf:
        cls = "safe_type_promotion"
    elif name_exact and not type_same and not safe_promo and not structural:
        # Exact name but incompatible types — do not look like 99%.
        cls = "weak_or_conflicted"
    elif not name_exact and pattern < 0.5:
        cls = "semantic_inference"
    elif not name_exact:
        cls = "semantic_inference"
    else:
        cls = "safe_type_promotion" if safe_promo else "semantic_inference"

    # Wrong-role demotion: numeric-looking name on text-heavy country/currency fields, etc.
    if profile.get("likely_numeric") and any(
        t in tgt_l for t in ("country", "currency", "status", "state", "city", "email")
    ):
        if cls in {"exact_name_type", "safe_type_promotion"}:
            cls = "weak_or_conflicted"

    anchor = CONFIDENCE_CLASS_ANCHORS[cls]
    # Axes (0–1): independent of the single display percentage.
    mapping_axis = 0.99 if name_exact and (type_same or safe_promo) else (
        0.72 if name_exact else (0.55 if pattern >= 0.5 else 0.40)
    )
    transform_axis = (
        0.95 if structural and not has_custom_xf
        else 0.88 if not has_custom_xf
        else (0.55 if sparse else 0.72)
    )
    semantic_axis = min(1.0, max(0.15, pattern if pattern > 0 else (0.85 if name_exact else 0.45)))
    dq_axis = 0.5 if not samples else max(0.2, min(1.0, 1.0 - null_rate) * min(1.0, len(samples) / 8.0))

    return {
        "confidence_class": cls,
        "confidence_class_label": CONFIDENCE_CLASS_LABELS[cls],
        "type_path_class": str(mapping.get("conversion_class") or ""),
        "semantic_role": str(
            mapping.get("semantic_role")
            or profile.get("semantic_role")
            or ""
        ),
        "anchor": anchor,
        "axes": {
            "mapping": round(mapping_axis, 3),
            "transform_safety": round(transform_axis, 3),
            "semantic": round(semantic_axis, 3),
            "data_quality": round(dq_axis, 3),
        },
    }


def apply_confidence_class(
    confidence: float,
    classification: dict[str, Any],
) -> float:
    """Blend raw confidence toward the evidence-class anchor (calibrated bands)."""
    anchor = float(classification.get("anchor") or confidence)
    cls = str(classification.get("confidence_class") or "")
    conf = float(confidence)
    if cls == "create_new_projected":
        conv = str(classification.get("type_path_class") or "").lower()
        if conv in {"equivalent", "identity"}:
            role = str(classification.get("semantic_role") or "").lower()
            if role in {"categorical", "market_segment"}:
                return round(min(0.88, max(conf, 0.82)), 3)
            return round(min(IDENTITY_PASSTHROUGH_CONF_CAP, max(conf, 0.91)), 3)
        if conv in {"widening", "lossless"}:
            return round(min(0.90, max(conf, 0.86)), 3)
        if conv in {"representation", "normalization"}:
            return round(min(0.89, max(conf, 0.82)), 3)
        return round(min(conf, IDENTITY_PASSTHROUGH_CONF_CAP), 3)
    if cls == "exact_name_type":
        # Exact proven pairs should read near-certain, not 0.93.
        return round(min(0.99, max(conf, 0.97, anchor - 0.02)), 3)
    if cls == "safe_type_promotion":
        return round(min(0.98, max(min(conf, 0.97), 0.94)), 3)
    if cls == "structural_json":
        return round(min(0.94, max(min(conf, 0.93), 0.88)), 3)
    if cls == "semantic_inference":
        return round(min(0.82, max(0.55, min(conf, anchor + 0.04))), 3)
    if cls == "custom_transform_sparse":
        return round(min(0.72, max(0.50, min(conf, anchor + 0.04))), 3)
    if cls == "weak_or_conflicted":
        return round(min(0.70, max(0.40, min(conf, anchor + 0.08))), 3)
    return round(min(0.99, max(0.0, conf)), 3)


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
    is_identity = mapping.get("assignment_strategy") == "identity_passthrough" or mapping.get("create_new")
    tgt_type = str(mapping.get("target_type") or mapping.get("dest_type") or "").upper()
    src_type = str(mapping.get("source_type") or mapping.get("inferred_type") or "").upper()

    # Semantic role alignment boosts
    if profile.get("likely_email") and ("email" in tgt or "mail" in tgt):
        delta += 0.05
        notes.append("email pattern aligned")
    elif profile.get("likely_email") and "email" not in tgt and "mail" not in tgt:
        # VARCHAR/STRING is a valid physical home for email on Snowflake etc.
        # Frame as PII/semantic classification, not a type-mismatch defect.
        if _is_string_type(tgt_type):
            notes.append(
                "email-like values (PII) — choose Mask / hash / tokenize / preserve; "
                "VARCHAR is a valid physical type"
            )
        else:
            delta -= 0.08
            notes.append("email-like source mapped to non-string target")

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

    # Temporal gate is type-aware: TIMESTAMP→TIMESTAMP must not warn even when
    # the target name lacks date/time tokens (e.g. last_login TIMESTAMP).
    if profile.get("likely_date"):
        if _target_is_temporal(tgt, tgt_type):
            delta += 0.04
            notes.append("date-like source aligned to temporal target")
        else:
            delta -= 0.03
            notes.append("date-like source mapped to non-temporal target")

    if profile.get("likely_boolean") and any(k in tgt for k in ("flag", "is_", "active", "enabled", "verified")):
        delta += 0.03
        notes.append("boolean pattern aligned")

    # Name token overlap without full semantic match
    src_tokens = set(re.split(r"[_\s-]+", src_lower))
    tgt_tokens = set(re.split(r"[_\s-]+", tgt))
    overlap = src_tokens & tgt_tokens - {"", "id", "code", "num", "no", "number"}
    if overlap and float(mapping.get("confidence", 0)) < 0.8:
        delta += min(0.03 * len(overlap), 0.06)

    # Strong exact / normalized-name match boost — skip for create-new identity
    # passthrough so we do not inflate "will CREATE" into 0.99.
    if not is_identity:
        if src_lower == tgt:
            delta += 0.75
        elif src_lower in tgt or tgt in src_lower:
            delta += 0.45
        elif overlap and float(mapping.get("confidence", 0)) < 0.7:
            delta += 0.25

    # String enum → BOOLEAN is a common false positive (status=active/invalidated).
    # Apply AFTER name boost so exact-name matches cannot hide the type conflict.
    samples = [str(x).strip() for x in (profile.get("samples") or []) if str(x).strip()]
    if not samples and profile.get("sample_values"):
        samples = [str(x).strip() for x in profile["sample_values"] if str(x).strip()]
    distinct = {s.lower() for s in samples}
    looks_enum = len(distinct) > 2 or any(
        s not in {"true", "false", "t", "f", "yes", "no", "y", "n", "0", "1", "on", "off"}
        for s in distinct
    )
    if looks_enum and ("BOOL" in tgt_type or tgt_type == "BOOLEAN" or "bool" in tgt):
        delta -= 0.85
        notes.append(
            "string enum cannot map cleanly to BOOLEAN — use VARCHAR on new tables; "
            "for existing BOOLEAN columns remap or ALTER (mapping Widen is not DDL)"
        )
    elif looks_enum and "BOOL" in src_type:
        delta -= 0.5
        notes.append("source typed BOOLEAN but samples look like a string enum — widen to VARCHAR")

    return delta, notes


def refine_mappings_with_quality(
    mappings: list[dict],
    *,
    source_schemas: list[dict] | None = None,
    destination_db_type: str = "",
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
            profile_cache[src_name] = merge_column_profile(
                analyze_column_profile(src_name, samples),
                schema_row=src,
            )
        profile = profile_cache[src_name]

        delta, notes = score_mapping_pair(m, source_profile=profile)
        out = dict(m)
        if not out.get("semantic_role"):
            from services.semantic_analyzer import analyze_column

            schema_row = src_by_name.get(src_name, {})
            analyzed = analyze_column(
                src_name,
                str(out.get("source_type") or schema_row.get("inferred_type") or "VARCHAR"),
                [str(x) for x in (schema_row.get("samples") or [])],
            )
            out["semantic_role"] = analyzed.get("semantic_role")
        if not out.get("conversion_class") and out.get("source_type") and (
            out.get("target_type") or out.get("dest_type")
        ):
            from services.conversion_contract import classify_conversion

            out["conversion_class"] = classify_conversion(
                str(out.get("source_type") or out.get("inferred_type") or ""),
                str(out.get("target_type") or out.get("dest_type") or ""),
                dest_db=destination_db_type,
                transform=str(out.get("transform") or "none"),
            ).get("conversion_class")
        conf = min(0.99, max(0.0, float(m.get("confidence", 0.0)) + delta))
        classification = classify_mapping_confidence(
            out,
            source_profile=profile,
            destination_db_type=destination_db_type,
        )
        conf = apply_confidence_class(conf, classification)
        out["confidence"] = round(conf, 3)
        out["confidence_class"] = classification["confidence_class"]
        out["confidence_class_label"] = classification["confidence_class_label"]
        out["confidence_axes"] = classification["axes"]
        if notes:
            reason = m.get("reasoning", "")
            tag = f"quality: {', '.join(notes[:2])}"
            if tag.lower() not in reason.lower():
                out["reasoning"] = f"{reason} · {tag}".strip(" ·")
        if delta < -0.05 or classification["confidence_class"] in {
            "weak_or_conflicted",
            "custom_transform_sparse",
            "semantic_inference",
        }:
            if classification["confidence_class"] != "create_new_projected":
                out["requires_review"] = True
        if classification["confidence_class"] in {"weak_or_conflicted", "custom_transform_sparse"}:
            out["requires_review"] = True
        out["column_profile"] = column_profile_for_map(profile)
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

    for m in mappings:
        src_name = m.get("source") or ""
        if src_name not in profile_cache:
            src = src_by_name.get(src_name, {})
            profile_cache[src_name] = analyze_column_profile(
                src_name,
                [str(x) for x in (src.get("samples") or [])],
            )
        prof = profile_cache[src_name]
        samples = [str(x).strip().lower() for x in (prof.get("samples") or []) if str(x).strip()]
        tgt_type = str(m.get("target_type") or "").upper()
        if not samples:
            continue
        strict_bool = {"true", "false", "t", "f", "yes", "no", "y", "n", "0", "1", "on", "off"}
        if any(s not in strict_bool for s in samples) and (
            "BOOL" in tgt_type or "bool" in (m.get("target") or "").lower()
        ):
            issues.append(
                f"'{src_name}' looks like a string enum (e.g. {', '.join(sorted(set(samples))[:4])}) "
                f"but target '{m.get('target')}' is BOOLEAN — for a new table use VARCHAR; "
                f"for an existing table remap or widen the column."
            )

    return issues
