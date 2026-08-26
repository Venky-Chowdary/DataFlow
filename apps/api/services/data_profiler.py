"""Statistical column profiling — infer types and quality from sample values."""

from __future__ import annotations

import base64
import json
import re
from collections import Counter
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from services.value_serializer import json_default
from services.transform_engine import (
    _parse_boolean,
    _parse_date,
    _parse_datetime,
    _parse_decimal,
    _parse_integer,
    _parse_uuid,
    decimal_wire_value,
)

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{12}$"
)
PHONE_RE = re.compile(r"^\+?[\d\s().-]{7,20}$")
DATE_PATTERN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}|^\d{2}/\d{2}/\d{4}|^\d{8}$")


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return json.dumps(value, default=json_default)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    return str(value).strip()


def _percentile(sorted_vals: list[Decimal], p: float) -> Decimal | None:
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    k = Decimal(n - 1) * Decimal(str(p))
    floor = int(k)
    ceil = floor if k == floor else min(floor + 1, n - 1)
    if floor == ceil:
        return sorted_vals[floor]
    frac = k - Decimal(floor)
    return sorted_vals[floor] * (Decimal(1) - frac) + sorted_vals[ceil] * frac


def _numeric_values(values: list[str]) -> list[Decimal]:
    out: list[Decimal] = []
    for raw in values:
        parsed = decimal_wire_value(raw)
        if parsed is None:
            continue
        out.append(parsed)
    return out


def _numeric_stats(values: list[str]) -> dict[str, Any]:
    nums = _numeric_values(values)
    if not nums:
        return {}
    sorted_nums = sorted(nums)
    n = len(nums)
    mean = sum(nums, Decimal(0)) / Decimal(n)
    variance = sum((x - mean) ** 2 for x in nums) / Decimal(n)
    stddev = variance.sqrt() if variance >= 0 else Decimal(0)
    quantum = Decimal("0.000001")
    return {
        "min": sorted_nums[0],
        "max": sorted_nums[-1],
        "mean": mean.quantize(quantum) if mean.is_finite() else mean,
        "stddev": stddev.quantize(quantum) if stddev.is_finite() else stddev,
        "p25": _percentile(sorted_nums, 0.25),
        "p50": _percentile(sorted_nums, 0.50),
        "p75": _percentile(sorted_nums, 0.75),
        "p95": _percentile(sorted_nums, 0.95),
        "numeric_parse_rate": round(n / max(len(values), 1), 4),
    }


def _infer_pattern(values: list[str]) -> str | None:
    """Detect dominant value pattern (Great Expectations style)."""
    if not values:
        return None
    sample = values[:100]
    scores = {
        "email": sum(1 for v in sample if EMAIL_RE.match(v)),
        "uuid": sum(1 for v in sample if UUID_RE.match(v)),
        "phone": sum(1 for v in sample if PHONE_RE.match(v)),
        "date": sum(1 for v in sample if DATE_PATTERN_RE.match(v) or _parse_date(v)),
        "integer": sum(1 for v in sample if _parse_integer(v) is not None),
        "boolean": sum(1 for v in sample if _parse_boolean(v) is not None),
    }
    best = max(scores.items(), key=lambda x: x[1])
    if best[1] / len(sample) >= 0.7:
        return best[0]
    return None


def _histogram(values: list[Decimal], buckets: int = 10) -> list[dict[str, Any]]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    if lo == hi:
        return [{"bucket": 0, "low": lo, "high": hi, "count": len(values)}]
    width = (hi - lo) / buckets
    counts = [0] * buckets
    for v in values:
        idx = min(buckets - 1, int((v - lo) / width))
        counts[idx] += 1
    quantum = Decimal("0.0001")
    return [
        {
            "bucket": i,
            "low": (lo + i * width).quantize(quantum),
            "high": (lo + (i + 1) * width).quantize(quantum),
            "count": c,
        }
        for i, c in enumerate(counts)
    ]


def _type_scores(values: list[str]) -> dict[str, float]:
    if not values:
        return {"VARCHAR": 1.0}
    scores = {
        "BOOLEAN": 0.0,
        "INTEGER": 0.0,
        "DECIMAL": 0.0,
        "DATE": 0.0,
        "TIMESTAMP": 0.0,
        "UUID": 0.0,
        "JSON": 0.0,
        "VARCHAR": 0.0,
    }
    for raw in values:
        if not raw:
            continue
        if _parse_boolean(raw) is not None:
            scores["BOOLEAN"] += 1
        if _parse_integer(raw) is not None:
            scores["INTEGER"] += 1
        if _parse_decimal(raw) is not None:
            scores["DECIMAL"] += 1
        if _parse_date(raw):
            scores["DATE"] += 1
        if _parse_datetime(raw):
            scores["TIMESTAMP"] += 1
        if _parse_uuid(raw):
            scores["UUID"] += 1
        if raw.startswith("{") or raw.startswith("["):
            scores["JSON"] += 0.5
        if EMAIL_RE.match(raw):
            scores["VARCHAR"] += 0.3
    non_empty = sum(1 for v in values if v) or 1
    return {k: round(v / non_empty, 3) for k, v in scores.items()}


def profile_column(name: str, values: list[Any], *, sample_limit: int = 200) -> dict[str, Any]:
    """Profile one column: type inference, statistics, patterns, quality signals."""
    strings = [_as_str(v) for v in values[:sample_limit]]
    non_empty = [s for s in strings if s]
    null_rate = 1.0 - (len(non_empty) / max(len(strings), 1))
    distinct = len(set(non_empty))
    distinct_ratio = distinct / max(len(non_empty), 1)

    scores = _type_scores(non_empty)
    from services.schema_inference import infer_column

    intel = infer_column(non_empty, field_name=name)
    best_type = str(intel["logical_type"])
    if best_type in scores:
        best_score = max(0.5, scores[best_type])
    else:
        best_score = max(0.75, float(intel.get("confidence") or 0.75), 1.0 - null_rate)

    # Top-K value frequencies (cardinality analysis)
    freq = Counter(non_empty).most_common(10)
    top_values = [{"value": v[:60], "count": c, "pct": round(c / max(len(non_empty), 1), 4)} for v, c in freq]

    stats: dict[str, Any] = {}
    histogram: list[dict[str, Any]] = []
    from services.type_system import normalize_logical_type as _nlt_prof

    numeric_logical = _nlt_prof(best_type) in {"integer", "decimal", "float"}
    if numeric_logical or best_type in {"INTEGER", "DECIMAL", "NUMERIC", "FLOAT"}:
        stats = _numeric_stats(non_empty)
        if stats:
            histogram = _histogram(_numeric_values(non_empty))
        # Sample-aware DECIMAL(p,s) / IEEE kind for Map profiling strip.
        from services.decimal_observe import observe_source_numeric_samples

        obs = observe_source_numeric_samples(non_empty)
        if obs.get("kind") not in {None, "empty"}:
            stats = {
                **stats,
                "observed_precision": obs.get("precision"),
                "observed_scale": obs.get("scale"),
                "numeric_kind": obs.get("kind"),
                "ieee_signals": obs.get("ieee_signals") or [],
                "suggested_carrier": obs.get("carrier"),
            }

    pattern = _infer_pattern(non_empty)
    pii = bool(re.search(r"email|phone|ssn|password|secret|name|address", name, re.I))
    if pii and best_type in {"VARCHAR", "TEXT"} and any(bool(EMAIL_RE.match(s)) for s in non_empty[:8]):
        pii = True

    # Uniqueness estimate — high distinct ratio on id-like columns
    is_likely_key = bool(distinct_ratio > 0.95 and distinct >= 5 and re.search(r"id|key|uuid|code", name, re.I))

    return {
        "name": name,
        "inferred_type": best_type,
        "semantic_role": intel.get("semantic_role"),
        "confidence": round(min(0.99, best_score), 3),
        "null_rate": round(null_rate, 3),
        "distinct_count": distinct,
        "distinct_ratio": round(distinct_ratio, 3),
        "likely_primary_key": is_likely_key,
        "sample_count": len(strings),
        "non_empty_count": len(non_empty),
        "likely_pii": pii,
        "detected_pattern": pattern,
        "type_scores": scores,
        "statistics": stats,
        "histogram": histogram,
        "top_values": top_values,
        "notes": intel.get("notes") or [],
    }


def profile_dataset(
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    sample_limit: int = 500,
) -> dict[str, Any]:
    """Profile all columns; returns schema map + per-column stats."""
    sample = rows[:sample_limit]
    profiles: dict[str, dict[str, Any]] = {}
    schema: dict[str, str] = {}
    primary_key_candidates: list[str] = []

    for col in columns:
        col_values = [row.get(col) for row in sample]
        prof = profile_column(col, col_values)
        profiles[col] = prof
        schema[col] = prof["inferred_type"]
        if prof.get("likely_primary_key"):
            primary_key_candidates.append(col)

    quality_score = 0.0
    if profiles:
        quality_score = sum(p["confidence"] for p in profiles.values()) / len(profiles) * 100

    return {
        "schema": schema,
        "columns": profiles,
        "quality_score": round(quality_score, 1),
        "row_sample_size": len(sample),
        "primary_key_candidates": primary_key_candidates,
    }


# Types a naive header scan emits when it does not really know. Only these may
# be replaced by statistical inference.
_WEAK_DECLARED_TYPES = frozenset({
    "", "VARCHAR", "TEXT", "STRING", "CHAR", "UNKNOWN", "OBJECT", "ANY",
})


def _is_weak_declared(declared: str) -> bool:
    """True when the declared type is a placeholder rather than real evidence."""
    t = (declared or "").strip().upper()
    if "(" in t:
        # Parameterised types (DECIMAL(12,2), VARCHAR(50)) carry real precision.
        return False
    return t in _WEAK_DECLARED_TYPES


def source_types_are_authoritative(source_kind: str, source_format: str = "") -> bool:
    """True when the source declared its own types and inference must defer.

    A relational or warehouse source hands us real DDL. A CSV hands us a header
    row, and a document store hands us types that were themselves inferred from
    sampled documents — in both of those cases inference is the better evidence,
    not the worse one. Callers used to decide this ad hoc, which is why the same
    Postgres table produced ``DECIMAL(12,2)`` through one route and a re-inferred
    bare ``DECIMAL`` through another.

    ``source_format`` is overloaded across the codebase: ``csv``/``parquet`` for
    uploads, ``postgresql``/``mongodb`` for connectors. Both are resolved through
    the capability registry's ``requires_schema`` flag rather than a second
    hand-maintained list.
    """
    kind = (source_kind or "").strip().lower()
    if not kind or kind in {"file", "file_export", "upload", "object_store"}:
        return False
    fmt = (source_format or "").strip().lower()
    if not fmt:
        return True
    try:
        from services.connector_capability_registry import is_schemaless

        return not is_schemaless(fmt)
    except Exception:
        # Unknown engine: prefer inference over a type we cannot vouch for.
        return False


# Carriers a schemaless/text source declares for *every* column (CSV header,
# Parquet string, thin SaaS describe). Profiling off one of these is evidence;
# profiling *onto* one is a loss of evidence.
UNTYPED_TEXT_LOGICALS = frozenset({"string", "text", "varchar", "unknown"})


def _inference_would_demote_to_text(declared: str, inferred: str) -> bool:
    """True when profiling stringified samples would erase a typed declaration.

    Profiling reads ``cell_to_string`` output, so a Mongo ``ObjectId`` arrives as
    ``"6991173f8d64fcf16f3a0805"`` and a ``Decimal128`` as ``"12.34"``. Those look
    like text, but the declaration came from the BSON type itself — strictly
    stronger evidence than the string that was rendered from it. Letting the
    profile win typed ``OBJECTID`` down to ``VARCHAR`` made the engine invent a
    ``TEXT`` destination column and then report its own invent as an
    ``OBJECTID → TEXT`` fidelity collapse, blocking every Mongo→SQL create-new
    route. Upgrades off an untyped carrier (``VARCHAR`` → ``DECIMAL``) and
    same-family widening (``INTEGER`` → ``BIGINT``) are unaffected.
    """
    if not declared or not inferred:
        return False
    from services.type_system import normalize_logical_type

    try:
        declared_logical = normalize_logical_type(declared)
        inferred_logical = normalize_logical_type(inferred)
    except Exception:
        return False
    if declared_logical == inferred_logical:
        return False
    return (
        declared_logical not in UNTYPED_TEXT_LOGICALS
        and inferred_logical in UNTYPED_TEXT_LOGICALS
    )


def merge_profiler_schema(
    existing: dict[str, str],
    profiled: dict[str, str],
    *,
    authoritative_existing: bool = False,
) -> dict[str, str]:
    """Combine declared column types with statistical inference from samples.

    Statistical inference exists for sources that declare nothing useful — a
    CSV header scan calls every column VARCHAR. It must not outrank a type the
    source actually declared. Overwriting unconditionally meant an introspected
    ``DECIMAL(12,2)`` was re-inferred as bare ``DECIMAL`` and then created as
    ``DECIMAL(38,15)`` downstream, a ``DOUBLE PRECISION`` became ``DECIMAL``
    (different rounding semantics), and ``JSONB`` degraded to ``VARCHAR``.

    Precedence:

    * ``authoritative_existing`` (database introspection): the declared type
      always wins; inference only fills columns that declared nothing.
    * Otherwise (files): inference wins, except that a parameterised declared
      type keeps its precision when inference agrees on the same logical family
      but drops the parameters, and except that inference may never demote a
      typed declaration to text (see :func:`_inference_would_demote_to_text`).
    """
    from services.type_system import normalize_logical_type

    merged = dict(existing)
    for col, inferred in profiled.items():
        declared = str(existing.get(col) or "").strip()
        if not inferred:
            continue
        if authoritative_existing:
            # The source declared this column. Even a bare TEXT is a fact here
            # (Postgres TEXT is unbounded), not the placeholder a header scan
            # would emit, so inference only fills genuine gaps.
            if declared:
                continue
            merged[col] = inferred
            continue
        if _inference_would_demote_to_text(declared, str(inferred)):
            continue
        if "(" in declared and "(" not in str(inferred):
            try:
                same_family = normalize_logical_type(declared) == normalize_logical_type(inferred)
            except Exception:
                same_family = False
            if same_family:
                # Keep DECIMAL(12,2) rather than widening to a bare DECIMAL.
                continue
        merged[col] = inferred
    return merged
