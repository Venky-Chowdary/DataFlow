"""Statistical column profiling — infer types and quality from sample values."""

from __future__ import annotations

import base64
import json
import math
import re
from collections import Counter
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any

from services.schema_inference import infer_type
from services.transform_engine import _parse_boolean, _parse_date, _parse_datetime, _parse_integer, _parse_uuid

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
        return json.dumps(value, default=str)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    return str(value).strip()


def _percentile(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def _numeric_stats(values: list[str]) -> dict[str, Any]:
    nums: list[float] = []
    for raw in values:
        try:
            nums.append(float(Decimal(raw.replace(",", "").replace("$", ""))))
        except (InvalidOperation, ValueError):
            continue
    if not nums:
        return {}
    sorted_nums = sorted(nums)
    n = len(nums)
    mean = sum(nums) / n
    variance = sum((x - mean) ** 2 for x in nums) / n
    return {
        "min": sorted_nums[0],
        "max": sorted_nums[-1],
        "mean": round(mean, 6),
        "stddev": round(math.sqrt(variance), 6),
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


def _histogram(values: list[float], buckets: int = 10) -> list[dict[str, Any]]:
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
    return [
        {"bucket": i, "low": round(lo + i * width, 4), "high": round(lo + (i + 1) * width, 4), "count": c}
        for i, c in enumerate(counts)
    ]


def _type_scores(values: list[str]) -> dict[str, float]:
    if not values:
        return {"VARCHAR": 1.0}
    n = len(values)
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
        try:
            Decimal(raw.replace(",", ""))
            scores["DECIMAL"] += 1
        except InvalidOperation:
            pass
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
    best_type = infer_type(non_empty, field_name=name)
    if best_type in scores:
        best_score = max(0.5, scores[best_type])
    else:
        best_score = max(0.75, 1.0 - null_rate)

    # Top-K value frequencies (cardinality analysis)
    freq = Counter(non_empty).most_common(10)
    top_values = [{"value": v[:60], "count": c, "pct": round(c / max(len(non_empty), 1), 4)} for v, c in freq]

    stats: dict[str, Any] = {}
    histogram: list[dict[str, Any]] = []
    if best_type in {"INTEGER", "DECIMAL", "NUMERIC", "FLOAT"}:
        stats = _numeric_stats(non_empty)
        if stats:
            nums = []
            for raw in non_empty:
                try:
                    nums.append(float(Decimal(raw.replace(",", "").replace("$", ""))))
                except (InvalidOperation, ValueError):
                    pass
            histogram = _histogram(nums)

    pattern = _infer_pattern(non_empty)
    pii = bool(re.search(r"email|phone|ssn|password|secret|name|address", name, re.I))
    if pii and best_type in {"VARCHAR", "TEXT"} and any(bool(EMAIL_RE.match(s)) for s in non_empty[:8]):
        pii = True

    # Uniqueness estimate — high distinct ratio on id-like columns
    is_likely_key = bool(distinct_ratio > 0.95 and distinct >= 5 and re.search(r"id|key|uuid|code", name, re.I))

    return {
        "name": name,
        "inferred_type": best_type,
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


def merge_profiler_schema(existing: dict[str, str], profiled: dict[str, str]) -> dict[str, str]:
    """Prefer statistical inference over naive typeof when confident."""
    merged = dict(existing)
    for col, inferred in profiled.items():
        merged[col] = inferred
    return merged
