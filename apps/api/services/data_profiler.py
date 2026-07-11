"""Statistical column profiling — infer types and quality from sample values."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from services.transform_engine import _parse_boolean, _parse_date, _parse_datetime, _parse_integer, _parse_uuid

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
    """Profile one column from raw cell values."""
    strings = [_as_str(v) for v in values[:sample_limit]]
    non_empty = [s for s in strings if s]
    null_rate = 1.0 - (len(non_empty) / max(len(strings), 1))
    distinct = len(set(non_empty))
    distinct_ratio = distinct / max(len(non_empty), 1)

    scores = _type_scores(non_empty)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_type, best_score = ranked[0]
    if best_score < 0.5:
        best_type = "VARCHAR"
        best_score = max(0.5, 1.0 - null_rate)

    pii = bool(re.search(r"email|phone|ssn|password|secret|name", name, re.I))
    if pii and best_type == "VARCHAR" and any(EMAIL_RE.match(s) for s in non_empty[:8]):
        pii = True

    return {
        "name": name,
        "inferred_type": best_type,
        "confidence": round(min(0.99, best_score), 3),
        "null_rate": round(null_rate, 3),
        "distinct_ratio": round(distinct_ratio, 3),
        "sample_count": len(strings),
        "non_empty_count": len(non_empty),
        "likely_pii": pii,
        "type_scores": scores,
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
    for col in columns:
        col_values = [row.get(col) for row in sample]
        prof = profile_column(col, col_values)
        profiles[col] = prof
        schema[col] = prof["inferred_type"]

    quality_score = 0.0
    if profiles:
        quality_score = sum(p["confidence"] for p in profiles.values()) / len(profiles) * 100

    return {
        "schema": schema,
        "columns": profiles,
        "quality_score": round(quality_score, 1),
        "row_sample_size": len(sample),
    }


def merge_profiler_schema(existing: dict[str, str], profiled: dict[str, str]) -> dict[str, str]:
    """Prefer statistical inference over naive Python typeof when confident."""
    merged = dict(existing)
    for col, inferred in profiled.items():
        merged[col] = inferred
    return merged
