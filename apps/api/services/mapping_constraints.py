"""Destination-aware mapping constraints — enforce known target columns."""

from __future__ import annotations

import re
from typing import Any


def _norm(name: str) -> str:
    """Normalize internal whitespace/dashes to a single underscore while preserving
    leading and trailing underscores so `id` and `_id` stay distinct."""
    return re.sub(r"[\s-]+", "_", name.strip().lower())


def known_target(name: str, target_columns: list[str]) -> bool:
    """True when name matches a declared destination column (case/underscore insensitive)."""
    if not target_columns:
        return True
    needle = _norm(name)
    return any(_norm(col) == needle for col in target_columns)


def enforce_destination_constraints(
    mappings: list[dict],
    target_columns: list[str],
    *,
    confidence_floor: float = 0.55,
) -> tuple[list[dict], list[str], list[str]]:
    """
    Keep only mappings whose target exists in the destination schema.

    Returns (kept_mappings, dropped_sources, invented_targets).
    """
    if not target_columns:
        return mappings, [], []

    kept: list[dict] = []
    dropped: list[str] = []
    invented: list[str] = []

    for m in mappings:
        src = m["source"]
        tgt = m["target"]
        if not known_target(tgt, target_columns):
            invented.append(src)
            dropped.append(src)
            continue
        conf = float(m.get("confidence", 0.0))
        if conf < confidence_floor:
            dropped.append(src)
            continue
        out = dict(m)
        # Resolve canonical target spelling from the destination schema.
        # Prefer an exact (case-insensitive) match so columns like `id` and
        # `_id` do not collapse to the first normalized hit.
        exact = next((c for c in target_columns if c.lower() == tgt.lower()), None)
        canon = exact or next((c for c in target_columns if _norm(c) == _norm(tgt)), tgt)
        out["target"] = canon
        kept.append(out)

    return kept, dropped, invented


def detect_duplicate_targets(mappings: list[dict]) -> list[str]:
    seen: dict[str, str] = {}
    dupes: list[str] = []
    for m in mappings:
        key = _norm(m["target"])
        if key in seen and seen[key] != m["source"]:
            dupes.append(m["target"])
        seen[key] = m["source"]
    return dupes


def unmapped_sources(source_columns: list[str], mappings: list[dict]) -> list[str]:
    mapped = {m["source"] for m in mappings}
    return [s for s in source_columns if s not in mapped]


def mapping_plan_summary(
    *,
    source_columns: list[str],
    target_columns: list[str],
    mappings: list[dict],
    dropped_sources: list[str] | None = None,
    invented_targets: list[str] | None = None,
) -> dict[str, Any]:
    dropped = dropped_sources or []
    invented = invented_targets or []
    dupes = detect_duplicate_targets(mappings)
    unmapped = unmapped_sources(source_columns, mappings)
    coverage = len(mappings) / max(len(source_columns), 1)
    return {
        "source_count": len(source_columns),
        "target_count": len(target_columns),
        "mapped_count": len(mappings),
        "coverage_pct": round(coverage * 100, 1),
        "unmapped_sources": unmapped,
        "dropped_sources": dropped,
        "invented_targets_blocked": invented,
        "duplicate_targets": dupes,
        "requires_review_count": sum(1 for m in mappings if m.get("requires_review")),
        "low_confidence_count": sum(1 for m in mappings if float(m.get("confidence", 0)) < 0.75),
    }
