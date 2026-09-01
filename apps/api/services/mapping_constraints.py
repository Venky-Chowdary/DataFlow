"""Destination-aware mapping constraints — enforce known target columns."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


def _norm(name: str) -> str:
    """Normalize internal whitespace/dashes to a single underscore while preserving
    leading and trailing underscores so `id` and `_id` stay distinct."""
    return re.sub(r"[\s-]+", "_", name.strip().lower())


_OMIT_TRANSFORMS = frozenset({"omit", "intentional_omit", "drop", "exclude"})


def is_intentional_omit(mapping: Mapping[str, Any] | None) -> bool:
    """True when the operator explicitly excluded this source from the write map."""
    if not mapping:
        return False
    if mapping.get("intentional_omit") or mapping.get("intentionalOmit"):
        return True
    for key in ("transform", "engine_transform", "engineTransform"):
        raw = str(mapping.get(key) or "").strip().lower()
        if raw in _OMIT_TRANSFORMS:
            return True
    return False


def write_mappings(mappings: list[dict] | None) -> list[dict]:
    """Mappings that participate in DDL / row projection (excludes intentional omits)."""
    return [m for m in (mappings or []) if not is_intentional_omit(m)]


def known_target(name: str, target_columns: list[str]) -> bool:
    """True when name matches a declared destination column (case/underscore insensitive)."""
    if not target_columns:
        return True
    needle = _norm(name)
    return any(_norm(col) == needle for col in target_columns)


def _is_create_new_mapping(m: dict) -> bool:
    """True when the mapper intentionally proposes ADD COLUMN / create-new DDL."""
    if m.get("create_new"):
        return True
    strategy = str(m.get("assignment_strategy") or "")
    return strategy in {"create_compatible_new", "identity_passthrough"}


def enforce_destination_constraints(
    mappings: list[dict],
    target_columns: list[str],
    *,
    confidence_floor: float = 0.55,
) -> tuple[list[dict], list[str], list[str]]:
    """
    Keep mappings whose target exists in the destination schema.

    Create-new / ADD COLUMN proposals (ObjectId→new VARCHAR, etc.) are kept
    even when the target is not yet on the destination — blocking them emptied
    Transfer Studio Map after type-safe remaps.

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
        if _is_create_new_mapping(m):
            out = dict(m)
            out["create_new"] = True
            kept.append(out)
            continue
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
        if is_intentional_omit(m) or not m.get("target"):
            continue
        key = _norm(m["target"])
        if key in seen and seen[key] != m["source"]:
            dupes.append(m["target"])
        seen[key] = m["source"]
    return dupes


def unmapped_sources(source_columns: list[str], mappings: list[dict]) -> list[str]:
    """Sources with no write mapping and no intentional-omit policy.

    A row that names a source but carries no target and no omission policy
    used to count as accounted, so a mapping the pipeline had already dropped
    kept its source out of this list and the column left silently.
    """
    return classify_source_coverage(source_columns, mappings)["unaccounted"]


def classify_source_coverage(
    source_columns: list[str] | None,
    mappings: list[dict] | None,
) -> dict[str, Any]:
    """Account for every source column: written, declared omitted, or neither.

    A source column the operator never mentioned is not a decision — it is an
    unanswered question. Writing anyway drops it silently, which is the failure
    mode this product exists to prevent (30 source columns into a 20-column
    destination must not quietly become 20). Callers gate on
    ``unaccounted``; ``omitted`` is the operator's recorded decision and
    belongs in the decision artifact and proof bundle.
    """
    cols = [str(c) for c in (source_columns or []) if str(c).strip()]
    written: list[str] = []
    omitted: list[str] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        src = str(m.get("source") or "").strip()
        if not src:
            continue
        if is_intentional_omit(m):
            omitted.append(src)
        elif str(m.get("target") or "").strip():
            written.append(src)
    written_l = {_norm(s) for s in written}
    omitted_l = {_norm(s) for s in omitted}
    unaccounted = [c for c in cols if _norm(c) not in written_l and _norm(c) not in omitted_l]
    return {
        "source_count": len(cols),
        "written": written,
        "omitted": omitted,
        "unaccounted": unaccounted,
        "accounted": len(cols) - len(unaccounted),
        "complete": not unaccounted,
    }


def retain_dest_exists_write_mappings(
    mappings: list[dict] | None,
    dest_columns: list[str] | None,
) -> list[dict]:
    """Dest-exists overwrite writes dest columns only.

    Extra source columns stay unaccounted so G13 can block. Inventing a
    create-new dest column from source position is the dest-exists jumble
    (write by dest name — never source position).
    """
    dest = {_norm(c) for c in (dest_columns or []) if str(c).strip()}
    if not dest:
        return list(mappings or [])
    kept: list[dict] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        if is_intentional_omit(m):
            kept.append(m)
            continue
        tgt = str(m.get("target") or "").strip()
        if tgt and _norm(tgt) in dest:
            kept.append(m)
    return kept


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
    active = write_mappings(mappings)
    omitted = [m for m in mappings if is_intentional_omit(m)]
    dupes = detect_duplicate_targets(mappings)
    unmapped = unmapped_sources(source_columns, mappings)
    coverage = len(active) / max(len(source_columns), 1)
    return {
        "source_count": len(source_columns),
        "target_count": len(target_columns),
        "mapped_count": len(active),
        "omitted_count": len(omitted),
        "coverage_pct": round(coverage * 100, 1),
        "unmapped_sources": unmapped,
        "dropped_sources": dropped,
        "intentional_omits": [str(m.get("source") or "") for m in omitted if m.get("source")],
        "invented_targets_blocked": invented,
        "duplicate_targets": dupes,
        "requires_review_count": sum(1 for m in active if m.get("requires_review")),
        "low_confidence_count": sum(1 for m in active if float(m.get("confidence", 0)) < 0.75),
    }
