"""Governance operations recorded on the migration certificate.

Mask, hash, and redact are one-way on purpose: the destination must not hold
the original cell. G16 already explains *dropped* columns; this module explains
columns that *did* land, but not as the source spelled them.

The certificate re-states what the write path was told to do. It never embeds
source PII. An empty list means no governance transform was **declared**, not
that the source held no PII.

Write-path ids that actually mutate cells: ``mask_pii``, ``hash_pii``,
``redact``. Studio/preflight aliases (``mask``, ``hash``, ``md5``, …) are
recorded when declared so an operator can see the intent, and are marked
``write_path_applied=false`` until they resolve to an engine id the writer
executes.
"""

from __future__ import annotations

from typing import Any

SCHEMA = "governance_operations_v1"

# Operator-facing families (handover language: mask / hash / redact).
OP_MASK = "mask"
OP_HASH = "hash"
OP_REDACT = "redact"

# Engine ids the write path executes (see transform_engine.apply_transform).
WRITE_PATH_IDS: frozenset[str] = frozenset({"mask_pii", "hash_pii", "redact"})

# Declared id (after UI alias resolution) → family.
_FAMILY: dict[str, str] = {
    "mask_pii": OP_MASK,
    "mask": OP_MASK,
    "pii_mask": OP_MASK,
    "hash_pii": OP_HASH,
    "hash": OP_HASH,
    "md5": OP_HASH,
    "sha256": OP_HASH,
    "redact": OP_REDACT,
    "anonymize": OP_REDACT,
    "encrypt": OP_REDACT,
}

EMPTY_NOTE = (
    "No governance transform (mask, hash, or redact) was declared on this "
    "run's mappings. That is not proof the source held no PII."
)

APPLIED_NOTE = (
    "These columns were deliberately mutated on the write path. The destination "
    "does not hold the original values. This certificate does not prove the "
    "source was deleted, or that an HMAC key is unavailable to someone who "
    "already has it."
)

MIXED_NOTE = (
    APPLIED_NOTE
    + " Rows marked write_path_applied=false used a declared alias the writer "
    "does not execute — those cells are refused or carried as identity unless "
    "the mapping uses mask_pii, hash_pii, or redact."
)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def resolve_declared_transform(raw: Any) -> str:
    """Engine id for a mapping transform, including Studio aliases.

    Uses the UI→engine table only — does not infer casts. A blank or identity
    transform is not a governance operation.
    """
    key = str(raw or "").strip().lower()
    if not key or key in {"none", "identity", "passthrough", "null"}:
        return ""
    try:
        from services.transform_resolver import UI_TO_ENGINE

        if key in UI_TO_ENGINE:
            return str(UI_TO_ENGINE[key]).strip().lower()
    except Exception:
        pass
    return key


def classify_governance_transform(raw: Any) -> tuple[str, str] | None:
    """Return ``(engine_id, family)`` when ``raw`` is a governance transform."""
    engine_id = resolve_declared_transform(raw)
    if not engine_id:
        return None
    family = _FAMILY.get(engine_id) or _FAMILY.get(str(raw or "").strip().lower())
    if not family:
        return None
    # Prefer the family key's canonical engine id when the alias itself is
    # classified (``hash`` stays ``hash``, not rewritten to ``hash_pii``).
    declared = engine_id if engine_id in _FAMILY else str(raw or "").strip().lower()
    return declared, family


def _mapping_rows(job: dict[str, Any]) -> list[dict[str, Any]]:
    """Every mapping list a job might still hold after redaction paths."""
    rows: list[dict[str, Any]] = []

    def _absorb(raw: Any) -> None:
        if isinstance(raw, list):
            rows.extend(m for m in raw if isinstance(m, dict))

    _absorb(job.get("mappings"))
    _absorb(_as_dict(job.get("transfer_request")).get("mappings"))
    _absorb(_as_dict(job.get("mapping_proof")).get("mappings"))
    pf = _as_dict(job.get("preflight"))
    _absorb(pf.get("mappings"))
    _absorb(_as_dict(pf.get("proof_bundle")).get("mappings"))
    return rows


def _entry(mapping: dict[str, Any]) -> dict[str, Any] | None:
    classified = classify_governance_transform(mapping.get("transform"))
    if classified is None:
        return None
    engine_id, family = classified
    source = str(mapping.get("source") or "").strip()
    target = str(mapping.get("target") or source).strip()
    if not source and not target:
        return None
    applied = engine_id in WRITE_PATH_IDS
    return {
        "source": source,
        "target": target,
        "transform": engine_id,
        "operation": family,
        "reversible": False,
        "write_path_applied": applied,
    }


def empty_governance_operations() -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "applied": [],
        "count": 0,
        "note": EMPTY_NOTE,
    }


def normalize_governance_operations(raw: Any) -> dict[str, Any]:
    """Coerce a stamp or harvest into the canonical certificate shape."""
    if not isinstance(raw, dict):
        return empty_governance_operations()
    applied: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in raw.get("applied") or []:
        if not isinstance(item, dict):
            continue
        classified = classify_governance_transform(item.get("transform") or item.get("operation"))
        if classified is None:
            # Stamp already classified — keep a well-shaped row even if the
            # transform id is only a family name.
            family = str(item.get("operation") or "").strip().lower()
            if family not in {OP_MASK, OP_HASH, OP_REDACT}:
                continue
            engine_id = str(item.get("transform") or family)
        else:
            engine_id, family = classified
        source = str(item.get("source") or "").strip()
        target = str(item.get("target") or source).strip()
        key = (source, target, engine_id)
        if key in seen:
            continue
        seen.add(key)
        applied.append(
            {
                "source": source,
                "target": target,
                "transform": engine_id,
                "operation": family,
                "reversible": False,
                "write_path_applied": bool(
                    item["write_path_applied"]
                    if "write_path_applied" in item
                    else engine_id in WRITE_PATH_IDS
                ),
            }
        )
    applied.sort(key=lambda row: (row["source"], row["target"], row["transform"]))
    any_unapplied = any(not row["write_path_applied"] for row in applied)
    if not applied:
        note = str(raw.get("note") or EMPTY_NOTE)
    elif any_unapplied:
        note = MIXED_NOTE
    else:
        note = APPLIED_NOTE
    return {
        "schema": SCHEMA,
        "applied": applied,
        "count": len(applied),
        "note": note,
    }


def harvest_governance_operations(job: dict[str, Any] | None) -> dict[str, Any]:
    """Derive the ledger from mappings still attached to the job or request."""
    if not isinstance(job, dict):
        return empty_governance_operations()
    applied: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for mapping in _mapping_rows(job):
        entry = _entry(mapping)
        if entry is None:
            continue
        key = (entry["source"], entry["target"], entry["transform"])
        if key in seen:
            continue
        seen.add(key)
        applied.append(entry)
    return normalize_governance_operations({"applied": applied})


def collect_governance_operations(job: dict[str, Any] | None) -> dict[str, Any]:
    """Prefer the execute stamp; harvest from mappings when it is absent.

    Mappings can be stripped from ``transfer_request`` redaction paths (same
    reason accepted risk contracts are stamped at execute). A stamp with an
    ``applied`` list is authoritative even when empty.
    """
    if not isinstance(job, dict):
        return empty_governance_operations()
    stamped = job.get("governance_operations")
    if isinstance(stamped, dict) and "applied" in stamped:
        return normalize_governance_operations(stamped)
    return harvest_governance_operations(job)
