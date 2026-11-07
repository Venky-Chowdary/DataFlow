"""Execute creates the table Validate sized — one source of truth for widths.

Validate re-projects every create-new carrier from the type it judges the
source against and only ever widens it (``decision_kernel.invent``), then
proves the population fits *that* carrier. Execute used to keep the mapping
Map stamped from its 8-row automap and carried back only the widens the
population scan itself proposed, so a column Validate had already widened
(``NUMERIC(2,1)`` -> ``NUMERIC(3,1)``) reached the CREATE at the Map width and
the load quarantined the rows Validate had just proved fit (QA round 11,
``clean.parquet``: 491 of 500 rows).

:func:`adopt_validate_create_widths` applies both: the population scan's proven
widens and Validate's stamped create-new widths. Only widenings are adopted —
never a narrower or different-family type, never over an operator's choice.
"""

from __future__ import annotations

from typing import Any, Mapping

_TYPE_KEYS = ("target_type", "dest_type", "destination_type", "destType")


def _is_widening(have: str, want: str, dest_db: str) -> bool:
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not want or want.strip().upper() == (have or "").strip().upper():
        return False
    if not have:
        return False
    if normalize_logical_type(have) != normalize_logical_type(want):
        return False
    try:
        return not is_lossy_coercion(have, want, dest_db=dest_db) and is_lossy_coercion(
            want, have, dest_db=dest_db
        )
    except Exception:  # noqa: BLE001 - an undecidable pair is not adopted
        return False


def adopt_validate_create_widths(
    mappings: list[dict[str, Any]],
    preflight: Mapping[str, Any] | None,
    *,
    dest_db: str = "",
) -> list[dict[str, str]]:
    """Stamp Validate's create-new widths onto the mappings Execute writes with."""
    from services.population_fit_scan import stamp_create_widens

    pf = preflight or {}
    applied = stamp_create_widens(mappings, pf.get("population_fit"))
    widened = {str(item.get("column") or "") for item in applied}
    stamped = {
        str(row.get("source") or ""): row
        for row in pf.get("stamped_mappings") or []
        if isinstance(row, Mapping) and row.get("create_new")
    }
    for mapping in mappings:
        source = str(mapping.get("source") or "")
        if source in widened or source not in stamped:
            continue
        if mapping.get("user_override") or mapping.get("userOverride") or mapping.get("risk_acknowledged"):
            continue
        have = str(mapping.get("target_type") or mapping.get("dest_type") or "")
        want = str(stamped[source].get("target_type") or "")
        if not _is_widening(have, want, dest_db):
            continue
        for key in _TYPE_KEYS:
            if key in mapping or key in ("target_type", "dest_type"):
                mapping[key] = want
        applied.append({"column": source, "from": have, "to": want, "reason": "validate_create_stamp"})
    return applied
