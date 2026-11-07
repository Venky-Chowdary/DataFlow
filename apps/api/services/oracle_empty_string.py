"""Oracle stores ``''`` as NULL — the one place the run says so (D210).

Owner decision 4: a cell that is (or changes to) ``''`` is written to Oracle
as NULL together with the rest of the row; a stale value is never kept. The
preflight says it once, and the read-back treats ``''`` and NULL as equal for
an Oracle destination only (:func:`services.reconciliation.destination_empty_string_is_null`).
"""

from __future__ import annotations

from typing import Any, Iterable

WARNING_ID = "oracle_empty_string_is_null"


def empty_is_null_destination(engine: str | None) -> bool:
    """True only for an Oracle destination (``''`` and NULL are one value there)."""
    from services.reconciliation import destination_empty_string_is_null

    return destination_empty_string_is_null(engine)


def normalize_oracle_empty(value: Any, engine: str | None) -> Any:
    """The value an ``engine`` destination stores for ``value`` (``''`` → NULL on Oracle)."""
    if isinstance(value, str) and value == "" and empty_is_null_destination(engine):
        return None
    return value


def _text_targets(plan_mappings: Iterable[Any]) -> list[str]:
    from services.type_system import normalize_logical_type

    out: list[str] = []
    for pm in plan_mappings or []:
        target = getattr(pm, "target", None) or (pm.get("target") if isinstance(pm, dict) else None)
        ttype = getattr(pm, "target_type", None) or (
            pm.get("target_type") if isinstance(pm, dict) else None
        )
        if not target:
            continue
        if not ttype or normalize_logical_type(str(ttype)) in {"string", "text"}:
            out.append(str(target))
    return out


def attach_oracle_empty_string_warning(
    out: dict[str, Any], dest_kind: str | None, plan_mappings: Iterable[Any]
) -> None:
    """Add the single Oracle ``''`` → NULL warning to a preflight result."""
    if not empty_is_null_destination(dest_kind):
        return
    columns = _text_targets(plan_mappings)
    if not columns:
        return
    warnings = out.setdefault("warnings", [])
    if not isinstance(warnings, list):
        return
    if any(isinstance(w, dict) and w.get("id") == WARNING_ID for w in warnings):
        return
    shown = ", ".join(columns[:6]) + (f" (+{len(columns) - 6} more)" if len(columns) > 6 else "")
    warnings.append({
        "id": WARNING_ID,
        "message": (
            "Oracle stores '' (empty string) as NULL: an empty text value in "
            f"{shown} is written as NULL, also when an existing row is updated. "
            "Verification treats '' and NULL as equal for this Oracle destination only."
        ),
        "details": {"columns": columns},
    })
