"""Soft relational constraint hints — informational only, never a GateId.

Core Validate remains G1–G9. This module surfaces optional FK / constraint
awareness for Studio and host policy layers without blocking transfer approval.
Do not market these hints as an additional numbered gate.
"""

from __future__ import annotations

from typing import Any, Mapping


def _as_mapping(ctx: Any) -> Mapping[str, Any]:
    """Normalize PreflightContext, TransferPlan-ish objects, or plain dicts."""
    if isinstance(ctx, Mapping):
        return ctx
    plan = getattr(ctx, "plan", None)
    if plan is not None:
        return {
            "source_columns": list(getattr(getattr(plan, "source", None), "columns", None) or []),
            "destination_columns": list(
                getattr(getattr(plan, "destination", None), "target_columns", None) or []
            ),
            "mappings": list(getattr(plan, "mappings", None) or []),
            "destination_foreign_keys": list(
                getattr(plan, "destination_foreign_keys", None) or []
            ),
            "destination_pk_columns": list(
                getattr(plan, "destination_pk_columns", None) or []
            ),
            "destination_unique_keys": list(
                getattr(plan, "destination_unique_keys", None) or []
            ),
            "table_exists": getattr(
                getattr(plan, "destination", None), "table_exists", None
            ),
        }
    return {
        "source_columns": list(getattr(ctx, "source_columns", None) or []),
        "destination_columns": list(getattr(ctx, "destination_columns", None) or []),
        "mappings": list(getattr(ctx, "mappings", None) or []),
        "destination_foreign_keys": list(
            getattr(ctx, "destination_foreign_keys", None) or []
        ),
        "destination_pk_columns": list(getattr(ctx, "destination_pk_columns", None) or []),
        "destination_unique_keys": list(
            getattr(ctx, "destination_unique_keys", None) or []
        ),
        "table_exists": getattr(ctx, "table_exists", None),
    }


def _col_name(item: Any) -> str:
    if item is None:
        return ""
    if isinstance(item, str):
        return item.strip()
    name = getattr(item, "name", None)
    if name is not None:
        return str(name).strip()
    if isinstance(item, Mapping):
        return str(item.get("name") or "").strip()
    return str(item).strip()


def _mapped_targets(mappings: list[Any]) -> set[str]:
    targets: set[str] = set()
    for m in mappings:
        if isinstance(m, Mapping):
            tgt = str(m.get("target") or "").strip()
            omitted = bool(m.get("intentional_omit") or m.get("intentionalOmit"))
        else:
            tgt = str(getattr(m, "target", "") or "").strip()
            omitted = bool(getattr(m, "intentional_omit", False))
        if tgt and not omitted:
            targets.add(tgt.lower())
    return targets


def _fk_columns(fk: Mapping[str, Any]) -> list[str]:
    raw = fk.get("columns") or fk.get("column") or fk.get("fk_columns") or []
    if isinstance(raw, str):
        raw = [raw]
    return [str(c).strip() for c in raw if str(c).strip()]


def assess_constraint_compatibility(ctx: Any) -> list[str]:
    """Return soft warning strings for relational constraint awareness.

    Empty / unknown schemas yield no hints. Foreign-key columns declared on the
    destination that are not covered by write mappings produce informational
    warnings. Never raises into a Validate blocker and never allocates a GateId.
    """
    data = _as_mapping(ctx)
    source_cols = [_col_name(c) for c in list(data.get("source_columns") or [])]
    dest_cols = [_col_name(c) for c in list(data.get("destination_columns") or [])]
    source_cols = [c for c in source_cols if c]
    dest_cols = [c for c in dest_cols if c]

    # Empty schema → nothing to assess (host may still attach [] on the result).
    if not source_cols and not dest_cols:
        return []

    mappings = list(data.get("mappings") or [])
    fks = [fk for fk in list(data.get("destination_foreign_keys") or []) if isinstance(fk, Mapping)]
    if not fks:
        return []

    mapped = _mapped_targets(mappings)
    dest_names = {c.lower() for c in dest_cols}
    hints: list[str] = []

    for fk in fks:
        cols = _fk_columns(fk)
        if not cols:
            continue
        ref_table = str(fk.get("referenced_table") or fk.get("ref_table") or "").strip()
        ref_cols = fk.get("referenced_columns") or fk.get("ref_columns") or []
        if isinstance(ref_cols, str):
            ref_cols = [ref_cols]
        ref_cols = [str(c).strip() for c in ref_cols if str(c).strip()]

        missing = [c for c in cols if c.lower() not in mapped]
        if missing:
            col_list = ", ".join(missing)
            ref_bit = ""
            if ref_table:
                ref_target = ".".join(
                    [ref_table, ", ".join(ref_cols) if ref_cols else ""]
                ).rstrip(".")
                ref_bit = f" → {ref_target}" if ref_target else f" → {ref_table}"
            hints.append(
                f"Foreign key column(s) {col_list}{ref_bit} are not covered by "
                "the current mapping; destination FK checks may reject rows "
                "(informational — not a core G1–G9 gate)."
            )
            continue

        # Mapped FK columns that are absent from the known destination schema.
        if dest_names:
            unknown = [c for c in cols if c.lower() not in dest_names]
            if unknown:
                hints.append(
                    f"Mapped foreign key column(s) {', '.join(unknown)} are not "
                    "present on the destination schema snapshot "
                    "(informational — verify introspected FK metadata)."
                )

    return hints
