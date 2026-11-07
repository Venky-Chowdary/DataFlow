"""What Map decided, read back by the schema-drift gate (QA demo 1030).

Two defects on Studio's manual-review policy, one root: the drift gate never
read the operator's decision in Map.

* The loop. A new source column under ``manual_review`` was held with "open
  Map to include the changed columns". Map then included it (an approved
  create-new mapping, ``user_override``), but the gate re-derived the same
  ADD COLUMN from the live table and held again. Only ``propagate`` escaped.
  An approved mapping is the review the policy asks for.
* The silent rename. A source column renamed ``amount`` -> ``total_amount``
  was auto-matched onto the existing ``amount`` column at 82% and nothing
  called it drift. A cross-name match onto an existing destination column,
  where the old name is gone from the source and the new name has no column of
  its own, is a rename: a dropped column plus a new column. Under manual review
  it needs approval in Map; auto-match only suggests it.
"""

from __future__ import annotations

from typing import Any, Iterable

from connectors.sql_identifiers import sanitized_spellings

#: Confidence an explicit, exact mapping carries; anything below is a suggestion.
EXACT_CONFIDENCE = 0.999


def _lower(values: Iterable[Any]) -> set[str]:
    return {str(v).strip().lower() for v in values if str(v or "").strip()}


def approved_in_map(mapping: dict[str, Any]) -> bool:
    """The operator approved this row in Map (Approve / Accept risk)."""
    return bool(mapping.get("user_override") is True or mapping.get("risk_acknowledged") is True)


def auto_matched_renames(
    active_mappings: list[dict[str, Any]],
    *,
    source_columns: list[str],
    target_columns: list[str],
    source_schema: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Cross-name matches onto an existing column whose old name left the source."""
    sources = _lower(source_columns)
    targets = _lower(target_columns)
    out: list[dict[str, Any]] = []
    for m in active_mappings:
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or "").strip()
        if not src or not tgt or src.lower() == tgt.lower():
            continue
        if tgt.lower() not in targets:  # create-new is an ADD, judged elsewhere
            continue
        if tgt.lower() in sources:  # the old name still feeds its own column
            continue
        if src.lower() in targets:  # the new name already has a column
            continue
        # The column DataFlow created for this same source name (``First Name``
        # -> ``First_Name``, legacy ``Café`` -> ``Caf_``) is not a rename (D55).
        if tgt.lower() in sanitized_spellings(src):
            continue
        try:
            confidence = float(m.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        out.append({
            "kind": "rename",
            "column": tgt,
            "to": src,
            "new_type": str((source_schema or {}).get(src) or ""),
            "confidence": round(confidence, 4),
            "reason": "auto_matched_rename",
            "approved_in_map": approved_in_map(m) or confidence >= EXACT_CONFIDENCE,
        })
    return out


def strip_map_approved(classification: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Remove changes the operator approved in Map; return them for the audit."""
    if not isinstance(classification, dict):
        return []
    approved: list[dict[str, Any]] = []
    for key in ("additive", "breaking"):
        kept = []
        for item in classification.get(key) or []:
            if isinstance(item, dict) and item.get("approved_in_map"):
                approved.append(item)
            else:
                kept.append(item)
        classification[key] = kept
    if approved and not classification.get("additive") and not classification.get("breaking"):
        classification["severity"] = "none"
    return approved


def rename_issue(renames: list[dict[str, Any]]) -> str:
    pairs = ", ".join(
        f"{r.get('column')}→{r.get('to')} ({round(float(r.get('confidence') or 0) * 100)}%)"
        for r in renames[:8]
    )
    return (
        f"Source column(s) renamed: {pairs} — the old column is gone from the "
        "source and the new name was only auto-matched onto it. Approve the "
        "mapping in Map (or remap it) before Execute."
    )


def flag_rename_suggestions(
    mappings: list[dict[str, Any]],
    *,
    source_columns: list[str],
    target_columns: list[str],
    schema_policy: str,
    table_exists: bool | None,
) -> list[dict[str, Any]]:
    """Map's own output: an auto-matched rename is a suggestion, not a match.

    Under manual approval the row comes back ``requires_review`` with
    ``review_kind="rename"`` so Studio asks for Approve instead of carrying it
    as matched. Approved rows (``user_override``) are left alone.
    """
    from services.schema_drift import normalize_schema_policy

    if table_exists is not True or not target_columns or not mappings:
        return mappings
    if normalize_schema_policy(schema_policy) not in {"manual_review", "type_locked"}:
        return mappings
    renames = auto_matched_renames(
        mappings, source_columns=source_columns, target_columns=target_columns
    )
    pending = {
        (str(r["to"]).lower(), str(r["column"]).lower())
        for r in renames if not r.get("approved_in_map")
    }
    if not pending:
        return mappings
    for m in mappings:
        key = (str(m.get("source") or "").lower(), str(m.get("target") or "").lower())
        if key in pending:
            m["requires_review"] = True
            m["review_kind"] = "rename"
            m["schema_drift"] = "rename"
            note = (
                f"Possible rename: '{m.get('target')}' is no longer in the source and "
                f"'{m.get('source')}' is new — approve this mapping to accept the rename"
            )
            prior = str(m.get("reasoning") or "")
            m["reasoning"] = f"{note} · {prior}" if prior else note
    return mappings
