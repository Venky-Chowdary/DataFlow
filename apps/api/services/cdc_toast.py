"""PostgreSQL TOAST-aware CDC tuple merge.

When ``pgoutput`` encodes an UPDATE, unchanged TOAST columns are marked ``'u'``
and omitted from the new tuple. Naively upserting that sparse row would
**null/wipe** large columns on the destination (silent data loss).

With ``REPLICA IDENTITY FULL`` (or INDEX covering the TOAST col), the old
tuple carries prior values. Merge policy:

1. Prefer new-tuple values for every present key (including explicit null/empty).
2. Fill keys present only on the old tuple (TOAST-unchanged / omitted).
3. If the new tuple is sparse and no old tuple exists, flag
   ``toast_incomplete`` so callers refuse to apply a destructive upsert.

Delivery remains at-least-once; this only prevents TOAST wipe under redelivery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Sentinel stored in decoded tuples for pgoutput kind ``'u'`` before merge.
TOAST_UNCHANGED = object()


@dataclass
class ToastMergeResult:
    row: dict[str, Any]
    toast_unchanged_cols: list[str] = field(default_factory=list)
    toast_incomplete: bool = False
    filled_from_old: int = 0


def merge_toast_aware_update(
    old_tuple: dict[str, Any] | None,
    new_tuple: dict[str, Any] | None,
    *,
    relation_columns: list[str] | None = None,
) -> ToastMergeResult:
    """Merge old+new so TOAST-unchanged columns are not dropped.

    ``TOAST_UNCHANGED`` sentinels in ``new_tuple`` keep the old value.
    Absent keys in ``new_tuple`` are filled from ``old_tuple`` when present.
    """
    old = dict(old_tuple or {})
    raw_new = dict(new_tuple or {})
    unchanged: list[str] = []
    new: dict[str, Any] = {}
    for key, value in raw_new.items():
        if value is TOAST_UNCHANGED:
            unchanged.append(str(key))
            continue
        new[key] = value

    if not old and not new and not unchanged:
        return ToastMergeResult(row={}, toast_incomplete=True)

    merged = dict(old)
    filled = 0
    for key, value in new.items():
        merged[key] = value
    for key in unchanged:
        if key in old:
            merged[key] = old[key]
            filled += 1
        # else: cannot fill — leave absent
    # Fill other omitted keys from old (pgoutput may omit rather than emit 'u')
    for key, value in old.items():
        if key not in merged and key not in new:
            merged[key] = value
            filled += 1
            if key not in unchanged:
                unchanged.append(str(key))

    incomplete = False
    if relation_columns:
        missing = [c for c in relation_columns if c not in merged]
        # Incomplete when TOAST cols omitted and we had no old values to fill.
        if missing and not old:
            incomplete = True
        elif unchanged and any(c not in merged for c in unchanged):
            incomplete = True
    elif unchanged and not old:
        incomplete = True
    elif new and not old and relation_columns and len(merged) < len(relation_columns):
        incomplete = True

    return ToastMergeResult(
        row=merged,
        toast_unchanged_cols=unchanged,
        toast_incomplete=incomplete,
        filled_from_old=filled,
    )


def apply_update_row_or_raise(
    old_tuple: dict[str, Any] | None,
    new_tuple: dict[str, Any] | None,
    *,
    relation_columns: list[str] | None = None,
    table: str = "",
) -> dict[str, Any]:
    """Return a safe update row or raise if TOAST gaps would cause data loss."""
    result = merge_toast_aware_update(
        old_tuple, new_tuple, relation_columns=relation_columns
    )
    if result.toast_incomplete:
        raise CdcToastIncompleteError(
            f"CDC UPDATE on {table or 'table'} has TOAST-unchanged columns "
            f"without an old tuple to merge ({result.toast_unchanged_cols}); "
            "set REPLICA IDENTITY FULL (or INCLUDE the TOAST columns) — "
            "refusing destructive sparse upsert",
            table=table,
            columns=list(result.toast_unchanged_cols),
        )
    return result.row


class CdcToastIncompleteError(RuntimeError):
    """Sparse UPDATE would wipe TOAST columns — fail closed."""

    def __init__(
        self,
        message: str,
        *,
        table: str = "",
        columns: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.table = table
        self.columns = list(columns or [])

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": "cdc_toast_incomplete",
            "message": str(self),
            "table": self.table,
            "columns": self.columns,
        }
