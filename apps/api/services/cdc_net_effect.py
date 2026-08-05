"""Debezium-style per-PK net effect for CDC transaction buffers.

Within one source transaction a PK may be DELETE then INSERT (ORM replace).
Demuxing into parallel insert/update/delete lists and applying inserts→deletes
silently drops the recreation. Coalesce in event order before emit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CdcTxnEvent:
    op: str  # "i" | "u" | "d"
    pk: str
    row: dict[str, Any] | None = None
    lsn: str | None = None


_PK_HINT_FIELDS = ("id", "_id", "pk", "ID", "Id", "uuid", "UUID", "key")


def infer_row_pk(row: dict[str, Any] | None, *, explicit: str | None = None) -> str:
    """Resolve the PK string used to coalesce in-transaction DML.

    Callers that know the primary key **must** pass ``explicit``. Guessing from
    the first non-empty column value collapsed two distinct rows that shared
    that value into one, and made a DELETE keyed on the real PK miss the
    INSERT that followed it in the same transaction — so the recreated row
    was deleted. The only remaining fallback is well-known id column names;
    anything else returns empty and the coalescer keeps the row under a
    synthetic key rather than inventing a collision.
    """
    if explicit:
        return str(explicit)
    if not row:
        return ""
    for field in _PK_HINT_FIELDS:
        if field in row and row[field] is not None and str(row[field]).strip() != "":
            return str(row[field])
    return ""


def coalesce_cdc_txn_events(
    events: list[CdcTxnEvent],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Collapse ordered DML into net inserts / updates / deletes per PK.

    Rules (last op wins for each PK):
    - last delete → delete only
    - last insert → insert only (drops prior delete for that PK)
    - last update → update only (if a prior delete existed in-txn, treat as insert)
    """
    # pk -> ("i"|"u"|"d", row|None)
    net: dict[str, tuple[str, dict[str, Any] | None]] = {}
    for ev in events:
        pk = (ev.pk or "").strip()
        if not pk:
            # Unkeyed insert/update — keep as-is under synthetic key.
            if ev.op in {"i", "u"} and isinstance(ev.row, dict):
                synthetic = f"__row_{id(ev.row)}"
                net[synthetic] = (ev.op, dict(ev.row))
            continue
        if ev.op == "d":
            net[pk] = ("d", None)
        elif ev.op in {"i", "u"} and isinstance(ev.row, dict):
            prior = net.get(pk)
            row = dict(ev.row)
            if prior and prior[0] == "d":
                # DELETE then INSERT/UPDATE in same txn → recreation lands as insert.
                net[pk] = ("i", row)
            elif ev.op == "i":
                net[pk] = ("i", row)
            else:
                # update: if no prior row in txn, keep as update; if prior insert, stay insert.
                if prior and prior[0] == "i":
                    net[pk] = ("i", row)
                else:
                    net[pk] = ("u", row)

    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    deletes: list[str] = []
    for pk, (op, row) in net.items():
        if op == "d":
            if not pk.startswith("__row_"):
                deletes.append(pk)
        elif op == "i" and row is not None:
            inserts.append(row)
        elif op == "u" and row is not None:
            updates.append(row)
    return inserts, updates, deletes
