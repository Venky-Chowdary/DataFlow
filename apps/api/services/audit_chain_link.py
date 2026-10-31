"""How one audit record joins the chain: what is hashed, and what it links to.

Three defects made untouched production chains fail verification (R7):

* **The hashed bytes were not the stored bytes.** The HMAC was taken over the
  Python objects passed to ``append_audit_event``. BSON then stored a
  ``datetime`` truncated to milliseconds and without its zone (and refused
  UUID / date / >64-bit ints / sets outright, sending the record to the file
  store instead). A re-walk hashed what came back, not what was hashed.
  ``canonical_record`` fixes the record to JSON-native values *before* it is
  hashed, so the store returns exactly the bytes the HMAC covers.
* **Several writer processes, one process-local lock.** API, worker and
  scheduler each read the tip and inserted. Two racing appends claimed the
  same predecessor. Records now carry ``chain_v = 2`` and Mongo enforces a
  unique ``chain_seq`` across processes; the losing writer re-reads both tips
  and retries, so neither the global nor a workspace line can fork.
* **One chain for every tenant.** Each record now also links to the previous
  record *of its own workspace* (``ws_prev_hash``), so a
  workspace's chain can be verified on its own and a workspace admin's
  verdict does not depend on other tenants' records.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

#: Record format that hashes canonical JSON and carries per-workspace links.
CHAIN_VERSION = 2

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_INDEXED: set[int] = set()


def _bson_safe(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not _INT64_MIN <= value <= _INT64_MAX:
        # BSON has no wider int; its exact digits as text, never a float.
        return str(value)
    if isinstance(value, dict):
        return {k: _bson_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_bson_safe(v) for v in value]
    return value


def canonical_record(event: dict[str, Any]) -> dict[str, Any]:
    """The record as the store will return it: JSON-native, BSON-encodable.

    A ``datetime`` becomes its ISO text (microseconds and zone kept), a tuple a
    list, a non-string key a string. Hashing this instead of the caller's
    objects is what makes a re-walk recompute the same HMAC.
    """
    body = {k: v for k, v in event.items() if k != "_id"}
    canon = json.loads(json.dumps(body, default=json_default))
    out = _bson_safe(canon)
    if "_id" in event:
        out["_id"] = event["_id"]
    return out


def workspace_key(event: dict[str, Any] | None) -> str:
    return str((event or {}).get("workspace_id") or "").strip()


def _seq(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def link_fields(
    *,
    tip: dict[str, Any] | None,
    workspace_tip: dict[str, Any] | None,
) -> dict[str, Any]:
    """Global and per-workspace predecessor fields for the next record."""
    return {
        "prev_hash": str(tip.get("event_hash")) if tip and tip.get("event_hash") else None,
        "chain_seq": _seq((tip or {}).get("chain_seq")) + 1,
        "ws_prev_hash": (
            str(workspace_tip.get("event_hash"))
            if workspace_tip and workspace_tip.get("event_hash")
            else None
        ),
        "chain_v": CHAIN_VERSION,
    }


def mongo_workspace_tip(coll: Any, workspace_id: str) -> dict[str, Any] | None:
    """Newest record of ``workspace_id``, by write position."""
    order = [("chain_seq", -1), ("time", -1)]
    doc = next(iter(coll.find({"workspace_id": workspace_id}).sort(order).limit(1)), None)
    return {k: v for k, v in doc.items() if k != "_id"} if doc else None


def file_workspace_tip(lines: list[str], workspace_id: str) -> dict[str, Any] | None:
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(ev, dict) and ev.get("event_hash") and workspace_key(ev) == workspace_id:
            return ev
    return None


def ensure_chain_indexes(coll: Any) -> None:
    """Unique write positions for v2 records, enforced by the store itself.

    Every append takes the next global position, so one unique index
    serializes all writers — the workspace link is read inside the same retry.

    Partial on ``chain_v`` so history written before this (which may already
    hold duplicate positions from the old race) does not block the index.
    """
    key = id(coll)
    if key in _INDEXED:
        return
    flt = {"chain_v": {"$gte": CHAIN_VERSION}}
    try:
        coll.create_index(
            [("chain_seq", 1)], unique=True, partialFilterExpression=flt,
            name="chain_seq_v2_unique",
        )
        _INDEXED.add(key)
    except Exception as exc:  # noqa: BLE001 — appends still work; the race is logged
        logger.warning("audit chain unique index could not be created: %s", exc)


def is_duplicate_position(exc: BaseException) -> bool:
    try:
        from pymongo.errors import DuplicateKeyError
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, DuplicateKeyError)


def append_with_retry(
    build: Callable[[], dict[str, Any]],
    insert: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    attempts: int = 25,
) -> dict[str, Any]:
    """Build-and-insert until no other writer took the same position."""
    last: BaseException | None = None
    for _ in range(attempts):
        event = build()
        try:
            return insert(event)
        except Exception as exc:  # noqa: BLE001
            if not is_duplicate_position(exc):
                raise
            last = exc
    raise RuntimeError(f"audit chain append lost {attempts} position races") from last
