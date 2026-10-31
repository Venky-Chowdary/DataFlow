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
  same predecessor. R7 added a unique index on ``chain_seq``, but that index
  is optional: when the server cannot build it (no privilege, or one historic
  duplicate position already in the store — exactly what the old race leaves
  behind) the append logged a warning and carried on unserialized (D221).
  Since D221 the position is the record's ``_id`` (``audit-chain:<seq>``):
  MongoDB enforces ``_id`` uniqueness on every collection, unconditionally,
  so two writers can never both take a position. The losing writer re-reads
  both tips and retries, so neither the global nor a workspace line can fork.
  The file store takes an ``flock`` around read-tip-and-append, so processes
  sharing one data volume are serialized too.
* **One chain for every tenant.** Each record now also links to the previous
  record *of its own workspace* (``ws_prev_hash``), so a
  workspace's chain can be verified on its own and a workspace admin's
  verdict does not depend on other tenants' records.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

#: Record format: canonical JSON hash input, per-workspace links (2); the
#: position as ``_id`` and a ``key_id`` naming the signing key (3, D221).
CHAIN_VERSION = 3
#: Oldest format the position index covers (v2 records already had positions).
_INDEXED_FROM_VERSION = 2
POSITION_ID_PREFIX = "audit-chain:"
_INDEX_RETRY_SEC = 300.0

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_INDEX_ATTEMPTS: dict[str, float] = {}
_FILE_LOCKS = threading.local()


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


def position_id(chain_seq: int) -> str:
    """The ``_id`` a record at ``chain_seq`` takes. Unique in every MongoDB collection."""
    return f"{POSITION_ID_PREFIX}{int(chain_seq)}"


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
    """Index the write position (lookup speed, and a second guard).

    Serialization no longer depends on it: the position is the record's
    ``_id``. So a failure is logged and retried at most every few minutes
    instead of on every append. Partial on ``chain_v`` so history written
    before positions were enforced (which may hold duplicates from the old
    race) does not block the build.
    """
    try:
        key = f"{coll.database.name}.{coll.name}"
    except Exception:  # noqa: BLE001
        key = str(id(coll))
    last = _INDEX_ATTEMPTS.get(key)
    if last is not None and (last == float("inf") or time.monotonic() - last < _INDEX_RETRY_SEC):
        return
    _INDEX_ATTEMPTS[key] = time.monotonic()
    flt = {"chain_v": {"$gte": _INDEXED_FROM_VERSION}}
    try:
        coll.create_index(
            [("chain_seq", 1)], unique=True, partialFilterExpression=flt,
            name="chain_seq_v2_unique",
        )
        _INDEX_ATTEMPTS[key] = float("inf")
    except Exception as exc:  # noqa: BLE001 — positions stay unique via _id
        logger.warning(
            "audit chain position index could not be created (appends stay "
            "serialized by the position _id): %s", str(exc)[:300],
        )


@contextlib.contextmanager
def file_store_lock(store_path: Path) -> Iterator[None]:
    """Exclusive cross-process lock for read-tip-and-append on the JSONL store."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - non-POSIX: process-local lock only
        yield
        return
    lock_path = store_path.with_name(store_path.name + ".lock")
    held = getattr(_FILE_LOCKS, "held", None)
    if held is None:
        held = _FILE_LOCKS.held = set()
    if str(lock_path) in held:  # re-entered by this thread (e.g. purge inside append)
        yield
        return
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        held.add(str(lock_path))
        try:
            yield
        finally:
            held.discard(str(lock_path))
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


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
