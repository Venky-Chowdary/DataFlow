"""Which audit records a verification walk reads, and from which store (A1).

``evidence_chain.read_chain`` used to read the OLDEST ``limit`` records of the
whole store and leave the workspace filter to the caller, so a workspace whose
records were not among them walked nothing — and nothing was reported as
"verified". It also answered a Mongo read error from the local JSONL file,
which in a Mongo deployment holds only records an insert failed to write: a
different chain, verified under the real one's name.

A window here is always:

* **scoped in the store**: a workspace walk queries that workspace's records
  (Mongo ``find({"workspace_id": ...})``; the file store is filtered as it is
  read), so other tenants' volume cannot push them out;
* **the newest** ``limit`` **records** of that scope, in write order, with the
  scope's total and ``truncated_by_limit`` — the verifier treats the window's
  first link as explained by the limit, never as a deletion;
* **honest about the store**: ``store`` names the one walked, and a Mongo error
  is returned as ``error`` with no records, never swapped for the file.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ChainWindow:
    events: list[dict[str, Any]] = field(default_factory=list)
    #: "mongo" | "file"
    store: str = "file"
    #: Records in scope in that store (the window holds at most ``limit``).
    total: int = 0
    limit: int = 0
    #: Machine-readable reason the store could not be read ("" = read fine).
    error: str = ""
    #: The oldest record in the whole store (retention checkpoints explain it).
    store_first: dict[str, Any] | None = None
    #: Records for this scope sitting in the file store while Mongo is the
    #: store (left by failed Mongo inserts). Not walked; reported.
    fallback_file_records: int = 0

    @property
    def truncated_by_limit(self) -> bool:
        return self.total > len(self.events)


def _strip_id(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in doc.items() if k != "_id"}


def _mongo_window(coll: Any, workspace_id: str, limit: int) -> ChainWindow:
    from services import audit_log

    query: dict[str, Any] = {"workspace_id": workspace_id} if workspace_id else {}
    newest_first = [(k, -d) for k, d in audit_log.CHAIN_ORDER]
    window = ChainWindow(store="mongo", limit=limit)
    try:
        window.total = int(coll.count_documents(query))
        cursor = coll.find(query).sort(newest_first).limit(limit)
        if hasattr(cursor, "batch_size"):
            cursor = cursor.batch_size(1000)
        newest = [_strip_id(doc) for doc in cursor]
        window.events = list(reversed(newest))
        first = next(iter(coll.find({}).sort(list(audit_log.CHAIN_ORDER)).limit(1)), None)
        window.store_first = _strip_id(first) if first else None
    except Exception as exc:  # noqa: BLE001 - any driver error is reported, not hidden
        logger.warning("audit chain read from Mongo failed (%s)", type(exc).__name__)
        return ChainWindow(store="mongo", limit=limit, error=_error_code(exc))
    window.fallback_file_records = _file_scope_count(workspace_id)
    return window


def _error_code(exc: BaseException) -> str:
    """A stable reason, never the driver's text (it can name hosts and users)."""
    name = type(exc).__name__
    if "Timeout" in name or "ServerSelection" in name or "Connection" in name or "AutoReconnect" in name:
        return "store_unreachable"
    if "OperationFailure" in name or "Authorization" in name:
        return "store_refused_read"
    return "store_read_failed"


def _file_lines() -> list[str]:
    from services import audit_log

    path = audit_log.STORE_PATH
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []


def _parse(line: str) -> dict[str, Any] | None:
    if not line.strip():
        return None
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None
    return event if isinstance(event, dict) else None


def _in_scope(event: dict[str, Any], workspace_id: str) -> bool:
    return not workspace_id or str(event.get("workspace_id") or "").strip() == workspace_id


def _file_scope_count(workspace_id: str) -> int:
    count = 0
    for line in _file_lines():
        event = _parse(line)
        if event is not None and _in_scope(event, workspace_id):
            count += 1
    return count


def _file_window(workspace_id: str, limit: int) -> ChainWindow:
    window = ChainWindow(store="file", limit=limit)
    newest: deque[dict[str, Any]] = deque(maxlen=limit)
    for line in _file_lines():
        event = _parse(line)
        if event is None:
            continue
        if window.store_first is None:
            window.store_first = event
        if _in_scope(event, workspace_id):
            window.total += 1
            newest.append(event)
    window.events = list(newest)
    return window


def read_window(*, workspace_id: str = "", limit: int) -> ChainWindow:
    """The newest ``limit`` records of ``workspace_id`` ("" = every record)."""
    from services import audit_log

    ws = (workspace_id or "").strip()
    limit = max(1, int(limit))
    coll = audit_log._mongo_collection()
    if coll is not None:
        return _mongo_window(coll, ws, limit)
    return _file_window(ws, limit)
