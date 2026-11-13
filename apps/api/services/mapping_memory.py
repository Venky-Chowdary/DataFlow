"""Mapping memory: the source→target pairs a workspace has confirmed before.

When an operator approves ``cust_fname → given_name`` on Map, that decision is
evidence for the next table with the same columns. Auto-map reads this memory
as its ``learned`` tier (:mod:`services.mapping_name_match`), so a workspace's
own vocabulary outranks the generic synonym dictionary.

What it is not:

* Not training data. Nothing here leaves the workspace or feeds a shared model
  (``DATAFLOW_TRAINING`` governs that); one tenant's names never suggest
  anything to another.
* Not an approval. A learned pair is a *suggestion* with a confidence; the row
  still needs Approve and Validate like any other.

Records are one per workspace (``SharedRecordSet`` partitioned by
``workspace_id``) so every API replica reads the same memory, keyed by the
normalized names so ``FirstName`` and ``first_name`` count as one pair.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.mapping_name_match import normalize_name
from services.platform_config import data_dir
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet
from services.workspace_filter import current_workspace_stamp

#: Pairs kept per workspace; the least recently confirmed are evicted first.
MAX_PAIRS_PER_WORKSPACE = 5000
#: Pairs one request may record (a 1,000-column table + slack).
MAX_PAIRS_PER_REQUEST = 2000
_NAME_LIMIT = 256


def _store_path():
    return data_dir() / "mapping_memory.json"


def _file_load() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _file_save(data: dict[str, Any]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_store_path(), dict(data))


_DOC = SharedRecordSet(
    "mapping_memory",
    file_load=_file_load,
    file_save=_file_save,
    file_path=_store_path,
    partition_key="workspace_id",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _key(workspace_id: str) -> str:
    return f"memory::{workspace_id}"


def _owner(workspace_id: str | None) -> str:
    explicit = str(workspace_id or "").strip()
    return explicit or current_workspace_stamp()


def _record(workspace_id: str) -> dict[str, Any]:
    data = _DOC.load(keys=[_key(workspace_id)])
    found = data.get(_key(workspace_id))
    if isinstance(found, dict) and str(found.get("workspace_id") or "") == workspace_id:
        return found
    return {"workspace_id": workspace_id, "pairs": {}}


def _clean(name: Any) -> str:
    return str(name or "").strip()[:_NAME_LIMIT]


@_DOC.retrying
def record_confirmed(
    pairs: list[dict[str, Any]],
    *,
    workspace_id: str | None = None,
    actor: str = "",
) -> dict[str, int]:
    """Count each confirmed ``{source, target}`` once more for this workspace."""
    owner = _owner(workspace_id)
    data = _DOC.load(keys=[_key(owner)])
    record = data.get(_key(owner))
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != owner:
        record = {"workspace_id": owner, "pairs": {}}
    stored: dict[str, Any] = dict(record.get("pairs") or {})
    recorded = 0
    seen: set[str] = set()
    for pair in pairs[:MAX_PAIRS_PER_REQUEST]:
        source, target = _clean(pair.get("source")), _clean(pair.get("target"))
        s_norm, t_norm = normalize_name(source), normalize_name(target)
        if not s_norm or not t_norm:
            continue
        key = f"{s_norm}␟{t_norm}"
        if key in seen:
            continue
        seen.add(key)
        prior = stored.get(key) if isinstance(stored.get(key), dict) else {}
        stored[key] = {
            "source": source,
            "target": target,
            "source_norm": s_norm,
            "target_norm": t_norm,
            "count": int(prior.get("count") or 0) + 1,
            "first_confirmed_at": prior.get("first_confirmed_at") or _now(),
            "last_confirmed_at": _now(),
            "last_confirmed_by": str(actor or "")[:_NAME_LIMIT],
        }
        recorded += 1
    if len(stored) > MAX_PAIRS_PER_WORKSPACE:
        ranked = sorted(stored.items(), key=lambda kv: str(kv[1].get("last_confirmed_at") or ""))
        stored = dict(ranked[-MAX_PAIRS_PER_WORKSPACE:])
    record = {"workspace_id": owner, "pairs": stored, "updated_at": _now()}
    data[_key(owner)] = record
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return {"recorded": recorded, "total": len(stored)}


def list_learned(*, workspace_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    """This workspace's learned pairs, most confirmed first."""
    pairs = list((_record(_owner(workspace_id)).get("pairs") or {}).values())
    pairs = [p for p in pairs if isinstance(p, dict)]
    pairs.sort(key=lambda p: (-int(p.get("count") or 0), str(p.get("source_norm") or "")))
    return pairs[: max(0, int(limit))]


def learned_index(*, workspace_id: str | None = None) -> dict[str, dict[str, int]]:
    """``{source_norm: {target_norm: count}}`` for :func:`score_pair`."""
    index: dict[str, dict[str, int]] = {}
    for pair in (_record(_owner(workspace_id)).get("pairs") or {}).values():
        if not isinstance(pair, dict):
            continue
        s, t = str(pair.get("source_norm") or ""), str(pair.get("target_norm") or "")
        if s and t:
            index.setdefault(s, {})[t] = int(pair.get("count") or 0)
    return index


@_DOC.retrying
def forget_all(*, workspace_id: str | None = None) -> int:
    """Erase this workspace's memory; returns how many pairs were removed."""
    owner = _owner(workspace_id)
    data = _DOC.load(keys=[_key(owner)])
    record = data.get(_key(owner))
    removed = len((record or {}).get("pairs") or {}) if isinstance(record, dict) else 0
    data[_key(owner)] = {"workspace_id": owner, "pairs": {}, "updated_at": _now()}
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return removed
