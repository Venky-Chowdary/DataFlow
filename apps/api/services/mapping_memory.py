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

Scope (review M4). A pair is remembered *for one destination* (connector,
database, schema, table): a confirmation on ``customers`` says nothing certain
about ``orders``. On the same destination it is the learned tier; on another
destination of the workspace it is a weaker fallback. Records are one per
(workspace, destination) in a ``SharedRecordSet`` partitioned by
``workspace_id``, so every API replica reads the same memory, keyed by the
normalized names so ``FirstName`` and ``first_name`` count as one pair.
"""

from __future__ import annotations

import hashlib
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
#: Pairs one request may record: one confirmation per destination column.
MAX_PAIRS_PER_REQUEST = 200
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


def destination_scope(destination: dict[str, Any] | None) -> str:
    """``connector/database/schema/table`` (host:port for an inline endpoint), lower-case."""
    d = dict(destination or {})
    owner = str(d.get("connector_id") or "").strip()
    if not owner and d.get("host"):
        owner = f"{d.get('host')}:{d.get('port') or ''}"
    parts = [owner, d.get("database"), d.get("schema") or d.get("db_schema"),
             d.get("table") or d.get("collection")]
    return "/".join(str(p or "").strip().lower() for p in parts)[:_NAME_LIMIT * 2]


def _key(workspace_id: str, scope: str) -> str:
    digest = hashlib.sha256(scope.encode("utf-8"), usedforsecurity=False).hexdigest()[:24]
    return f"memory::{workspace_id}::{digest}"


def _owner(workspace_id: str | None) -> str:
    explicit = str(workspace_id or "").strip()
    return explicit or current_workspace_stamp()


def _workspace_records(workspace_id: str) -> list[dict[str, Any]]:
    data = _DOC.load(part=workspace_id)
    return [
        r for r in data.values()
        if isinstance(r, dict) and str(r.get("workspace_id") or "") == workspace_id
    ]


def _clean(name: Any) -> str:
    return str(name or "").strip()[:_NAME_LIMIT]


@_DOC.retrying
def record_confirmed(
    pairs: list[dict[str, Any]],
    *,
    workspace_id: str | None = None,
    destination: dict[str, Any] | None = None,
    actor: str = "",
) -> dict[str, int]:
    """Count each confirmed ``{source, target}`` once more for this destination."""
    owner = _owner(workspace_id)
    scope = destination_scope(destination)
    key = _key(owner, scope)
    data = _DOC.load(keys=[key])
    record = data.get(key)
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != owner:
        record = {"workspace_id": owner, "destination": scope, "pairs": {}}
    stored: dict[str, Any] = dict(record.get("pairs") or {})
    recorded = 0
    seen: set[str] = set()
    for pair in pairs[:MAX_PAIRS_PER_REQUEST]:
        source, target = _clean(pair.get("source")), _clean(pair.get("target"))
        s_norm, t_norm = normalize_name(source), normalize_name(target)
        if not s_norm or not t_norm:
            continue
        pair_key = f"{s_norm}\u241f{t_norm}"
        if pair_key in seen:
            continue
        seen.add(pair_key)
        prior = stored.get(pair_key) if isinstance(stored.get(pair_key), dict) else {}
        stored[pair_key] = {
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
    data[key] = {"workspace_id": owner, "destination": scope, "pairs": stored, "updated_at": _now()}
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return {"recorded": recorded, "total": len(stored)}


def list_learned(*, workspace_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    """This workspace's learned pairs (every destination), most confirmed first."""
    out: list[dict[str, Any]] = []
    for record in _workspace_records(_owner(workspace_id)):
        for pair in (record.get("pairs") or {}).values():
            if isinstance(pair, dict):
                out.append({**pair, "destination": record.get("destination") or ""})
    out.sort(key=lambda p: (-int(p.get("count") or 0), str(p.get("source_norm") or "")))
    return out[: max(0, int(limit))]


def learned_index(
    *,
    workspace_id: str | None = None,
    destination: dict[str, Any] | None = None,
) -> dict[str, dict[str, tuple[int, bool]]]:
    """``{source_norm: {target_norm: (count, same_destination)}}`` for :func:`score_pair`.

    A pair confirmed for this destination wins over the same pair learned on
    another destination of the workspace, which is only a fallback.
    """
    scope = destination_scope(destination)
    index: dict[str, dict[str, tuple[int, bool]]] = {}
    for record in _workspace_records(_owner(workspace_id)):
        here = str(record.get("destination") or "") == scope
        for pair in (record.get("pairs") or {}).values():
            if not isinstance(pair, dict):
                continue
            s, t = str(pair.get("source_norm") or ""), str(pair.get("target_norm") or "")
            if not s or not t:
                continue
            count = int(pair.get("count") or 0)
            prior = index.setdefault(s, {}).get(t)
            if prior is None or (here and not prior[1]) or (here == prior[1] and count > prior[0]):
                index[s][t] = (count, here)
    return index


@_DOC.retrying
def forget_all(*, workspace_id: str | None = None) -> int:
    """Erase this workspace's memory on every destination; returns pairs removed."""
    owner = _owner(workspace_id)
    data = _DOC.load(part=owner)
    removed = 0
    for key, record in list(data.items()):
        if not isinstance(record, dict) or str(record.get("workspace_id") or "") != owner:
            continue
        removed += len(record.get("pairs") or {})
        data[key] = {**record, "pairs": {}, "updated_at": _now()}
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return removed
