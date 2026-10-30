"""The one read model for a contract's status, version and columns.

Sign writes status/version to the contract store. Every reader — the Contracts
list and drawer (``/api/v1/contracts``), the Pilot briefing and contract list,
and the run gate — reads it back through here, so they cannot disagree about
whether an agreement is "Signed v3" or "Draft v1".

QA saw the list and drawer stay "Draft v1 (Signed 0)" after a successful Sign
while the briefing said "all signed". The store was right; the readers each had
their own path (a stale client copy, a 50-row briefing page called the
population, a column field the UI did not know), and each told a different story.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from services import contract_store as _contract_store
from services.data_contract import DataContract

_LABELS = {"signed": "Signed", "draft": "Draft", "broken": "Broken", "deprecated": "Deprecated"}
SIGNED_STATUSES = frozenset({"signed", "active"})


def status_value(contract: Any) -> str:
    raw = getattr(contract, "status", None)
    if isinstance(contract, dict):
        raw = contract.get("status")
    return str(getattr(raw, "value", raw) or "draft").strip().lower()


def _version(contract: Any) -> int:
    raw = contract.get("version") if isinstance(contract, dict) else getattr(contract, "version", 1)
    try:
        return max(int(raw or 1), 1)
    except (TypeError, ValueError):
        return 1


def status_label(contract: Any) -> str:
    """"Signed v3" / "Draft v1" — the words the list, drawer and Pilot all show."""
    status = status_value(contract)
    return f"{_LABELS.get(status, status.title() or 'Draft')} v{_version(contract)}"


def _last_signature(contract: Any) -> dict[str, Any]:
    meta = (contract.get("metadata") if isinstance(contract, dict) else getattr(contract, "metadata", None)) or {}
    for rev in reversed(list(meta.get("revisions") or [])):
        if isinstance(rev, dict) and str(rev.get("to_status") or "").lower() == "signed":
            return rev
    return {}


def column_view(column: dict[str, Any]) -> dict[str, Any]:
    """A stored column rule plus the ``name``/``type`` a reader displays.

    The rule keeps ``source_name``/``target_name``/``source_type``/``target_type``;
    a reader that looked for ``name`` found nothing and showed ``col_<i>``.
    """
    out = dict(column)
    name = str(
        column.get("name")
        or column.get("target_name")
        or column.get("source_name")
        or column.get("column_name")
        or ""
    )
    ctype = str(column.get("type") or column.get("target_type") or column.get("source_type") or "")
    out["name"] = name
    out["type"] = ctype
    return out


def contract_view(contract: DataContract) -> dict[str, Any]:
    """The contract as every reader sees it: stored fields plus the status view."""
    data = contract.to_dict()
    signature = _last_signature(data)
    status = status_value(data)
    data["status"] = status
    data["version"] = _version(data)
    data["status_label"] = status_label(data)
    data["signed_version"] = int(signature.get("version") or data["version"]) if signature else None
    data["signed_at"] = str(signature.get("at") or "") if signature else ""
    data["columns"] = [column_view(c) for c in data.get("columns") or []]
    return data


def load_contract(contract_id: str) -> DataContract | None:
    cid = (contract_id or "").strip()
    return _contract_store.get_contract_store().get_contract(cid) if cid else None


def visible_population(
    *,
    allows: Callable[[Any], bool] | None = None,
    workspace_id: str = "",
    limit: int | None = None,
) -> list[DataContract]:
    """Every contract the caller may see, newest first — the whole population.

    ``allows`` is the request's tenant predicate (the router's
    ``tenant_scope(...).allows``); without one the bound Pilot/MCP scope
    decides. ``workspace_id`` narrows to one workspace (unowned rows stay, as
    the list shows them when the scope allows unowned rows). ``limit`` reads
    only the newest page (the list view); a census never passes one.
    """
    want = (workspace_id or "").strip()
    out: list[DataContract] = []
    for contract in _contract_store.get_contract_store().list_contracts(limit=limit or 100_000):
        owner = str(((getattr(contract, "metadata", None) or {}).get("workspace_id")) or "")
        if allows is not None:
            if not allows(owner or None):
                continue
        elif not _contract_store.contract_visible(contract):
            continue
        if want and owner and owner != want:
            continue
        out.append(contract)
    return out


def census(contracts: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for contract in contracts:
        status = status_value(contract)
        counts[status] = counts.get(status, 0) + 1
    return counts


def briefing_summary(*, workspace_id: str = "", recent: int = 3) -> dict[str, Any]:
    """Contract facts for the Pilot briefing, from the list's own population."""
    population = visible_population(workspace_id=workspace_id)
    by_status = census(population)
    unsigned = sum(n for status, n in by_status.items() if status not in SIGNED_STATUSES)
    return {
        "contract_count": len(population),
        "contracts_unsigned": unsigned,
        "contracts_by_status": by_status,
        "recent_contracts": [
            {"id": c.id, "name": c.name, "status_label": status_label(c)}
            for c in population[: max(int(recent), 0)]
        ],
    }
