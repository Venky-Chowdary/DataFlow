"""Pre-release risk approvals: never honoured, listed for re-approval.

Migration Risk Contracts are signed with a keyed HMAC
(services.migration_risk_contract). Contracts approved before that carry an
unkeyed digest (``mrc-sha256:``) anyone can compute, and the pre-release code
left no evidence that would tell a server-signed one from one an API caller
wrote, so none of them counts any more: each is refused with "re-approve this
risk".

So that operators can re-approve before schedules resume, every start of the
API rebuilds the upgrade report from the schedules, plans and jobs the server
stores (:func:`refresh_upgrade_report`): one entry per pre-release contract,
naming its schedule, plan or job and route. It is shown in the start log, at
``GET /api/v1/ops/risk-contract-upgrade-report`` (workspace admins) and as a
``risk_contract_reapprove`` warning in Validate for that route
(:func:`reapproval_warnings`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

COLLECTION = "risk_contracts"
LEGACY_PREFIX = "mrc-sha256:"
_REPORT_ID = "_upgrade_report"
_CONTRACT_KEYS = ("risk_contract", "riskContract", "empty_as_null_contract")


def _collection() -> Any | None:
    from services.control_plane_store import mongo_collection

    return mongo_collection(COLLECTION)


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "risk_contracts.json"


def is_pre_release(contract: Any) -> bool:
    """A contract carrying the pre-release unkeyed signature."""
    return isinstance(contract, Mapping) and str(contract.get("signature") or "").startswith(LEGACY_PREFIX)


def _mapping_contracts(doc: Any) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    """(mapping, pre-release contract) pairs anywhere in a stored record."""
    out: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    stack: list[Any] = [doc]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            out.extend((item, item[k]) for k in _CONTRACT_KEYS if is_pre_release(item.get(k)))
            stack.extend(v for k, v in item.items() if k not in _CONTRACT_KEYS)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return out


def _route_of(doc: Mapping[str, Any], m: Mapping[str, Any]) -> str:
    req = doc.get("transfer_request") if isinstance(doc.get("transfer_request"), Mapping) else doc
    src = req.get("source") if isinstance(req.get("source"), Mapping) else {}
    dst = req.get("destination") if isinstance(req.get("destination"), Mapping) else {}
    a = str(doc.get("source_table") or src.get("table") or "?")
    b = str(doc.get("dest_table") or doc.get("destination_table") or dst.get("table") or "?")
    return f"{a} → {b} ({m.get('source') or '?'} → {m.get('target') or '?'})"


def report_entries(kind: str, doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    """One upgrade-report entry per pre-release contract in one stored record."""
    return [{
        "kind": kind, "record_id": str(doc.get("id") or doc.get("_id") or ""), "name": str(doc.get("name") or ""),
        "workspace_id": str(doc.get("workspace_id") or ""), "route": _route_of(doc, m),
        "risk_id": str(c.get("risk_id") or ""), "approved_by": str(c.get("approved_by") or ""),
    } for m, c in _mapping_contracts(doc)]


def _stored_records() -> Iterable[tuple[str, Mapping[str, Any]]]:
    from dataclasses import asdict, is_dataclass

    from services.schedule_store import list_schedules
    from services.transfer_plan_store import _load_all

    for sched in list_schedules():
        yield "schedule", asdict(sched) if is_dataclass(sched) else dict(sched)
    for plan in _load_all():
        yield "plan", asdict(plan) if is_dataclass(plan) else dict(plan)
    from services.mongodb_service import get_mongodb_service

    mongo = get_mongodb_service()
    offset = 0
    while mongo is not None:
        page = mongo.list_jobs(limit=500, offset=offset)
        if not page:
            break
        for job in page:
            yield "job", job
        offset += len(page)


def refresh_upgrade_report() -> list[dict[str, Any]]:
    """Rebuild the upgrade report from the stored records (every start; idempotent) and return it."""
    entries = [e for kind, doc in _stored_records() for e in report_entries(kind, doc)]
    at = datetime.now(timezone.utc).isoformat()
    coll = _collection()
    if coll is not None:
        coll.replace_one({"_id": _REPORT_ID}, {"_id": _REPORT_ID, "entries": entries, "at": at}, upsert=True)
        return entries
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {}) as data:
        data["upgrade_report"] = entries
        data["at"] = at
    return entries


def upgrade_report() -> list[dict[str, Any]]:
    """The pre-release risk approvals found at the last start: each must be re-approved."""
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": _REPORT_ID})
        return list((doc or {}).get("entries") or [])
    from services.metadata_backend import load_json_doc

    return list(load_json_doc(_file_path(), {}).get("upgrade_report") or [])


def reapproval_warnings(mappings: Any) -> list[dict[str, Any]]:
    """One Validate warning per mapping that still carries a pre-release risk approval."""
    out = []
    for m in mappings or []:
        if not isinstance(m, Mapping):
            continue
        for key in _CONTRACT_KEYS:
            c = m.get(key)
            if is_pre_release(c):
                out.append({
                    "id": "risk_contract_reapprove",
                    "message": (f"{m.get('source') or '?'} → {m.get('target') or '?'}: its risk approval "
                                f"({c.get('risk_id')}, approved by {c.get('approved_by') or '?'}) was made "
                                "before this release and no longer counts. Re-approve this risk once."),
                    "details": {"risk_id": c.get("risk_id"), "column": m.get("source"), "target": m.get("target")},
                })
    return out
