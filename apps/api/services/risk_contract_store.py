"""The server's own record of the Migration Risk Contracts it signed.

Every contract :func:`services.migration_risk_contract.create_migration_risk_contract`
signs is recorded here by ``risk_id`` and signature digest, with its workspace,
kind and origin, so a contract can be looked up by id + digest.

Contracts signed before the keyed signature (``mrc-sha256:``, an unkeyed digest
anyone could recompute) are honoured only when this store holds that exact
contract: :func:`backfill_legacy_contracts` records the ones found in records
the server itself stored before :data:`LEGACY_CUTOVER` (schedules, transfer
plans, jobs). A legacy contract presented by a client with no server record is
refused ("re-approve this risk").
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "risk_contracts"
#: Records the server stored before this instant may carry unkeyed contracts it
#: created; anything stored later must carry a keyed one. Overridable with
#: DATAFLOW_RISK_CONTRACT_CUTOVER (ISO-8601) by the operator who deploys it.
LEGACY_CUTOVER = "2026-11-28T00:00:00+00:00"
LEGACY_PREFIX = "mrc-sha256:"
_CONTRACT_KEYS = ("risk_contract", "riskContract", "empty_as_null_contract")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collection() -> Any | None:
    from services.control_plane_store import mongo_collection

    return mongo_collection(COLLECTION)


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "risk_contracts.json"


def _doc_id(risk_id: str, signature: str) -> str:
    return f"{risk_id}|{signature}"


def record(contract: Mapping[str, Any], *, origin: str, workspace_id: str = "") -> None:
    """Record one contract the server signed (or adopted from its own storage)."""
    risk_id, signature = str(contract.get("risk_id") or ""), str(contract.get("signature") or "")
    if not risk_id or not signature:
        return
    doc = {
        "risk_id": risk_id, "signature": signature, "origin": origin, "recorded_at": _now(),
        "workspace_id": workspace_id or str((contract.get("metadata") or {}).get("workspace_id") or ""),
        "kind": str(contract.get("loss_classification") or ""),
        "approved_by": str(contract.get("approved_by") or ""),
    }
    coll = _collection()
    if coll is not None:
        coll.update_one({"_id": _doc_id(risk_id, signature)}, {"$setOnInsert": doc}, upsert=True)
        return
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"contracts": {}}) as data:
        data.setdefault("contracts", {}).setdefault(_doc_id(risk_id, signature), doc)


def holds(risk_id: str, signature: str) -> bool:
    """True when the server recorded exactly this contract (id and digest)."""
    if not risk_id or not signature:
        return False
    try:
        coll = _collection()
        if coll is not None:
            return coll.find_one({"_id": _doc_id(risk_id, signature)}) is not None
        from services.metadata_backend import load_json_doc

        data = load_json_doc(_file_path(), {"contracts": {}})
        return _doc_id(risk_id, signature) in (data.get("contracts") or {})
    except Exception as exc:  # noqa: BLE001 - an unreadable store honours nothing (fail closed)
        logger.warning("risk contract store unreadable: %s", exc)
        return False


def cutover() -> datetime:
    from services.brand_env import getenv_brand

    raw = (getenv_brand("RISK_CONTRACT_CUTOVER", "") or "").strip() or LEGACY_CUTOVER
    at = datetime.fromisoformat(raw)
    return at if at.tzinfo else at.replace(tzinfo=timezone.utc)


def _stored_before_cutover(*stamps: Any) -> bool:
    """Every server-set timestamp of the record is before the cutover (an edit after it is not)."""
    seen = []
    for raw in stamps:
        if not raw:
            continue
        try:
            at = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return False
        seen.append(at if at.tzinfo else at.replace(tzinfo=timezone.utc))
    return bool(seen) and max(seen) < cutover()


def legacy_contracts_in(value: Any) -> list[dict[str, Any]]:
    """The unkeyed contracts nested anywhere in a stored record, whose digest is intact."""
    from services.migration_risk_contract import legacy_digest

    found: list[dict[str, Any]] = []
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            for key, child in item.items():
                if key in _CONTRACT_KEYS and isinstance(child, Mapping):
                    sig = str(child.get("signature") or "")
                    if sig.startswith(LEGACY_PREFIX) and sig == legacy_digest(dict(child)):
                        found.append(dict(child))
                stack.append(child)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return found


def adopt_stored(record_doc: Mapping[str, Any], *, origin: str) -> int:
    """Record the legacy contracts of one server-stored record created and last changed before the cutover."""
    if not _stored_before_cutover(record_doc.get("created_at"), record_doc.get("updated_at")):
        return 0
    contracts = legacy_contracts_in(record_doc)
    workspace = str(record_doc.get("workspace_id") or "")
    for c in contracts:
        record(c, origin=origin, workspace_id=workspace)
    return len(contracts)


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


def backfill_legacy_contracts() -> int:
    """Record the unkeyed contracts the server stored before the cutover. Idempotent."""
    adopted = 0
    for origin, doc in _stored_records():
        adopted += adopt_stored(doc, origin=f"legacy:{origin}")
    return adopted
