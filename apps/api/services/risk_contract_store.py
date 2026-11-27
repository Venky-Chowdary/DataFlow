"""The server's own record of the Migration Risk Contracts it signed.

Every contract :func:`services.migration_risk_contract.create_migration_risk_contract`
signs is recorded here by ``risk_id`` and signature digest, with its workspace,
kind and origin, so a contract can be looked up by id + digest.

Contracts signed before the keyed signature (``mrc-sha256:``, an unkeyed digest
anyone could recompute) are honoured only when this store holds that exact
contract: :func:`backfill_legacy_contracts` records the ones found in records
the server itself stored before the recorded cutover (schedules, transfer
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
#: The cutover is the moment this release first started against the store:
#: recorded once (first writer wins, so every replica reads the same one) by
#: :func:`record_cutover`. Records the server stored before it may carry
#: unkeyed contracts the old code created; anything stored later must carry a
#: keyed one. DATAFLOW_RISK_CONTRACT_CUTOVER (ISO-8601) is only an explicit
#: operator override.
_CUTOVER_ID = "_cutover"
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


def record(contract: Mapping[str, Any], *, origin: str, workspace_id: str = "") -> bool:
    """Record one contract the server signed (or adopted); ``True`` when it was not recorded before."""
    risk_id, signature = str(contract.get("risk_id") or ""), str(contract.get("signature") or "")
    if not risk_id or not signature:
        return False
    doc = {
        "risk_id": risk_id, "signature": signature, "origin": origin, "recorded_at": _now(),
        "workspace_id": workspace_id or str((contract.get("metadata") or {}).get("workspace_id") or ""),
        "kind": str(contract.get("loss_classification") or ""),
        "approved_by": str(contract.get("approved_by") or ""),
    }
    coll = _collection()
    if coll is not None:
        res = coll.update_one({"_id": _doc_id(risk_id, signature)}, {"$setOnInsert": doc}, upsert=True)
        return res.upserted_id is not None
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"contracts": {}}) as data:
        contracts = data.setdefault("contracts", {})
        if _doc_id(risk_id, signature) in contracts:
            return False
        contracts[_doc_id(risk_id, signature)] = doc
        return True


def holds(risk_id: str, signature: str, *, workspace: str | None = None) -> bool:
    """True when the server recorded exactly this contract (id and digest).

    With ``workspace``, only in that workspace: the recorded workspace must
    equal it exactly, and an empty workspace never matches.
    """
    if not risk_id or not signature:
        return False
    try:
        coll = _collection()
        if coll is not None:
            doc = coll.find_one({"_id": _doc_id(risk_id, signature)})
        else:
            from services.metadata_backend import load_json_doc

            data = load_json_doc(_file_path(), {"contracts": {}})
            doc = (data.get("contracts") or {}).get(_doc_id(risk_id, signature))
    except Exception as exc:  # noqa: BLE001 - an unreadable store honours nothing (fail closed)
        logger.warning("risk contract store unreadable: %s", exc)
        return False
    if doc is None:
        return False
    if workspace is None:
        return True
    bound = str(doc.get("workspace_id") or "")
    return bool(bound) and bound == str(workspace or "")


def _parse(raw: Any) -> datetime:
    at = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return at if at.tzinfo else at.replace(tzinfo=timezone.utc)


def _override() -> datetime | None:
    from services.brand_env import getenv_brand

    raw = (getenv_brand("RISK_CONTRACT_CUTOVER", "") or "").strip()
    return _parse(raw) if raw else None


def recorded_cutover() -> datetime | None:
    """The cutover this store recorded, or ``None`` before this release first started."""
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": _CUTOVER_ID})
        return _parse(doc["at"]) if doc and doc.get("at") else None
    from services.metadata_backend import load_json_doc

    raw = (load_json_doc(_file_path(), {"contracts": {}}).get("cutover") or {}).get("at")
    return _parse(raw) if raw else None


def record_cutover() -> datetime:
    """Record now as the cutover unless one is recorded already (first writer wins); return it."""
    now = _now()
    coll = _collection()
    if coll is not None:
        coll.update_one({"_id": _CUTOVER_ID}, {"$setOnInsert": {"at": now}}, upsert=True)
        return _parse(coll.find_one({"_id": _CUTOVER_ID})["at"])
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"contracts": {}}) as data:
        doc = data.setdefault("cutover", {"at": now})
        return _parse(doc["at"])


def cutover() -> datetime | None:
    """The operator's override, else the recorded cutover (``None`` when none is recorded yet)."""
    return _override() or recorded_cutover()


def _stored_before_cutover(*stamps: Any) -> bool:
    """Every server-set timestamp of the record is before the cutover (an edit after it is not)."""
    limit = cutover()
    if limit is None:
        return False  # no cutover recorded yet: nothing is adopted
    seen = []
    for raw in stamps:
        if not raw:
            continue
        try:
            seen.append(_parse(raw))
        except ValueError:
            return False
    return bool(seen) and max(seen) < limit


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


def _audited_before(contract: Mapping[str, Any], workspace: str, limit: datetime) -> bool:
    """The server's own audit chain holds a ``migration_risk_contract.signed`` event for this contract.

    Same workspace (never an empty one), same risk id, approver and column
    (and digest when the event recorded one), written before the cutover, and
    its chain signature verifies. Contract JSON inside a stored plan, job or
    schedule was written by an API caller and proves nothing by itself.
    """
    if not workspace:
        return False
    from services.audit_chain_keys import check_signature
    from services.audit_log import list_audit_events

    rid = str(contract.get("risk_id") or "")
    events = list_audit_events(action="migration_risk_contract.signed", workspace_id=workspace,
                               search=f"risk_contract:{rid}", limit=50)
    for ev in events:
        d = ev.get("details") if isinstance(ev.get("details"), Mapping) else {}
        if str(ev.get("workspace_id") or "") != workspace or str(d.get("risk_id") or "") != rid:
            continue
        if any(d.get(k) and str(d.get(k)) != str(contract.get(k) or "") for k in ("approved_by", "column")):
            continue
        if d.get("digest") and str(d.get("digest")) != str(contract.get("signature") or ""):
            continue
        try:
            at = _parse(ev.get("time") or ev.get("timestamp") or "")
        except ValueError:
            continue
        if at < limit and check_signature(dict(ev))[0] == "ok":
            return True
    return False


def adopt_stored(record_doc: Mapping[str, Any], *, origin: str) -> int:
    """Record (count newly) the legacy contracts of one stored record the server provably signed before the cutover.

    The record must be created and last changed before the cutover, belong to a
    workspace, and each contract must have its own pre-cutover audit event
    (:func:`_audited_before`). Anything else is left unrecorded: refused, to be
    re-approved.
    """
    if not _stored_before_cutover(record_doc.get("created_at"), record_doc.get("updated_at")):
        return 0
    limit = cutover()
    workspace = str(record_doc.get("workspace_id") or "")
    adopted = 0
    for c in legacy_contracts_in(record_doc):
        if limit is not None and _audited_before(c, workspace, limit) \
                and record(c, origin=origin, workspace_id=workspace):
            adopted += 1
    return adopted


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
    """Record the cutover (first start only), then the unkeyed contracts stored before it. Idempotent."""
    record_cutover()
    adopted = 0
    for origin, doc in _stored_records():
        adopted += adopt_stored(doc, origin=f"legacy:{origin}")
    return adopted
