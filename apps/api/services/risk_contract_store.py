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
_REPORT_ID = "_upgrade_report"
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


#: Keys under which an old audit event may name the server-owned record it was signed for.
_TARGET_KEYS = ("schedule_id", "plan_id", "job_id", "record_id")


def _record_workspace(kind: str, ref: str) -> str:
    """The workspace the server stored on the schedule / plan / job ``ref`` (never a client field)."""
    kinds = [kind] if kind != "record_id" else ["schedule_id", "plan_id", "job_id"]
    for k in kinds:
        if k == "schedule_id":
            from services.schedule_store import get_schedule

            found: Any = get_schedule(ref)
        elif k == "plan_id":
            from services.transfer_plan_store import _load_all

            found = next((p for p in _load_all() if str(getattr(p, "id", "")) == ref), None)
        else:
            from services.mongodb_service import get_mongodb_service

            mongo = get_mongodb_service()
            found = mongo.get_job(ref) if mongo is not None else None
        if found is not None:
            ws = found.get("workspace_id") if isinstance(found, Mapping) else getattr(found, "workspace_id", "")
            return str(ws or "")
    return ""


def _event_target(ev: Mapping[str, Any]) -> tuple[str, str] | None:
    d = ev.get("details") if isinstance(ev.get("details"), Mapping) else {}
    for key in _TARGET_KEYS:
        ref = str(d.get(key) or ev.get(key) or "").strip()
        if ref:
            return key, ref
    return None


def _audit_evidence(contract: Mapping[str, Any], workspace: str, limit: datetime) -> tuple[str, str]:
    """``(workspace to adopt it in, "")`` when the server's audit chain proves it signed this contract.

    A ``migration_risk_contract.signed`` event written before the cutover with
    the same risk id, approver and column (and digest when recorded), whose
    chain signature verifies, in either:

    * the same (non-empty) workspace as the stored record; or
    * no workspace (the pre-release code wrote none) but naming a schedule,
      plan or job: the workspace the server stored on that record.

    Otherwise ``("", reason)``: ``audited_without_target`` (the old event names
    nothing a workspace can be derived from) or ``not_audited``. Contract JSON
    inside a stored plan, job or schedule was written by an API caller and
    proves nothing by itself.
    """
    from services.audit_chain_keys import check_signature
    from services.audit_log import list_audit_events

    rid = str(contract.get("risk_id") or "")
    events = list_audit_events(action="migration_risk_contract.signed",
                               search=f"risk_contract:{rid}", limit=50)
    reason = "not_audited"
    for ev in events:
        d = ev.get("details") if isinstance(ev.get("details"), Mapping) else {}
        if str(d.get("risk_id") or "") != rid:
            continue
        if any(d.get(k) and str(d.get(k)) != str(contract.get(k) or "") for k in ("approved_by", "column")):
            continue
        if d.get("digest") and str(d.get("digest")) != str(contract.get("signature") or ""):
            continue
        try:
            at = _parse(ev.get("time") or ev.get("timestamp") or "")
        except ValueError:
            continue
        if at >= limit or check_signature(dict(ev))[0] != "ok":
            continue
        ev_ws = str(ev.get("workspace_id") or "")
        if ev_ws:
            if workspace and ev_ws == workspace:
                return ev_ws, ""
            continue
        target = _event_target(ev)
        derived = _record_workspace(*target) if target else ""
        if derived:
            return derived, ""
        reason = "audited_without_target"
    return "", reason


def _route_of(doc: Mapping[str, Any], m: Mapping[str, Any]) -> str:
    req = doc.get("transfer_request") if isinstance(doc.get("transfer_request"), Mapping) else doc
    src = req.get("source") if isinstance(req.get("source"), Mapping) else {}
    dst = req.get("destination") if isinstance(req.get("destination"), Mapping) else {}
    a = str(doc.get("source_table") or src.get("table") or "?")
    b = str(doc.get("dest_table") or doc.get("destination_table") or dst.get("table") or "?")
    return f"{a} → {b} ({m.get('source') or '?'} → {m.get('target') or '?'})"


def _mapping_contracts(doc: Mapping[str, Any]) -> list[tuple[Mapping[str, Any], dict[str, Any]]]:
    """(mapping, legacy contract) pairs anywhere in a stored record."""
    out = []
    stack: list[Any] = [doc]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            for c in legacy_contracts_in({k: v for k, v in item.items() if k in _CONTRACT_KEYS}):
                out.append((item, c))
            stack.extend(v for k, v in item.items() if k not in _CONTRACT_KEYS)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return out


def adopt_stored(record_doc: Mapping[str, Any], *, origin: str,
                 report: list[dict[str, Any]] | None = None) -> int:
    """Record (count newly) the legacy contracts of one stored record the server provably signed.

    The record must be created and last changed before the cutover; each
    contract needs the audit evidence of :func:`_audit_evidence`, and is
    recorded for that workspace only. Each one that is not adopted is appended
    to ``report`` (what an operator must re-approve before resuming it).
    """
    if not _stored_before_cutover(record_doc.get("created_at"), record_doc.get("updated_at")):
        return 0
    limit = cutover()
    if limit is None:
        return 0
    workspace = str(record_doc.get("workspace_id") or "")
    adopted = 0
    for mapping, c in _mapping_contracts(record_doc):
        ws, reason = _audit_evidence(c, workspace, limit)
        if ws:
            adopted += 1 if record(c, origin=origin, workspace_id=ws) else 0
        elif report is not None:
            report.append({
                "kind": origin.split(":")[-1], "record_id": str(record_doc.get("id") or record_doc.get("_id") or ""),
                "name": str(record_doc.get("name") or ""), "workspace_id": workspace,
                "route": _route_of(record_doc, mapping), "risk_id": str(c.get("risk_id") or ""),
                "approved_by": str(c.get("approved_by") or ""), "reason": reason,
            })
    return adopted


def upgrade_report() -> list[dict[str, Any]]:
    """Pre-release risk contracts the last startup scan could not prove: re-approve each one."""
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": _REPORT_ID})
        return list((doc or {}).get("entries") or [])
    from services.metadata_backend import load_json_doc

    return list(load_json_doc(_file_path(), {"contracts": {}}).get("upgrade_report") or [])


def _save_report(entries: list[dict[str, Any]]) -> None:
    coll = _collection()
    if coll is not None:
        coll.replace_one({"_id": _REPORT_ID}, {"_id": _REPORT_ID, "entries": entries, "at": _now()}, upsert=True)
        return
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"contracts": {}}) as data:
        data["upgrade_report"] = entries


def reapproval_warnings(mappings: Any) -> list[dict[str, Any]]:
    """One Validate warning per mapping whose pre-release contract this server cannot prove it signed."""
    from services.migration_risk_contract import verify_risk_contract

    out = []
    for m in mappings or []:
        if not isinstance(m, Mapping):
            continue
        for key in _CONTRACT_KEYS:
            c = m.get(key)
            if isinstance(c, Mapping) and str(c.get("signature") or "").startswith(LEGACY_PREFIX) \
                    and not verify_risk_contract(dict(c)):
                out.append({
                    "id": "risk_contract_reapprove",
                    "message": (f"{m.get('source') or '?'} → {m.get('target') or '?'}: its risk approval "
                                f"({c.get('risk_id')}, approved by {c.get('approved_by') or '?'}) predates this "
                                "upgrade and this server cannot prove it signed it, so it no longer counts. "
                                "Re-approve this risk once (it is listed in the upgrade report)."),
                    "details": {"risk_id": c.get("risk_id"), "column": m.get("source"), "target": m.get("target")},
                })
    return out


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
    """Record the cutover (first start only), then the pre-release contracts the server can prove it signed.

    Idempotent (a second start records nothing new). The contracts it cannot
    prove are saved as the upgrade report (:func:`upgrade_report`), replaced on
    every start.
    """
    record_cutover()
    adopted = 0
    report: list[dict[str, Any]] = []
    for origin, doc in _stored_records():
        adopted += adopt_stored(doc, origin=f"legacy:{origin}", report=report)
    _save_report(report)
    return adopted
