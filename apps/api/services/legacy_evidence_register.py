"""The legacy evidence register: what was signed with the login secret before the cutover.

Before evidence signing keys existed, proof packs, Migration Certificates,
parity sign-off packs and Exit Evidence Packs were HMAC-signed with the login
token secret (``key_id`` ``platform_auth_secret``). Anyone holding that secret
can still compute such a signature today, so a login-secret signature alone
proves nothing. What does: the server recorded, before the cutover, that it
issued evidence with exactly this content digest.

This register is that record. At the cutover (the first evidence keyring,
:mod:`services.evidence_signing_keys`) the server snapshots the digest of every
legacy-signed evidence object it holds: proof packs and certificates from the
audit log's export records and chain anchors (the whole store, not the newest
20,000 records), signed parity sign-off packs from the job store, signed Exit
Evidence Packs from the exit store. A deployment that created its first keyring
before this module existed takes the snapshot on first need, against that
keyring's creation time. Records are append-only: written once, never changed
or deleted, and the audit retention purge does not touch this store.

A login-secret pack whose digest is not on record is refused: it was not
recorded before the cutover, so it cannot be told apart from a forgery. A pack
issued before the cutover whose records the audit retention had already purged
before the snapshot ran is in the same position; that is the honest limit.

Field-reduction ledgers are not snapshotted on their own: no route signs one
outside a proof pack, so the pack's record covers it.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

COLLECTION = "legacy_evidence_register"
_META_ID = "__snapshot__"
_LOCK = threading.Lock()

#: Audit actions whose records carry a pack's content digest.
_EXPORT_ACTIONS = {
    "proof_pack.export": "proof_pack",
    "migration_certificate.export": "migration_certificate",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001 - no database = file store
        return None


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "legacy_evidence_register.json"


def _key(digest: str) -> str:
    return str(digest or "").strip().lower()


def _read_all() -> dict[str, dict[str, Any]]:
    coll = _collection()
    if coll is not None:
        return {str(d["_id"]): {k: v for k, v in d.items() if k != "_id"} for d in coll.find({})}
    from services.metadata_backend import load_json_doc

    return dict(load_json_doc(_file_path(), {"records": {}}).get("records") or {})


def _insert_if_absent(key: str, doc: dict[str, Any]) -> bool:
    """Append ``doc`` under ``key`` unless a record is already there (never overwrite)."""
    coll = _collection()
    if coll is not None:
        from pymongo.errors import DuplicateKeyError

        try:
            coll.insert_one({"_id": key, **doc})
            return True
        except DuplicateKeyError:
            return False
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"records": {}}) as data:
        records = data.setdefault("records", {})
        if key in records:
            return False
        records[key] = doc
        return True


def lookup(digest: str) -> dict[str, Any] | None:
    """The register's record for ``digest``, or None."""
    key = _key(digest)
    if not key or key == _META_ID:
        return None
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": key})
        return {k: v for k, v in doc.items() if k != "_id"} if isinstance(doc, dict) else None
    record = _read_all().get(key)
    return dict(record) if isinstance(record, dict) else None


def snapshot_taken() -> bool:
    coll = _collection()
    if coll is not None:
        return coll.find_one({"_id": _META_ID}) is not None
    return _META_ID in _read_all()


def _parse_time(value: Any) -> datetime | None:
    from services.evidence_signing_keys import _parse_time as parse

    return parse(value)


def _legacy_signed(pack: Any) -> bool:
    from services.evidence_signing_keys import LEGACY_KEY_ID

    if not isinstance(pack, dict):
        return False
    sig = pack.get("signature") if isinstance(pack.get("signature"), dict) else {}
    return str(sig.get("key_id") or "") in ("", LEGACY_KEY_ID)


def _audit_events(actions: set[str]) -> Iterable[dict[str, Any]]:
    """Every audit record with one of ``actions``: the whole store, not a window."""
    from services import audit_log

    coll = audit_log._mongo_collection()
    if coll is not None:
        yield from coll.find({"action": {"$in": sorted(actions)}})
        return
    from services.audit_chain_window import _file_lines, _parse

    for line in _file_lines():
        event = _parse(line)
        if event is not None and str(event.get("action") or "") in actions:
            yield event


def _audit_records(cutover: datetime | None) -> Iterable[dict[str, Any]]:
    from services.evidence_chain import EVIDENCE_ACTION

    for event in _audit_events({*_EXPORT_ACTIONS, EVIDENCE_ACTION}):
        filed = _parse_time(event.get("time"))
        if cutover is not None and (filed is None or filed >= cutover):
            continue
        details = event.get("details") if isinstance(event.get("details"), dict) else {}
        action = str(event.get("action") or "")
        if action == EVIDENCE_ACTION:
            digest, kind = details.get("evidence_sha256"), "proof_pack_chain_anchor"
        else:
            digest, kind = details.get("content_sha256"), _EXPORT_ACTIONS[action]
        if digest:
            yield {
                "digest": str(digest), "evidence_type": kind,
                "created_at": str(event.get("time") or ""),
                "ref_id": str(details.get("job_id") or event.get("resource") or ""),
                "workspace_id": str(event.get("workspace_id") or ""),
            }


def _pack_record(pack: dict[str, Any], kind: str, ref_id: str, workspace_id: str,
                 created_at: str) -> dict[str, Any] | None:
    if not _legacy_signed(pack) or not pack.get("content_sha256"):
        return None
    return {"digest": str(pack["content_sha256"]), "evidence_type": kind,
            "created_at": created_at, "ref_id": ref_id, "workspace_id": workspace_id}


def _parity_records() -> Iterable[dict[str, Any]]:
    """Signed parity sign-off packs, held in each job's parity state."""
    from services.mongodb_service import get_mongodb_service

    mongo = get_mongodb_service()
    jobs: Iterable[dict[str, Any]]
    if type(mongo).__name__ == "MemoryMongoDBService":
        jobs = list(getattr(mongo, "_jobs", {}).values())
    else:
        jobs = mongo.get_database()["transfer_jobs"].find(
            {"parity.signed.pack": {"$exists": True}},
            {"job_id": 1, "workspace_id": 1, "parity.signed": 1},
        )
    for job in jobs:
        signed = ((job or {}).get("parity") or {}).get("signed") or {}
        rec = _pack_record(signed.get("pack") or {}, "parity_signoff_pack",
                           str(job.get("job_id") or ""), str(job.get("workspace_id") or ""),
                           str(signed.get("signed_at") or ""))
        if rec:
            yield rec


def _exit_records() -> Iterable[dict[str, Any]]:
    """Signed Exit Evidence Packs, held on each rehearsal."""
    from services import exit_store

    for doc in exit_store.list_docs(exit_store.REHEARSALS):
        signed = doc.get("signed") if isinstance(doc.get("signed"), dict) else {}
        rec = _pack_record(signed.get("pack") or {}, "exit_evidence_pack",
                           str(doc.get("id") or doc.get("_id") or ""),
                           str(doc.get("workspace_id") or ""), str(signed.get("signed_at") or ""))
        if rec:
            yield rec


def _sources(cutover: datetime | None) -> list[tuple[str, Any]]:
    return [
        ("audit", lambda: _audit_records(cutover)),
        ("parity", _parity_records),
        ("exit", _exit_records),
    ]


def snapshot(cutover: datetime | None) -> dict[str, Any]:
    """Record every legacy-signed evidence digest the server holds (idempotent).

    ``cutover``: audit records filed at or after it are not legacy evidence.
    A source that cannot be read is reported, not hidden; the snapshot marker
    is written only when every source was read, so a later call retries.
    """
    counts: dict[str, int] = {}
    unreadable: list[str] = []
    for name, source in _sources(cutover):
        try:
            for rec in source():
                key = _key(rec["digest"])
                if key and _insert_if_absent(key, {k: v for k, v in rec.items() if k != "digest"}):
                    counts[rec["evidence_type"]] = counts.get(rec["evidence_type"], 0) + 1
        except Exception as exc:  # noqa: BLE001 - reported; the snapshot retries later
            logger.error("legacy evidence register: %s unreadable: %s", name, type(exc).__name__)
            unreadable.append(name)
    meta = {"taken_at": _now(), "cutover": cutover.isoformat() if cutover else None,
            "recorded": counts, "unreadable": unreadable}
    if not unreadable:
        _insert_if_absent(_META_ID, meta)
    return meta


def ensure_snapshot(cutover: datetime | None = None) -> None:
    """Take the snapshot once: at the cutover, or on first need after it."""
    if snapshot_taken():
        return
    with _LOCK:
        if snapshot_taken():
            return
        if cutover is None:
            from services.evidence_signing_keys import legacy_cutover_at

            cutover = legacy_cutover_at()
        snapshot(cutover)


def digest_of(payload: dict[str, Any]) -> str:
    """The content digest a signed body commits to (``content_sha256``)."""
    from services.signed_proof_pack import canonical_json, sha256_hex

    body = {k: v for k, v in payload.items() if k not in ("content_sha256", "signature")}
    return sha256_hex(canonical_json(body))


__all__ = ["COLLECTION", "digest_of", "ensure_snapshot", "lookup", "snapshot", "snapshot_taken"]
