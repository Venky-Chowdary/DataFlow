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
    if not key or key.startswith(_META_ID):
        return None
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": key})
        return {k: v for k, v in doc.items() if k != "_id"} if isinstance(doc, dict) else None
    record = _read_all().get(key)
    return dict(record) if isinstance(record, dict) else None


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
            ref = str(details.get("job_id") or event.get("resource") or "")
            yield {
                "digest": str(digest), "evidence_type": kind,
                "created_at": str(event.get("time") or ""),
                "ref_id": ref,
                # Export records filed with no workspace (review 3 item 5): the
                # job's own workspace.
                "workspace_id": str(event.get("workspace_id") or "") or job_workspace(ref),
            }


def _genuine_legacy_digest(pack: dict[str, Any]) -> str:
    """The pack's recomputed digest when its login-secret signature verifies, else ''.

    A stored ``content_sha256`` is not trusted (review 3 item 4): the body is
    hashed again and the HMAC checked, so a pack edited in the store, or
    signed with something else, is never recorded.
    """
    import hmac as _hmac

    from services.signed_proof_pack import _platform_secret, hmac_sha256_hex, pack_subject

    digest = digest_of(pack)
    if digest != str(pack.get("content_sha256") or "").strip().lower():
        return ""
    sig = pack.get("signature") if isinstance(pack.get("signature"), dict) else {}
    claimed = str(sig.get("value") or "")
    expected = hmac_sha256_hex(_platform_secret(), f"{digest}:{pack_subject(pack)}")
    return digest if claimed and _hmac.compare_digest(claimed, expected) else ""


def _before(created_at: str, cutover: datetime | None) -> bool:
    if cutover is None:
        return True
    when = _parse_time(created_at)
    return when is not None and when < cutover


def job_workspace(ref_id: str) -> str:
    """The workspace of the job ``ref_id`` names (``job:<id>`` or ``<id>``), or ''."""
    job_id = str(ref_id or "").strip()
    if job_id.startswith("job:"):
        job_id = job_id[4:]
    if not job_id:
        return ""
    try:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id)
    except Exception:  # noqa: BLE001 - unknown workspace; the caller decides
        logger.warning("legacy evidence register: job lookup failed", exc_info=True)
        return ""
    return str((job or {}).get("workspace_id") or "") if isinstance(job, dict) else ""


def _pack_record(pack: dict[str, Any], kind: str, ref_id: str, workspace_id: str,
                 created_at: str, cutover: datetime | None) -> dict[str, Any] | None:
    if not _legacy_signed(pack) or not _before(created_at, cutover):
        return None
    digest = _genuine_legacy_digest(pack)
    if not digest:
        return None
    return {"digest": digest, "evidence_type": kind,
            "created_at": created_at, "ref_id": ref_id, "workspace_id": workspace_id}


def _parity_records(cutover: datetime | None) -> Iterable[dict[str, Any]]:
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
                           str(signed.get("signed_at") or ""), cutover)
        if rec:
            yield rec


def _exit_records(cutover: datetime | None) -> Iterable[dict[str, Any]]:
    """Signed Exit Evidence Packs, held on each rehearsal."""
    from services import exit_store

    for doc in exit_store.list_docs(exit_store.REHEARSALS):
        signed = doc.get("signed") if isinstance(doc.get("signed"), dict) else {}
        rec = _pack_record(signed.get("pack") or {}, "exit_evidence_pack",
                           str(doc.get("id") or doc.get("_id") or ""),
                           str(doc.get("workspace_id") or ""), str(signed.get("signed_at") or ""),
                           cutover)
        if rec:
            yield rec


def _sources(cutover: datetime | None) -> list[tuple[str, Any]]:
    return [
        ("audit", lambda: _audit_records(cutover)),
        ("parity", lambda: _parity_records(cutover)),
        ("exit", lambda: _exit_records(cutover)),
    ]


#: A source that could not be read is retried at most this many times, and no
#: more often than every ``_RETRY_SECONDS``: a broken store never turns every
#: legacy verify into a whole-store rescan (review 3 item 4).
_MAX_RETRIES = 3
_RETRY_SECONDS = 15 * 60


def snapshot(cutover: datetime | None, only: set[str] | None = None) -> dict[str, Any]:
    """Record every legacy-signed evidence digest the server holds (idempotent).

    ``cutover``: evidence made at or after it is not legacy evidence. ``only``:
    the sources to read (a retry of the ones that failed). Returns the counts
    and the sources that could not be read.
    """
    counts: dict[str, int] = {}
    unreadable: list[str] = []
    for name, source in _sources(cutover):
        if only is not None and name not in only:
            continue
        try:
            for rec in source():
                key = _key(rec["digest"])
                if key and _insert_if_absent(key, {k: v for k, v in rec.items() if k != "digest"}):
                    counts[rec["evidence_type"]] = counts.get(rec["evidence_type"], 0) + 1
        except Exception as exc:  # noqa: BLE001 - recorded on the marker; retried, bounded
            logger.error("legacy evidence register: %s unreadable: %s", name, type(exc).__name__)
            unreadable.append(name)
    return {"taken_at": _now(), "cutover": cutover.isoformat() if cutover else None,
            "recorded": counts, "unreadable": unreadable}


def _meta(key: str) -> dict[str, Any] | None:
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": key})
        return {k: v for k, v in doc.items() if k != "_id"} if isinstance(doc, dict) else None
    record = _read_all().get(key)
    return dict(record) if isinstance(record, dict) else None


def _pending_retry(first: dict[str, Any]) -> tuple[int, set[str], datetime | None]:
    """``(attempt, sources still unread, when last tried)`` from the markers."""
    unread = set(first.get("unreadable") or [])
    last = _parse_time(first.get("taken_at"))
    attempt = 0
    while unread and attempt < _MAX_RETRIES:
        retry = _meta(f"{_META_ID}retry:{attempt + 1}")
        if retry is None:
            break
        attempt += 1
        unread = set(retry.get("unreadable") or [])
        last = _parse_time(retry.get("taken_at")) or last
    return attempt, unread, last


def ensure_snapshot(cutover: datetime | None = None) -> None:
    """Take the snapshot once (at the cutover or on first need); retry unread sources, bounded.

    The marker is written whatever happened, naming the sources that could
    not be read. Those alone are read again, at most ``_MAX_RETRIES`` times
    and no more often than every ``_RETRY_SECONDS``; each retry is its own
    append-only marker.
    """
    with _LOCK:
        first = _meta(_META_ID)
        if first is None:
            if cutover is None:
                from services.evidence_signing_keys import legacy_cutover_at

                cutover = legacy_cutover_at()
            _insert_if_absent(_META_ID, snapshot(cutover))
            return
        attempt, unread, last = _pending_retry(first)
        if not unread or attempt >= _MAX_RETRIES:
            return
        now = datetime.now(timezone.utc)
        if last is not None and (now - last).total_seconds() < _RETRY_SECONDS:
            return
        recorded_cutover = _parse_time(first.get("cutover"))
        _insert_if_absent(f"{_META_ID}retry:{attempt + 1}", snapshot(recorded_cutover, only=unread))


def snapshot_taken() -> bool:
    """True once the snapshot marker is written (some sources may still be retried)."""
    return _meta(_META_ID) is not None


def digest_of(payload: dict[str, Any]) -> str:
    """The content digest a signed body commits to (``content_sha256``)."""
    from services.signed_proof_pack import canonical_json, sha256_hex

    body = {k: v for k, v in payload.items() if k not in ("content_sha256", "signature")}
    return sha256_hex(canonical_json(body))


__all__ = ["COLLECTION", "digest_of", "ensure_snapshot", "lookup", "snapshot", "snapshot_taken"]
