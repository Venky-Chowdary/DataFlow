"""Which columns were the key when a key-blind Decision Artifact stamp was stored.

The feature/Cleanup (base) and PR #435 interim stamp digests leave each Map
row's key flag out, so the stamp alone cannot say whether the key changed
since it was taken: a stamp minted for a Map keyed on ``id`` matched the same
Map keyed on ``code`` (review 3 item 1). The current digest covers key flags;
these records are only for the stamps the previous releases minted.

A record is ``(workspace, schedule id, stamp) -> key columns``, append-only
(the first record stands; nothing overwrites it):

* a schedule saved by an earlier release: its stored rows are the rows saved
  with that stamp, so the first time this release reads it, before any edit
  (every edit path records the unedited schedule first, then edits), it
  records their key;
* a schedule this release creates with a key-blind stamp, or an edit that
  brings one, gets a record with no key set; a failure to write it fails the
  save.

An edit of a schedule's route, mappings, key, sync mode or error handling
also drops a key-blind stamp (:mod:`services.schedule_store`), so the
schedule has to be validated again.

Execute (:func:`services.decision_kernel.execute_gate.enforce_decision_artifact`)
accepts a key-blind stamp only when the key set recorded for it is the
current one: the schedule's record for a schedule run, else the Decision
Artifact the server stored for that stamp on a job (its rows keep the key
flag the digest dropped). With neither it refuses ("re-Validate this route").
Stored in the shared control-plane collection when there is one, else in
``<data_dir>/decision_stamp_key_records.json``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

COLLECTION = "decision_stamp_key_records"
_FILE = "decision_stamp_key_records.json"
#: Stored for a record that names no trustworthy key set.
UNKNOWN = "__unknown__"


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _record_id(workspace_id: str, schedule_id: str, stamp: str) -> str:
    return "|".join(_norm(v) for v in (workspace_id, schedule_id, stamp))


def _path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / _FILE


def _collection() -> Any | None:
    from services.control_plane_store import mongo_collection

    return mongo_collection(COLLECTION)


def key_columns_of_rows(rows: Iterable[Any]) -> list[str]:
    """The key columns a saved Map's rows mark (destination names, lower case)."""
    from services.decision_kernel.models import _row_is_key

    return sorted({
        _norm(row.get("target") or row.get("source"))
        for row in rows or []
        if isinstance(row, dict) and _row_is_key(row) and _norm(row.get("target") or row.get("source"))
    })


def record_if_absent(
    *,
    workspace_id: str,
    schedule_id: str,
    stamp: str,
    key_columns: list[str] | None,
) -> bool:
    """Record the key set of ``stamp`` for one schedule unless one is recorded (``None``: unknown)."""
    if len(_norm(stamp)) != 64 or not _norm(schedule_id):
        return False
    rid = _record_id(workspace_id, schedule_id, stamp)
    doc = {"keys": UNKNOWN if key_columns is None else sorted({_norm(k) for k in key_columns})}
    coll = _collection()
    if coll is not None:
        from pymongo.errors import DuplicateKeyError

        try:
            coll.insert_one({"_id": rid, **doc})
            return True
        except DuplicateKeyError:
            return False
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), {"records": {}}) as data:
        records = data.setdefault("records", {})
        if rid in records:
            return False
        records[rid] = doc
        return True


def lookup(*, workspace_id: str, schedule_id: str, stamp: str) -> frozenset[str] | None:
    """The schedule's recorded key set for ``stamp``, or None (no record / unknown)."""
    rid = _record_id(workspace_id, schedule_id, stamp)
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": rid})
    else:
        from services.metadata_backend import load_json_doc

        doc = (load_json_doc(_path(), {"records": {}}).get("records") or {}).get(rid)
    keys = doc.get("keys") if isinstance(doc, dict) else None
    if not isinstance(keys, list):
        return None
    return frozenset(_norm(k) for k in keys)


def record_schedule(sched: Any, *, trusted_rows: bool, strict: bool = False) -> None:
    """Record a stored schedule's stamp: its rows' key set, or unknown.

    ``trusted_rows``: the schedule's stored rows are the rows saved with its
    stamp (a stored schedule read before any edit by this release).
    ``strict``: a failed write raises (a save must not go on without its
    record); otherwise it is logged, and a key-blind stamp then has no record.
    """
    get = sched.get if isinstance(sched, dict) else (lambda k, d=None: getattr(sched, k, d))
    stamp = _norm(get("approved_decision_artifact_hash", ""))
    if len(stamp) != 64:
        return
    try:
        record_if_absent(
            workspace_id=str(get("workspace_id", "") or ""),
            schedule_id=str(get("id", "") or ""),
            stamp=stamp,
            key_columns=key_columns_of_rows(get("mappings", None) or []) if trusted_rows else None,
        )
    except Exception:
        if strict:
            raise
        logger.warning("Could not record a schedule stamp's key columns", exc_info=True)


def _stored_artifacts(workspace_id: str, stamp: str) -> list[dict[str, Any]]:
    """Decision Artifacts the server stored on jobs under ``stamp`` (newest 50)."""
    from services.mongodb_service import get_mongodb_service

    mongo = get_mongodb_service()
    if type(mongo).__name__ == "MemoryMongoDBService":
        jobs = [j for j in getattr(mongo, "_jobs", {}).values()
                if isinstance(j, dict) and _norm(j.get("decision_artifact_hash")) == stamp
                and _norm(j.get("workspace_id")) == _norm(workspace_id)]
    else:
        jobs = list(mongo.get_database()["transfer_jobs"].find(
            {"decision_artifact_hash": stamp, "workspace_id": workspace_id or ""},
            {"decision_artifact": 1},
        ).sort("created_at", -1).limit(50))
    return [j["decision_artifact"] for j in jobs if isinstance(j.get("decision_artifact"), dict)]


def stored_artifact_keys(workspace_id: str, stamp: str) -> frozenset[str] | None:
    """The key set the stored Decision Artifacts of ``stamp`` agree on, or None.

    Only an artifact whose key-blind digest is ``stamp`` counts. Artifacts
    that disagree (the previous release let a key move under the same stamp)
    name no key set.
    """
    from services.decision_kernel.models import decision_key_columns, stamp_format

    sets = {
        decision_key_columns(art)
        for art in _stored_artifacts(workspace_id, _norm(stamp))
        if stamp_format(stamp, art) in ("base", "interim")
    }
    return next(iter(sets)) if len(sets) == 1 else None


def lookup_for_request(request: Any, stamp: str) -> frozenset[str] | None:
    """The key set recorded for ``stamp``: the schedule's record, else stored artifacts."""
    workspace = str(getattr(request, "workspace_id", "") or "")
    schedule_id = str(getattr(request, "decision_stamp_schedule_id", "") or "")
    try:
        if schedule_id:
            return lookup(workspace_id=workspace, schedule_id=schedule_id, stamp=stamp)
        return stored_artifact_keys(workspace, stamp)
    except Exception:  # noqa: BLE001 - an unreadable record is no record
        logger.warning("Stamp key records unreadable", exc_info=True)
        return None


__all__ = [
    "COLLECTION", "key_columns_of_rows", "lookup", "lookup_for_request",
    "record_if_absent", "record_schedule", "stored_artifact_keys",
]
