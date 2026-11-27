"""Which columns were the key when a key-blind Decision Artifact stamp was stored.

The feature/Cleanup (base) and PR #435 interim stamp digests leave each Map
row's key flag out, so the stamp alone cannot say whether the key changed
since it was taken: a stamp minted for a Map keyed on ``id`` matched the same
Map keyed on ``code`` (review 3 item 1). The current digest covers key flags;
these records are only for the stamps the previous releases minted.

A record is ``(workspace, stamp, destination connector, destination table) ->
key columns``, append-only (the first record stands; nothing overwrites it):

* a schedule saved by an earlier release: its stored rows are the rows saved
  with that stamp, so the first time this release reads it, before any edit
  (schedule update, or the scheduler building a run), it records their key;
* a schedule this release creates with a key-blind stamp gets a record with
  no key set: nothing shows which key that stamp was taken over, so a keyed
  Map on it must be validated again.

Execute (:func:`services.decision_kernel.execute_gate.enforce_decision_artifact`)
then accepts a key-blind stamp for a keyed Map only when the recorded key set
is the current one; with no record it refuses with "re-Validate this route".
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


def _record_id(workspace_id: str, stamp: str, connector_id: str, table: str) -> str:
    return "|".join(_norm(v) for v in (workspace_id, stamp, connector_id, table))


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
    stamp: str,
    dest_connector_id: str,
    dest_table: str,
    key_columns: list[str] | None,
) -> bool:
    """Record the key set of ``stamp`` unless one is recorded (``None``: unknown)."""
    if len(_norm(stamp)) != 64:
        return False
    rid = _record_id(workspace_id, stamp, dest_connector_id, dest_table)
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


def lookup(
    *, workspace_id: str, stamp: str, dest_connector_id: str, dest_table: str,
) -> frozenset[str] | None:
    """The recorded key set of ``stamp`` for this destination, or None (no record / unknown)."""
    rid = _record_id(workspace_id, stamp, dest_connector_id, dest_table)
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


def record_schedule(sched: Any, *, trusted_rows: bool) -> None:
    """Record a stored schedule's stamp: its rows' key set, or unknown.

    ``trusted_rows``: the schedule's stored rows are the rows saved with its
    stamp (a stored schedule read before any edit by this release). Never
    raises: without a record a keyed Map on a key-blind stamp is refused.
    """
    get = sched.get if isinstance(sched, dict) else (lambda k, d=None: getattr(sched, k, d))
    stamp = _norm(get("approved_decision_artifact_hash", ""))
    if len(stamp) != 64:
        return
    try:
        record_if_absent(
            workspace_id=str(get("workspace_id", "") or ""),
            stamp=stamp,
            dest_connector_id=str(get("dest_connector_id", "") or ""),
            dest_table=str(get("dest_table", "") or ""),
            key_columns=key_columns_of_rows(get("mappings", None) or []) if trusted_rows else None,
        )
    except Exception:  # noqa: BLE001 - no record means a keyed Map re-validates
        logger.warning("Could not record a schedule stamp's key columns", exc_info=True)


def lookup_for_request(request: Any, stamp: str) -> frozenset[str] | None:
    """The recorded key set of ``stamp`` for the request's destination, or None."""
    dest = getattr(request, "destination", None)
    try:
        return lookup(
            workspace_id=str(getattr(request, "workspace_id", "") or ""),
            stamp=stamp,
            dest_connector_id=str(getattr(dest, "connector_id", "") or ""),
            dest_table=str(getattr(dest, "table", "") or getattr(dest, "collection", "") or ""),
        )
    except Exception:  # noqa: BLE001 - an unreadable record is no record
        logger.warning("Stamp key records unreadable", exc_info=True)
        return None


__all__ = [
    "COLLECTION", "key_columns_of_rows", "lookup", "lookup_for_request",
    "record_if_absent", "record_schedule",
]
