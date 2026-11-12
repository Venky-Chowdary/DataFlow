"""Importing a node's file into an empty record store, under a lease (ops review round 2, High 2).

The first process to reach an empty shared store imports its local file. While
it does, every replica fails closed for that store (a half-imported ACL or
tenant list must not be served as complete). The claim in the store's status
document (``control_plane_docs`` ``_id: <store>``) names its owner and holds a
lease (``lease_until``, wall-clock seconds) that the importer renews before
every record it writes.

A claim whose lease ran out belongs to a process that died or stalled:

* a process holding the same file joins the import and finishes it (records
  are written with ``$setOnInsert`` under deterministic ids, so nothing is
  written twice);
* any other process takes the claim over (compare-and-swap on owner and
  lease), removes the records that import wrote (each carries ``imp``, the
  file's digest) and deletes the claim. The store is then empty and usable,
  and a process with its own file imports that one.

``DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS`` (default 30, 0.5 to 600) sets
the lease. A record larger than the store's record limit is not imported: it
is logged and listed in the import report, and the import completes.
"""

from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path
from typing import Any

from services import shared_doc_store as _sds
from services.brand_env import getenv_brand
from services.control_plane_store import ControlPlaneUnavailable
from services.shared_doc_store import (
    DOCS_COLLECTION,
    _audit_import,
    _file_digest,
    _file_stamp,
    _is_duplicate,
    _node_name,
    _record_conflict,
)

logger = logging.getLogger(__name__)

LEASE_SETTING = "DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS"
DEFAULT_LEASE_SECONDS = 30.0
_NONCE = uuid.uuid4().hex[:8]
#: States in which a store's claim keeps every replica failing closed.
_BUSY = ("importing", "abandoning")


def import_lease_seconds() -> float:
    try:
        value = float(getenv_brand("CONTROL_STORE_IMPORT_LEASE_SECONDS", "") or DEFAULT_LEASE_SECONDS)
    except ValueError:
        value = DEFAULT_LEASE_SECONDS
    return min(600.0, max(0.5, value))


def owner_token() -> str:
    """This process, as an import claim's owner (host, pid, and a nonce for pid reuse)."""
    return f"{_node_name()}:{os.getpid()}:{_NONCE}"


class ImportTakenOver(ControlPlaneUnavailable):
    """This process's import claim was taken over (its lease ran out): it stops writing."""


class LeasedImport:
    """The import half of :class:`services.shared_record_store.SharedRecordSet`."""

    name: str
    _settled: bool
    _cache: Any
    _imported: set[str]
    _seen_stamps: set[tuple[str, int, int]]

    # -- the lease -----------------------------------------------------
    def _lease_left(self, meta: dict[str, Any]) -> float:
        until = meta.get("lease_until")
        if until is None:
            # A claim written before leases existed: it runs from its time, or
            # from when this process first saw it.
            started = meta.get("at")
            if started is None:
                seen = self.__dict__.setdefault("_claims_first_seen", {})
                started = seen.setdefault(repr(meta.get("import_sources")), _sds._now())
            until = float(started) + import_lease_seconds()
        return float(until) - _sds._now()

    def _renew_lease(self, docs: Any, digest: str) -> bool:
        """Extend the claim before writing a record. False: another process already finished this import."""
        result = docs.update_one(
            {"_id": self.name, "state": "importing", "import_sources": digest},
            {"$set": {"lease_until": _sds._now() + import_lease_seconds()}},
        )
        if getattr(result, "matched_count", 0):
            return True
        if self._import_done(docs, digest):
            return False
        raise ImportTakenOver(f"the import into control-plane store {self.name} was taken over by another process")

    def _import_done(self, docs: Any, digest: str) -> bool:
        meta = docs.find_one({"_id": self.name}) or {}
        return meta.get("state") == "done" and digest in (meta.get("import_sources") or [])

    def _abandon(self, coll: Any, docs: Any, meta: dict[str, Any]) -> bool:
        """Take over a claim whose lease ran out and remove what its import wrote. False: someone else acted first."""
        me = owner_token()
        taken = docs.update_one(
            {"_id": self.name, "state": meta.get("state"), "owner": meta.get("owner"),
             "lease_until": meta.get("lease_until")},
            {"$set": {"state": "abandoning", "owner": me, "node": _node_name(),
                      "lease_until": _sds._now() + import_lease_seconds()}},
        )
        if not getattr(taken, "matched_count", 0):
            return False
        sources = [str(s) for s in meta.get("import_sources") or []]
        removed = coll.delete_many({"store": self.name, "imp": {"$in": sources}})
        docs.delete_one({"_id": self.name, "owner": me})
        report = {"action": "import_abandoned", "sha256": sources, "previous_owner": meta.get("owner"),
                  "records_removed": getattr(removed, "deleted_count", None)}
        logger.warning(
            "control-plane store %s: abandoned the import claimed by %s (lease expired %.0fs ago); removed the "
            "records it had written. The store is empty again; a node holding its own file imports that one.",
            self.name, meta.get("owner") or meta.get("node") or "an earlier build", -self._lease_left(meta),
        )
        _audit_import(self.name, Path(str(meta.get("node") or "unknown")), report, abandoned=True)
        self._cache = {}
        return True

    # -- reads -----------------------------------------------------------
    def _refuse_while_importing(self, coll: Any) -> None:
        """Fail closed while another process imports its file into this empty store.

        Checked on a read from the database (not on a cached answer) until the
        store has records or a finished import: then it can no longer start.
        A claim whose lease ran out is abandoned instead of waited on.
        """
        if self._settled:
            return
        docs = coll.database[DOCS_COLLECTION]
        meta = docs.find_one({"_id": self.name})
        if meta and meta.get("state") in _BUSY:
            if self._lease_left(meta) > 0 or not self._abandon(coll, docs, meta):
                # Serving a half-imported ACL or tenant list would open what
                # the file restricts: fail closed until it is done.
                raise ControlPlaneUnavailable(f"control-plane store {self.name} is being imported; retry shortly")
            meta = None
        self._settled = bool(meta) or coll.find_one({"store": self.name}) is not None

    # -- the import ------------------------------------------------------
    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
        """Import this node's file into the empty store (claim, records, done); else a conflict."""
        path = self._file_path()  # type: ignore[attr-defined]
        stamp = _file_stamp(path)
        if stamp is None or stamp in self._seen_stamps:
            return None
        found = _file_digest(path)
        if found is None:
            return None
        digest = found[0]
        key = f"{path}:{digest}"
        if key in self._imported:
            self._seen_stamps.add(stamp)
            return None
        docs = coll.database[DOCS_COLLECTION]
        report: dict[str, Any] | None = None
        settled = False
        for _ in range(4):
            meta = docs.find_one({"_id": self.name})
            if meta is None:
                if coll.find_one({"store": self.name}) is not None:
                    report = _record_conflict(docs, self.name, path, digest)
                    settled = True
                    break
                now = _sds._now()
                try:
                    docs.insert_one({"_id": self.name, "kind": "store", "import_sources": [digest],
                                     "state": "importing", "node": _node_name(), "owner": owner_token(),
                                     "lease_until": now + import_lease_seconds(), "at": now})
                except Exception as exc:
                    if _is_duplicate(exc):
                        continue
                    raise
                meta = {"import_sources": [digest], "state": "importing"}
            sources = meta.get("import_sources") or []
            if meta.get("state") == "abandoning" or (meta.get("state") == "importing" and digest not in sources):
                # Another node's file is being imported (or abandoned): wait for
                # its lease, then take over and import this file instead.
                if self._lease_left(meta) > 0 or not self._abandon(coll, docs, meta):
                    break
                continue
            if digest not in sources:
                report = _record_conflict(docs, self.name, path, digest)
                settled = True
                break
            if meta.get("state") == "importing":
                report = self._write_import(coll, docs, digest, path)
            settled = True
            break
        if settled:
            self._imported.add(key)
            self._seen_stamps.add(stamp)
        self._cache = {}
        quiet = ("conflict_already_recorded", "import_finished_by_another_process")
        return report if report and report["action"] not in quiet else None

    def _write_import(self, coll: Any, docs: Any, digest: str, path: Path) -> dict[str, Any]:
        from services.shared_record_store import MAX_RECORD_BYTES

        records = self._records(self._file_load())  # type: ignore[attr-defined]
        skipped: list[str] = []
        for index, (rkey, row) in enumerate(records.items()):
            size = len(row[1].encode("utf-8"))
            if size > MAX_RECORD_BYTES:
                skipped.append(rkey)
                logger.error(
                    "control-plane store %s: record %s in %s is %d KB, over the %d KB record limit; it is not "
                    "imported (the rest of the file is).", self.name, rkey, path, size // 1024, MAX_RECORD_BYTES // 1024,
                )
                continue
            if not self._renew_lease(docs, digest):
                return {"action": "import_finished_by_another_process", "sha256": digest}
            self._write_imported(coll, docs, rkey, row, index, stamp=digest)  # type: ignore[attr-defined]
        done = docs.update_one(
            {"_id": self.name, "state": "importing", "import_sources": digest},
            {"$set": {"state": "done", "records": len(records) - len(skipped), "at": _sds._now()},
             "$unset": {"owner": "", "lease_until": ""}},
        )
        if not getattr(done, "matched_count", 0):
            if self._import_done(docs, digest):  # a process that joined the import finished it first
                return {"action": "import_finished_by_another_process", "sha256": digest}
            raise ImportTakenOver(f"the import into control-plane store {self.name} was taken over")
        report = {"action": "imported", "sha256": digest, "records": len(records) - len(skipped),
                  "skipped_too_large": skipped}
        logger.warning("control-plane store %s: imported %s into the shared store (%s)", self.name, path, report)
        _audit_import(self.name, path, report)
        return report
