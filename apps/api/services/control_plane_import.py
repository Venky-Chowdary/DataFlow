"""Importing a node's file into an empty record store, under a lease (ops review rounds 2 and 3).

The first process to reach an empty shared store imports its local file. The
claim in the store's status document (``control_plane_docs`` ``_id: <store>``)
names its owner, an ``epoch`` (from a per-store counter, so every claim gets a
new, larger one) and a lease (``lease_until``) that the importer renews before
every record it writes. While a claim is held every replica fails closed for
that store.

The import is staged (round 3, Medium A). Each imported record carries
``stg: <epoch>`` and is invisible to every read, and to the check that decides
a store has data, until the import completes. Completing it is one write: the
claim becomes ``state: done`` for its epoch, and from then on the records
staged under that epoch are served. They are then unstaged (``stg`` removed),
and records staged under any other epoch, which can only be left-overs of an
abandoned import, are deleted. A reader sees all of an import or none of it.

A claim whose lease ran out belongs to a process that died or stalled:

* a process holding the same file joins the import under the same epoch and
  finishes it (a record already staged under the epoch is left as it is, so
  nothing is written twice);
* any other process takes the claim over (compare-and-swap on owner and
  lease), deletes the records staged under its epoch, leaves a tombstone
  (``import_abandoned:<store>:<epoch>``) and deletes the claim. The store is
  then empty, and a process with its own file imports that one under a new
  epoch.

A stalled holder that resumes cannot make a partial store look complete: a
record it writes late is staged under its abandoned epoch, so it is never
served and never replaces a record of a newer epoch, and it is deleted by the
holder itself (its next renewal finds the claim gone) or by the next import's
clean-up. Leases are timed on the database server's clock (``hello``'s
``localTime``), so a replica whose own clock runs ahead does not abandon a
live import.

``DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS`` (default 30, 0.5 to 600) sets
the lease. A record larger than the store's record limit is not imported: it
is logged and listed in the import report, and the import completes.
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
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
#: The field marking a record as staged by the import of that epoch.
STAGED = "stg"
TOMBSTONE_KIND = "import_abandoned"


def import_lease_seconds() -> float:
    try:
        value = float(getenv_brand("CONTROL_STORE_IMPORT_LEASE_SECONDS", "") or DEFAULT_LEASE_SECONDS)
    except ValueError:
        value = DEFAULT_LEASE_SECONDS
    return min(600.0, max(0.5, value))


def owner_token() -> str:
    """This process, as an import claim's owner (host, pid, and a nonce for pid reuse)."""
    return f"{_node_name()}:{os.getpid()}:{_NONCE}"


def server_now(docs: Any) -> float:
    """The database server's clock, in seconds since the epoch: leases are timed on it, never on a replica's."""
    stamp = (docs.database.command("hello") or {}).get("localTime")
    if not isinstance(stamp, datetime):
        logger.warning("control-plane database reported no server time; this import lease uses the local clock")
        return _sds._now()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.timestamp()


@dataclass(frozen=True)
class ImportTicket:
    """This process's part in one import: the file's digest, the claim's epoch, and the server clock."""

    digest: str
    epoch: int
    server_at_start: float
    local_at_start: float

    def lease_until(self) -> float:
        """Now on the server's clock (measured once, advanced by this host's monotonic clock), plus the lease."""
        return self.server_at_start + (_sds._clock() - self.local_at_start) + import_lease_seconds()


class ImportTakenOver(ControlPlaneUnavailable):
    """This process's import claim was taken over (its lease ran out): it stops writing."""


class LeasedImport:
    """The import half of :class:`services.shared_record_store.SharedRecordSet`."""

    name: str
    _settled: bool
    _cache: Any
    _imported: set[str]
    _seen_stamps: set[tuple[str, int, int]]
    #: The epoch of the store's completed import, whose staged records reads serve; None: no such import.
    _committed: int | None = None

    def _visible(self) -> Any:
        """The ``stg`` condition of a record a read may return: not staged, or staged by the completed import."""
        return None if self._committed is None else {"$in": [None, self._committed]}

    # -- the lease -----------------------------------------------------
    def _lease_left(self, meta: dict[str, Any], now: float) -> float:
        """Seconds left on the claim's lease at ``now`` (the server's clock)."""
        until = meta.get("lease_until")
        if until is None:
            # A claim written before leases existed: it runs from its time, or
            # from when this process first saw it.
            started = meta.get("at")
            if started is None:
                seen = self.__dict__.setdefault("_claims_first_seen", {})
                started = seen.setdefault(repr(meta.get("import_sources")), now)
            until = float(started) + import_lease_seconds()
        return float(until) - now

    def _next_epoch(self, docs: Any) -> int:
        """An epoch larger than that of every earlier claim on this store (only the claim's winner uses it)."""
        key = f"import_epoch:{self.name}"
        docs.update_one({"_id": key}, {"$inc": {"n": 1}, "$set": {"kind": "import_epoch", "store": self.name}},
                        upsert=True)
        return int((docs.find_one({"_id": key}) or {}).get("n") or 1)

    def _renew_lease(self, docs: Any, ticket: ImportTicket) -> bool:
        """Extend the claim before writing a record. False: another process already finished this import."""
        result = docs.update_one(
            {"_id": self.name, "state": "importing", "epoch": ticket.epoch},
            {"$set": {"lease_until": ticket.lease_until()}},
        )
        if getattr(result, "matched_count", 0):
            return True
        if self._import_done(docs, ticket):
            return False
        raise ImportTakenOver(f"the import into control-plane store {self.name} was taken over by another process")

    def _import_done(self, docs: Any, ticket: ImportTicket) -> bool:
        meta = docs.find_one({"_id": self.name}) or {}
        return meta.get("state") == "done" and meta.get("epoch") == ticket.epoch

    def _abandon(self, coll: Any, docs: Any, meta: dict[str, Any], now: float) -> bool:
        """Take over a claim whose lease ran out and remove what its import wrote. False: someone else acted first."""
        me = owner_token()
        taken = docs.update_one(
            {"_id": self.name, "state": meta.get("state"), "owner": meta.get("owner"),
             "lease_until": meta.get("lease_until")},
            {"$set": {"state": "abandoning", "owner": me, "node": _node_name(),
                      "lease_until": now + import_lease_seconds()}},
        )
        if not getattr(taken, "matched_count", 0):
            return False
        sources = [str(s) for s in meta.get("import_sources") or []]
        epoch = meta.get("epoch")
        if epoch is None:  # a claim of an earlier build: its records were written visible, stamped ``imp``
            removed = coll.delete_many({"store": self.name, "imp": {"$in": sources}})
        else:
            removed = coll.delete_many({"store": self.name, STAGED: epoch})
        report = {"action": "import_abandoned", "sha256": sources, "epoch": epoch,
                  "previous_owner": meta.get("owner"), "records_removed": getattr(removed, "deleted_count", None)}
        # The tombstone, for the operator: this epoch is never served (and never claimed again).
        tomb = epoch if epoch is not None else ",".join(sources)
        docs.update_one({"_id": f"{TOMBSTONE_KIND}:{self.name}:{tomb}"},
                        {"$set": {**report, "kind": TOMBSTONE_KIND, "store": self.name, "at": _sds._now()}},
                        upsert=True)
        docs.delete_one({"_id": self.name, "owner": me})
        logger.warning(
            "control-plane store %s: abandoned the import claimed by %s (lease expired %.0fs ago); removed the "
            "records it had staged. The store is empty again; a node holding its own file imports that one.",
            self.name, meta.get("owner") or meta.get("node") or "an earlier build", -self._lease_left(meta, now),
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
            now = server_now(docs)
            if self._lease_left(meta, now) > 0 or not self._abandon(coll, docs, meta, now):
                # Serving a half-imported ACL or tenant list would open what
                # the file restricts: fail closed until it is done.
                raise ControlPlaneUnavailable(f"control-plane store {self.name} is being imported; retry shortly")
            meta = None
        done = bool(meta) and meta.get("state") == "done" and meta.get("epoch") is not None
        self._committed = int(meta["epoch"]) if done else None
        if done and not meta.get("unstaged"):
            self._unstage(coll, docs, int(meta["epoch"]))  # its importer stopped after completing it
        self._settled = bool(meta) or coll.find_one({"store": self.name, STAGED: self._visible()}) is not None

    def _unstage(self, coll: Any, docs: Any, epoch: int) -> None:
        """After an import completed: delete other epochs' left-overs, then make its records ordinary ones."""
        coll.delete_many({"store": self.name, STAGED: {"$exists": True, "$ne": epoch}})
        coll.update_many({"store": self.name, STAGED: epoch}, {"$unset": {STAGED: ""}})
        docs.update_one({"_id": self.name, "state": "done", "epoch": epoch}, {"$set": {"unstaged": True}})

    # -- the import ------------------------------------------------------
    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
        """Import this node's file into the empty store (claim, staged records, done); else a conflict."""
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
        for _ in range(5):
            meta = docs.find_one({"_id": self.name})
            if meta is None:
                if coll.find_one({"store": self.name, STAGED: None}) is not None:
                    report = _record_conflict(docs, self.name, path, digest)
                    settled = True
                    break
                epoch = self._next_epoch(docs)
                now = server_now(docs)
                try:
                    docs.insert_one({"_id": self.name, "kind": "store", "import_sources": [digest],
                                     "state": "importing", "epoch": epoch, "node": _node_name(),
                                     "owner": owner_token(), "lease_until": now + import_lease_seconds(), "at": now})
                except Exception as exc:
                    if _is_duplicate(exc):
                        continue
                    raise
                meta = {"import_sources": [digest], "state": "importing", "epoch": epoch}
            sources = meta.get("import_sources") or []
            if meta.get("state") == "abandoning" or (meta.get("state") == "importing" and digest not in sources):
                # Another node's file is being imported (or abandoned): wait for
                # its lease, then take over and import this file instead.
                now = server_now(docs)
                if self._lease_left(meta, now) > 0 or not self._abandon(coll, docs, meta, now):
                    break
                continue
            if digest not in sources:
                report = _record_conflict(docs, self.name, path, digest)
                settled = True
                break
            if meta.get("state") == "importing":
                if meta.get("epoch") is None:
                    # A claim of an earlier build with this same file: give it an epoch, then finish it.
                    docs.update_one({"_id": self.name, "state": "importing", "epoch": {"$exists": False}},
                                    {"$set": {"epoch": self._next_epoch(docs)}})
                    continue
                ticket = ImportTicket(digest, int(meta["epoch"]), server_now(docs), _sds._clock())
                report = self._write_import(coll, docs, ticket, path)
            settled = True
            break
        if settled:
            self._imported.add(key)
            self._seen_stamps.add(stamp)
        self._cache = {}
        quiet = ("conflict_already_recorded", "import_finished_by_another_process")
        return report if report and report["action"] not in quiet else None

    def _write_import(self, coll: Any, docs: Any, ticket: ImportTicket, path: Path) -> dict[str, Any]:
        try:
            return self._stage_and_complete(coll, docs, ticket, path)
        except ImportTakenOver:
            # Taken over while this process stalled: whatever it staged late goes (the epoch is never served).
            coll.delete_many({"store": self.name, STAGED: ticket.epoch})
            raise

    def _stage_and_complete(self, coll: Any, docs: Any, ticket: ImportTicket, path: Path) -> dict[str, Any]:
        from services.shared_record_store import MAX_RECORD_BYTES

        records = self._records(self._file_load())  # type: ignore[attr-defined]
        # Left-overs of abandoned imports must not hold a unique claim this import's records need.
        coll.delete_many({"store": self.name, STAGED: {"$exists": True, "$lt": ticket.epoch}})
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
            if not self._renew_lease(docs, ticket):
                return {"action": "import_finished_by_another_process", "sha256": ticket.digest}
            self._write_imported(coll, docs, rkey, row, index,  # type: ignore[attr-defined]
                                 stamp=ticket.digest, epoch=ticket.epoch)
        # The one write that makes the whole import visible.
        done = docs.update_one(
            {"_id": self.name, "state": "importing", "epoch": ticket.epoch},
            {"$set": {"state": "done", "records": len(records) - len(skipped), "at": _sds._now()},
             "$unset": {"owner": "", "lease_until": ""}},
        )
        if not getattr(done, "matched_count", 0):
            if self._import_done(docs, ticket):  # a process that joined the import finished it first
                return {"action": "import_finished_by_another_process", "sha256": ticket.digest}
            raise ImportTakenOver(f"the import into control-plane store {self.name} was taken over")
        self._unstage(coll, docs, ticket.epoch)
        report = {"action": "imported", "sha256": ticket.digest, "records": len(records) - len(skipped),
                  "skipped_too_large": skipped}
        logger.warning("control-plane store %s: imported %s into the shared store (%s)", self.name, path, report)
        _audit_import(self.name, path, report)
        return report

    def _stage_record(self, coll: Any, doc: dict[str, Any], epoch: int) -> bool:
        """Write one record staged under ``epoch``. False: the claims index refused it.

        It replaces only a record staged under an older epoch (an abandoned
        import's left-over): never a visible record, and never one of this or
        a newer epoch, so a stalled importer that resumes cannot overwrite
        the current import, and a joining process does not write twice.
        """
        fields = {k: v for k, v in doc.items() if k != "_id"}
        update: dict[str, Any] = {"$set": {**fields, STAGED: epoch}}
        if "uniq" not in fields:
            update["$unset"] = {"uniq": ""}
        try:
            coll.update_one({"_id": doc["_id"], STAGED: {"$lt": epoch}}, update, upsert=True)
        except Exception as exc:
            if not _is_duplicate(exc):
                raise
            from services.shared_record_store import _claim_index_hit

            if _claim_index_hit(exc):
                return False
            # The record exists, visible or staged under this or a newer epoch: it stays as it is.
        return True
