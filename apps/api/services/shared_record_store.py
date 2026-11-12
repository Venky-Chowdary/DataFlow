"""Record-list control-plane stores: one MongoDB document per record (ops review High 3).

Tenants, BYOK key records, resource ACL grants, notification channels and
shape drafts were each one shared document holding every tenant's records.
MongoDB caps a document at 16 MB, so one workspace saving large drafts could
stop every other workspace's writes to the same store, and every write
rewrote (and contended on) the whole store.

:class:`SharedRecordSet` keeps the store's interface (``load()`` returns the
whole store in its file shape, ``save()`` takes it back) and stores it in
``control_plane_items`` as one document per record::

    {_id: "<store>:<record id>", store, key, part, rev, seq, body}

``save`` writes only the records that changed, each by compare-and-swap on
its own ``rev``. Records belong to a partition (the tenant or workspace they
carry).

Invariants that span records (one tenant per workspace, one grant per
principal and resource, one active BYOK key per tenant) are enforced by the
database, not by the order of a check and an insert: each record family
declares its unique claims (``unique=``), stored as ``uniq`` and guarded by a
partial unique index on ``(store, uniq)``. Whatever the timing, the second
record making a claim is refused (:class:`UniqueConflict`); ``retrying``
then reads again, so the losing caller's own check sees the winner (ops review
round 2, High 1). Records written before the claims existed are backfilled
once; a duplicate already in the database keeps working, loses the claim and
is reported in the store's status (``duplicate_records``). The per-partition
fence documents an earlier build wrote are ignored.

The store's status document (``control_plane_docs`` ``_id: <store>``) holds
the import claim of a node's file (only while the store is empty, see
:mod:`services.shared_doc_store`). A whole-store document written by an
earlier build of this feature is split into records on first load,
idempotently, and its ``body`` is then removed.

A record whose JSON is larger than :data:`MAX_RECORD_BYTES` is refused with
:class:`RecordTooLarge` before anything is written.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from services import shared_doc_store as _sds
from services.shared_doc_store import (
    DOCS_COLLECTION,
    SharedDoc,
    SharedDocConflict,
    UniqueConflict,
    _audit_import,
    _dumps,
    _file_digest,
    _file_stamp,
    _is_duplicate,
    _node_name,
    _record_conflict,
    tracked,
)

logger = logging.getLogger(__name__)

ITEMS_COLLECTION = "control_plane_items"
MAX_RECORD_BYTES = 1024 * 1024
DUPLICATE_KIND = "duplicate_record"
#: The partial unique index that enforces every store's claims.
UNIQUE_INDEX = "store_uniq_unique"

__all__ = ["ITEMS_COLLECTION", "MAX_RECORD_BYTES", "RecordTooLarge", "SharedRecordSet", "UniqueConflict"]


class RecordTooLarge(ValueError):
    """One record is larger than a control-plane store accepts."""


class ReadToken:
    """What a load saw: each record's body, revision and partition."""

    __slots__ = ("bodies", "revs", "parts")

    def __init__(self, bodies: dict[str, str], revs: dict[str, int], parts: dict[str, str]) -> None:
        self.bodies = bodies
        self.revs = revs
        self.parts = parts


def _claim_index_hit(exc: Exception) -> bool:
    """A duplicate-key error raised by the claims index (not by ``_id``)."""
    details = getattr(exc, "details", None) or {}
    pattern = details.get("keyPattern") or {}
    return "uniq" in pattern or UNIQUE_INDEX in str(details.get("index") or exc)


class SharedRecordSet(SharedDoc):
    """A record-list store, one document per record in MongoDB (see module docstring).

    ``list_key`` names the list in the store's shape (``{"tenants": [...]}``);
    None means the store is a mapping of record key to record (shape drafts).
    ``id_key`` is the record id inside a listed record, ``partition_key`` the
    field naming the tenant or workspace a record belongs to. ``unique`` maps
    a record to the claims no other record of the store may hold.
    """

    kind = "records"

    def __init__(
        self,
        name: str,
        *,
        file_load: Callable[[], Any],
        file_save: Callable[[Any], None],
        file_path: Callable[[], Path],
        empty: Callable[[], Any] = dict,
        list_key: str | None = None,
        id_key: str = "id",
        partition_key: str = "workspace_id",
        unique: Callable[[dict[str, Any]], list[str]] | None = None,
    ) -> None:
        super().__init__(name, file_load=file_load, file_save=file_save, file_path=file_path, empty=empty)
        self._list_key = list_key
        self._id_key = id_key
        self._partition_key = partition_key
        self._unique = unique
        # The database the indexes and the claims backfill were done for (a test may switch databases).
        self._indexed: Any = None
        self._backfilled: Any = None
        self._split_checked = False
        self._settled = False

    # -- shape ---------------------------------------------------------
    def _item_id(self, key: str) -> str:
        return f"{self.name}:{key}"

    def _partition(self, record: Any) -> str:
        return str(record.get(self._partition_key) or "") if isinstance(record, dict) else ""

    def _claims(self, record: Any) -> list[str]:
        if self._unique is None or not isinstance(record, dict):
            return []
        return sorted({str(c) for c in self._unique(record) if c})

    def _records(self, data: Any) -> dict[str, tuple[str, str, list[str]]]:
        """``{key: (partition, body, claims)}`` for the store in its file shape."""
        out: dict[str, tuple[str, str, list[str]]] = {}
        if self._list_key is None:
            for key, record in (data.items() if isinstance(data, dict) else ()):
                out[str(key)] = (self._partition(record), _dumps(record), self._claims(record))
            return out
        rows = data.get(self._list_key) if isinstance(data, dict) else None
        for record in rows or []:
            if not isinstance(record, dict):
                continue
            body = _dumps(record)
            key = str(record.get(self._id_key) or "") or "sha256-" + hashlib.sha256(body.encode()).hexdigest()[:32]
            if key in out:
                logger.warning("control-plane store %s: duplicate record id %s; the first is kept", self.name, key)
                continue
            out[key] = (self._partition(record), body, self._claims(record))
        return out

    def _assemble(self, ordered: list[tuple[str, str]]) -> Any:
        if self._list_key is None:
            return {key: json.loads(body) for key, body in ordered}
        data = self._empty()
        data[self._list_key] = [json.loads(body) for _key, body in ordered]
        return data

    # -- reads ---------------------------------------------------------
    def load(self) -> Any:
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            return tracked(self._file_load(), None)
        self._prepare(coll)
        ttl = self._ttl()
        cached = self._cache
        if (
            cached is not None
            and ttl > 0
            and not getattr(self._scope, "depth", 0)
            and _sds._clock() - cached[0] < ttl
        ):
            items = cached[1]
        else:
            self._refuse_while_importing(coll)
            items = self._read(coll)
            self._cache = (_sds._clock(), items)
        return self._tracked(items)

    @staticmethod
    def _ttl() -> float:
        return _sds._cache_seconds()

    def _read(self, coll: Any) -> dict[str, tuple[int, str, int, str]]:
        items: dict[str, tuple[int, str, int, str]] = {}
        for doc in coll.find({"store": self.name}):
            if "fence" in doc:  # a per-partition fence an earlier build wrote: not a record
                continue
            items[str(doc.get("key"))] = (
                int(doc.get("seq") or 0), str(doc.get("part") or ""), int(doc.get("rev") or 0), str(doc.get("body") or "{}"),
            )
        return items

    def _tracked(self, items: dict[str, tuple[int, str, int, str]]) -> Any:
        ordered = sorted(items.items(), key=lambda kv: (kv[1][0], kv[0]))
        token = ReadToken(
            {key: row[3] for key, row in ordered},
            {key: row[2] for key, row in ordered},
            {key: row[1] for key, row in ordered},
        )
        return tracked(self._assemble([(key, row[3]) for key, row in ordered]), token)

    # -- writes --------------------------------------------------------
    def save(self, data: Any, *, rev: Any = None) -> None:
        """Write the records that changed since ``data`` was loaded.

        A record changed or deleted by another writer since this caller's
        read raises :class:`SharedDocConflict`, and a record whose unique
        claim another record holds raises :class:`UniqueConflict`
        (``retrying`` reads again and re-applies). A plain dict that was
        never loaded replaces whatever is there.
        """
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            self._file_save(data)
            return
        new = self._records(data)
        for key, (_part, body, _claims) in new.items():
            size = len(body.encode("utf-8"))
            if size > MAX_RECORD_BYTES:
                raise RecordTooLarge(
                    f"A {self.name} record is {size // 1024} KB; the limit is {MAX_RECORD_BYTES // 1024} KB."
                )
        token = rev if rev is not None else getattr(data, "rev", None)
        self._cache = None
        if self._indexed != self._where(coll):
            self._ensure_indexes(coll)
        blind = not isinstance(token, ReadToken)
        if blind:
            items = self._read(coll)
            token = ReadToken(
                {k: v[3] for k, v in items.items()}, {k: v[2] for k, v in items.items()},
                {k: v[1] for k, v in items.items()},
            )
        changed = [k for k in new if k in token.bodies and token.bodies[k] != new[k][1]]
        added = [k for k in new if k not in token.bodies]
        removed = [k for k in token.bodies if k not in new]
        if not (changed or added or removed):
            return
        seq = time.time_ns()
        for key in changed:
            flt: dict[str, Any] = {"_id": self._item_id(key)}
            if not blind:
                flt["rev"] = token.revs[key]
            try:
                result = coll.update_one(flt, self._record_update(new[key]))
            except Exception as exc:
                self._raise_if_claim_lost(exc, key)
                raise
            if not getattr(result, "matched_count", 0):
                raise SharedDocConflict(f"{self.name}:{key}")
            token.revs[key] = token.revs[key] + 1
        for key in removed:
            flt = {"_id": self._item_id(key)}
            if not blind:
                flt["rev"] = token.revs[key]
            result = coll.delete_one(flt)
            if not blind and not getattr(result, "deleted_count", 0):
                raise SharedDocConflict(f"{self.name}:{key}")
            for table in (token.bodies, token.revs, token.parts):
                table.pop(key, None)
        for index, key in enumerate(added):
            doc = self._record_doc(key, new[key], seq + index)
            try:
                coll.insert_one(doc)
            except Exception as exc:
                self._raise_if_claim_lost(exc, key)
                if not _is_duplicate(exc):
                    raise
                if not blind:
                    raise SharedDocConflict(f"{self.name}:{key}") from exc
                try:
                    coll.update_one({"_id": doc["_id"]}, self._record_update(new[key]))
                except Exception as again:
                    self._raise_if_claim_lost(again, key)
                    raise
            token.revs[key] = 1
            token.parts[key] = new[key][0]
        for key in changed + added:
            token.bodies[key] = new[key][1]
        if hasattr(data, "rev"):
            data.rev = token

    def _record_doc(self, key: str, row: tuple[str, str, list[str]], seq: int) -> dict[str, Any]:
        doc = {"_id": self._item_id(key), "store": self.name, "key": key, "part": row[0], "rev": 1,
               "seq": seq, "body": row[1], "updated_at": _sds._now()}
        if row[2]:
            doc["uniq"] = row[2]
        return doc

    @staticmethod
    def _record_update(row: tuple[str, str, list[str]]) -> dict[str, Any]:
        update: dict[str, Any] = {"$set": {"body": row[1], "part": row[0], "updated_at": _sds._now()}, "$inc": {"rev": 1}}
        if row[2]:
            update["$set"]["uniq"] = row[2]
        else:
            update["$unset"] = {"uniq": ""}
        return update

    def _raise_if_claim_lost(self, exc: Exception, key: str) -> None:
        if _is_duplicate(exc) and _claim_index_hit(exc):
            raise UniqueConflict(f"{self.name}:{key}: another record holds the same unique claim") from exc

    # -- first load: indexes, claims, split of an earlier whole document, import --
    @staticmethod
    def _where(coll: Any) -> Any:
        return getattr(coll, "full_name", None) or id(coll.database)

    def _prepare(self, coll: Any) -> None:
        here = self._where(coll)
        if self._indexed != here:
            self._ensure_indexes(coll)
        docs = coll.database[DOCS_COLLECTION]
        if not self._split_checked:
            self._split_whole_document(coll, docs)
        if self._indexed == here and self._backfilled != here:
            self._backfill_claims(coll, docs)
        self._import_local_file(coll)

    def _ensure_indexes(self, coll: Any) -> None:
        """The partition index and the claims index. Without the claims index the invariants are unguarded."""
        from services.control_plane_store import ControlPlaneUnavailable

        try:
            coll.create_index([("store", 1)])
            coll.create_index(
                [("store", 1), ("uniq", 1)], name=UNIQUE_INDEX, unique=True,
                partialFilterExpression={"uniq": {"$exists": True}},
            )
        except ControlPlaneUnavailable:
            raise
        except Exception:  # noqa: BLE001 - logged loudly and tried again on the next read
            logger.error(
                "control_plane_items: the unique claims index could not be created; one tenant per workspace "
                "and one grant per principal are not enforced across replicas until it exists", exc_info=True,
            )
            return
        self._indexed = self._where(coll)

    def _backfill_claims(self, coll: Any, docs: Any) -> None:
        """Give records written before claims existed their claims, once per store (earliest record wins)."""
        marker = f"claims:{self.name}"
        if self._unique is None or docs.find_one({"_id": marker}) is not None:
            self._backfilled = self._where(coll)
            return
        rows = [d for d in coll.find({"store": self.name}) if "fence" not in d]
        rows.sort(key=lambda d: (int(d.get("seq") or 0), str(d.get("_id"))))
        for doc in rows:
            claims = self._claims(json.loads(str(doc.get("body") or "{}")))
            if claims and doc.get("uniq") != claims:
                self._claim_each(coll, docs, doc, claims)
        docs.update_one({"_id": marker}, {"$set": {"kind": "claims", "store": self.name, "at": _sds._now()}},
                        upsert=True)
        self._backfilled = self._where(coll)
        self._cache = None

    def _claim_each(self, coll: Any, docs: Any, doc: dict[str, Any], claims: list[str]) -> list[str]:
        """Claim what is free for an existing record; report each claim another record holds."""
        held: list[str] = []
        for claim in claims:
            try:
                coll.update_one({"_id": doc["_id"]}, {"$set": {"uniq": held + [claim]}})
            except Exception as exc:
                if not (_is_duplicate(exc) and _claim_index_hit(exc)):
                    raise
                self._report_duplicate(docs, str(doc.get("key")), claim)
                continue
            held.append(claim)
        return held

    def _report_duplicate(self, docs: Any, key: str, claim: str) -> None:
        try:
            docs.insert_one({"_id": f"{DUPLICATE_KIND}:{self.name}:{key}:{claim}", "kind": DUPLICATE_KIND,
                             "store": self.name, "key": key, "claim": claim, "at": _sds._now()})
        except Exception as exc:
            if _is_duplicate(exc):
                return
            raise
        logger.warning(
            "control-plane store %s: record %s duplicates the claim %s that another record holds (written "
            "before claims were enforced). It is kept but no longer holds the claim; remove one of the two.",
            self.name, key, claim,
        )

    def _write_imported(self, coll: Any, docs: Any, key: str, row: tuple[str, str, list[str]], seq: int) -> None:
        """Insert one imported or split record unless it exists; a claim another record holds is reported."""
        doc = self._record_doc(key, row, seq)
        doc.pop("_id")
        try:
            coll.update_one({"_id": self._item_id(key)}, {"$setOnInsert": doc}, upsert=True)
        except Exception as exc:
            if not (_is_duplicate(exc) and _claim_index_hit(exc)):
                raise
            doc.pop("uniq", None)
            coll.update_one({"_id": self._item_id(key)}, {"$setOnInsert": doc}, upsert=True)
            self._claim_each(coll, docs, {"_id": self._item_id(key), "key": key}, row[2])

    def status(self) -> dict[str, Any]:
        """The store's status, plus records that duplicate another record's unique claim."""
        out = super().status()
        out["duplicate_records"] = []
        if out.get("backend") == "mongo":
            docs = _sds._collection(DOCS_COLLECTION)
            rows = docs.find({"kind": DUPLICATE_KIND, "store": self.name})
            out["duplicate_records"] = sorted(
                ({k: row.get(k) for k in ("key", "claim", "at")} for row in rows),
                key=lambda row: (str(row.get("key")), str(row.get("claim"))),
            )
        return out

    def _refuse_while_importing(self, coll: Any) -> None:
        """Fail closed while another process imports its file into this empty store.

        Checked on a read from the database (not on a cached answer) until the
        store has records or a finished import: then it can no longer start.
        """
        if not self._settled:
            docs = coll.database[DOCS_COLLECTION]
            meta = docs.find_one({"_id": self.name})
            if meta and meta.get("state") == "importing":
                from services.control_plane_store import ControlPlaneUnavailable

                # Another process is importing its node's file into this empty
                # store. Serving a half-imported ACL or tenant list would open
                # what the file restricts: fail closed until it is done.
                raise ControlPlaneUnavailable(f"control-plane store {self.name} is being imported; retry shortly")
            self._settled = bool(meta) or coll.find_one({"store": self.name}) is not None

    def _split_whole_document(self, coll: Any, docs: Any) -> None:
        """Turn ``{_id: <store>, rev, body}`` from an earlier build into records, idempotently."""
        for _ in range(3):
            meta = docs.find_one({"_id": self.name})
            if not meta or "body" not in meta:
                self._split_checked = True
                return
            records = self._records(json.loads(str(meta.get("body") or "{}")))
            for index, (key, row) in enumerate(records.items()):
                self._write_imported(coll, docs, key, row, index)
            result = docs.update_one(
                {"_id": self.name, "rev": meta.get("rev")},
                {"$unset": {"body": ""}, "$set": {"state": "done", "split_from_rev": meta.get("rev"),
                                                  "split_at": _sds._now(), "records": len(records)}},
            )
            if getattr(result, "matched_count", 0):
                logger.warning(
                    "control-plane store %s: split the whole-store document (rev %s) into %d records",
                    self.name, meta.get("rev"), len(records),
                )
                self._split_checked = True
                self._cache = None
                return

    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
        """Import this node's file into the empty store (claim, records, done); else a conflict."""
        path = self._file_path()
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
        for _ in range(3):
            meta = docs.find_one({"_id": self.name})
            if meta is None:
                if coll.find_one({"store": self.name}) is not None:
                    report = _record_conflict(docs, self.name, path, digest)
                    settled = True
                    break
                try:
                    docs.insert_one({"_id": self.name, "kind": "store", "import_sources": [digest],
                                     "state": "importing", "node": _node_name(), "at": _sds._now()})
                except Exception as exc:
                    if _is_duplicate(exc):
                        continue
                    raise
                meta = {"import_sources": [digest], "state": "importing"}
            if digest not in (meta.get("import_sources") or []):
                report = _record_conflict(docs, self.name, path, digest)
                settled = True
                break
            if meta.get("state") == "importing":
                records = self._records(self._file_load())
                for index, (rkey, row) in enumerate(records.items()):
                    self._write_imported(coll, docs, rkey, row, index)
                docs.update_one({"_id": self.name}, {"$set": {"state": "done", "records": len(records), "at": _sds._now()}})
                report = {"action": "imported", "sha256": digest, "records": len(records)}
                logger.warning("control-plane store %s: imported %s into the shared store (%s)", self.name, path, report)
                _audit_import(self.name, path, report)
            settled = True
            break
        if settled:
            self._imported.add(key)
            self._seen_stamps.add(stamp)
        self._cache = None
        return report if report and report["action"] != "conflict_already_recorded" else None

    def import_local_file(self) -> dict[str, Any] | None:
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            return None
        if not self._split_checked:
            self._split_whole_document(coll, coll.database[DOCS_COLLECTION])
        return self._import_local_file(coll)
