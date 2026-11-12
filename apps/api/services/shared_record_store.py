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
its own ``rev``. Records also belong to a partition (the tenant or workspace
they carry); a save first advances a small per-partition fence document
(``{_id: "<store>#fence:<partition>", fence, rev}``) by compare-and-swap, so a
check-then-insert inside one tenant (one tenant per workspace, one grant per
principal, one active BYOK key) stays serialized across replicas while writes
for different tenants never contend.

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


class RecordTooLarge(ValueError):
    """One record is larger than a control-plane store accepts."""


class ReadToken:
    """What a load saw: each record's body, revision and partition, and each fence's revision."""

    __slots__ = ("bodies", "revs", "parts", "fences")

    def __init__(
        self,
        bodies: dict[str, str],
        revs: dict[str, int],
        parts: dict[str, str],
        fences: dict[str, int],
    ) -> None:
        self.bodies = bodies
        self.revs = revs
        self.parts = parts
        self.fences = fences


class SharedRecordSet(SharedDoc):
    """A record-list store, one document per record in MongoDB (see module docstring).

    ``list_key`` names the list in the store's shape (``{"tenants": [...]}``);
    None means the store is a mapping of record key to record (shape drafts).
    ``id_key`` is the record id inside a listed record, ``partition_key`` the
    field naming the tenant or workspace a record belongs to.
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
    ) -> None:
        super().__init__(name, file_load=file_load, file_save=file_save, file_path=file_path, empty=empty)
        self._list_key = list_key
        self._id_key = id_key
        self._partition_key = partition_key
        self._indexed = False
        self._split_checked = False
        self._settled = False

    # -- shape ---------------------------------------------------------
    def _item_id(self, key: str) -> str:
        return f"{self.name}:{key}"

    def _fence_id(self, part: str) -> str:
        return f"{self.name}#fence:{part}"

    def _partition(self, record: Any) -> str:
        return str(record.get(self._partition_key) or "") if isinstance(record, dict) else ""

    def _records(self, data: Any) -> dict[str, tuple[str, str]]:
        """``{key: (partition, body)}`` for the store in its file shape."""
        out: dict[str, tuple[str, str]] = {}
        if self._list_key is None:
            for key, record in (data.items() if isinstance(data, dict) else ()):
                out[str(key)] = (self._partition(record), _dumps(record))
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
            out[key] = (self._partition(record), body)
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
            and _sds._now() - cached[0] < ttl
        ):
            items, fences = cached[1], cached[2]
        else:
            items, fences = self._read(coll)
            self._cache = (_sds._now(), items, fences)
        return self._tracked(items, fences)

    @staticmethod
    def _ttl() -> float:
        return _sds._cache_seconds()

    def _read(self, coll: Any) -> tuple[dict[str, tuple[int, str, int, str]], dict[str, int]]:
        items: dict[str, tuple[int, str, int, str]] = {}
        fences: dict[str, int] = {}
        for doc in coll.find({"store": self.name}):
            if "fence" in doc:
                fences[str(doc["fence"])] = int(doc.get("rev") or 0)
                continue
            items[str(doc.get("key"))] = (
                int(doc.get("seq") or 0), str(doc.get("part") or ""), int(doc.get("rev") or 0), str(doc.get("body") or "{}"),
            )
        return items, fences

    def _tracked(self, items: dict[str, tuple[int, str, int, str]], fences: dict[str, int]) -> Any:
        ordered = sorted(items.items(), key=lambda kv: (kv[1][0], kv[0]))
        token = ReadToken(
            {key: row[3] for key, row in ordered},
            {key: row[2] for key, row in ordered},
            {key: row[1] for key, row in ordered},
            dict(fences),
        )
        return tracked(self._assemble([(key, row[3]) for key, row in ordered]), token)

    # -- writes --------------------------------------------------------
    def save(self, data: Any, *, rev: Any = None) -> None:
        """Write the records that changed since ``data`` was loaded.

        A record changed or deleted by another writer since this caller's
        read, or a partition another writer saved into, raises
        :class:`SharedDocConflict` (``retrying`` reads again and re-applies).
        A plain dict that was never loaded replaces whatever is there.
        """
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            self._file_save(data)
            return
        new = self._records(data)
        for key, (_part, body) in new.items():
            size = len(body.encode("utf-8"))
            if size > MAX_RECORD_BYTES:
                raise RecordTooLarge(
                    f"A {self.name} record is {size // 1024} KB; the limit is {MAX_RECORD_BYTES // 1024} KB."
                )
        token = rev if rev is not None else getattr(data, "rev", None)
        self._cache = None
        blind = not isinstance(token, ReadToken)
        if blind:
            items, fences = self._read(coll)
            token = ReadToken(
                {k: v[3] for k, v in items.items()}, {k: v[2] for k, v in items.items()},
                {k: v[1] for k, v in items.items()}, fences,
            )
        changed = [k for k in new if k in token.bodies and token.bodies[k] != new[k][1]]
        added = [k for k in new if k not in token.bodies]
        removed = [k for k in token.bodies if k not in new]
        if not (changed or added or removed):
            return
        parts = {new[k][0] for k in changed + added} | {token.parts[k] for k in changed + removed}
        for part in sorted(parts):
            token.fences[part] = self._advance_fence(coll, part, token.fences.get(part, 0), blind)
        seq = time.time_ns()
        for key in changed:
            flt: dict[str, Any] = {"_id": self._item_id(key)}
            if not blind:
                flt["rev"] = token.revs[key]
            result = coll.update_one(flt, {"$set": {"body": new[key][1], "part": new[key][0], "updated_at": _sds._now()},
                                           "$inc": {"rev": 1}})
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
            doc = {"_id": self._item_id(key), "store": self.name, "key": key, "part": new[key][0], "rev": 1,
                   "seq": seq + index, "body": new[key][1], "updated_at": _sds._now()}
            try:
                coll.insert_one(doc)
            except Exception as exc:
                if not _is_duplicate(exc):
                    raise
                if not blind:
                    raise SharedDocConflict(f"{self.name}:{key}") from exc
                coll.update_one({"_id": doc["_id"]}, {"$set": {"body": doc["body"], "part": doc["part"]},
                                                      "$inc": {"rev": 1}})
            token.revs[key] = 1
            token.parts[key] = new[key][0]
        for key in changed + added:
            token.bodies[key] = new[key][1]
        if hasattr(data, "rev"):
            data.rev = token

    def _advance_fence(self, coll: Any, part: str, rev: int, blind: bool) -> int:
        fence_id = self._fence_id(part)
        if blind:
            coll.update_one({"_id": fence_id}, {"$set": {"store": self.name, "fence": part}, "$inc": {"rev": 1}},
                            upsert=True)
            doc = coll.find_one({"_id": fence_id}) or {}
            return int(doc.get("rev") or 0)
        if rev == 0:
            try:
                coll.insert_one({"_id": fence_id, "store": self.name, "fence": part, "rev": 1})
            except Exception as exc:
                if _is_duplicate(exc):
                    raise SharedDocConflict(f"{self.name}#{part}") from exc
                raise
            return 1
        result = coll.update_one({"_id": fence_id, "rev": rev}, {"$set": {"rev": rev + 1}})
        if not getattr(result, "matched_count", 0):
            raise SharedDocConflict(f"{self.name}#{part}")
        return rev + 1

    # -- first load: index, split of an earlier whole document, import --
    def _prepare(self, coll: Any) -> None:
        if not self._indexed:
            self._indexed = True
            try:
                coll.create_index([("store", 1)])
            except Exception:  # noqa: BLE001 - an index is an optimisation, not correctness
                logger.debug("control_plane_items index not created", exc_info=True)
        docs = coll.database[DOCS_COLLECTION]
        if not self._split_checked:
            self._split_whole_document(coll, docs)
        self._import_local_file(coll)
        if not self._settled:
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
            for index, (key, (part, body)) in enumerate(records.items()):
                coll.update_one(
                    {"_id": self._item_id(key)},
                    {"$setOnInsert": {"store": self.name, "key": key, "part": part, "rev": 1, "seq": index,
                                      "body": body, "updated_at": _sds._now()}},
                    upsert=True,
                )
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
                for index, (rkey, (part, body)) in enumerate(records.items()):
                    coll.update_one(
                        {"_id": self._item_id(rkey)},
                        {"$setOnInsert": {"store": self.name, "key": rkey, "part": part, "rev": 1, "seq": index,
                                          "body": body, "updated_at": _sds._now()}},
                        upsert=True,
                    )
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
