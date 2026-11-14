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

Reads query only what they need (ops review round 2, High 3): ``load(part=)``
one tenant or workspace, ``load(parts=)`` several, ``load(keys=)`` records by
id, ``load(tag=)`` records carrying a lookup tag (``tags=``, stored as
``tags``, for example a tenant's custom domain or a grant's resource); each is
an indexed query, and ``page()`` lists the store newest first, a page at a
time. ``load()`` with no scope still reads the whole store (maintenance
paths). In file mode every load returns the whole file, so callers always
filter what they get. A save diffs against what its load read, so it only
touches records in that scope. The read cache (``CONTROL_STORE_CACHE_SECONDS``,
default 2 s) is kept per query.

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

A save that writes more than one record runs in one multi-document
transaction when the database supports them (a replica set, DocumentDB,
Atlas), so readers see all of it or none of it (a BYOK rotation: the new key
in and the old one retired together). On a standalone server the writes are
applied in order: retired first, then added.

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
from services.control_plane_import import STAGED, LeasedImport
from services.shared_doc_store import (
    DOCS_COLLECTION,
    SharedDoc,
    SharedDocConflict,
    UniqueConflict,
    _dumps,
    _is_duplicate,
    tracked,
)

logger = logging.getLogger(__name__)

ITEMS_COLLECTION = "control_plane_items"
MAX_RECORD_BYTES = 1024 * 1024
#: The per-query read cache is emptied when it would hold more queries than this.
MAX_CACHED_QUERIES = 4096
#: Records updated per round trip by the one-time backfill of claims and lookup tags.
BACKFILL_BATCH = 500
#: Bumped when a store's claims or lookup tags change, so stored records get them again.
FIELDS_VERSION = 2
#: One record as stored: (partition, body, unique claims, lookup tags).
Row = tuple[str, str, list[str], list[str]]
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


def _transaction_client(coll: Any) -> Any | None:
    """The MongoDB client when this deployment supports multi-document transactions (a replica set)."""
    raw = getattr(coll, "_coll", coll)
    client = getattr(getattr(raw, "database", None), "client", None)
    kind = getattr(getattr(client, "topology_description", None), "topology_type_name", "")
    return client if kind in ("ReplicaSetWithPrimary", "Sharded", "LoadBalanced") else None


def _abort(session: Any) -> None:
    if not getattr(session, "in_transaction", False):
        return
    try:
        session.abort_transaction()
    except Exception:  # noqa: BLE001 - the server aborts an unfinished transaction on its own
        logger.debug("control-plane transaction abort failed", exc_info=True)


def _claim_index_hit(exc: Exception) -> bool:
    """A duplicate-key error raised by the claims index (not by ``_id``)."""
    details = getattr(exc, "details", None) or {}
    pattern = details.get("keyPattern") or {}
    return "uniq" in pattern or UNIQUE_INDEX in str(details.get("index") or exc)


class SharedRecordSet(LeasedImport, SharedDoc):
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
        tags: Callable[[dict[str, Any]], list[str]] | None = None,
    ) -> None:
        super().__init__(name, file_load=file_load, file_save=file_save, file_path=file_path, empty=empty)
        self._list_key = list_key
        self._id_key = id_key
        self._partition_key = partition_key
        self._unique = unique
        self._tags = tags
        self._cache: dict[str, tuple[float, dict[str, tuple[int, str, int, str]]]] = {}  # type: ignore[assignment]
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

    def _lookup_tags(self, record: Any) -> list[str]:
        if self._tags is None or not isinstance(record, dict):
            return []
        return sorted({str(t) for t in self._tags(record) if t})

    def _row(self, record: Any, body: str) -> Row:
        return (self._partition(record), body, self._claims(record), self._lookup_tags(record))

    def _records(self, data: Any) -> dict[str, Row]:
        """``{key: (partition, body, claims, tags)}`` for the store in its file shape."""
        out: dict[str, Row] = {}
        if self._list_key is None:
            for key, record in (data.items() if isinstance(data, dict) else ()):
                out[str(key)] = self._row(record, _dumps(record))
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
            out[key] = self._row(record, body)
        return out

    def _assemble(self, ordered: list[tuple[str, str]]) -> Any:
        if self._list_key is None:
            return {key: json.loads(body) for key, body in ordered}
        data = self._empty()
        data[self._list_key] = [json.loads(body) for _key, body in ordered]
        return data

    # -- reads ---------------------------------------------------------
    def load(
        self,
        *,
        fresh: bool = False,
        part: str | None = None,
        parts: Any = None,
        keys: Any = None,
        tag: str | None = None,
    ) -> Any:
        """The records in scope, in the store's file shape (see module docstring).

        ``fresh`` reads the database even inside the cache window. In file
        mode the whole file is returned whatever the scope.
        """
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            return tracked(self._file_load(), None)
        self._prepare(coll)
        flt = self._scope_filter(part=part, parts=parts, keys=keys, tag=tag)
        query = _dumps(flt)
        ttl = self._ttl()
        cached = self._cache.get(query) if isinstance(self._cache, dict) else None
        if (
            cached is not None
            and not fresh
            and ttl > 0
            and not getattr(self._scope, "depth", 0)
            and _sds._clock() - cached[0] < ttl
        ):
            items = cached[1]
        else:
            self._refuse_while_importing(coll)
            items = self._read(coll, flt)
            if not isinstance(self._cache, dict) or len(self._cache) >= MAX_CACHED_QUERIES:
                self._cache = {}
            self._cache[query] = (_sds._clock(), items)
        return self._tracked(items)

    def _scope_filter(self, *, part: Any = None, parts: Any = None, keys: Any = None, tag: Any = None) -> dict[str, Any]:
        flt: dict[str, Any] = {"store": self.name}
        if part is not None:
            flt["part"] = str(part)
        elif parts is not None:
            flt["part"] = {"$in": sorted({str(p) for p in parts})}
        elif keys is not None:
            flt["_id"] = {"$in": sorted({self._item_id(str(k)) for k in keys})}
        elif tag is not None:
            flt["tags"] = str(tag)
        return flt

    def page(self, *, offset: int = 0, limit: int = 100) -> tuple[Any, int | None]:
        """One page of the store, newest first, and the offset of the next page (None: no more)."""
        offset, limit = max(0, int(offset)), max(1, int(limit))
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            data = self._file_load()
            if self._list_key is None:
                rows = list((data or {}).items())[::-1]
                chosen = dict(rows[offset:offset + limit])
            else:
                rows = list((data or {}).get(self._list_key) or [])[::-1]
                chosen = {self._list_key: rows[offset:offset + limit]}
            more = len(rows) > offset + limit
            return chosen, (offset + limit if more else None)
        self._prepare(coll)
        self._refuse_while_importing(coll)
        cursor = coll.find({"store": self.name, "fence": {"$exists": False}, STAGED: self._visible()})
        docs = list(cursor.sort([("seq", -1), ("_id", -1)]).skip(offset).limit(limit + 1))
        more = len(docs) > limit
        ordered = [(str(d.get("key")), str(d.get("body") or "{}")) for d in docs[:limit]]
        data = self._assemble(ordered)
        return data, (offset + limit if more else None)

    def trim(self, part: str, *, keep: int) -> int:
        """Delete all but the ``keep`` most recently written records of one partition. Returns how many went.

        Reads ids and write times only, never the records themselves. Does
        nothing in file mode (the store's own save applies its cap there).
        """
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            return 0
        cursor = coll.find({"store": self.name, "part": str(part), STAGED: self._visible()}, {"_id": 1, "updated_at": 1})
        older = list(cursor.sort([("updated_at", -1), ("_id", -1)]).skip(max(0, int(keep))))
        if not older:
            return 0
        coll.delete_many({"_id": {"$in": [d["_id"] for d in older]}})
        self._cache = {}
        return len(older)

    @staticmethod
    def _ttl() -> float:
        return _sds._cache_seconds()

    def _read(self, coll: Any, flt: dict[str, Any] | None = None) -> dict[str, tuple[int, str, int, str]]:
        items: dict[str, tuple[int, str, int, str]] = {}
        # Records an unfinished import staged are not part of the store (services.control_plane_import).
        for doc in coll.find({**(flt or {"store": self.name}), STAGED: self._visible()}):
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
        for key, (_part, body, _claims, _tags) in new.items():
            size = len(body.encode("utf-8"))
            if size > MAX_RECORD_BYTES:
                raise RecordTooLarge(
                    f"A {self.name} record is {size // 1024} KB; the limit is {MAX_RECORD_BYTES // 1024} KB."
                )
        token = rev if rev is not None else getattr(data, "rev", None)
        self._cache = {}
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
        plan = (new, changed, removed, added, blind)
        client = _transaction_client(coll) if len(changed) + len(removed) + len(added) > 1 else None
        if client is None:
            self._apply(coll, token, plan, {})
        else:
            self._apply_atomically(client, coll, token, plan)
        if hasattr(data, "rev"):
            data.rev = token

    def _apply_atomically(self, client: Any, coll: Any, token: ReadToken, plan: tuple[Any, ...]) -> None:
        """All of one save's writes in one transaction: readers see all of them or none.

        A BYOK rotation (new key in, old key out) has no moment without an
        active key, and a save that fails part-way changes nothing.
        """
        from pymongo.errors import PyMongoError

        from services.control_plane_bounds import deadline

        session = client.start_session()
        try:
            session.start_transaction()
            self._apply(coll, token, plan, {"session": session})
            with deadline(write=True):
                session.commit_transaction()
        except PyMongoError as exc:
            _abort(session)
            if exc.has_error_label("TransientTransactionError") or getattr(exc, "code", None) == 112:
                raise SharedDocConflict(f"{self.name}: a concurrent transaction wrote the same records") from exc
            raise
        except BaseException:
            _abort(session)
            raise
        finally:
            session.end_session()

    def _apply(self, coll: Any, token: ReadToken, plan: tuple[Any, ...], kw: dict[str, Any]) -> None:
        new, changed, removed, added, blind = plan
        seq = time.time_ns()
        for key in changed:
            flt: dict[str, Any] = {"_id": self._item_id(key)}
            if not blind:
                flt["rev"] = token.revs[key]
            try:
                result = coll.update_one(flt, self._record_update(new[key]), **kw)
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
            result = coll.delete_one(flt, **kw)
            if not blind and not getattr(result, "deleted_count", 0):
                raise SharedDocConflict(f"{self.name}:{key}")
            for table in (token.bodies, token.revs, token.parts):
                table.pop(key, None)
        for index, key in enumerate(added):
            doc = self._record_doc(key, new[key], seq + index)
            try:
                coll.insert_one(doc, **kw)
            except Exception as exc:
                self._raise_if_claim_lost(exc, key)
                if not _is_duplicate(exc):
                    raise
                if not blind:
                    raise SharedDocConflict(f"{self.name}:{key}") from exc
                try:
                    coll.update_one({"_id": doc["_id"]}, self._record_update(new[key]), **kw)
                except Exception as again:
                    self._raise_if_claim_lost(again, key)
                    raise
            token.revs[key] = 1
            token.parts[key] = new[key][0]
        for key in changed + added:
            token.bodies[key] = new[key][1]

    def _record_doc(self, key: str, row: Row, seq: int) -> dict[str, Any]:
        doc = {"_id": self._item_id(key), "store": self.name, "key": key, "part": row[0], "rev": 1,
               "seq": seq, "body": row[1], "updated_at": _sds._now(), "tags": row[3]}
        if row[2]:
            doc["uniq"] = row[2]
        return doc

    @staticmethod
    def _record_update(row: Row) -> dict[str, Any]:
        update: dict[str, Any] = {"$set": {"body": row[1], "part": row[0], "updated_at": _sds._now(), "tags": row[3]},
                                  "$inc": {"rev": 1}}
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

    def _prepare(self, coll: Any, *, import_file: bool = True) -> None:
        here = self._where(coll)
        if self._indexed != here:
            self._ensure_indexes(coll)
        docs = coll.database[DOCS_COLLECTION]
        if not self._split_checked:
            self._split_whole_document(coll, docs)
        if self._indexed == here and self._backfilled != here:
            with self._lock:  # one backfill per process; other threads wait for it (never on the event loop)
                if self._backfilled != here:
                    self._backfill_fields(coll, docs)
        if import_file:
            self._import_local_file(coll)

    def _ensure_indexes(self, coll: Any) -> None:
        """The partition index and the claims index. Without the claims index the invariants are unguarded."""
        from services.control_plane_store import ControlPlaneUnavailable

        try:
            coll.create_index([("store", 1), ("seq", -1)])
            coll.create_index([("store", 1), ("part", 1)])
            coll.create_index([("store", 1), ("tags", 1)])
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

    def _backfill_fields(self, coll: Any, docs: Any) -> None:
        """Give records written before claims and lookup tags existed both, once per store.

        Claims go to the earliest record (by insertion); a later duplicate is reported.
        """
        marker = f"index_fields:v{FIELDS_VERSION}:{self.name}"
        if (self._unique is None and self._tags is None) or docs.find_one({"_id": marker}) is not None:
            self._backfilled = self._where(coll)
            return
        rows = [d for d in coll.find({"store": self.name}) if "fence" not in d]
        rows.sort(key=lambda d: (int(d.get("seq") or 0), str(d.get("_id"))))
        pending = []
        for doc in rows:
            record = json.loads(str(doc.get("body") or "{}"))
            tags, claims = self._lookup_tags(record), self._claims(record)
            if doc.get("tags") != tags or (claims and doc.get("uniq") != claims):
                pending.append((doc, tags, claims))
        self._write_fields(coll, docs, pending)
        docs.update_one({"_id": marker}, {"$set": {"kind": "index_fields", "store": self.name, "at": _sds._now()}},
                        upsert=True)
        self._backfilled = self._where(coll)
        self._cache = {}

    def _write_fields(self, coll: Any, docs: Any, pending: list[tuple[dict[str, Any], list[str], list[str]]]) -> None:
        """Set tags and claims in ordered batches; a record whose claim is taken claims what is free, alone."""
        from pymongo import UpdateOne
        from pymongo.errors import BulkWriteError

        done = 0
        while done < len(pending):
            batch = pending[done:done + BACKFILL_BATCH]
            ops = [UpdateOne({"_id": doc["_id"]}, {"$set": {"tags": tags, **({"uniq": claims} if claims else {})}})
                   for doc, tags, claims in batch]
            try:
                coll.bulk_write(ops, ordered=True)
            except BulkWriteError as exc:
                errors = (exc.details or {}).get("writeErrors") or []
                if not errors or errors[0].get("code") != 11000:
                    raise
                index = int(errors[0].get("index") or 0)
                doc, tags, claims = batch[index]
                coll.update_one({"_id": doc["_id"]}, {"$set": {"tags": tags}})
                self._claim_each(coll, docs, doc, claims)
                done += index + 1
            else:
                done += len(batch)

    def _claim_each(self, coll: Any, docs: Any, doc: dict[str, Any], claims: list[str]) -> list[str]:
        """Claim what is free for an existing record; report each claim another record holds."""
        held: list[str] = []
        for claim in claims:
            try:
                coll.update_one({"_id": doc["_id"]}, {"$set": {"uniq": held + [claim]}})
            except Exception as exc:
                if not (_is_duplicate(exc) and _claim_index_hit(exc)):
                    raise
                # Not lost: the record stays and the duplicate is reported for the operator.
                self._report_duplicate(docs, str(doc.get("key")), claim)
            else:
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

    def _write_imported(
        self, coll: Any, docs: Any, key: str, row: Row, seq: int, *, stamp: str = "", epoch: int | None = None,
    ) -> None:
        """Insert one imported or split record unless it exists; a claim another record holds is reported.

        ``stamp`` is the imported file's digest. An imported record is staged
        under the import's ``epoch``: invisible until the import completes.
        """
        doc = self._record_doc(key, row, seq)
        if stamp:
            doc["imp"] = stamp
        if epoch is not None:
            if not self._stage_record(coll, doc, epoch):
                doc.pop("uniq", None)
                self._stage_record(coll, doc, epoch)
                self._claim_each(coll, docs, {"_id": doc["_id"], "key": key}, row[2])
            return
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
                self._cache = {}
                return

    def import_local_file(self) -> dict[str, Any] | None:
        """At API start: indexes, the one-time backfill and the split before any request, then the import."""
        coll = _sds._collection(ITEMS_COLLECTION)
        if coll is None:
            return None
        self._prepare(coll, import_file=False)
        return self._import_local_file(coll)
