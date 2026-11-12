"""Control-plane documents and logs that every replica must agree on.

Tenant settings, BYOK key records, resource ACLs and their neighbours were one
JSON file per pod. With several API replicas an IP allowlist saved on one pod
was unknown to the others (audit H-5). This module keeps each store's existing
interface and file format, and adds a shared home for it:

``SharedDoc``  one JSON document per store (``control_plane_docs``), written by
               compare-and-swap on a revision number, so two replicas changing
               the same store cannot drop each other's write. For small
               single-object documents only: the record-list stores use
               ``services.shared_record_store.SharedRecordSet``, one document
               per record (ops review High 3).
``SharedLog``  one record per line of an append log (``control_plane_records``).

Backend choice (``DATAFLOW_CONTROL_STORE_BACKEND`` / ``DATAWRAP_…``):

``file``   the store's own file under the data directory (single node, dev).
``mongo``  the shared collection; unreachable means the call fails, it never
           falls back to this node's file.
unset      ``mongo`` when ``MULTI_REPLICA=1`` or a control-plane database is
           part of this deployment, else ``file``.

The first process to reach an empty shared store imports its local file. Once
the shared store holds a store's records, a local file is never imported or
merged again: one that differs (another node's file, or one an old build
wrote during a rollback) is logged once and recorded in the store's status as
conflicting local data, and the shared state stays authoritative (ops review
High 2). The file is never deleted or rewritten.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import random
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from services.brand_env import getenv_brand
from services.control_plane_bounds import (  # noqa: F401 - re-exported for callers and tests
    bounded,
    deadline,
    note_outage,
    operation_timeout_seconds,
    outage_retry_at,
    outage_seconds,
    raise_if_recent_outage,
    reset_outage_state,
)
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

DOCS_COLLECTION = "control_plane_docs"
RECORDS_COLLECTION = "control_plane_records"
BACKEND_SETTING = "DATAFLOW_CONTROL_STORE_BACKEND"
#: A read-modify-write that lost the compare-and-swap waits a random time in
#: [0, min(cap, base * 2**attempt)] before it reads again ("full jitter"), so
#: replicas that conflicted together do not retry in lockstep, and keeps
#: retrying for up to RETRY_BUDGET_SECONDS (ops review M10). Writers in
#: different tenants never contend (one document per record, a fence per
#: tenant). Measured on live MongoDB, 16 processes x 25 grants all in ONE
#: tenant: no failures, at most 16 attempts, slowest call 4-9 s. A conflict
#: that outlasts the budget is a 409 with Retry-After. _MAX_ATTEMPTS is only
#: a backstop.
_MAX_ATTEMPTS = 50
RETRY_BUDGET_SECONDS = 10.0
BACKOFF_BASE_SECONDS = 0.01
BACKOFF_CAP_SECONDS = 0.5
_jitter = random.uniform  # nosec B311 - retry spacing, not security


def _backoff(attempt: int) -> float:
    return _jitter(0.0, min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * 2**attempt))
_REGISTERED: dict[str, Any] = {}

F = TypeVar("F", bound=Callable[..., Any])


class SharedDocConflict(RuntimeError):
    """Another writer changed the document between this caller's read and write."""


class TrackedDict(dict):
    """A loaded document that remembers the revision it was read at."""

    rev: int | None = None


class TrackedList(list):
    rev: int | None = None


def tracked(data: Any, rev: int | None) -> Any:
    """``data`` as a dict/list carrying ``rev`` (None in file mode)."""
    out: Any = TrackedList(data) if isinstance(data, list) else TrackedDict(data or {})
    out.rev = rev
    return out


def control_store_backend() -> str:
    """``file`` or ``mongo`` for every store in this module."""
    raw = (getenv_brand("CONTROL_STORE_BACKEND", "") or "").strip().lower()
    if raw in ("file", "mongo"):
        return raw
    from services.process_role import multi_replica

    if multi_replica():
        return "mongo"
    from services.control_plane_store import control_plane_configured

    return "mongo" if control_plane_configured() else "file"


def _collection(name: str) -> Any | None:
    """The shared collection, or None in file mode. Raises when it must exist and does not.

    Every call on the returned collection is bounded (about 1 s) and an
    unreachable database is remembered for a short window, so an outage costs
    a request at most one short wait and never the client's 5 s timeout
    (services.control_plane_bounds).
    """
    if control_store_backend() == "file":
        return None
    from services.control_plane_store import ControlPlaneUnavailable, shared_collection

    raise_if_recent_outage()
    try:
        with deadline():  # a reconnect attempt is bounded like any other call
            coll = shared_collection(name)
    except ControlPlaneUnavailable as exc:
        note_outage(exc)
        raise
    if coll is None:
        raise ControlPlaneUnavailable(
            f"{BACKEND_SETTING}=mongo (or MULTI_REPLICA=1) needs a MongoDB control-plane "
            "database, and none is connected. Configure MONGODB_URI, or set "
            f"{BACKEND_SETTING}=file on a single-node install."
        )
    return bounded(coll)


def _cache_seconds() -> float:
    try:
        return max(0.0, float(getenv_brand("CONTROL_STORE_CACHE_SECONDS", "2") or 0))
    except ValueError:
        return 2.0


def _dumps(data: Any) -> str:
    return json.dumps(data, default=json_default, sort_keys=True)


def _now() -> float:
    return time.time()


def _is_duplicate(exc: Exception) -> bool:
    return type(exc).__name__ in ("DuplicateKeyError", "BulkWriteError")


def _file_stamp(path: Path) -> tuple[str, int, int] | None:
    """Identity of the file as it is now, without reading it (None: no file)."""
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), stat.st_mtime_ns, stat.st_size) if stat.st_size else None


def _file_digest(path: Path) -> tuple[str, bytes] | None:
    try:
        if not path.is_file():
            return None
        raw = path.read_bytes()
    except OSError:
        return None
    if not raw.strip():
        return None
    return hashlib.sha256(raw).hexdigest(), raw


def _audit_import(store: str, path: Path, details: dict[str, Any], *, conflict: bool = False) -> None:
    try:
        from services.audit_coverage import record_security_event

        fields = {"resource": store, "actor": "system", "details": {"source": str(path), **details}}
        if conflict:
            record_security_event("control_store.local_file_conflict", outcome="skipped", **fields)
        else:
            record_security_event("control_store.imported", outcome="succeeded", **fields)
    except Exception:  # noqa: BLE001 - what happened is already in the log line the caller wrote
        logger.debug("control-store audit event not written", exc_info=True)


def _node_name() -> str:
    import socket

    return socket.gethostname()


CONFLICT_KIND = "local_file_conflict"


def _record_conflict(docs: Any, store: str, path: Path, digest: str) -> dict[str, Any]:
    """Record ``path`` as conflicting local data in the store's status (once per content).

    The file is never merged: the shared store already holds this store's
    records, and the file was changed after that (by another node before the
    upgrade, or by an old build during a rollback). Merging it would bring
    back records revoked or deleted in the shared store (ops review High 2).
    """
    entry = {
        "_id": f"{CONFLICT_KIND}:{store}:{digest}",
        "kind": CONFLICT_KIND,
        "store": store,
        "path": str(path),
        "sha256": digest,
        "node": _node_name(),
        "at": _now(),
    }
    try:
        docs.insert_one(entry)
    except Exception as exc:
        if _is_duplicate(exc):
            return {"action": "conflict_already_recorded", "sha256": digest}
        raise
    report = {"action": "conflicting_local_data_not_imported", "sha256": digest}
    logger.warning(
        "control-plane store %s: %s (sha256 %s) holds conflicting local data. The shared store "
        "already has this store's records, so the file was not imported and is not merged; "
        "the shared store stays authoritative. See the operator notes on upgrades and rollback.",
        store, path, digest,
    )
    _audit_import(store, path, report, conflict=True)
    return report


def _status_conflicts(docs: Any, store: str) -> list[dict[str, Any]]:
    rows = docs.find({"kind": CONFLICT_KIND, "store": store})
    return sorted(
        ({k: row.get(k) for k in ("path", "sha256", "node", "at")} for row in rows),
        key=lambda row: (row.get("at") or 0, row.get("sha256") or ""),
    )


class SharedDoc:
    """One store's JSON document: file on a single node, compare-and-swap in MongoDB.

    ``file_load`` / ``file_save`` are the store's existing file functions, so a
    single-node install reads and writes exactly what it did before. The file
    is imported only while the shared store has nothing for this store yet; a
    file that differs later is conflicting local data (:func:`_record_conflict`).
    """

    kind = "doc"

    def __init__(
        self,
        name: str,
        *,
        file_load: Callable[[], Any],
        file_save: Callable[[Any], None],
        file_path: Callable[[], Path],
        empty: Callable[[], Any] = dict,
    ) -> None:
        self.name = name
        self._file_load = file_load
        self._file_save = file_save
        self._file_path = file_path
        self._empty = empty
        self._lock = threading.RLock()
        self._cache: tuple[float, int, str] | None = None
        self._imported: set[str] = set()
        self._seen_stamps: set[tuple[str, int, int]] = set()
        self._scope = threading.local()
        _REGISTERED[name] = self

    # -- reads ---------------------------------------------------------
    def load(self) -> Any:
        """The document, carrying the revision it was read at."""
        coll = _collection(DOCS_COLLECTION)
        if coll is None:
            return tracked(self._file_load(), None)
        self._import_local_file(coll)
        ttl = _cache_seconds()
        cached = self._cache
        if (
            cached is not None
            and ttl > 0
            and not getattr(self._scope, "depth", 0)
            and _now() - cached[0] < ttl
        ):
            return tracked(json.loads(cached[2]), cached[1])
        rev, body = self._read(coll)
        if body is None:
            # Nothing shared yet: the store's own "empty" shape, at revision 0
            # so the first save is an insert that loses to a concurrent one.
            return tracked(self._empty(), 0)
        self._cache = (_now(), rev, body)
        return tracked(json.loads(body), rev)

    def _read(self, coll: Any) -> tuple[int, str | None]:
        doc = coll.find_one({"_id": self.name})
        if not doc:
            return 0, None
        return int(doc.get("rev") or 0), str(doc.get("body") or "{}")

    # -- writes --------------------------------------------------------
    def save(self, data: Any, *, rev: int | None = None) -> None:
        """Write the document; in MongoDB only if nobody wrote since ``rev``.

        ``rev`` defaults to the revision ``data`` was loaded at. A plain dict
        (never loaded) replaces whatever is there.
        """
        coll = _collection(DOCS_COLLECTION)
        if coll is None:
            self._file_save(data)
            return
        expected = rev if rev is not None else getattr(data, "rev", None)
        body = _dumps(data)
        stamp = {"body": body, "updated_at": _now()}
        self._cache = None
        if expected is None:
            coll.update_one({"_id": self.name}, {"$set": stamp, "$inc": {"rev": 1}}, upsert=True)
            return
        if expected == 0:
            try:
                coll.insert_one({"_id": self.name, "rev": 1, "import_sources": [], **stamp})
            except Exception as exc:
                if _is_duplicate(exc):
                    raise SharedDocConflict(self.name) from exc
                raise
            new_rev = 1
        else:
            result = coll.update_one(
                {"_id": self.name, "rev": expected}, {"$set": {**stamp, "rev": expected + 1}}
            )
            if not getattr(result, "matched_count", 0):
                raise SharedDocConflict(self.name)
            new_rev = expected + 1
        if hasattr(data, "rev"):
            data.rev = new_rev
        self._cache = (_now(), new_rev, body)

    def retrying(self, fn: F) -> F:
        """Run a read-modify-write function again when its write lost the swap.

        Inside the call every load is a fresh read (no cache), so the retry
        sees the other writer's change. In file mode the calls of one process
        are serialized instead.
        """

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self._scope.depth = getattr(self._scope, "depth", 0) + 1
            deadline = time.monotonic() + RETRY_BUDGET_SECONDS
            try:
                for attempt in range(_MAX_ATTEMPTS):
                    try:
                        with self._lock:
                            return fn(*args, **kwargs)
                    except SharedDocConflict:
                        left = deadline - time.monotonic()
                        if attempt == _MAX_ATTEMPTS - 1 or left <= 0:
                            raise
                        time.sleep(min(_backoff(attempt), left))
            finally:
                self._scope.depth -= 1
            raise SharedDocConflict(self.name)  # pragma: no cover - loop always returns or raises

        return wrapper  # type: ignore[return-value]

    # -- one-time import ----------------------------------------------
    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
        """Import this node's file, only while the shared store has no document yet.

        Returns what happened, None when there was nothing to do. The file is
        marked as handled only after a definite outcome, so a failed attempt
        is retried by the next call.
        """
        path = self._file_path()
        stamp = _file_stamp(path)
        if stamp is None or stamp in self._seen_stamps:
            return None  # no file, or this exact file was already handled: no read, no hash
        found = _file_digest(path)
        if found is None:
            return None
        digest = found[0]
        key = f"{path}:{digest}"
        if key in self._imported:
            self._seen_stamps.add(stamp)
            return None
        report: dict[str, Any] | None = None
        settled = False
        for _ in range(3):
            doc = coll.find_one({"_id": self.name})
            if doc and digest in (doc.get("import_sources") or []):
                settled = True
                break
            if doc:
                report = _record_conflict(coll, self.name, path, digest)
                settled = True
                break
            try:
                coll.insert_one(
                    {"_id": self.name, "rev": 1, "body": _dumps(self._file_load()), "updated_at": _now(),
                     "import_sources": [digest]}
                )
            except Exception as exc:
                if _is_duplicate(exc):
                    continue  # another replica created it first: decide again on what it holds
                raise
            report = {"action": "imported", "sha256": digest}
            logger.warning("control-plane store %s: imported %s into the shared store (%s)", self.name, path, report)
            _audit_import(self.name, path, report)
            settled = True
            break
        if settled:
            self._imported.add(key)
            self._seen_stamps.add(stamp)
        self._cache = None
        return report if report and report["action"] != "conflict_already_recorded" else None

    def status(self) -> dict[str, Any]:
        """Backend, imported file digests and conflicting local files of this store."""
        coll = _collection(DOCS_COLLECTION)
        if coll is None:
            return {"backend": "file", "import_sources": [], "local_file_conflicts": []}
        doc = coll.find_one({"_id": self.name}) or {}
        return {
            "backend": "mongo",
            "import_sources": list(doc.get("import_sources") or []),
            "local_file_conflicts": _status_conflicts(coll, self.name),
        }

    def import_local_file(self) -> dict[str, Any] | None:
        """Import this node's file now. Returns what happened, None when nothing to do."""
        coll = _collection(DOCS_COLLECTION)
        if coll is None:
            return None
        return self._import_local_file(coll)


class SharedLog:
    """An append log: the store's JSONL file on a single node, one record per document in MongoDB.

    The store keeps its file code and asks :meth:`backend` first; None means
    "use the file". Records are returned oldest first, like the file's lines.
    """

    kind = "log"

    def __init__(self, name: str, *, file_path: Callable[[], Path], key_field: str = "id") -> None:
        self.name = name
        self._file_path = file_path
        self._key_field = key_field
        self._imported: set[str] = set()
        self._seen_stamps: set[tuple[str, int, int]] = set()
        self._indexed = False
        _REGISTERED[name] = self

    def backend(self) -> Any | None:
        coll = _collection(RECORDS_COLLECTION)
        if coll is not None:
            if not self._indexed:
                self._indexed = True
                try:
                    coll.create_index([("store", 1), ("seq", -1)])
                    coll.create_index([("store", 1), ("key", 1)])
                except Exception:  # noqa: BLE001 - an index is an optimisation, not correctness
                    logger.debug("control_plane_records index not created", exc_info=True)
            self._import_local_file(coll)
        return coll

    def _doc(self, record: dict[str, Any], *, doc_id: str | None = None, seq: int | None = None) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "store": self.name,
            "seq": seq if seq is not None else time.time_ns(),
            "key": str(record.get(self._key_field) or ""),
            "rev": 1,
            "body": _dumps(record),
        }
        if doc_id is not None:
            doc["_id"] = doc_id
        return doc

    def append(self, coll: Any, record: dict[str, Any]) -> None:
        coll.insert_one(self._doc(record))

    def records(self, coll: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
        """Oldest first; ``limit`` keeps the newest ``limit`` records."""
        cursor = coll.find({"store": self.name}).sort([("seq", -1), ("_id", -1)])
        if limit:
            cursor = cursor.limit(int(limit))
        out = [json.loads(str(doc.get("body") or "{}")) for doc in cursor]
        out.reverse()
        return out

    def get(self, coll: Any, key: str) -> dict[str, Any] | None:
        doc = coll.find_one({"store": self.name, "key": str(key)})
        return json.loads(str(doc.get("body") or "{}")) if doc else None

    def update(self, coll: Any, key: str, change: Callable[[dict[str, Any]], dict[str, Any] | None]) -> dict[str, Any] | None:
        """Compare-and-swap one record. ``change`` returns the new record, or None to leave it."""
        for attempt in range(_MAX_ATTEMPTS):
            if attempt:
                time.sleep(_backoff(attempt - 1))
            doc = coll.find_one({"store": self.name, "key": str(key)})
            if not doc:
                return None
            current = json.loads(str(doc.get("body") or "{}"))
            updated = change(dict(current))
            if updated is None:
                return current
            rev = int(doc.get("rev") or 0)
            result = coll.update_one(
                {"_id": doc["_id"], "rev": rev}, {"$set": {"body": _dumps(updated), "rev": rev + 1}}
            )
            if getattr(result, "matched_count", 0):
                return updated
        raise SharedDocConflict(f"{self.name}:{key}")

    def update_each(self, coll: Any, change: Callable[[dict[str, Any]], dict[str, Any] | None]) -> int:
        """Apply ``change`` to every record (maintenance backfills). Returns how many changed."""
        changed = 0
        for doc in list(coll.find({"store": self.name})):
            updated = change(json.loads(str(doc.get("body") or "{}")))
            if updated is None:
                continue
            rev = int(doc.get("rev") or 0)
            result = coll.update_one(
                {"_id": doc["_id"], "rev": rev}, {"$set": {"body": _dumps(updated), "rev": rev + 1}}
            )
            changed += 1 if getattr(result, "matched_count", 0) else 0
        return changed

    def trim(self, coll: Any, keep: int) -> None:
        """Drop all but the newest ``keep`` records."""
        if coll.count_documents({"store": self.name}) <= keep:
            return
        edge = list(coll.find({"store": self.name}).sort([("seq", -1), ("_id", -1)]).skip(keep).limit(1))
        if edge:
            coll.delete_many({"store": self.name, "seq": {"$lte": edge[0]["seq"]}})

    # -- one-time import ----------------------------------------------
    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
        """Import this node's log file, only while the shared log has no records yet.

        A store-level claim (``import:<store>`` in ``control_plane_docs``)
        names the one file that is imported. A crashed import resumes for
        that file (record ids are deterministic, so nothing is inserted
        twice); any other file, or a changed one, is conflicting local data.
        """
        path = self._file_path()
        stamp = _file_stamp(path)
        if stamp is None or stamp in self._seen_stamps:
            return None
        found = _file_digest(path)
        if found is None:
            return None
        digest, raw = found
        key = f"{path}:{digest}"
        if key in self._imported:
            self._seen_stamps.add(stamp)
            return None
        docs = coll.database[DOCS_COLLECTION]
        claim_id = f"import:{self.name}"
        report: dict[str, Any] | None = None
        settled = False
        for _ in range(3):
            claim = docs.find_one({"_id": claim_id})
            if claim is None:
                # Records without a claim: live appends, or an import by an
                # earlier build (one marker per file digest).
                if docs.find_one({"_id": f"import:{self.name}:{digest}"}):
                    settled = True
                    break
                if coll.find_one({"store": self.name}):
                    report = _record_conflict(docs, self.name, path, digest)
                    settled = True
                    break
                try:
                    docs.insert_one({"_id": claim_id, "kind": "import", "store": self.name, "sha256": digest,
                                     "state": "importing", "at": _now()})
                except Exception as exc:
                    if _is_duplicate(exc):
                        continue
                    raise
                claim = {"sha256": digest, "state": "importing"}
            if claim.get("sha256") != digest:
                report = _record_conflict(docs, self.name, path, digest)
                settled = True
                break
            if claim.get("state") != "done":
                inserted = self._insert_lines(coll, raw)
                docs.update_one({"_id": claim_id}, {"$set": {"state": "done", "records": inserted, "at": _now()}})
                report = {"action": "imported", "sha256": digest, "records": inserted}
                logger.warning("control-plane store %s: imported %s into the shared store (%s)", self.name, path, report)
                _audit_import(self.name, path, report)
            settled = True
            break
        if settled:
            self._imported.add(key)
            self._seen_stamps.add(stamp)
        return report if report and report["action"] != "conflict_already_recorded" else None

    def _insert_lines(self, coll: Any, raw: bytes) -> int:
        inserted = 0
        base = time.time_ns() - 10**12  # imported lines sort before anything appended live
        seen: dict[str, int] = {}
        for index, line in enumerate(raw.decode("utf-8", errors="replace").splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            # The id is the line itself (and which repeat of it), so a crashed
            # import run again inserts nothing twice.
            seen[line] = seen.get(line, 0) + 1
            doc_id = hashlib.sha256(f"{self.name}|{seen[line]}|{line}".encode()).hexdigest()
            try:
                coll.insert_one(self._doc(record, doc_id=doc_id, seq=base + index))
                inserted += 1
            except Exception as exc:
                if not _is_duplicate(exc):
                    raise
        return inserted

    def status(self) -> dict[str, Any]:
        coll = _collection(RECORDS_COLLECTION)
        if coll is None:
            return {"backend": "file", "import_sources": [], "local_file_conflicts": []}
        docs = coll.database[DOCS_COLLECTION]
        claim = docs.find_one({"_id": f"import:{self.name}"}) or {}
        return {
            "backend": "mongo",
            "import_sources": [claim["sha256"]] if claim.get("sha256") else [],
            "local_file_conflicts": _status_conflicts(docs, self.name),
        }

    def import_local_file(self) -> dict[str, Any] | None:
        coll = _collection(RECORDS_COLLECTION)
        if coll is None:
            return None
        return self._import_local_file(coll)


def registered_stores() -> dict[str, Any]:
    """Every SharedDoc / SharedLog constructed so far, by store name."""
    return dict(_REGISTERED)


def store_status() -> dict[str, dict[str, Any]]:
    """Per store: backend, imported file digests and conflicting local files (never merged)."""
    return {name: store.status() for name, store in sorted(_REGISTERED.items())}


_STORE_MODULES = (
    "services.tenant_store",
    "services.byok_key_manager",
    "services.resource_acl",
    "services.notification_store",
    "services.shape_draft_store",
    "services.preflight_run_store",
    "services.cdc_mapping_review",
    "services.mcp_invocation_log",
    "services.audit_anchor",
)


def import_local_files() -> dict[str, dict[str, Any]]:
    """Import every store's local file into the shared store. Safe to run again.

    Called at API start so the import does not wait for the first request that
    happens to read a store. Does nothing in file mode. A file is imported
    once per content (the shared store remembers its SHA-256) and is never
    deleted or rewritten. Returns ``{store: report}`` for the files imported
    by this call.
    """
    import importlib

    if control_store_backend() == "file":
        return {}
    for module in _STORE_MODULES:
        importlib.import_module(module)
    reports: dict[str, dict[str, Any]] = {}
    for name, store in sorted(_REGISTERED.items()):
        report = store.import_local_file()
        if report is not None:
            reports[name] = report
    return reports


# ---------------------------------------------------------------------------
# What a multi-replica deployment may not keep per pod
# ---------------------------------------------------------------------------

#: (store name, file under the data directory, has a shared backend in this build)
CONTROL_PLANE_STORES: tuple[tuple[str, str, bool], ...] = (
    ("tenant_settings", "tenants.json", True),
    ("byok_keys", "byok_keys.json", True),
    ("resource_acls", "resource_acls.jsonl", True),
    ("notification_channels", "notifications.json", True),
    ("shape_drafts", "shape_drafts.json", True),
    ("preflight_runs", "preflight_runs.jsonl", True),
    ("cdc_mapping_reviews", "cdc_mapping_reviews.jsonl", True),
    ("mcp_invocations", "mcp_invocations.jsonl", True),
    ("audit_tip_anchors", "audit_tip_anchors.jsonl", True),
    # No shared backend yet: their modules still read and write the file directly.
    ("transfer_plans", "transfer_plans.json", False),
    ("pilot_results", "pilot_results.json", False),
)
POD_LOCAL_ACK_SETTING = "DATAFLOW_POD_LOCAL_STORES_ACK"


def pod_local_stores_acknowledged() -> frozenset[str]:
    raw = getenv_brand("POD_LOCAL_STORES_ACK", "") or ""
    return frozenset(part.strip().lower() for part in raw.split(",") if part.strip())


def store_topology_errors(*, role_explicit: bool) -> list[str]:
    """Why this multi-replica process may not start with its control-plane stores.

    The caller (:func:`services.process_role.topology_errors`) has already
    established ``MULTI_REPLICA=1``. Stores without a shared backend are
    refused only once the operator names a process role, like the other split
    checks: the one-replica API that never set a role keeps booting.
    """
    errors: list[str] = []
    if control_store_backend() == "file":
        names = ", ".join(f"{name} ({file})" for name, file, shared in CONTROL_PLANE_STORES if shared)
        errors.append(
            f"{BACKEND_SETTING}=file cannot be used when DATAFLOW_MULTI_REPLICA=1. These "
            f"control-plane stores would be one file per pod: {names}. A tenant IP allowlist, "
            "require-SSO flag, BYOK key or resource ACL saved on one replica would be unknown "
            f"to the others. Unset the variable or set {BACKEND_SETTING}=mongo."
        )
    if role_explicit:
        acked = pod_local_stores_acknowledged()
        for name, file, shared in CONTROL_PLANE_STORES:
            if shared or name in acked:
                continue
            errors.append(
                f"Control-plane store {name} ({file}) has no shared backend in this build, so "
                "with DATAFLOW_MULTI_REPLICA=1 every replica keeps its own copy. Mount one "
                "shared data volume on every replica, or accept per-pod behaviour for this "
                f"store, then add it to {POD_LOCAL_ACK_SETTING} (for example "
                f"{POD_LOCAL_ACK_SETTING}={name})."
            )
    return errors
