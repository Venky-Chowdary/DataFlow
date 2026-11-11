"""Control-plane documents and logs that every replica must agree on.

Tenant settings, BYOK key records, resource ACLs and their neighbours were one
JSON file per pod. With several API replicas an IP allowlist saved on one pod
was unknown to the others (audit H-5). This module keeps each store's existing
interface and file format, and adds a shared home for it:

``SharedDoc``  one JSON document per store (``control_plane_docs``), written by
               compare-and-swap on a revision number, so two replicas changing
               the same store cannot drop each other's write.
``SharedLog``  one record per line of an append log (``control_plane_records``).

Backend choice (``DATAFLOW_CONTROL_STORE_BACKEND`` / ``DATAWRAP_…``):

``file``   the store's own file under the data directory (single node, dev).
``mongo``  the shared collection; unreachable means the call fails, it never
           falls back to this node's file.
unset      ``mongo`` when ``MULTI_REPLICA=1`` or a control-plane database is
           part of this deployment, else ``file``.

The first time a process reaches the shared store it imports the local file
(once per file content, recorded in the shared document). The file is never
deleted or rewritten afterwards.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from services.brand_env import getenv_brand
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

DOCS_COLLECTION = "control_plane_docs"
RECORDS_COLLECTION = "control_plane_records"
BACKEND_SETTING = "DATAFLOW_CONTROL_STORE_BACKEND"
_MAX_ATTEMPTS = 8
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
    """The shared collection, or None in file mode. Raises when it must exist and does not."""
    if control_store_backend() == "file":
        return None
    from services.control_plane_store import ControlPlaneUnavailable, shared_collection

    coll = shared_collection(name)
    if coll is None:
        raise ControlPlaneUnavailable(
            f"{BACKEND_SETTING}=mongo (or MULTI_REPLICA=1) needs a MongoDB control-plane "
            "database, and none is connected. Configure MONGODB_URI, or set "
            f"{BACKEND_SETTING}=file on a single-node install."
        )
    return coll


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


def _audit_import(store: str, path: Path, details: dict[str, Any]) -> None:
    logger.warning("control-plane store %s: imported %s into the shared store (%s)", store, path, details)
    try:
        from services.audit_coverage import record_security_event

        record_security_event(
            "control_store.imported",
            outcome="succeeded",
            resource=store,
            actor="system",
            details={"source": str(path), **details},
        )
    except Exception:  # noqa: BLE001 - the import already happened; the log line above records it
        logger.debug("control_store.imported audit event not written", exc_info=True)


class SharedDoc:
    """One store's JSON document: file on a single node, compare-and-swap in MongoDB.

    ``file_load`` / ``file_save`` are the store's existing file functions, so a
    single-node install reads and writes exactly what it did before.
    ``merge(shared, local)`` folds a second replica's file into a document a
    first replica already imported; without it the later file is recorded as
    skipped and logged.
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
        merge: Callable[[Any, Any], Any] | None = None,
    ) -> None:
        self.name = name
        self._file_load = file_load
        self._file_save = file_save
        self._file_path = file_path
        self._empty = empty
        self._merge = merge
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
            try:
                for attempt in range(_MAX_ATTEMPTS):
                    try:
                        with self._lock:
                            return fn(*args, **kwargs)
                    except SharedDocConflict:
                        if attempt == _MAX_ATTEMPTS - 1:
                            raise
                        time.sleep(0.005 * (attempt + 1))
            finally:
                self._scope.depth -= 1
            raise SharedDocConflict(self.name)  # pragma: no cover - loop always returns or raises

        return wrapper  # type: ignore[return-value]

    # -- one-time import ----------------------------------------------
    def _import_local_file(self, coll: Any) -> dict[str, Any] | None:
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
        for _ in range(_MAX_ATTEMPTS):
            doc = coll.find_one({"_id": self.name})
            if doc and digest in (doc.get("import_sources") or []):
                break
            local = self._file_load()
            if not doc:
                try:
                    coll.insert_one(
                        {"_id": self.name, "rev": 1, "body": _dumps(local), "updated_at": _now(),
                         "import_sources": [digest]}
                    )
                except Exception as exc:
                    if _is_duplicate(exc):
                        continue
                    raise
                report = {"action": "imported", "sha256": digest}
                break
            rev = int(doc.get("rev") or 0)
            if self._merge is None:
                update: dict[str, Any] = {"$addToSet": {"import_sources": digest}}
                report = {"action": "skipped_shared_store_already_has_data", "sha256": digest}
            else:
                merged = self._merge(json.loads(str(doc.get("body") or "{}")), local)
                update = {
                    "$set": {"body": _dumps(merged), "rev": rev + 1, "updated_at": _now()},
                    "$addToSet": {"import_sources": digest},
                }
                report = {"action": "merged", "sha256": digest}
            result = coll.update_one({"_id": self.name, "rev": rev}, update)
            if getattr(result, "matched_count", 0):
                break
            report = None
        self._imported.add(key)
        self._seen_stamps.add(stamp)
        self._cache = None
        if report is not None:
            _audit_import(self.name, path, report)
        return report

    def import_local_file(self) -> dict[str, Any] | None:
        """Import this node's file now. Returns what happened, None when nothing to do."""
        coll = _collection(DOCS_COLLECTION)
        if coll is None:
            return None
        return self._import_local_file(coll)


def merge_records_by_id(list_key: str, id_key: str = "id") -> Callable[[Any, Any], Any]:
    """Merge for ``{list_key: [records]}`` documents: add local records the shared copy lacks.

    A record both sides hold keeps the shared copy — it may have been edited
    through the shared store since the file was written.
    """

    def merge(shared: Any, local: Any) -> Any:
        out = dict(shared) if isinstance(shared, dict) else {}
        rows = [r for r in (out.get(list_key) or []) if isinstance(r, dict)]
        seen = {str(r.get(id_key)) for r in rows}
        for row in (local.get(list_key) if isinstance(local, dict) else None) or []:
            if isinstance(row, dict) and str(row.get(id_key)) not in seen:
                rows.append(row)
                seen.add(str(row.get(id_key)))
        out[list_key] = rows
        return out

    return merge


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
        for _ in range(_MAX_ATTEMPTS):
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
        marker_id = f"import:{self.name}:{digest}"
        docs = coll.database[DOCS_COLLECTION]
        report: dict[str, Any] | None = None
        if not docs.find_one({"_id": marker_id}):
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
                # The id is the line itself (and which repeat of it), so a second
                # replica importing the same file, or a crashed import run again,
                # inserts nothing twice.
                seen[line] = seen.get(line, 0) + 1
                doc_id = hashlib.sha256(f"{self.name}|{seen[line]}|{line}".encode()).hexdigest()
                try:
                    coll.insert_one(self._doc(record, doc_id=doc_id, seq=base + index))
                    inserted += 1
                except Exception as exc:
                    if not _is_duplicate(exc):
                        raise
            try:
                docs.insert_one({"_id": marker_id, "store": self.name, "records": inserted, "at": _now()})
            except Exception as exc:
                if not _is_duplicate(exc):
                    raise
            report = {"action": "imported", "sha256": digest, "records": inserted}
        self._imported.add(key)
        self._seen_stamps.add(stamp)
        if report is not None:
            _audit_import(self.name, path, report)
        return report

    def import_local_file(self) -> dict[str, Any] | None:
        coll = _collection(RECORDS_COLLECTION)
        if coll is None:
            return None
        return self._import_local_file(coll)


def registered_stores() -> dict[str, Any]:
    """Every SharedDoc / SharedLog constructed so far, by store name."""
    return dict(_REGISTERED)


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
