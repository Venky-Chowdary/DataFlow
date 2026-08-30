"""MongoDB Change Streams CDC reader.

Implements log-based CDC for MongoDB using the native ``watch()``/oplog tail.
Initial backfill is read from the collection using the same batched reader as
batch transfers; subsequent invocations tail the change stream and emit
``ChangeBatch`` objects (inserts, updates, deletes) with a resume token.

A real MongoDB replica set is required for ``watch()`` to work. Single-node
instances and the local ``dataflow-mongo`` test container usually do not
support change streams, so the caller should fall back to query-based CDC when
this module raises ``OperationFailure``.

Retention
---------
Resume tokens are valid only while the oplog entry exists (Atlas window is
hours). ``ChangeStreamHistoryLost`` (286) and collection ``invalidate`` are
retention gaps. Poll must not open ``watch()`` without the expired token —
that starts at current clusterTime and skips the lost window. ``when_needed``
blocking-snapshots current documents, then captures a **new** resume token.
Idle namespaces are the default Atlas failure: no events means the token
never moves and the capped oplog wraps. PyMongo updates ``resume_token``
after empty getMores (``postBatchResumeToken``). Poll persists that token
as a position-only heartbeat — the same identity as a Postgres idle-slot
advance and the Kafka connector heartbeat topic. Dummy writes to a
heartbeat collection remain a future enhancement of this kernel when the
server does not advance a collection-scoped token. Not lag proof. Not
continuous CDC. Not ``migration_proven``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

from bson import json_util
from services.brand_env import getenv_brand
from services.cdc_cursor_gap import CdcCursorGapError, CdcOplogGapError
from services.cdc_engine import ChangeBatch

from connectors.mongodb_common import _new_mongo_client
from connectors.mongodb_reader import (
    _connection_string,
    _serialize,
    read_collection_cursor_batch,
)

logger = logging.getLogger(__name__)


def mongo_history_lost(exc: BaseException) -> bool:
    """True for MongoDB error 286 / ChangeStreamHistoryLost (oplog wrap)."""
    code = getattr(exc, "code", None)
    name = str(getattr(exc, "codeName", None) or type(exc).__name__ or "")
    text = str(exc).lower()
    return (
        code == 286
        or name == "ChangeStreamHistoryLost"
        or "resume point may no longer be in the oplog" in text
        or "changestreamhistorylost" in text
    )


def usable_change_stream_resume(token: Any) -> Any | None:
    """Return a watch()-ready resume token, or None if it is not a stream cursor.

    Snapshot-phase wrappers and the synthetic ``phase=streaming`` handoff
    without ``_data`` must not be passed to ``resumeAfter`` — that is not a
    clusterTime. Nested ``token`` on a streaming wrapper is unwrapped.
    """
    if token is None:
        return None
    if isinstance(token, dict):
        phase = str(token.get("phase") or "").strip().lower()
        if phase == "snapshot":
            return usable_change_stream_resume(token.get("token"))
        if "token" in token and phase == "streaming":
            nested = usable_change_stream_resume(token.get("token"))
            if nested is not None:
                return nested
        if token.get("_data"):
            return token
        return None
    text = str(token).strip()
    if not text or text.lower() in {"none", "null", "~"}:
        return None
    if text.startswith("{"):
        try:
            return usable_change_stream_resume(json_util.loads(text))
        except Exception:
            return {"_data": text} if text else None
    return {"_data": text}


def _oplog_unix(ts: Any) -> int | None:
    if ts is None:
        return None
    try:
        if hasattr(ts, "time"):
            return int(ts.time)
        return int(ts)
    except (TypeError, ValueError):
        return None


def _database_name(cfg: dict[str, Any]) -> str:
    from connectors.mongodb_common import mongodb_database_from_uri

    return cfg.get("database") or mongodb_database_from_uri(_connection_string(cfg)) or "test"


def _has_key(pk: str) -> bool:
    from services.cdc_identity import is_present_cdc_row_key

    return is_present_cdc_row_key(pk)


def _doc_to_record(doc: dict[str, Any], columns: list[str] | None) -> dict[str, Any]:
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if not columns:
        return {k: _serialize(v) for k, v in doc.items()}
    # Known columns first, then any sparse extras — never silent-drop mid-stream fields.
    out = {c: _serialize(doc.get(c)) for c in columns}
    for k, v in doc.items():
        if k not in out:
            out[k] = _serialize(v)
    return out


class MongodbChangeStreamCdc:
    """Log-based CDC for MongoDB: snapshot + change-stream tail."""

    def __init__(
        self,
        cfg: dict[str, Any],
        collection: str,
        primary_key: str,
        columns: list[str] | None = None,
        resume_token: dict[str, Any] | str | None = None,
        batch_size: int = 1000,
        max_wait_seconds: float = 30.0,
        full_document: str = "updateLookup",
    ) -> None:
        self.cfg = cfg
        self.db_name = _database_name(cfg)
        self.collection = collection
        self.primary_key = primary_key or "_id"
        self.columns = columns
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.full_document = full_document
        # Dedicated (uncached) client: this stream owns the lifecycle and calls
        # close() on lease release. Sharing the pooled client would let one
        # stream's shutdown poison every concurrent bulk reader/writer on the
        # same URI ("Cannot use MongoClient after close").
        self.client = _new_mongo_client(_connection_string(cfg))
        self.coll = self.client[self.db_name][collection]
        if isinstance(resume_token, str):
            try:
                self.resume_token = json_util.loads(resume_token) if resume_token.startswith("{") else {"_data": resume_token}
            except Exception:
                self.resume_token = {"_data": resume_token}
        else:
            self.resume_token = resume_token
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "mongodb"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self._processed_signal_ids: set[str] = set()
        self.signal_collection = str(cfg.get("signal_table") or cfg.get("signal_collection") or "dataflow_signal")
        self._last_signal_poll_at = 0.0
        self._signal_poll_interval_sec = float(cfg.get("signal_poll_interval_sec") or 15)
        self._signal_index_ready = False
        import os

        from services.cdc_lease import CdcLeaseGuard

        cursor_key = str(cfg.get("cursor_key") or f"mongodb:{self.db_name}:{collection}")
        self.cursor_key = cursor_key
        holder = str(
            cfg.get("lease_holder_id") or getenv_brand("CDC_LEASE_HOLDER") or ""
        )
        self._lease = CdcLeaseGuard(
            cursor_key=cursor_key,
            resource=f"mongo_cs:{self.db_name}:{collection}",
            holder_id=holder,
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "engine": "mongodb",
                "database": self.db_name,
                "collection": collection,
            },
        )
        self._oplog_catalog_cache: dict[str, Any] | None = None
        self._oplog_catalog_cache_at: float = 0.0
        # A delete event publishes ``documentKey`` (``_id``) and nothing else. A
        # pipeline keyed on a business column can only name the deleted row from
        # the collection's pre-image, so ask for one whenever the key is not
        # ``_id`` — and refuse the delete rather than drop it if none arrives.
        self._needs_pre_image = self.primary_key != "_id"
        self._pre_images_enabled: bool | None = None

    @property
    def lease_holder_id(self) -> str:
        return self._lease.holder_id

    @lease_holder_id.setter
    def lease_holder_id(self, value: str) -> None:
        self._lease.holder_id = value

    @property
    def _lease_acquired(self) -> bool:
        return self._lease.acquired

    def _acquire_cdc_lease(self) -> None:
        """Fail-fast if another worker already owns this change-stream cursor."""
        self._lease.ensure()

    def _usable_resume(self) -> Any | None:
        return usable_change_stream_resume(self.resume_token)

    def _oplog_catalog_status(self, *, max_age_sec: float = 2.0) -> dict[str, Any]:
        """Oldest/newest ``local.oplog.rs`` timestamps vs resume-token clusterTime."""
        import time as _time

        from services.cdc_retention_probe import resume_token_unix_seconds

        now = _time.monotonic()
        if (
            self._oplog_catalog_cache is not None
            and (now - float(self._oplog_catalog_cache_at or 0.0))
            < max(0.25, float(max_age_sec))
        ):
            return dict(self._oplog_catalog_cache)

        resume = self._usable_resume()
        out: dict[str, Any] = {
            "plugin": "mongodb_change_stream",
            "resume_unix": resume_token_unix_seconds(resume),
            "oldest_oplog_unix": None,
            "newest_oplog_unix": None,
            "collection": self.collection,
        }
        try:
            oplog = self.client.local["oplog.rs"]
            oldest_doc = oplog.find_one(sort=[("$natural", 1)]) or {}
            newest_doc = oplog.find_one(sort=[("$natural", -1)]) or {}
            out["oldest_oplog_unix"] = _oplog_unix(oldest_doc.get("ts"))
            out["newest_oplog_unix"] = _oplog_unix(newest_doc.get("ts"))
        except Exception as exc:
            logger.debug("MongoDB oplog catalog probe failed: %s", exc)
            out["error"] = str(exc)[:300]
        self._oplog_catalog_cache = dict(out)
        self._oplog_catalog_cache_at = now
        return dict(out)

    def _raise_oplog_gap(
        self,
        probe: Any,
        *,
        history_lost: bool = False,
        invalidated: bool = False,
    ) -> None:
        raise CdcOplogGapError(
            probe.message,
            resume_unix=probe.resume,
            oldest_oplog_unix=probe.retained,
            cursor_key=self.cursor_key,
        )

    def _assert_resume_in_oplog(self, resume: Any = None) -> None:
        """Raise :class:`CdcOplogGapError` when the token is before retained oplog.

        Unknown catalog (no ``local`` privilege / mongos) does not invent a gap —
        ``watch()`` still fail-closes on 286. Poll must never open ``watch()``
        without the token in order to "skip" a lost window.
        """
        from services.cdc_retention_probe import (
            classify_mongo_oplog_retention,
            resume_token_unix_seconds,
        )

        token = resume if resume is not None else self._usable_resume()
        if token is None:
            return
        catalog: dict[str, Any] = {}
        try:
            catalog = dict(self._oplog_catalog_status(max_age_sec=0) or {})
        except Exception as exc:
            logger.debug("oplog catalog during resume assert: %s", exc)
        probe = classify_mongo_oplog_retention(
            catalog.get("resume_unix")
            if catalog.get("resume_unix") is not None
            else resume_token_unix_seconds(token),
            catalog.get("oldest_oplog_unix"),
            newest_oplog_unix=catalog.get("newest_oplog_unix"),
            cursor_key=self.cursor_key,
        )
        if probe.status == "gap":
            self._raise_oplog_gap(probe)

    def _token_is_oplog_gap(self, token: Any) -> bool:
        from services.cdc_retention_probe import (
            classify_mongo_oplog_retention,
            resume_token_unix_seconds,
        )

        resume_unix = resume_token_unix_seconds(token)
        if resume_unix is None:
            return False
        try:
            catalog = dict(self._oplog_catalog_status(max_age_sec=0) or {})
        except Exception:
            return False
        oldest = catalog.get("oldest_oplog_unix")
        if oldest is None:
            return False
        probe = classify_mongo_oplog_retention(
            resume_unix, oldest, cursor_key=self.cursor_key
        )
        return probe.status == "gap"

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk for signal-driven incremental snapshots (_id or configured PK)."""
        pk = sig.primary_key or self.primary_key or "_id"
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        query: dict[str, Any] = {}
        if last_pk:
            try:
                from bson import ObjectId

                if pk == "_id" and ObjectId.is_valid(last_pk):
                    query[pk] = {"$gt": ObjectId(last_pk)}
                else:
                    query[pk] = {"$gt": last_pk}
            except Exception:
                query[pk] = {"$gt": last_pk}
        cursor = self.coll.find(query).sort(pk, 1).limit(limit)
        records: list[dict[str, Any]] = []
        new_last = last_pk
        for doc in cursor:
            rec = _doc_to_record(doc, self.columns)
            records.append(rec)
            raw = doc.get(pk)
            new_last = str(raw) if raw is not None else new_last
        done = len(records) < limit
        return records, new_last, done

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Yield the full collection as INSERT-only batches.

        Opens a change stream briefly first to capture a resume token so the
        subsequent poll does not miss events that arrived during the snapshot
        (at-least-once; duplicates possible).
        """
        self._acquire_cdc_lease()
        try:
            start_token: Any = None
            try:
                with self.coll.watch(full_document=self.full_document, max_await_time_ms=100) as stream:
                    stream.try_next()
                    start_token = stream.resume_token
            except Exception:
                start_token = None

            last_id: str | None = None
            legacy_offset: int | None = None
            if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
                raw_last = self.resume_token.get("last_id")
                if raw_last not in (None, ""):
                    last_id = str(raw_last)
                else:
                    # Pre-Wave Y offset tokens — honor once, then switch to _id keyset.
                    legacy_offset = int(self.resume_token.get("offset") or 0)
                start_token = self.resume_token.get("token") or start_token
                if start_token is not None and self._token_is_oplog_gap(start_token):
                    # Snapshot recovery must not hand off an expired nested token
                    # (same class as recreating a PG slot only during snapshot).
                    start_token = None
            while True:
                if legacy_offset is not None:
                    from connectors.mongodb_reader import read_collection_batch

                    batch = read_collection_batch(
                        cfg=self.cfg,
                        database=self.db_name,
                        collection=self.collection,
                        columns=self.columns,
                        offset=legacy_offset,
                        limit=self.batch_size,
                    )
                    legacy_offset = None
                else:
                    # _id keyset is delete-safe; SKIP/LIMIT is not under concurrent deletes.
                    batch = read_collection_cursor_batch(
                        cfg=self.cfg,
                        database=self.db_name,
                        collection=self.collection,
                        cursor_column="_id",
                        cursor_after=last_id,
                        cursor_type="STRING",
                        columns=self.columns,
                        limit=self.batch_size,
                    )
                if not batch.rows:
                    break
                records = [_doc_to_record(dict(zip(batch.headers, row)), self.columns) for row in batch.rows]
                if "_id" in batch.headers:
                    last_id = str(batch.rows[-1][batch.headers.index("_id")])
                # Persist snapshot progress + change-stream handoff on every batch.
                yield ChangeBatch(
                    inserts=records,
                    resume_token={
                        "phase": "snapshot",
                        "last_id": last_id,
                        "token": start_token,
                        "collection": self.collection,
                    },
                )
                if len(batch.rows) < self.batch_size:
                    break
            if start_token is not None:
                yield ChangeBatch(resume_token=start_token)
            else:
                yield ChangeBatch(
                    resume_token={"phase": "streaming", "offset": 0, "collection": self.collection}
                )
        finally:
            # Snapshot is a one-off backfill; release the lease so a resuming
            # poll consumer (same or different process) can take over immediately.
            self._lease.release()

    def pre_images_enabled(self) -> bool:
        """Whether the collection records change-stream pre-images (Mongo 6+)."""
        if self._pre_images_enabled is None:
            enabled = False
            try:
                info = self.client[self.db_name].command(
                    "listCollections", filter={"name": self.collection}
                )
                for entry in info.get("cursor", {}).get("firstBatch", []):
                    opts = entry.get("options") or {}
                    images = opts.get("changeStreamPreAndPostImages") or {}
                    enabled = bool(images.get("enabled"))
            except Exception as exc:
                logger.warning("pre-image capability probe failed: %s", exc)
            self._pre_images_enabled = enabled
        return self._pre_images_enabled

    def _watch_kwargs(self, base: dict[str, Any]) -> dict[str, Any]:
        """Add ``fullDocumentBeforeChange`` when deletes need a business key."""
        if self._needs_pre_image and self.pre_images_enabled():
            base["full_document_before_change"] = "whenAvailable"
        return base

    def _delete_key(self, change: dict[str, Any]) -> str:
        """Business key of a deleted document, or fail closed.

        The pre-image is the only place a delete event can carry a non-``_id``
        column. Returning "no key" here would silently leave the row at the
        destination, which is the divergence log-based CDC exists to prevent, so
        an unresolvable delete raises with the ``collMod`` remedy instead.
        """
        before = change.get("fullDocumentBeforeChange")
        if isinstance(before, dict):
            pk = self._pk_value(before)
            if _has_key(pk):
                return pk
        pk = self._pk_value(change)
        if _has_key(pk):
            return pk
        from services.cdc_capability import (
            LogCaptureUnavailable,
            mongo_delete_key_refusal,
        )

        raise LogCaptureUnavailable(
            mongo_delete_key_refusal(self.db_name, self.collection, self.primary_key),
            "mongodb",
        )

    def _pk_value(self, doc: dict[str, Any]) -> str:
        from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

        value = doc.get(self.primary_key)
        if value is None and "documentKey" in doc:
            value = doc["documentKey"].get(self.primary_key)
        if value is None:
            return SQL_NULL_SENTINEL
        return cell_to_string(value, preserve_sql_null=True)

    def _full_doc(self, change: dict[str, Any]) -> dict[str, Any] | None:
        return change.get("fullDocument") or change.get("documentKey")

    def _poll_signal_collection(self) -> None:
        import time as _time

        now = _time.monotonic()
        if (now - self._last_signal_poll_at) < max(1.0, self._signal_poll_interval_sec):
            return
        from services.cdc_signal_table import poll_mongo_signal_collection

        try:
            _, self._processed_signal_ids = poll_mongo_signal_collection(
                self.client[self.db_name],
                source_key=self.source_key,
                collection=self.signal_collection,
                default_table=self.collection,
                primary_key=self.primary_key,
                processed_ids=self._processed_signal_ids,
                ensure_index=not self._signal_index_ready,
            )
            self._signal_index_ready = True
            self._last_signal_poll_at = now
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    def close(self) -> None:
        """Release the CDC lease and MongoClient — required under multi-job load."""
        try:
            self._lease.release()
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        try:
            self.client.close()
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking change-stream peek for DDD-3 stream-wins during incremental snapshot."""
        events: list[dict[str, Any]] = []
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        watch_kwargs: dict[str, Any] = self._watch_kwargs({
            "full_document": self.full_document,
            "max_await_time_ms": 200,
        })
        resume = self._usable_resume()
        if resume:
            watch_kwargs["resume_after"] = resume
        try:
            self._assert_resume_in_oplog(resume)
            with self.coll.watch(None, **watch_kwargs) as stream:
                deadline = time.monotonic() + 1.5
                while time.monotonic() < deadline and len(events) < peek_limit:
                    change = stream.try_next()
                    if change is None:
                        break
                    op = change.get("operationType")
                    doc = self._full_doc(change)
                    if op == "insert" and doc:
                        events.append({"op": "c", "row": _doc_to_record(doc, self.columns)})
                    elif op in ("update", "replace") and doc:
                        events.append({"op": "u", "row": _doc_to_record(doc, self.columns)})
                    elif op == "delete":
                        pk = self._delete_key(change)
                        events.append({"op": "d", "pk": pk, "row": {self.primary_key: pk}})
                    elif op == "invalidate":
                        from services.cdc_retention_probe import classify_mongo_oplog_retention

                        probe = classify_mongo_oplog_retention(
                            None, None, invalidated=True, cursor_key=self.cursor_key
                        )
                        self._raise_oplog_gap(probe, invalidated=True)
        except CdcCursorGapError:
            raise
        except Exception as exc:
            if mongo_history_lost(exc):
                from services.cdc_retention_probe import classify_mongo_oplog_retention

                probe = classify_mongo_oplog_retention(
                    None, None, history_lost=True, cursor_key=self.cursor_key
                )
                self._raise_oplog_gap(probe, history_lost=True)
            return events
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        """Tail the change stream for a bounded window and yield one ChangeBatch."""
        self._acquire_cdc_lease()
        if isinstance(self.resume_token, dict) and self.resume_token.get("phase") == "snapshot":
            yield from self.snapshot()
            return

        self._poll_signal_collection()

        from services.cdc_incremental_runner import interleave_incremental_snapshot

        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.collection,
            fetch_chunk=self._fetch_incremental_chunk,
            stream_events_during_chunk=self._peek_stream_events_during_chunk,
            max_chunks_per_poll=1,
            dest_resume=self.resume_token,
        )

        pipeline: list[dict[str, Any]] | None = None
        watch_kwargs: dict[str, Any] = self._watch_kwargs({
            "full_document": self.full_document,
            "max_await_time_ms": 1000,
        })
        resume = self._usable_resume()
        self._assert_resume_in_oplog(resume)
        if resume:
            watch_kwargs["resume_after"] = resume

        try:
            with self.coll.watch(pipeline, **watch_kwargs) as stream:
                inserts: list[dict[str, Any]] = []
                updates: list[dict[str, Any]] = []
                deletes: list[str] = []
                start = time.monotonic()
                last_token: Any = None
                while time.monotonic() - start < self.max_wait_seconds:
                    change = stream.try_next()
                    # Empty getMore still publishes postBatchResumeToken.
                    # Skipping it is the Atlas idle-namespace wrap.
                    token = getattr(stream, "resume_token", None)
                    if token is not None:
                        last_token = token
                    if change is None:
                        continue
                    op = change.get("operationType")
                    doc = self._full_doc(change)
                    if op in ("insert", "replace", "update"):
                        if not doc:
                            continue
                        record = _doc_to_record(doc, self.columns)
                        if op == "insert":
                            inserts.append(record)
                        else:
                            updates.append(record)
                    elif op == "delete":
                        deletes.append(self._delete_key(change))
                    elif op == "invalidate":
                        from services.cdc_retention_probe import classify_mongo_oplog_retention

                        probe = classify_mongo_oplog_retention(
                            None, None, invalidated=True, cursor_key=self.cursor_key
                        )
                        self._raise_oplog_gap(probe, invalidated=True)

                    if len(inserts) + len(updates) + len(deletes) >= self.batch_size:
                        break

                # Empty DML + token is a position-only heartbeat (PG idle-slot
                # identity). Apply persists the watermark; lag is not 0.
                if inserts or updates or deletes or last_token is not None:
                    yield ChangeBatch(
                        inserts=inserts,
                        updates=updates,
                        deletes=deletes,
                        resume_token=last_token,
                    )
        except CdcCursorGapError:
            raise
        except Exception as exc:
            if mongo_history_lost(exc):
                from services.cdc_retention_probe import (
                    classify_mongo_oplog_retention,
                    resume_token_unix_seconds,
                )

                probe = classify_mongo_oplog_retention(
                    resume_token_unix_seconds(resume) if resume is not None else None,
                    None,
                    history_lost=True,
                    cursor_key=self.cursor_key,
                )
                self._raise_oplog_gap(probe, history_lost=True)
            raise

    def is_available(self) -> bool:
        """Return True if the deployment supports change streams."""
        try:
            with self.coll.watch(max_await_time_ms=100) as stream:
                stream.try_next()
            return True
        except Exception:
            return False
