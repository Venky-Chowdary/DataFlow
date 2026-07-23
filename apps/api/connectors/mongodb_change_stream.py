"""MongoDB Change Streams CDC reader.

Implements log-based CDC for MongoDB using the native ``watch()``/oplog tail.
Initial backfill is read from the collection using the same batched reader as
batch transfers; subsequent invocations tail the change stream and emit
``ChangeBatch`` objects (inserts, updates, deletes) with a resume token.

A real MongoDB replica set is required for ``watch()`` to work. Single-node
instances and the local ``dataflow-mongo`` test container usually do not
support change streams, so the caller should fall back to query-based CDC when
this module raises ``OperationFailure``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

from bson import json_util

from connectors.mongodb_common import _mongo_client
from connectors.mongodb_reader import (
    _connection_string,
    _serialize,
    read_collection_cursor_batch,
)
from services.cdc_engine import ChangeBatch


def _database_name(cfg: dict[str, Any]) -> str:
    from connectors.mongodb_common import mongodb_database_from_uri

    return cfg.get("database") or mongodb_database_from_uri(_connection_string(cfg)) or "test"


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
        self.client = _mongo_client(_connection_string(cfg))
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
        holder = str(
            cfg.get("lease_holder_id") or os.getenv("DATAFLOW_CDC_LEASE_HOLDER") or ""
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

    def _pk_value(self, doc: dict[str, Any]) -> str:
        from services.value_serializer import cell_to_string

        value = doc.get(self.primary_key)
        if value is None and "documentKey" in doc:
            value = doc["documentKey"].get(self.primary_key)
        return cell_to_string(value, preserve_sql_null=True) if value is not None else ""

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
        except Exception:
            pass

    def close(self) -> None:
        """Release the CDC lease and MongoClient — required under multi-job load."""
        try:
            self._lease.release()
        except Exception:
            pass
        try:
            self.client.close()
        except Exception:
            pass

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking change-stream peek for DDD-3 stream-wins during incremental snapshot."""
        events: list[dict[str, Any]] = []
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        watch_kwargs: dict[str, Any] = {
            "full_document": self.full_document,
            "max_await_time_ms": 200,
        }
        resume = self.resume_token
        if isinstance(resume, dict) and "token" in resume and resume.get("phase") == "streaming":
            resume = resume.get("token")
        if resume:
            watch_kwargs["resume_after"] = resume
        try:
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
                        pk = self._pk_value(change)
                        if pk:
                            events.append({"op": "d", "pk": pk, "row": {self.primary_key: pk}})
        except Exception:
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
        )

        pipeline: list[dict[str, Any]] | None = None
        watch_kwargs: dict[str, Any] = {
            "full_document": self.full_document,
            "max_await_time_ms": 1000,
        }
        resume = self.resume_token
        if isinstance(resume, dict) and "token" in resume and resume.get("phase") == "streaming":
            resume = resume.get("token")
        if resume:
            watch_kwargs["resume_after"] = resume

        with self.coll.watch(pipeline, **watch_kwargs) as stream:
            inserts: list[dict[str, Any]] = []
            updates: list[dict[str, Any]] = []
            deletes: list[str] = []
            start = time.monotonic()
            last_token: Any = None
            while time.monotonic() - start < self.max_wait_seconds:
                change = stream.try_next()
                if change is None:
                    continue
                last_token = stream.resume_token
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
                    pk = self._pk_value(change)
                    if pk:
                        deletes.append(pk)
                elif op == "invalidate":
                    break

                if len(inserts) + len(updates) + len(deletes) >= self.batch_size:
                    break

            if inserts or updates or deletes or last_token is not None:
                yield ChangeBatch(
                    inserts=inserts,
                    updates=updates,
                    deletes=deletes,
                    resume_token=last_token,
                )

    def is_available(self) -> bool:
        """Return True if the deployment supports change streams."""
        try:
            with self.coll.watch(max_await_time_ms=100) as stream:
                stream.try_next()
            return True
        except Exception:
            return False
