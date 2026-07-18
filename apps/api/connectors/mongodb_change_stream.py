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
    read_collection_batch,
)
from services.cdc_engine import ChangeBatch


def _database_name(cfg: dict[str, Any]) -> str:
    from connectors.mongodb_common import mongodb_database_from_uri

    return cfg.get("database") or mongodb_database_from_uri(_connection_string(cfg)) or "test"


def _doc_to_record(doc: dict[str, Any], columns: list[str] | None) -> dict[str, Any]:
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if columns:
        return {c: _serialize(doc.get(c)) for c in columns}
    return {k: _serialize(v) for k, v in doc.items()}


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

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Yield the full collection as INSERT-only batches."""
        offset = 0
        while True:
            batch = read_collection_batch(
                cfg=self.cfg,
                database=self.db_name,
                collection=self.collection,
                columns=self.columns,
                offset=offset,
                limit=self.batch_size,
            )
            if not batch.rows:
                break
            records = [_doc_to_record(dict(zip(batch.headers, row)), self.columns) for row in batch.rows]
            yield ChangeBatch(inserts=records)
            offset += len(batch.rows)
            if len(batch.rows) < self.batch_size:
                break

    def _pk_value(self, doc: dict[str, Any]) -> str:
        from services.value_serializer import cell_to_string

        value = doc.get(self.primary_key)
        if value is None and "documentKey" in doc:
            value = doc["documentKey"].get(self.primary_key)
        return cell_to_string(value) if value is not None else ""

    def _full_doc(self, change: dict[str, Any]) -> dict[str, Any] | None:
        return change.get("fullDocument") or change.get("documentKey")

    def poll(self) -> Iterator[ChangeBatch]:
        """Tail the change stream for a bounded window and yield one ChangeBatch."""
        pipeline: list[dict[str, Any]] | None = None
        watch_kwargs: dict[str, Any] = {
            "full_document": self.full_document,
            "max_await_time_ms": 1000,
        }
        if self.resume_token:
            watch_kwargs["resume_after"] = self.resume_token

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
