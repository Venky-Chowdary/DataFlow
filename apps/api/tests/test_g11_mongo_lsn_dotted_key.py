"""G11 review #5: the MongoDB CDC stale-event guard holds with a dotted key.

A CDC upsert keyed on a field path (``key.id`` -> ``{"key": {"id": ...}}``)
prefetches the stored ``_df_lsn`` per key to skip a redelivered older event.
The prefetch keyed stored documents with ``doc.get("key.id")``, which is
absent on a nested document, so no stored LSN matched and a redelivered
``0/10`` overwrote the newer ``0/20``.

Driven as the CDC applier drives the writer (``write_mode="upsert"``, the
``_df_lsn`` column mapped); live MongoDB, throwaway ``g11l_`` database. The
CDC route itself cannot replay a stale event on demand here.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from connectors.writer_common import DF_LSN_COL

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


@pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")
def test_redelivered_older_lsn_is_skipped_with_a_dotted_key():
    import pymongo

    from connectors.mongodb_writer import write_mapped_rows

    db = f"g11l_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        def write(amount: str, lsn: str, create: bool):
            return write_mapped_rows(
                host="localhost", port=27017, database=db, username="", password="",
                schema="", connection_string=MONGO_URI, ssl=False, table_name="orders",
                headers=["id", "amount", DF_LSN_COL],
                data_rows=[["1", amount, lsn]],
                mappings=[
                    {"source": "id", "target": "key.id", "confidence": 1.0},
                    {"source": "amount", "target": "amount", "confidence": 1.0},
                    {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1.0},
                ],
                column_types={"id": "INTEGER", "amount": "TEXT", DF_LSN_COL: "TEXT"},
                create_table=create, write_mode="upsert", conflict_columns=["key.id"],
            )

        assert write("new", "0/20", True).ok
        stale = write("old", "0/10", False)
        assert stale.ok, stale.error
        assert stale.rows_skipped == 1, (stale.rows_written, stale.rows_skipped)
        docs = list(client[db]["orders"].find({}, {"_id": 0}))
        assert docs == [{"key": {"id": 1}, "amount": "new", DF_LSN_COL: "0/20"}], docs
    finally:
        client.drop_database(db)
        client.close()


def test_conflict_key_reads_stored_nested_documents():
    from connectors.mongodb_writer import _mongo_conflict_key

    flat = {"key.id": 1, "x": 2}
    stored = {"key": {"id": 1}, "x": 2}
    assert _mongo_conflict_key(flat, ["key.id"]) == _mongo_conflict_key(stored, ["key.id"])
