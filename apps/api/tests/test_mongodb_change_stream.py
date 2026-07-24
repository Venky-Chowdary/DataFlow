"""Unit tests for the MongoDB Change Streams CDC reader.

Uses mocked PyMongo objects because a real replica set is not available in
most test environments.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.mongodb_change_stream import MongodbChangeStreamCdc
from services.cdc_engine import ChangeBatch


@pytest.fixture
def base_cfg():
    from services.cdc_lease import configure_store, reset_store

    configure_store(backend="memory")
    yield {
        "host": "localhost",
        "port": 27017,
        "database": "test",
        "username": "",
        "password": "",
        "auth_source": "",
        "ssl": False,
        "connection_string": "",
        "cursor_key": "test-mongo-cs-unit",
        "lease_holder_id": "mongo-unit-test",
    }
    reset_store()


def test_snapshot_reads_collection_batch(base_cfg: dict) -> None:
    reader = MongodbChangeStreamCdc(
        base_cfg,
        collection="orders",
        primary_key="_id",
        columns=["_id", "amount"],
    )
    batch = MagicMock()
    batch.rows = [["1", "100.00"], ["2", "200.00"]]
    batch.headers = ["_id", "amount"]
    batch.total_rows = 2

    with patch("connectors.mongodb_change_stream.read_collection_cursor_batch") as mock_read:
        mock_read.return_value = batch
        changes = list(reader.snapshot())

    # Data batch + streaming handoff token (Debezium-class snapshot→stream).
    assert len(changes) == 2
    assert isinstance(changes[0], ChangeBatch)
    assert len(changes[0].inserts) == 2
    assert changes[0].inserts[0] == {"_id": "1", "amount": "100.00"}
    assert changes[0].resume_token["last_id"] == "2"
    assert changes[1].resume_token is not None
    mock_read.assert_called_once()
    kwargs = mock_read.call_args.kwargs
    assert kwargs["cursor_column"] == "_id"
    assert kwargs["cursor_after"] is None


def test_snapshot_resume_uses_id_keyset_not_offset(base_cfg: dict) -> None:
    reader = MongodbChangeStreamCdc(
        base_cfg,
        collection="orders",
        primary_key="_id",
        columns=["_id", "amount"],
        resume_token={
            "phase": "snapshot",
            "last_id": "aaaaaaaaaaaaaaaaaaaaaaaa",
            "token": {"_data": "tok"},
            "collection": "orders",
        },
    )
    batch = MagicMock()
    batch.rows = [["bbbbbbbbbbbbbbbbbbbbbbbb", "9"]]
    batch.headers = ["_id", "amount"]

    with patch("connectors.mongodb_change_stream.read_collection_cursor_batch") as mock_read:
        mock_read.return_value = batch
        changes = list(reader.snapshot())

    assert changes[0].resume_token["last_id"] == "bbbbbbbbbbbbbbbbbbbbbbbb"
    kwargs = mock_read.call_args.kwargs
    assert kwargs["cursor_after"] == "aaaaaaaaaaaaaaaaaaaaaaaa"
    assert "offset" not in kwargs


def _change_events():
    yield {"operationType": "insert", "fullDocument": {"_id": "a1", "amount": 100}}
    yield {"operationType": "update", "fullDocument": {"_id": "a2", "amount": 200}}
    yield {"operationType": "delete", "documentKey": {"_id": "a3"}}
    while True:
        yield None


def test_poll_yields_insert_update_delete(base_cfg: dict) -> None:
    stream = MagicMock()
    stream.resume_token = {"_data": "resume123"}
    stream.try_next.side_effect = _change_events()

    coll = MagicMock()
    coll.watch.return_value.__enter__ = MagicMock(return_value=stream)
    coll.watch.return_value.__exit__ = MagicMock(return_value=False)

    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)

    with patch("connectors.mongodb_change_stream._mongo_client", return_value=client):
        reader = MongodbChangeStreamCdc(
            base_cfg,
            collection="orders",
            primary_key="_id",
            columns=["_id", "amount"],
            max_wait_seconds=0.5,
        )
        changes = list(reader.poll())

    assert len(changes) == 1
    change = changes[0]
    assert len(change.inserts) == 1
    assert len(change.updates) == 1
    assert len(change.deletes) == 1
    assert change.deletes == ["a3"]
    assert change.resume_token == {"_data": "resume123"}


def test_resume_token_string_parsing(base_cfg: dict) -> None:
    reader = MongodbChangeStreamCdc(
        base_cfg,
        collection="orders",
        primary_key="_id",
        resume_token='{"_data": "abc"}',
    )
    assert reader.resume_token == {"_data": "abc"}


def test_resume_token_data_string_parsing(base_cfg: dict) -> None:
    reader = MongodbChangeStreamCdc(
        base_cfg,
        collection="orders",
        primary_key="_id",
        resume_token="abc",
    )
    assert reader.resume_token == {"_data": "abc"}
