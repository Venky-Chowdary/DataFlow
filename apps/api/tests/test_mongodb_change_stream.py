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

    with patch("connectors.mongodb_change_stream._new_mongo_client", return_value=client):
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


def _mock_client_for(coll: MagicMock) -> MagicMock:
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    return client


def _insert_events(n: int, token_prefix: str) -> list:
    return [
        {
            "operationType": "insert",
            "fullDocument": {"_id": f"{token_prefix}{i}", "amount": str(i)},
        }
        for i in range(n)
    ]


def test_poll_advances_own_resume_token_so_catch_up_drains(base_cfg: dict) -> None:
    """Each poll round must resume past the window it just yielded.

    With ``batch_size`` events per round and a fixed ``resume_after``, the
    runner's drain loop replayed the same first window every round and a 7,500
    event catch-up landed 1,000 (Mongo→SQL CDC 100K run 2)."""
    tokens = [{"_data": "w1"}, {"_data": "w2"}]
    windows = [_insert_events(2, "a"), _insert_events(1, "b")]
    watch_calls: list[dict] = []

    def _watch(pipeline=None, **kwargs):
        watch_calls.append(dict(kwargs))
        idx = len(watch_calls) - 1
        stream = MagicMock()
        stream.resume_token = tokens[min(idx, len(tokens) - 1)]
        events = list(windows[idx]) if idx < len(windows) else []
        stream.try_next.side_effect = lambda: events.pop(0) if events else None
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=stream)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    coll = MagicMock()
    coll.watch.side_effect = _watch

    with patch(
        "connectors.mongodb_change_stream._new_mongo_client",
        return_value=_mock_client_for(coll),
    ):
        reader = MongodbChangeStreamCdc(
            base_cfg,
            collection="orders",
            primary_key="_id",
            columns=["_id", "amount"],
            resume_token={"_data": "w0"},
            batch_size=2,
            max_wait_seconds=0.2,
        )
        with patch.object(reader, "_assert_resume_in_oplog"), patch.object(
            reader, "_poll_signal_collection"
        ), patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            side_effect=lambda *a, **k: iter(()),
        ):
            first = list(reader.poll())
            token_after_first = reader.resume_token
            second = list(reader.poll())

    assert [len(b.inserts) for b in first] == [2]
    assert first[0].resume_token == {"_data": "w1"}
    assert token_after_first == {"_data": "w1"}
    assert watch_calls[0]["resume_after"] == {"_data": "w0"}
    assert watch_calls[1]["resume_after"] == {"_data": "w1"}
    assert [len(b.inserts) for b in second] == [1]
    assert reader.resume_token == {"_data": "w2"}


def test_snapshot_hands_stream_position_to_same_instance(base_cfg: dict) -> None:
    """A poll in the same job must start at the pre-snapshot token, not at now."""
    stream = MagicMock()
    stream.resume_token = {"_data": "pre-snapshot"}
    stream.try_next.return_value = None
    coll = MagicMock()
    coll.watch.return_value.__enter__ = MagicMock(return_value=stream)
    coll.watch.return_value.__exit__ = MagicMock(return_value=False)

    batch = MagicMock()
    batch.rows = [["1", "100.00"]]
    batch.headers = ["_id", "amount"]
    batch.total_rows = 1

    with patch(
        "connectors.mongodb_change_stream._new_mongo_client",
        return_value=_mock_client_for(coll),
    ):
        reader = MongodbChangeStreamCdc(
            base_cfg, collection="orders", primary_key="_id", columns=["_id", "amount"]
        )
        assert reader.resume_token is None
        with patch(
            "connectors.mongodb_change_stream.read_collection_cursor_batch",
            return_value=batch,
        ):
            changes = list(reader.snapshot())

    assert changes[-1].resume_token == {"_data": "pre-snapshot"}
    assert reader.resume_token == {"_data": "pre-snapshot"}
