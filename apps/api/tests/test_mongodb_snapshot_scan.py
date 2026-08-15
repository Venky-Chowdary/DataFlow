"""MongoDB snapshot extract is one find().sort(_id) cursor, not .skip(offset)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.mongodb_reader import read_collection_scan_batch
from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES, close_table_scan


def test_mongodb_is_a_snapshot_scan_source() -> None:
    assert "mongodb" in SNAPSHOT_SCAN_SOURCES


class _Cursor:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = [dict(d) for d in docs]
        self._i = 0
        self.skipped = None
        self.limited = None
        self.batch = None
        self.closed = False

    def sort(self, *a, **k):
        return self

    def batch_size(self, n):
        self.batch = n
        return self

    def skip(self, n):
        self.skipped = n
        return self

    def limit(self, n):
        self.limited = n
        return self

    def __iter__(self):
        return self

    def __next__(self):
        if self._i >= len(self._docs):
            raise StopIteration
        doc = self._docs[self._i]
        self._i += 1
        return doc

    def close(self):
        self.closed = True


def test_mongodb_scan_reuses_one_cursor_and_never_skips() -> None:
    docs = [
        {"_id": "a", "n": 1},
        {"_id": "b", "n": 2},
        {"_id": "c", "n": 3},
    ]
    cur = _Cursor(docs)
    coll = MagicMock()
    coll.count_documents.return_value = 3
    coll.find.return_value = cur
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    client.closed = False

    def _close():
        client.closed = True

    client.close.side_effect = _close

    state: dict = {}
    with patch("connectors.mongodb_reader._mongo_client", return_value=client):
        first = read_collection_scan_batch(
            cfg={"host": "localhost"},
            database="db",
            collection="orders",
            columns=["_id", "n"],
            offset=0,
            limit=2,
            scan_state=state,
        )
        second = read_collection_scan_batch(
            cfg={"host": "localhost"},
            database="db",
            collection="orders",
            columns=["_id", "n"],
            offset=2,
            limit=2,
            scan_state=state,
        )
        third = read_collection_scan_batch(
            cfg={"host": "localhost"},
            database="db",
            collection="orders",
            columns=["_id", "n"],
            offset=4,
            limit=2,
            scan_state=state,
        )

    assert cur.skipped is None
    assert coll.find.call_count == 1
    assert [row[first.headers.index("_id")] for row in first.rows] == ["a", "b"]
    assert [row[second.headers.index("_id")] for row in second.rows] == ["c"]
    assert third.rows == []
    assert cur.closed is True
    assert client.closed is True


def test_close_table_scan_closes_mongo_client() -> None:
    client = MagicMock()
    cur = MagicMock()
    state = {"started": True, "client": client, "cur": cur}
    close_table_scan(state)
    cur.close.assert_called()
    client.close.assert_called()
    assert state == {}


def test_batch_readers_dispatch_mongodb_scan() -> None:
    from src.transfer.batch_readers import _read_batch_impl

    state: dict = {}
    batch = MagicMock()
    with patch(
        "connectors.mongodb_reader.read_collection_scan_batch", return_value=batch
    ) as scan:
        out = _read_batch_impl(
            "mongodb",
            {"host": "localhost"},
            "orders",
            ["_id"],
            0,
            50,
            database="db",
            scan_state=state,
        )
    assert out is batch
    scan.assert_called_once()
    assert scan.call_args.kwargs["scan_state"] is state
