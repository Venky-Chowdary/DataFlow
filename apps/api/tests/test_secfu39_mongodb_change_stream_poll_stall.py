"""MongoDB change-stream ``poll()`` reads the available batch even when its window is spent first (robustness).

CI shard 2 failed ``test_mongodb_change_stream.py::test_poll_yields_insert_update_delete``
with ``assert 0 == 1``: ``poll()`` started its ``max_wait_seconds`` window
before ``_cluster_head()`` (a ``hello`` round trip), so a stall longer than the
window there meant the read loop never ran and a batch that was already
available was not returned. ``poll()`` now always reads what the stream has
(until the first empty read or ``batch_size``); the window bounds only further
reads.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from connectors.mongodb_change_stream import MongodbChangeStreamCdc


@pytest.fixture
def base_cfg():
    from services.cdc_lease import configure_store, reset_store

    configure_store(backend="memory")
    yield {
        "host": "localhost", "port": 27017, "database": "test", "username": "", "password": "",
        "auth_source": "", "ssl": False, "connection_string": "",
        "cursor_key": "test-mongo-cs-stall", "lease_holder_id": "mongo-stall-test",
    }
    reset_store()


def _events():
    yield {"operationType": "insert", "fullDocument": {"_id": "a1", "amount": 100}}
    yield {"operationType": "update", "fullDocument": {"_id": "a2", "amount": 200}}
    yield {"operationType": "delete", "documentKey": {"_id": "a3"}}
    while True:
        yield None


def _client(stream: MagicMock) -> MagicMock:
    coll = MagicMock()
    coll.watch.return_value.__enter__ = MagicMock(return_value=stream)
    coll.watch.return_value.__exit__ = MagicMock(return_value=False)
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    client = MagicMock()
    client.__getitem__ = MagicMock(return_value=db)
    return client


def _poll(base_cfg: dict, stream: MagicMock, *, head_stall: float) -> tuple[list, float]:
    def slow_head(_self):
        time.sleep(head_stall)  # a slow ``hello`` (a loaded CI runner, a far replica set)
        return None

    with patch("connectors.mongodb_change_stream._new_mongo_client", return_value=_client(stream)), \
            patch.object(MongodbChangeStreamCdc, "_cluster_head", slow_head):
        reader = MongodbChangeStreamCdc(base_cfg, collection="orders", primary_key="_id",
                                        columns=["_id", "amount"], max_wait_seconds=0.5)
        started = time.monotonic()
        changes = list(reader.poll())
    return changes, time.monotonic() - started


def test_a_stall_before_the_read_loop_still_returns_the_available_batch(base_cfg):
    stream = MagicMock()
    stream.resume_token = {"_data": "resume123"}
    stream.try_next.side_effect = _events()

    changes, _elapsed = _poll(base_cfg, stream, head_stall=0.6)

    assert len(changes) == 1
    change = changes[0]
    assert (len(change.inserts), len(change.updates), change.deletes) == (1, 1, ["a3"])
    assert change.resume_token == {"_data": "resume123"}


def test_the_window_still_bounds_an_idle_stream(base_cfg):
    stream = MagicMock()
    stream.resume_token = None
    stream.try_next.return_value = None

    changes, elapsed = _poll(base_cfg, stream, head_stall=0.0)

    assert changes == []
    assert elapsed < 5.0
    assert stream.try_next.call_count >= 1
