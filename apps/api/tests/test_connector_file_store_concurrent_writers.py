"""The file-backed connector store does not lose a connector to a concurrent writer.

Every mutation of the file store is a read-modify-write of the whole list. A
job's terminal stamp on its connectors (``note_transfer_succeeded``, run on the
worker thread as the job turns completed) interleaving with a request saving a
new connector made each writer read the file, apply its own change and write
back; the later write dropped the other's row, so the connector a user had just
saved was "Connector not found" a moment later (D195 run 2 in CI, where the
MySQL load is fast enough for the two to overlap).
"""

from __future__ import annotations

import threading
import uuid

import pytest


@pytest.fixture()
def file_store(tmp_path, monkeypatch):
    store = tmp_path / "connectors.json"
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.setenv(f"{prefix}_CONNECTOR_STORE", str(store))
        monkeypatch.setenv(f"{prefix}_CONNECTOR_STORE_BACKEND", "file")
    from services import connector_store as cs

    monkeypatch.setattr(cs, "STORE_PATH", store)
    monkeypatch.setattr(cs, "_backend_choice", None)
    assert not cs._use_mongo()
    return cs


def _payload(name: str, role: str = "source") -> dict:
    return {
        "name": name, "type": "mysql", "role": role, "host": "127.0.0.1", "port": 3306,
        "database": "dataflow", "username": "u", "password": "p", "ssl": False,
    }


def test_terminal_stamp_racing_a_create_loses_neither(file_store):
    cs = file_store
    first = cs.create_connector(_payload("qa195-source-" + uuid.uuid4().hex[:6]))
    created: list[str] = []
    stamped: list[int] = []
    rounds = 40
    go = threading.Barrier(2)

    def saver():
        go.wait()
        for i in range(rounds):
            created.append(cs.create_connector(_payload(f"qa195-dest-{i}-{uuid.uuid4().hex[:4]}", "destination")).id)

    def stamper():
        go.wait()
        for _ in range(rounds):
            stamped.append(cs.note_transfer_succeeded(first.id))
            stamped.append(cs.mark_used(first.id))

    threads = [threading.Thread(target=saver), threading.Thread(target=stamper)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert all(not t.is_alive() for t in threads)

    with cs.bound_workspace_filter(None):
        missing = [cid for cid in created if cs.get_connector(cid) is None]
        assert not missing, f"{len(missing)} of {rounds} saved connectors lost to a concurrent stamp"
        kept = cs.get_connector(first.id)
    assert kept is not None and kept.last_transfer_ok_at and kept.last_used_at
    assert all(stamped), "a stamp missed the connector it was written for"
    assert len(created) == rounds


def test_update_and_delete_racing_creates_keep_every_survivor(file_store):
    cs = file_store
    target = cs.create_connector(_payload("qa195-target"))
    doomed = cs.create_connector(_payload("qa195-doomed"))
    created: list[str] = []
    go = threading.Barrier(2)

    def saver():
        go.wait()
        for i in range(30):
            created.append(cs.create_connector(_payload(f"qa195-new-{i}")).id)

    def editor():
        go.wait()
        for i in range(30):
            assert cs.update_connector(target.id, {"database": f"db{i}"}) is not None
        assert cs.delete_connector(doomed.id) is True

    threads = [threading.Thread(target=saver), threading.Thread(target=editor)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    with cs.bound_workspace_filter(None):
        assert [cid for cid in created if cs.get_connector(cid) is None] == []
        assert cs.get_connector(target.id).database == "db29"
        assert cs.get_connector(doomed.id) is None
