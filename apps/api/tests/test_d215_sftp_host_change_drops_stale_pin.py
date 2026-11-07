"""D215: moving an SFTP connector to a new host must not keep the old host's pin.

The edit merge keeps saved trust extras the payload does not restate (so a
blank pin field in the edit form never downgrades to trust-on-first-use). A
host-key pin and known_hosts entry belong to one endpoint, though: carried
across a host change they pin the new server to the old server's key, and the
record "reverts" to a stale key the operator never chose for that host.
"""
from __future__ import annotations

import pytest

_OLD = "SHA256:" + "A" * 43
_NEW = "SHA256:" + "B" * 43


@pytest.fixture
def store(tmp_path, monkeypatch):
    from services import connector_store

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    return connector_store


def _sftp(store):
    return store.create_connector({
        "name": "sftp-pin", "type": "sftp", "role": "source",
        "host": "old.example", "port": 22, "username": "u", "password": "p",
        "database": "/", "host_key": _OLD, "known_hosts": "old.example ssh-ed25519 AAAA",
        "host_key_policy": "strict",
    })


def test_host_change_without_a_new_pin_drops_the_old_hosts_pin(store):
    conn = _sftp(store)
    store.update_connector(conn.id, {"host": "new.example"})
    extra = store.get_connector(conn.id).extra
    assert "host_key" not in extra, extra
    assert "known_hosts" not in extra, extra
    # The operator's policy is not host-bound: strict stays strict, so the next
    # connect refuses and names the new host's fingerprint to pin.
    assert extra["host_key_policy"] == "strict"


def test_port_only_correction_keeps_the_pin(store):
    # Correcting a stale port reaches the same server (D208); a wrong key
    # still fails closed at connect, so the pin is kept.
    conn = _sftp(store)
    store.update_connector(conn.id, {"port": 2222})
    assert store.get_connector(conn.id).extra["host_key"] == _OLD


def test_host_change_with_a_new_pin_stores_the_new_pin(store):
    conn = _sftp(store)
    store.update_connector(conn.id, {"host": "new.example", "host_key": _NEW})
    assert store.get_connector(conn.id).extra["host_key"] == _NEW


def test_edit_that_keeps_the_host_keeps_the_pin(store):
    conn = _sftp(store)
    store.update_connector(conn.id, {"host": "OLD.example", "username": "u2"})
    store.update_connector(conn.id, {"password": "p2"})
    extra = store.get_connector(conn.id).extra
    assert extra["host_key"] == _OLD
    assert extra["known_hosts"].startswith("old.example")
