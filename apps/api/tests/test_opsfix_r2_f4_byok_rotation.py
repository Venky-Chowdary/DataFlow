"""Ops review round 2, High 4: BYOK rotation must never leave a tenant without an active key.

``rotate_key`` saved twice (old key -> rotated, then a new key), so a failure
between the two saves left the tenant with no active key, a reader on another
replica could see that gap, and ``encrypt_secret`` then stored the BYOK
tenant's next secret under the platform key. Concurrent rotations could also
leave two active keys. Rotation is now one write (one transaction on a
replica set; the new key is in before the old one is out), one active key per
tenant is a unique claim in the database, and a tenant that has BYOK keys but
no active one is refused, never downgraded to the platform key.
"""

from __future__ import annotations

import threading
import uuid

import pytest

from services import (
    byok_key_manager,
    control_plane_store,
    mongodb_service,
    secret_vault,
    shared_doc_store,
    tenant_store,
)
from services.secret_vault import SecretVaultError
from tests._control_plane_fakes import install_shared_store

TID = "tenant-b"


class _NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _pod(monkeypatch, root) -> None:
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(tenant_store, "STORE_PATH", root / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", root / "byok_keys.json")
    monkeypatch.setattr(byok_key_manager._DOC, "_lock", _NoLock())  # replicas share no process lock


def _active(tenant: str = TID) -> list[str]:
    return [k.id for k in byok_key_manager.list_keys(tenant) if k.status == "active"]


def test_a_byok_tenant_without_an_active_key_is_refused_not_downgraded(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _pod(monkeypatch, tmp_path)
    byok_key_manager.create_key(TID, "k0")
    data = byok_key_manager._DOC.load()
    data["keys"][0]["status"] = "rotated"  # what the failed second save left behind
    byok_key_manager._DOC.save(data)
    with pytest.raises(SecretVaultError, match="no active"):
        secret_vault.encrypt_secret("pw", tenant_id=TID)
    assert secret_vault.encrypt_secret("pw", tenant_id="tenant-without-byok").startswith("enc:")


def test_creating_a_key_while_one_is_active_retires_the_old_one(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _pod(monkeypatch, tmp_path)
    first = byok_key_manager.create_key(TID, "k0")
    second = byok_key_manager.create_key(TID, "k1")
    assert _active() == [second.id]
    assert {k.id: k.status for k in byok_key_manager.list_keys(TID)}[first.id] == "rotated"


def test_a_second_active_key_is_refused_by_the_database(tmp_path, monkeypatch):
    """The invariant does not depend on the caller's check: a blind insert of a second active key fails."""
    from services.shared_record_store import UniqueConflict

    install_shared_store(monkeypatch)
    _pod(monkeypatch, tmp_path)
    byok_key_manager.create_key(TID, "k0")
    data = byok_key_manager._DOC.load()
    clone = dict(data["keys"][0], id=str(uuid.uuid4()), label="sneaky")
    data["keys"].append(clone)
    with pytest.raises(UniqueConflict):
        byok_key_manager._DOC.save(data)
    assert len(_active()) == 1


@pytest.fixture
def live(monkeypatch, tmp_path):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_r2f4_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    shared_doc_store.reset_outage_state()
    for store in (byok_key_manager, tenant_store):
        monkeypatch.setattr(store._DOC, "_indexed", None, raising=False)
        monkeypatch.setattr(store._DOC, "_backfilled", None, raising=False)
    _pod(monkeypatch, tmp_path)
    try:
        yield svc
    finally:
        shared_doc_store.reset_outage_state()
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_a_rotation_that_fails_part_way_leaves_the_old_key_active(live, monkeypatch):
    import pymongo.collection

    old = byok_key_manager.create_key(TID, "k0")
    real_insert = pymongo.collection.Collection.insert_one

    def new_key_insert_fails(self, doc, *args, **kwargs):
        if str(doc.get("_id", "")).startswith("byok_keys:") and doc.get("_id") != f"byok_keys:{old.id}":
            raise RuntimeError("injected: the database stopped answering mid-rotation")
        return real_insert(self, doc, *args, **kwargs)

    monkeypatch.setattr(pymongo.collection.Collection, "insert_one", new_key_insert_fails)
    with pytest.raises(RuntimeError, match="injected"):
        byok_key_manager.rotate_key(TID)
    monkeypatch.setattr(pymongo.collection.Collection, "insert_one", real_insert)
    assert _active() == [old.id], "the old key stays active: the rotation is all or nothing"
    assert secret_vault.encrypt_secret("pw", tenant_id=TID).startswith(f"byok:{old.id}:")


def test_encrypting_during_rotations_always_uses_a_tenant_key(live):
    """A reader on another replica during rotations: never no key, never the platform key."""
    byok_key_manager.create_key(TID, "k0")
    stop = threading.Event()
    outcomes: dict[str, int] = {}
    errors: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                token = secret_vault.encrypt_secret("pw", tenant_id=TID)
            except Exception as exc:  # noqa: BLE001 - every failure is the finding
                errors.append(f"{type(exc).__name__}: {exc}")
                continue
            kind = token.split(":", 1)[0]
            outcomes[kind] = outcomes.get(kind, 0) + 1

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for thread in readers:
        thread.start()
    try:
        for _ in range(25):
            byok_key_manager.rotate_key(TID)
    finally:
        stop.set()
        for thread in readers:
            thread.join(30)
    assert errors == []
    assert set(outcomes) == {"byok"} and outcomes["byok"] > 25
    assert len(_active()) == 1


def test_concurrent_rotations_on_live_mongodb_leave_exactly_one_active_key(live):
    byok_key_manager.create_key(TID, "k0")
    for _wave in range(3):
        go = threading.Barrier(4)
        failures: list[BaseException] = []

        def rotator(go: threading.Barrier = go, failures: list = failures) -> None:
            go.wait()
            for _ in range(5):
                try:
                    byok_key_manager.rotate_key(TID)
                except shared_doc_store.SharedDocConflict as exc:  # a 409 is allowed, a second key is not
                    failures.append(exc)

        threads = [threading.Thread(target=rotator) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(120)
        assert len(_active()) == 1
