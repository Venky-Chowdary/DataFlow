"""Ops review round 3, mutation survivor R05: a cached "keys but none active" is read again.

``get_active_key_for_tenant`` reads the tenant's keys through the per-query
read cache. A cached read taken inside another replica's rotation (on a
server without transactions: the old key retired, the new one not yet in)
shows the tenant with keys but no active key, and ``encrypt_secret`` would
then refuse every save for the cache window although the database holds the
new active key. A tenant with keys but none active is therefore read once
more from the database. Removing that re-read survived every test; this one
kills it.
"""

from __future__ import annotations

import json

import pytest

from services import byok_key_manager, secret_vault, tenant_store
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"
TID = "tenant-r05"


@pytest.fixture
def cached_mid_rotation(tmp_path, monkeypatch):
    """This replica cached the tenant's keys mid-rotation; another replica then committed the new key."""
    db = install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "10")
    old = byok_key_manager.create_key(TID, "k0")
    item = db[ITEMS].find_one({"store": byok_key_manager._DOC.name, "key": old.id})
    retired = dict(json.loads(item["body"]), status="rotated")
    db[ITEMS].update_one({"_id": item["_id"]}, {"$set": {"body": json.dumps(retired, sort_keys=True)},
                                                "$unset": {"uniq": ""}, "$inc": {"rev": 1}})
    byok_key_manager._DOC._cache = {}
    assert [k.status for k in byok_key_manager.list_keys(TID)] == ["rotated"]  # cached: keys, none active
    # Another replica's rotation lands: the new active key, written behind this replica's cache.
    new = dict(retired, id="byok-new", status="active", label="k1", created_at="2999-01-01T00:00:00+00:00")
    doc = dict(item, _id=f"{byok_key_manager._DOC.name}:byok-new", key="byok-new", rev=1, seq=item["seq"] + 1,
               body=json.dumps(new, sort_keys=True))
    doc.pop("uniq", None)
    db[ITEMS].insert_one(doc)
    assert [k.status for k in byok_key_manager.list_keys(TID)] == ["rotated"], "still inside the cache window"
    return new


def test_keys_but_none_active_in_a_cached_read_is_read_again_from_the_database(cached_mid_rotation):
    active = byok_key_manager.get_active_key_for_tenant(TID)
    assert active is not None and active.id == "byok-new"


def test_a_secret_save_inside_the_cache_window_uses_the_new_key_and_is_not_refused(cached_mid_rotation):
    assert secret_vault.encrypt_secret("pw", tenant_id=TID).startswith("byok:byok-new:")
