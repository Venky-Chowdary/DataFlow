"""Ops review round 2, Medium 9: two tenants must not claim one custom domain.

``create_tenant`` and ``update_tenant`` checked the domain and saved later, so
replicas racing on one domain both stored it, and host routing then picked
one tenant by insertion order. A tenant's custom domain is now one of its
unique claims (with its workspace), enforced by the database.
"""

from __future__ import annotations

import pytest

from services import tenant_store
from services.shared_doc_store import SharedDocConflict
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"


class _NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _attempt(fn) -> str:
    try:
        fn()
        return "ok"
    except (ValueError, SharedDocConflict) as exc:
        return f"{type(exc).__name__}: {exc}"


def _race_in_the_gap(monkeypatch, db, other) -> dict:
    items = db[ITEMS]
    state: dict = {"raced": False}
    real_insert, real_update = items.insert_one, items.update_one

    def race_first(real):
        def wrapped(*args, **kwargs):
            target = args[0]
            if str(target.get("_id", "")).startswith("tenant_settings:") and not state["raced"]:
                state["raced"] = True
                state["other"] = _attempt(other)
            return real(*args, **kwargs)
        return wrapped

    monkeypatch.setattr(items, "insert_one", race_first(real_insert))
    monkeypatch.setattr(items, "update_one", race_first(real_update))
    return state


@pytest.fixture
def store(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(tenant_store._DOC, "_lock", _NoLock())
    return db


def test_two_new_tenants_racing_for_one_domain_leave_one_owner(store, monkeypatch):
    state = _race_in_the_gap(
        monkeypatch, store, lambda: tenant_store.create_tenant("ws-d1", "D1", custom_domain="data.example.com")
    )
    mine = _attempt(lambda: tenant_store.create_tenant("ws-d7", "D7", custom_domain="data.example.com"))
    assert state["raced"]
    outcomes = [mine, state["other"]]
    assert outcomes.count("ok") == 1, outcomes
    assert any("already in use" in o or "SharedDocConflict" in o for o in outcomes if o != "ok")
    owners = [t.workspace_id for t in tenant_store.list_tenants() if t.normalized_domain == "data.example.com"]
    assert len(owners) == 1


def test_an_update_racing_a_create_for_one_domain_leaves_one_owner(store, monkeypatch):
    tenant = tenant_store.create_tenant("ws-a", "A")
    state = _race_in_the_gap(
        monkeypatch, store, lambda: tenant_store.create_tenant("ws-b", "B", custom_domain="data.example.com")
    )
    mine = _attempt(lambda: tenant_store.update_tenant(tenant.id, custom_domain="data.example.com"))
    assert state["raced"]
    owners = [t.workspace_id for t in tenant_store.list_tenants() if t.normalized_domain == "data.example.com"]
    assert owners == ["ws-b"], (mine, state["other"])
    assert "already in use" in mine or "SharedDocConflict" in mine


def test_moving_a_domain_between_tenants_still_works(store):
    first = tenant_store.create_tenant("ws-a", "A", custom_domain="data.example.com")
    second = tenant_store.create_tenant("ws-b", "B")
    tenant_store.update_tenant(first.id, custom_domain="")
    tenant_store.update_tenant(second.id, custom_domain="data.example.com")
    assert tenant_store.get_tenant_by_domain("data.example.com").id == second.id
