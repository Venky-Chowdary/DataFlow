"""Workspace census defect: ``workspace_id=""`` means "no scope".

``collect_workspace_briefing`` calls ``list_connectors(workspace_id="")``.
Both ``_list_mongo`` and ``_list_file`` treated a non-None scope as a real
filter, so an empty string matched only docs whose ``workspace_id`` is
""/None — the workspace briefing reported ``connector_count=0`` while
``list_connectors()`` showed every saved connector (prod defect: 0 vs 66).

``connector_name_taken`` already applies the ``(workspace_id or "") or
None`` convention; the list/get/update/delete paths now normalize the same
way.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.connector_store as store  # noqa: E402
from test_connector_id_lookup import _FakeColl  # noqa: E402


@pytest.fixture
def fake_mongo_store(monkeypatch):
    coll = _FakeColl()
    monkeypatch.setattr(store, "_use_mongo", lambda: True)
    monkeypatch.setattr(store, "_mongo_collection", lambda: coll)
    return coll


def _doc(name: str, workspace_id: str | None, **over) -> dict:
    doc = {
        "_id": str(uuid.uuid4()),
        "name": name,
        "type": "postgresql",
        "host": "db.example.com",
        "role": "source",
        "created_at": "2026-10-19T00:00:00",
    }
    if workspace_id is not None:
        doc["workspace_id"] = workspace_id
    doc.update(over)
    return doc


def test_empty_workspace_scope_lists_every_connector(fake_mongo_store):
    """Prod shape: docs carry a real workspace scope; "" must not hide them."""
    scoped = fake_mongo_store.insert_one(_doc("PostgresVenkat", "ws-prod")).inserted_id
    scoped2 = fake_mongo_store.insert_one(_doc("Snowflake Trial", "ws-prod")).inserted_id
    unscoped = fake_mongo_store.insert_one(_doc("Legacy No Scope", None)).inserted_id

    ids = {c.id for c in store.list_connectors(workspace_id="")}
    assert ids == {scoped, scoped2, unscoped}, (
        "workspace_id='' must mean 'no scope', not 'scope is empty string'"
    )
    # A real scope still filters to its own docs plus unscoped globals.
    ids_scoped = {c.id for c in store.list_connectors(workspace_id="ws-prod")}
    assert ids_scoped == {scoped, scoped2, unscoped}


def test_brief_workspace_counts_connectors_with_empty_scope(fake_mongo_store):
    for _ in range(3):
        fake_mongo_store.insert_one(_doc(f"Conn {_}", "ws-prod"))
    fake_mongo_store.insert_one(_doc("Unscoped", None))

    from src.ai.copilot.workspace_briefing import collect_workspace_briefing

    brief = collect_workspace_briefing(workspace_id="")
    assert brief["connector_count"] == 4, (
        "brief_workspace must census the same connectors list_connectors sees"
    )


def test_empty_scope_still_resolves_single_connector(fake_mongo_store):
    cid = fake_mongo_store.insert_one(_doc("Scoped Conn", "ws-prod")).inserted_id
    found = store.get_connector(cid, workspace_id="")
    assert found is not None and found.id == cid


def test_scoped_file_list_treats_empty_as_unscoped(monkeypatch, tmp_path):
    """Same defect on the file backend: "" must not become a real filter."""
    monkeypatch.setattr(store, "_use_mongo", lambda: False)
    scoped = store.SavedConnector(
        id="c1", name="Scoped", type="postgresql", role="source", workspace_id="ws-prod"
    )
    unscoped = store.SavedConnector(
        id="c2", name="Plain", type="postgresql", role="source", workspace_id=""
    )
    monkeypatch.setattr(store, "_load_all", lambda: [scoped, unscoped])

    ids = {c.id for c in store.list_connectors(workspace_id="")}
    assert ids == {"c1", "c2"}


class _Req:
    """Minimal Request stub for _can_access_connector."""

    def __init__(self, email: str, role: str | None):
        self.state = type(
            "S", (), {"user_email": email, "user": {"role": role or ""}}
        )()
        self.headers: dict[str, str] = {}


def _no_memberships(monkeypatch):
    """Nobody is a member of anything (the tenant resolver's membership seam)."""
    import services.tenant_scope as ts

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setattr(ts, "ordered_memberships", lambda email: ())


def _conn(workspace_id: str) -> store.SavedConnector:
    return store.SavedConnector(
        id="c1", name="Scoped", type="postgresql", role="source",
        workspace_id=workspace_id,
    )


def test_admin_non_member_sees_scoped_connector(monkeypatch):
    """Layer-2 defect: platform ADMIN who is not a member of the connector's
    workspace must still see it — membership-only filtering hid all 66 rows
    from the admin UI (prod defect)."""
    from src.routers import saved_connectors_router as r

    _no_memberships(monkeypatch)
    req = _Req("admin@example.com", "admin")
    assert r._can_access_connector(req, _conn("ws-prod")) is True


def test_non_admin_non_member_still_denied(monkeypatch):
    """Tenancy stays fail-closed: a non-admin non-member gets nothing."""
    from src.routers import saved_connectors_router as r

    _no_memberships(monkeypatch)
    req = _Req("dev@example.com", "developer")
    assert r._can_access_connector(req, _conn("ws-prod")) is False


def test_malformed_doc_does_not_zero_the_list(monkeypatch):
    """Layer-5 prod defect: a doc whose stored credential cannot be parsed
    used to kill the entire list (find() comprehension threw -> file
    fallback -> []). List paths now materialize per-doc and keep a
    degraded entry marked record_unreadable."""
    from services import connector_store as store

    good = {"_id": "c1", "name": "Good", "type": "postgresql",
            "role": "source", "workspace_id": "", "password": ""}
    bad = {"_id": "c2", "name": "Snowflake Trial", "type": "snowflake",
           "role": "source", "workspace_id": "",
           "password": "not-valid-ciphertext"}

    class _Coll:
        def find(self, query=None):
            return iter([good, bad])

    monkeypatch.setattr(store, "_use_mongo", lambda: True)
    monkeypatch.setattr(store, "_mongo_collection", lambda: _Coll())
    monkeypatch.setattr(
        store.SavedConnector, "from_dict",
        classmethod(
            lambda cls, data, **kw: (
                (_ for _ in ()).throw(ValueError("undecryptable"))
                if data.get("name") == "Snowflake Trial"
                and kw.get("secrets_encrypted", True)
                else store.SavedConnector(
                    id=data["id"], name=data["name"], type=data["type"],
                    role="source",
                )
            )
        ),
    )
    rows = store.list_connectors()
    ids = {c.id for c in rows}
    assert ids == {"c1", "c2"}
    degraded = {c.id: c for c in rows}["c2"]
    assert degraded.extra.get("record_unreadable") is True


def test_resolve_workspace_admin_bypass_and_nonadmin_403(monkeypatch):
    """Layer-3: X-Workspace-Id naming a workspace the actor cannot read 403s
    the whole list before _can_access_connector — platform admin bypasses,
    non-admin still fails closed."""
    import pytest
    from fastapi import HTTPException

    from src.routers import saved_connectors_router as r

    _no_memberships(monkeypatch)
    admin = _Req("admin@example.com", "admin")
    assert r._resolve_workspace(admin, "ws-prod") == "ws-prod"
    dev = _Req("dev@example.com", "developer")
    with pytest.raises(HTTPException):
        r._resolve_workspace(dev, "ws-prod")
