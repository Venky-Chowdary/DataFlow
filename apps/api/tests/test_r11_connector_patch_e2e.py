"""``PATCH /connectors/saved/{id}`` edits only the fields it names (QA round 11, P4).

QA (production 9c75ada4, Connector CRUD): "PATCH returns 405" — only PUT was
registered, and PUT is a full replacement: a client that sends one field
resets every other one to its default.

Through the real routes on a live MongoDB connector store (``r8_ent_harness``)
against the live PostgreSQL: a partial edit keeps every field it does not
name (including the stored secret, so the connector still connects), masked or
empty secrets never overwrite, the egress policy, RBAC and workspace scoping
are the PUT route's, and NULL / '' / whitespace / unicode values behave.
"""

from __future__ import annotations

import uuid

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def world(ent, monkeypatch):
    h.pg_conn().close()
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "mongo")
    import services.connector_store as connector_store

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    ws = ent.workspace("R11 connector patch")
    editor = ent.member(email=f"r11-ed-{uuid.uuid4().hex[:6]}@example.com", workspace_id=ws,
                        role="editor")
    return ent, ws, editor


def _create(client) -> dict:
    res = client.post("/api/v1/connectors/saved", json={
        "name": f"r11 pg {uuid.uuid4().hex[:6]}", "type": "postgresql", "role": "both",
        "host": h.PG["host"], "port": h.PG["port"], "database": h.PG["database"],
        "username": h.PG["username"], "password": h.PG["password"], "schema": "public",
        "ssl": False,
    })
    assert res.status_code == 200, res.text
    return res.json()


def _tests_ok(client, cid: str) -> bool:
    res = client.post(f"/api/v1/connectors/saved/{cid}/test")
    assert res.status_code == 200, res.text
    return bool(res.json()["success"])


def test_patch_changes_only_the_named_fields_and_keeps_the_secret(world):
    ent, ws, editor = world
    made = _create(editor)
    cid = made["id"]
    assert _tests_ok(editor, cid)

    renamed = editor.patch(f"/api/v1/connectors/saved/{cid}", json={"name": "Prod PG — ünï 中文 ✓"})
    assert renamed.status_code == 200, renamed.text
    body = renamed.json()
    assert body["name"] == "Prod PG — ünï 中文 ✓"
    for key in ("host", "port", "database", "username", "type", "role"):
        assert body[key] == made[key], key
    assert body["password"] != h.PG["password"], "secret echoed in the response"
    assert _tests_ok(editor, cid), "the stored password was lost by a partial edit"

    # Masked, empty, whitespace or null secrets are "unchanged", never written.
    for placeholder in ("****", "", "   ", None):
        res = editor.patch(f"/api/v1/connectors/saved/{cid}", json={"password": placeholder})
        assert res.status_code == 200, (placeholder, res.text)
        assert _tests_ok(editor, cid), placeholder

    # A real change takes effect; changing it back restores the connection.
    assert editor.patch(f"/api/v1/connectors/saved/{cid}", json={"port": 1}).status_code == 200
    assert not _tests_ok(editor, cid)
    assert editor.patch(f"/api/v1/connectors/saved/{cid}",
                        json={"port": h.PG["port"]}).json()["port"] == h.PG["port"]
    assert _tests_ok(editor, cid)
    assert editor.patch(f"/api/v1/connectors/saved/{cid}", json={}).status_code == 200

    stored = ent.db["connectors"].find_one({"_id": cid}) or ent.db["connectors"].find_one({"id": cid})
    assert stored is not None and stored.get("password") != h.PG["password"], "secret not encrypted at rest"


def test_patch_keeps_the_put_rules(world):
    ent, ws, editor = world
    cid = _create(editor)["id"]

    metadata = editor.patch(f"/api/v1/connectors/saved/{cid}", json={"host": "169.254.169.254"})
    assert metadata.status_code == 400, metadata.text
    bad_port = editor.patch(f"/api/v1/connectors/saved/{cid}", json={"port": 70000})
    assert bad_port.status_code == 422, bad_port.text
    assert editor.patch(f"/api/v1/connectors/saved/nope-{uuid.uuid4().hex[:6]}",
                        json={"name": "x"}).status_code == 404

    viewer = ent.member(email=f"r11-v-{uuid.uuid4().hex[:6]}@example.com", workspace_id=ws,
                        role="viewer")
    assert viewer.patch(f"/api/v1/connectors/saved/{cid}", json={"name": "v"}).status_code == 403
    other_ws = ent.workspace("R11 other tenant")
    other = ent.member(email=f"r11-o-{uuid.uuid4().hex[:6]}@example.com", workspace_id=other_ws,
                       role="admin")
    assert other.patch(f"/api/v1/connectors/saved/{cid}", json={"name": "o"}).status_code == 404
    got = editor.get(f"/api/v1/connectors/saved/{cid}").json()
    assert got["name"] not in ("v", "o") and got["host"] == h.PG["host"]
