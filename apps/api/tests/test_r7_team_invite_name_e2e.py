"""Settings → Team: the full name typed on invite is what the member row shows.

QA (production 557f556c): "Full name entered on invite isn't shown; the member
row displays the email only." ``POST /team/workspaces/{id}/members`` passed
``name`` only into ``create_user`` — so with "Create a login" off (the email
invite flow) the name was dropped, and for an existing account without a name
it was dropped too. ``GET .../members`` read the name from the account alone.

Runs on the file store and on a live MongoDB team store (production's).
"""

from __future__ import annotations

import os
import uuid

import pytest
from fastapi.testclient import TestClient

from services import audit_log, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits

ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"


@pytest.fixture(params=["file", "mongo"])
def admin(request, tmp_path, monkeypatch):
    from src.main import app

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    client_db = None
    if request.param == "mongo":
        pymongo = pytest.importorskip("pymongo")
        client_db = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        try:
            client_db.admin.command("ping")
        except Exception:  # noqa: BLE001
            pytest.skip("MongoDB not reachable")
        name = f"r7_team_{uuid.uuid4().hex[:10]}"
        db = client_db[name]
        monkeypatch.setattr(team_store, "mongo_database", lambda: db)
        monkeypatch.setattr(user_store, "mongo_database", lambda: db)
    else:
        monkeypatch.setattr(team_store, "mongo_database", lambda: None)
        monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post("/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    ws = client.post("/api/v1/team/workspaces", json={"name": "Acme"})
    assert ws.status_code == 200, ws.text
    try:
        yield client, ws.json()["workspace"]["id"]
    finally:
        if client_db is not None:
            client_db.drop_database(name)


def _invite(client, ws, email, name, *, create_account):
    res = client.post(
        f"/api/v1/team/workspaces/{ws}/members",
        json={"email": email, "role": "viewer", "name": name, "create_account": create_account},
    )
    assert res.status_code == 200, res.text
    return res.json()


def _row(client, ws, email):
    res = client.get(f"/api/v1/team/workspaces/{ws}/members")
    assert res.status_code == 200, res.text
    return next(m for m in res.json()["members"] if m["email"] == email)


def test_name_shown_for_email_invite_without_a_login(admin):
    client, ws = admin
    body = _invite(client, ws, "ada@acme.example", "Ada Lovelace", create_account=False)
    assert body["temporary_password"] is None
    assert body["membership"]["name"] == "Ada Lovelace"
    row = _row(client, ws, "ada@acme.example")
    assert row["name"] == "Ada Lovelace"
    assert row["has_account"] is False


def test_name_shown_when_a_login_is_created(admin):
    client, ws = admin
    body = _invite(client, ws, "grace@acme.example", "Grace Hopper", create_account=True)
    assert body["temporary_password"]
    assert body["membership"]["name"] == "Grace Hopper"
    assert _row(client, ws, "grace@acme.example")["name"] == "Grace Hopper"


def test_unicode_kept_and_blank_names_fall_back_to_email(admin):
    client, ws = admin
    _invite(client, ws, "zoe@acme.example", "  Zoë Ñúñez  ", create_account=False)
    _invite(client, ws, "blank@acme.example", "   ", create_account=False)
    _invite(client, ws, "empty@acme.example", "", create_account=False)
    assert _row(client, ws, "zoe@acme.example")["name"] == "Zoë Ñúñez"
    assert _row(client, ws, "blank@acme.example")["name"] == ""
    assert _row(client, ws, "empty@acme.example")["name"] == ""


def test_existing_account_without_a_name_shows_the_invite_name(admin):
    client, ws = admin
    other = client.post("/api/v1/team/workspaces", json={"name": "Other"}).json()["workspace"]["id"]
    # Account created earlier with no name (e.g. invited elsewhere, name left blank).
    _invite(client, other, "lin@acme.example", "", create_account=True)
    _invite(client, ws, "lin@acme.example", "Lin Chen", create_account=False)
    assert _row(client, ws, "lin@acme.example")["name"] == "Lin Chen"


def test_name_survives_a_role_change(admin):
    client, ws = admin
    _invite(client, ws, "ada@acme.example", "Ada Lovelace", create_account=False)
    res = client.patch(
        f"/api/v1/team/workspaces/{ws}/members/ada@acme.example", json={"role": "editor"}
    )
    assert res.status_code == 200, res.text
    row = _row(client, ws, "ada@acme.example")
    assert row["role"] == "editor"
    assert row["name"] == "Ada Lovelace"
