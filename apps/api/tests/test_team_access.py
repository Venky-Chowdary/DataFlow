"""Accounts, workspaces, memberships and the role rules the API must enforce.

Covers the failure an operator reported — "I cannot add a member" — and the
class of defects behind it: memberships that could not sign in, a blob document
that lost concurrent writes, and authorization that answered 403 for every
cause. Both storage backends are exercised: the JSON file (single-process box)
and real MongoDB collections when a client is reachable.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from services import team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from services.password_hash import hash_password, verify_password
from src.main import app


@pytest.fixture
def stores(tmp_path, monkeypatch):
    """Point both metadata stores at this test's own files, Mongo excluded."""
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    return tmp_path


@pytest.fixture
def client(stores):
    # No lifespan: the app's startup (schedulers, RAG warm-up) is not restartable
    # inside one process, and these tests exercise routers, not startup.
    return TestClient(app)


def _workspace(client, name: str = "Team") -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


# ── Store-level rules ────────────────────────────────────────────────────────


def test_creator_becomes_first_admin(stores):
    ws = team_store.create_workspace(name="Platform", created_by="Owner@Example.com")
    assert team_store.get_workspace_role(workspace_id=ws.id, email="owner@example.com") == "admin"
    assert team_store.can_admin_workspace(ws.id, "owner@example.com") is True


def test_legacy_owner_role_reads_as_admin(stores):
    """Deployments written before the rename stored ``owner``; it means admin."""
    ws = team_store.create_workspace(name="Legacy", created_by="a@example.com")
    team_store._write_membership(
        team_store.Membership(workspace_id=ws.id, email="legacy@example.com", role="owner")
    )
    assert team_store.get_workspace_role(workspace_id=ws.id, email="legacy@example.com") == "admin"


def test_membership_is_upserted_not_duplicated(stores):
    ws = team_store.create_workspace(name="Dedup", created_by="a@example.com")
    for role in ("viewer", "editor"):
        team_store.add_workspace_member(
            workspace_id=ws.id,
            email="member@example.com",
            role=role,
            added_by="a@example.com",
            actor_is_platform_admin=True,
        )
    rows = [m for m in team_store.list_workspace_members(ws.id) if m["email"] == "member@example.com"]
    assert len(rows) == 1
    assert rows[0]["role"] == "editor"


def test_last_admin_cannot_be_removed(stores):
    ws = team_store.create_workspace(name="Solo", created_by="a@example.com")
    with pytest.raises(team_store.TeamStoreError):
        team_store.remove_workspace_member(
            workspace_id=ws.id,
            email="a@example.com",
            removed_by="a@example.com",
            actor_is_platform_admin=True,
        )


def test_role_capabilities(stores):
    ws = team_store.create_workspace(name="Engineering", created_by="admin@example.com")
    for email, role in (("editor@example.com", "editor"), ("viewer@example.com", "viewer")):
        team_store.add_workspace_member(
            workspace_id=ws.id, email=email, role=role, added_by="admin@example.com"
        )
    assert team_store.can_read_workspace(ws.id, "viewer@example.com") is True
    assert team_store.can_write_workspace(ws.id, "viewer@example.com") is False
    assert team_store.can_write_workspace(ws.id, "editor@example.com") is True
    assert team_store.can_admin_workspace(ws.id, "editor@example.com") is False
    assert team_store.can_admin_workspace(ws.id, "admin@example.com") is True
    assert team_store.can_read_workspace(ws.id, "stranger@example.com") is False


def test_viewer_cannot_add_members_and_editor_cannot_mint_admins(stores):
    ws = team_store.create_workspace(name="Guarded", created_by="admin@example.com")
    for email, role in (("editor@example.com", "editor"), ("viewer@example.com", "viewer")):
        team_store.add_workspace_member(
            workspace_id=ws.id, email=email, role=role, added_by="admin@example.com"
        )
    with pytest.raises(team_store.PermissionDenied):
        team_store.add_workspace_member(
            workspace_id=ws.id, email="x@example.com", role="viewer", added_by="viewer@example.com"
        )
    with pytest.raises(team_store.PermissionDenied):
        team_store.add_workspace_member(
            workspace_id=ws.id, email="x@example.com", role="admin", added_by="editor@example.com"
        )
    # …but an editor may bring in a peer
    assert (
        team_store.add_workspace_member(
            workspace_id=ws.id, email="peer@example.com", role="viewer", added_by="editor@example.com"
        ).role
        == "viewer"
    )


def test_editor_cannot_remove_an_admin(stores):
    ws = team_store.create_workspace(name="Finance", created_by="admin@example.com")
    team_store.add_workspace_member(
        workspace_id=ws.id, email="fin@example.com", role="editor", added_by="admin@example.com"
    )
    with pytest.raises(team_store.PermissionDenied):
        team_store.remove_workspace_member(
            workspace_id=ws.id, email="admin@example.com", removed_by="fin@example.com"
        )
    assert team_store.get_workspace_role(workspace_id=ws.id, email="admin@example.com") == "admin"


def test_missing_workspace_is_reported_as_such(stores):
    with pytest.raises(team_store.WorkspaceNotFound):
        team_store.add_workspace_member(
            workspace_id=str(uuid.uuid4()),
            email="a@example.com",
            role="viewer",
            added_by="a@example.com",
            actor_is_platform_admin=True,
        )


def test_workspaces_are_isolated_per_member(stores):
    mine = team_store.create_workspace(name="Mine", created_by="a@example.com")
    team_store.create_workspace(name="Theirs", created_by="b@example.com")
    visible = [w.id for w in team_store.list_workspaces_for_user("a@example.com")]
    assert visible == [mine.id]


def test_deleting_a_workspace_leaves_no_orphan_membership(stores):
    ws = team_store.create_workspace(name="Doomed", created_by="a@example.com")
    team_store.add_workspace_member(
        workspace_id=ws.id,
        email="m@example.com",
        role="viewer",
        added_by="a@example.com",
        actor_is_platform_admin=True,
    )
    assert team_store.delete_workspace(workspace_id=ws.id) is True
    assert team_store.list_memberships_for_user("m@example.com") == []


# ── Accounts ─────────────────────────────────────────────────────────────────


def test_created_account_can_authenticate_with_issued_password(stores):
    account, issued = user_store.create_user(email="New@Example.com", name="New")
    assert account["email"] == "new@example.com"
    assert account["must_change_password"] is True
    assert issued
    record = next(c for c in user_store.credentials_for_auth() if c["email"] == "new@example.com")
    assert verify_password(issued, record["password_hash"]) is True
    assert "password_hash" not in account


def test_duplicate_and_invalid_accounts_are_refused(stores):
    user_store.create_user(email="dup@example.com")
    with pytest.raises(user_store.UserStoreError):
        user_store.create_user(email="dup@example.com")
    with pytest.raises(user_store.UserStoreError):
        user_store.create_user(email="not-an-email")
    with pytest.raises(user_store.UserStoreError):
        user_store.create_user(email="short@example.com", password="tooshort")


def test_disabled_account_is_withheld_from_login_but_kept_for_audit(stores):
    user_store.create_user(email="gone@example.com")
    user_store.update_user(email="gone@example.com", status="disabled")
    assert [c["email"] for c in user_store.credentials_for_auth()] == []
    assert user_store.get_user("gone@example.com")["status"] == "disabled"


def test_reset_password_invalidates_the_previous_one(stores):
    _, first = user_store.create_user(email="reset@example.com")
    _, second = user_store.reset_password(email="reset@example.com")
    record = next(c for c in user_store.credentials_for_auth() if c["email"] == "reset@example.com")
    assert verify_password(second, record["password_hash"]) is True
    assert verify_password(first, record["password_hash"]) is False


def test_record_login_stamps_only_stored_accounts(stores):
    user_store.create_user(email="seen@example.com")
    user_store.record_login("seen@example.com")
    assert user_store.get_user("seen@example.com")["last_login_at"]
    user_store.record_login("env-provisioned@example.com")  # must not raise


def test_password_hashes_are_bcrypt_not_plain_sha256(stores):
    digest = hash_password("correct-horse-battery")
    assert digest.startswith("$2b$")
    assert verify_password("correct-horse-battery", digest) is True
    assert verify_password("wrong", digest) is False


# ── API surface ──────────────────────────────────────────────────────────────


def test_add_member_reports_whether_the_person_can_sign_in(client):
    ws_id = _workspace(client, "Data Platform")
    response = client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "viewer@example.com", "role": "viewer"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["membership"]["role"] == "viewer"
    assert body["has_account"] is False
    assert body["temporary_password"] is None

    response = client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "editor@example.com", "role": "editor", "create_account": True},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["has_account"] is True
    assert body["temporary_password"]

    members = client.get(f"/api/v1/team/workspaces/{ws_id}/members").json()["members"]
    statuses = {m["email"]: m["account_status"] for m in members}
    assert statuses["viewer@example.com"] == "no_account"
    assert statuses["editor@example.com"] == "active"


def test_member_survives_a_reload_of_the_store(client, stores):
    ws_id = _workspace(client, "Persisted")
    client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "keep@example.com", "role": "editor"},
    )
    assert (stores / "teams.json").exists()
    reread = team_store.list_workspace_members(ws_id)
    assert {m["email"] for m in reread} >= {"keep@example.com"}


def test_unknown_workspace_and_invalid_input_are_distinguishable(client):
    ws_id = _workspace(client)
    missing = client.post(
        f"/api/v1/team/workspaces/{uuid.uuid4()}/members",
        json={"email": "a@example.com", "role": "viewer"},
    )
    assert missing.status_code == 404

    bad_role = client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "a@example.com", "role": "superuser"},
    )
    assert bad_role.status_code == 422

    bad_email = client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "nope", "role": "viewer", "create_account": True},
    )
    assert bad_email.status_code == 400


def test_member_mutations_are_audited(client):
    ws_id = _workspace(client, "Audited")
    client.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "audit@example.com", "role": "viewer"},
    )
    events = client.get("/api/v1/audit/events", params={"limit": 100})
    assert events.status_code == 200, events.text
    actions = {e.get("action") for e in events.json().get("events", [])}
    assert "team.member.add" in actions


# ── Sign-in for accounts the admin created ───────────────────────────────────


@pytest.fixture
def authenticated(stores, monkeypatch):
    """A deployment with authentication switched on and one bootstrap admin."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    # The limiter is process-wide; these tests sign in many times legitimately.
    reset_auth_rate_limits()
    c = TestClient(app)
    login = c.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    c.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return c


def test_issued_password_signs_in_then_must_be_rotated(authenticated):
    created = authenticated.post(
        "/api/v1/team/users",
        json={"email": "newhire@example.com", "name": "New Hire"},
    )
    assert created.status_code == 200, created.text
    issued = created.json()["temporary_password"]
    assert issued

    fresh = TestClient(app)
    login = fresh.post(
        "/api/v1/auth/login",
        json={"email": "newhire@example.com", "password": issued},
    )
    assert login.status_code == 200, login.text
    assert login.json()["must_change_password"] is True
    fresh.headers["Authorization"] = f"Bearer {login.json()['token']}"

    rotated = fresh.post(
        "/api/v1/auth/change-password",
        json={"current_password": issued, "new_password": "Chosen-By-The-User-1"},
    )
    assert rotated.status_code == 200, rotated.text

    stale = fresh.post(
        "/api/v1/auth/login",
        json={"email": "newhire@example.com", "password": issued},
    )
    assert stale.status_code == 401

    again = fresh.post(
        "/api/v1/auth/login",
        json={"email": "newhire@example.com", "password": "Chosen-By-The-User-1"},
    )
    assert again.status_code == 200, again.text
    assert again.json()["must_change_password"] is False
    assert user_store.get_user("newhire@example.com")["last_login_at"]


def test_wrong_current_password_cannot_rotate_a_credential(authenticated):
    issued = authenticated.post(
        "/api/v1/team/users", json={"email": "target@example.com"}
    ).json()["temporary_password"]
    fresh = TestClient(app)
    token = fresh.post(
        "/api/v1/auth/login",
        json={"email": "target@example.com", "password": issued},
    ).json()["token"]
    fresh.headers["Authorization"] = f"Bearer {token}"
    denied = fresh.post(
        "/api/v1/auth/change-password",
        json={"current_password": "Not-The-Password", "new_password": "Another-Choice-99"},
    )
    assert denied.status_code == 403


def test_sole_admin_cannot_be_demoted_or_removed(stores):
    ws = team_store.create_workspace(name="Single", created_by="admin@example.com")
    with pytest.raises(team_store.LastAdminProtected):
        team_store.add_workspace_member(
            workspace_id=ws.id,
            email="admin@example.com",
            role="viewer",
            added_by="admin@example.com",
        )
    with pytest.raises(team_store.LastAdminProtected):
        team_store.remove_workspace_member(
            workspace_id=ws.id, email="admin@example.com", removed_by="admin@example.com"
        )
    team_store.add_workspace_member(
        workspace_id=ws.id, email="second@example.com", role="admin", added_by="admin@example.com"
    )
    team_store.remove_workspace_member(
        workspace_id=ws.id, email="admin@example.com", removed_by="admin@example.com"
    )
    assert [m["email"] for m in team_store.list_workspace_members(ws.id)] == ["second@example.com"]


def test_role_change_and_conflict_statuses(authenticated):
    ws_id = authenticated.post(
        "/api/v1/team/workspaces", json={"name": "Roles"}
    ).json()["workspace"]["id"]
    authenticated.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "person@example.com", "role": "viewer"},
    )
    promoted = authenticated.patch(
        f"/api/v1/team/workspaces/{ws_id}/members/person@example.com",
        json={"role": "editor"},
    )
    assert promoted.status_code == 200, promoted.text
    assert promoted.json()["membership"]["role"] == "editor"

    missing = authenticated.patch(
        f"/api/v1/team/workspaces/{ws_id}/members/nobody@example.com",
        json={"role": "editor"},
    )
    assert missing.status_code == 404

    # The creator is the only admin: both demotion and removal are conflicts.
    demote = authenticated.patch(
        f"/api/v1/team/workspaces/{ws_id}/members/root@example.com",
        json={"role": "viewer"},
    )
    assert demote.status_code == 409, demote.text
    dropped = authenticated.delete(
        f"/api/v1/team/workspaces/{ws_id}/members/root@example.com"
    )
    assert dropped.status_code == 409, dropped.text
    gone = authenticated.delete(
        f"/api/v1/team/workspaces/{ws_id}/members/never@example.com"
    )
    assert gone.status_code == 404, gone.text


def test_workspace_admin_manages_members_without_being_a_platform_admin(authenticated):
    """The client's own team lead: workspace admin, ordinary platform account."""
    ws_id = authenticated.post(
        "/api/v1/team/workspaces", json={"name": "Client Tenant"}
    ).json()["workspace"]["id"]
    issued = authenticated.post(
        "/api/v1/team/users",
        json={
            "email": "lead@example.com",
            "platform_role": "member",
            "workspace_id": ws_id,
            "workspace_role": "admin",
        },
    ).json()["temporary_password"]

    lead = TestClient(app)
    token = lead.post(
        "/api/v1/auth/login",
        json={"email": "lead@example.com", "password": issued},
    ).json()["token"]
    lead.headers["Authorization"] = f"Bearer {token}"

    added = lead.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "analyst@example.com", "role": "viewer"},
    )
    assert added.status_code == 200, added.text
    assert added.json()["membership"]["role"] == "viewer"

    # …but only inside their own tenant.
    other = authenticated.post(
        "/api/v1/team/workspaces", json={"name": "Someone Else"}
    ).json()["workspace"]["id"]
    outside = lead.post(
        f"/api/v1/team/workspaces/{other}/members",
        json={"email": "analyst@example.com", "role": "viewer"},
    )
    assert outside.status_code == 403, outside.text
    assert lead.get(f"/api/v1/team/workspaces/{other}/members").status_code == 403


def test_viewer_cannot_add_a_member_through_the_api(authenticated):
    ws_id = authenticated.post(
        "/api/v1/team/workspaces", json={"name": "Read Only"}
    ).json()["workspace"]["id"]
    issued = authenticated.post(
        "/api/v1/team/users",
        json={
            "email": "readonly@example.com",
            "platform_role": "member",
            "workspace_id": ws_id,
            "workspace_role": "viewer",
        },
    ).json()["temporary_password"]
    viewer = TestClient(app)
    token = viewer.post(
        "/api/v1/auth/login",
        json={"email": "readonly@example.com", "password": issued},
    ).json()["token"]
    viewer.headers["Authorization"] = f"Bearer {token}"
    denied = viewer.post(
        f"/api/v1/team/workspaces/{ws_id}/members",
        json={"email": "someone@example.com", "role": "viewer"},
    )
    assert denied.status_code == 403, denied.text
    assert viewer.get(f"/api/v1/team/workspaces/{ws_id}/members").status_code == 200


def test_non_admin_cannot_create_accounts_or_read_the_directory(authenticated):
    issued = authenticated.post(
        "/api/v1/team/users", json={"email": "plain@example.com"}
    ).json()["temporary_password"]
    member = TestClient(app)
    token = member.post(
        "/api/v1/auth/login",
        json={"email": "plain@example.com", "password": issued},
    ).json()["token"]
    member.headers["Authorization"] = f"Bearer {token}"
    assert member.get("/api/v1/team/users").status_code == 403
    escalate = member.post(
        "/api/v1/team/users",
        json={"email": "mine@example.com", "platform_role": "admin"},
    )
    assert escalate.status_code == 403


# ── MongoDB relationships ────────────────────────────────────────────────────


@pytest.fixture
def mongo_db(monkeypatch):
    """A throwaway MongoDB database, skipped when no real client is reachable."""
    uri = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = MongoClient(uri, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        pytest.skip(f"no live MongoDB at {uri}")
    name = f"dataflow_test_{uuid.uuid4().hex[:10]}"
    db = client[name]
    monkeypatch.setattr(team_store, "mongo_database", lambda: db)
    monkeypatch.setattr(user_store, "mongo_database", lambda: db)
    team_store._PREPARED_DATABASES.discard(name)
    try:
        yield db
    finally:
        client.drop_database(name)
        client.close()
        team_store._PREPARED_DATABASES.discard(name)


def test_mongo_keeps_workspaces_and_members_as_related_rows(mongo_db):
    ws = team_store.create_workspace(name="Mongo", created_by="admin@example.com")
    team_store.add_workspace_member(
        workspace_id=ws.id,
        email="viewer@example.com",
        role="viewer",
        added_by="admin@example.com",
        actor_is_platform_admin=True,
    )
    assert mongo_db[team_store.WORKSPACES_COLLECTION].count_documents({"id": ws.id}) == 1
    rows = list(mongo_db[team_store.MEMBERS_COLLECTION].find({"workspace_id": ws.id}))
    assert {r["email"] for r in rows} == {"admin@example.com", "viewer@example.com"}
    # every membership points at a workspace that exists
    for row in rows:
        assert team_store.get_workspace(row["workspace_id"]) is not None


def test_mongo_membership_pair_is_unique(mongo_db):
    ws = team_store.create_workspace(name="Unique", created_by="admin@example.com")
    for role in ("viewer", "admin"):
        team_store.add_workspace_member(
            workspace_id=ws.id,
            email="twice@example.com",
            role=role,
            added_by="admin@example.com",
            actor_is_platform_admin=True,
        )
    assert (
        mongo_db[team_store.MEMBERS_COLLECTION].count_documents(
            {"workspace_id": ws.id, "email": "twice@example.com"}
        )
        == 1
    )


def test_mongo_stores_no_plaintext_password(mongo_db):
    _, issued = user_store.create_user(email="secret@example.com")
    doc = mongo_db[user_store.COLLECTION].find_one({"_id": "secret@example.com"})
    assert doc is not None
    assert issued not in repr(doc)
    assert doc["password_hash"].startswith("$2b$")


def test_legacy_blob_document_migrates_into_collections(mongo_db):
    ws_id = str(uuid.uuid4())
    mongo_db["team_store"].insert_one(
        {
            "_id": "primary",
            "workspaces": [{"id": ws_id, "name": "Old", "created_by": "old@example.com"}],
            "memberships": [{"workspace_id": ws_id, "email": "old@example.com", "role": "owner"}],
        }
    )
    team_store._PREPARED_DATABASES.discard(mongo_db.name)

    assert team_store.get_workspace(ws_id) is not None
    assert team_store.get_workspace_role(workspace_id=ws_id, email="old@example.com") == "admin"
    assert mongo_db["team_store"].find_one({"_id": "primary"}) is None


def test_workspace_without_its_first_admin_is_never_left_behind(mongo_db, monkeypatch):
    """A failed membership write must not leave a workspace nobody can administer.

    Standalone MongoDB cannot open a transaction, so the two inserts run in
    sequence; the workspace row has to be removed again when the membership
    insert fails, or the deployment gains a workspace with no admin and no way
    to invite one.
    """
    real_insert = Collection.insert_one

    def insert_one(self, document, *args, **kwargs):
        if self.name == team_store.MEMBERS_COLLECTION:
            raise PyMongoError("membership write rejected")
        return real_insert(self, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", insert_one)
    with pytest.raises(PyMongoError):
        team_store.create_workspace(name="Doomed", created_by="admin@example.com")
    monkeypatch.setattr(Collection, "insert_one", real_insert)

    assert [ws.name for ws in team_store.list_workspaces() if ws.name == "Doomed"] == []
    assert mongo_db[team_store.WORKSPACES_COLLECTION].count_documents({"name": "Doomed"}) == 0


def test_every_created_workspace_has_an_admin_on_the_file_store(stores):
    ws = team_store.create_workspace(name="Filed", created_by="admin@example.com")
    assert team_store.get_workspace(ws.id) is not None
    assert team_store.get_workspace_role(workspace_id=ws.id, email="admin@example.com") == "admin"
