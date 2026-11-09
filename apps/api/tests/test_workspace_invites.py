"""Workspace invitations by email — end to end through the real app.

Admin invites (email + role) → branded mail with a single-use link → the
invitee accepts (new account, or signed in as the existing one) → membership.
Covers pending list, resend (old link superseded), revoke, expiry, RBAC
limits, duplicates, inviter authority at accept, rate limits, the outbox
dead-link skip and the audit trail. File stores; MongoDB is exercised by the
store's own Mongo branch elsewhere.
"""

from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from services import audit_log, mail_outbox, team_store, user_store
from services import workspace_invite_store as invite_store
from services.auth_rate_limit import reset_auth_rate_limits
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane

ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
PASSWORD = "a-long-invitee-password-2026"


MONGO_URI = "mongodb://localhost:27017/?directConnection=true"


def _live_mongo_client():
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        return None
    return client


@pytest.fixture
def env(tmp_path, monkeypatch):
    yield from _world(tmp_path, monkeypatch, db=None)


@pytest.fixture
def mongo_env(tmp_path, monkeypatch):
    """The same world with invites, members, accounts and the mail outbox on a
    live MongoDB replica set — the production tier."""
    client = _live_mongo_client()
    if client is None:
        pytest.skip(f"live MongoDB replica set not reachable at {MONGO_URI}")
    db = client[f"invite_race_{uuid.uuid4().hex[:10]}"]
    for cache in (invite_store._PREPARED, team_store._PREPARED_DATABASES, user_store._CANONICAL_INDEXED):
        cache.discard(db.name)
    monkeypatch.setattr(mail_outbox, "_INDEXES_READY", False)
    try:
        yield from _world(tmp_path, monkeypatch, db=db)
    finally:
        client.drop_database(db.name)
        client.close()


def _world(tmp_path, monkeypatch, *, db):
    from src.main import app

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    for mod in (team_store, user_store, invite_store, mail_outbox):
        monkeypatch.setattr(mod, "mongo_database", lambda: db)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    from services import auth_state, email_quota
    from services.hibp import pwned_count  # noqa: F401 — imported for patch target

    monkeypatch.setattr("services.hibp.pwned_count", lambda _pw: 0)
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_auth_rate_limits()
    reset_email_sender()
    drain_mail_lane()
    ConsoleEmailSender.OUTBOX.clear()
    client = TestClient(app)
    admin = _login(client, ADMIN_EMAIL, ADMIN_PASSWORD)
    ws = client.post("/api/v1/team/workspaces", json={"name": "Acme Analytics"}, headers=admin)
    assert ws.status_code == 200, ws.text
    yield client, admin, ws.json()["workspace"]["id"], tmp_path
    reset_email_sender()


def _login(client: TestClient, email: str, password: str) -> dict[str, str]:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['token']}"}


def _invite(client, headers, ws, email, role="viewer", name="", expect=200):
    res = client.post(f"/api/v1/team/workspaces/{ws}/invites",
                      json={"email": email, "role": role, "name": name}, headers=headers)
    assert res.status_code == expect, res.text
    return res.json()


def _token_for(email: str) -> str:
    drain_mail_lane()
    mails = [m for m in ConsoleEmailSender.OUTBOX if m.to == email and "accept-invite" in m.text]
    assert mails, [m.to for m in ConsoleEmailSender.OUTBOX]
    match = re.search(r"accept-invite#token=([A-Za-z0-9_\-]+)", mails[-1].text)
    assert match, mails[-1].text
    return match.group(1)


def _accept(client, token, headers=None, **body):
    return client.post("/api/v1/invites/accept", json={"token": token, **body}, headers=headers or {})


def _events(tmp_path) -> list[dict]:
    path = tmp_path / "audit.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _actions(tmp_path) -> list[str]:
    return [e.get("action") for e in _events(tmp_path)]


def _make_login(client, admin, ws, email, workspace_role):
    res = client.post("/api/v1/team/users", headers=admin, json={
        "email": email, "name": email.split("@")[0].title(), "platform_role": "member",
        "password": PASSWORD, "workspace_id": ws, "workspace_role": workspace_role})
    assert res.status_code == 200, res.text
    return _login(client, email, PASSWORD)


def test_new_invitee_accepts_creates_account_membership_and_session(env):
    client, admin, ws, tmp_path = env
    body = _invite(client, admin, ws, "Ada@Example.com", role="editor", name="Ada Lovelace")
    invite = body["invite"]
    assert invite["status"] == "pending" and invite["role"] == "editor"
    assert invite["email"] == "ada@example.com"
    assert "token_hash" not in invite and "token" not in json.dumps(body)

    token = _token_for("ada@example.com")
    mail = [m for m in ConsoleEmailSender.OUTBOX if m.to == "ada@example.com"][-1]
    assert mail.subject.endswith("invited you to Acme Analytics on Datawrap")
    assert "root@example.com" in mail.text and "Editor" in mail.text

    listed = client.get(f"/api/v1/team/workspaces/{ws}/invites", headers=admin).json()["invites"]
    assert [i["email"] for i in listed] == ["ada@example.com"]

    probe = client.post("/api/v1/invites/inspect", json={"token": token}).json()
    assert probe["ok"] and probe["account"] == "new" and probe["workspace_name"] == "Acme Analytics"

    weak = _accept(client, token, name="Ada", password="short")
    assert weak.status_code == 400 and weak.json()["error"]["code"] == "invalid_password"

    res = _accept(client, token, name="Ada Lovelace", password=PASSWORD)
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["account_created"] is True and out["role"] == "editor"
    assert out["workspace"]["id"] == ws and out["token"]
    assert "__Host-dw_session" in res.headers.get("set-cookie", "")
    assert team_store.get_workspace_role(workspace_id=ws, email="ada@example.com") == "editor"
    account = user_store.get_user_record("ada@example.com")
    assert account["email_verified_at"] and account["created_by"].startswith("invite:")

    me = client.get("/api/v1/team/workspaces", headers={"Authorization": f"Bearer {out['token']}"})
    assert me.status_code == 200 and ws in [w["id"] for w in me.json()["workspaces"]]

    again = _accept(client, token, name="Ada", password=PASSWORD)
    assert again.status_code == 410 and again.json()["error"]["code"] == "invite_accepted"
    assert client.get(f"/api/v1/team/workspaces/{ws}/invites", headers=admin).json()["invites"] == []

    drain_mail_lane()
    welcomes = [m for m in ConsoleEmailSender.OUTBOX if m.to == "ada@example.com" and m.subject == "Welcome to Datawrap"]
    assert len(welcomes) == 1 and "joined Acme Analytics as Editor" in welcomes[0].text
    actions = _actions(tmp_path)
    for name in ("team.invite.created", "team.invite.accepted", "team.invite.accept_failed", "signup.welcome_queued"):
        assert name in actions, name
    assert token not in (tmp_path / "audit.jsonl").read_text()


def test_duplicate_pending_and_existing_member_are_conflicts(env):
    client, admin, ws, _ = env
    _invite(client, admin, ws, "bob@example.com")
    dup = _invite(client, admin, ws, "BOB@example.com", expect=409)
    assert dup["error"]["code"] == "invite_pending" and dup["error"]["invite_id"]
    member = _invite(client, admin, ws, ADMIN_EMAIL, expect=409)
    assert member["error"]["code"] == "already_member"
    bad = client.post(f"/api/v1/team/workspaces/{ws}/invites", json={"email": "nope", "role": "viewer"}, headers=admin)
    assert bad.status_code == 400


def test_resend_supersedes_previous_link(env, monkeypatch):
    client, admin, ws, _ = env
    invite = _invite(client, admin, ws, "cy@example.com")["invite"]
    first = _token_for("cy@example.com")
    soon = client.post(f"/api/v1/team/workspaces/{ws}/invites/{invite['id']}/resend", headers=admin)
    assert soon.status_code == 429 and soon.json()["error"]["code"] == "resend_too_soon"
    monkeypatch.setattr("services.workspace_invites.RESEND_COOLDOWN_SEC", 0)
    res = client.post(f"/api/v1/team/workspaces/{ws}/invites/{invite['id']}/resend", headers=admin)
    assert res.status_code == 200, res.text
    assert res.json()["invite"]["sent_count"] == 2
    second = _token_for("cy@example.com")
    assert second != first
    assert client.post("/api/v1/invites/inspect", json={"token": first}).json() == {"ok": False, "status": "superseded"}
    old = _accept(client, first, name="Cy", password=PASSWORD)
    assert old.status_code == 410 and old.json()["error"]["code"] == "invite_superseded"
    from services.workspace_invites import outbox_skip_reason

    assert outbox_skip_reason(f"invite:{invite['id']}:1") == "superseded"
    assert outbox_skip_reason(f"invite:{invite['id']}:2") is None
    assert _accept(client, second, name="Cy", password=PASSWORD).status_code == 200
    assert outbox_skip_reason(f"invite:{invite['id']}:2") == "superseded"


def test_revoked_and_expired_links_are_refused(env, tmp_path):
    client, admin, ws, _ = env
    revoked = _invite(client, admin, ws, "dee@example.com")["invite"]
    token = _token_for("dee@example.com")
    res = client.delete(f"/api/v1/team/workspaces/{ws}/invites/{revoked['id']}", headers=admin)
    assert res.status_code == 200 and res.json()["invite"]["status"] == "revoked"
    gone = _accept(client, token, name="Dee", password=PASSWORD)
    assert gone.status_code == 410 and gone.json()["error"]["code"] == "invite_revoked"
    twice = client.delete(f"/api/v1/team/workspaces/{ws}/invites/{revoked['id']}", headers=admin)
    assert twice.status_code == 409

    lapsed = _invite(client, admin, ws, "eve@example.com")["invite"]
    token = _token_for("eve@example.com")
    path = tmp_path / "workspace_invites.json"
    data = json.loads(path.read_text())
    data["invites"][lapsed["id"]]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    path.write_text(json.dumps(data))
    assert client.post("/api/v1/invites/inspect", json={"token": token}).json()["status"] == "expired"
    late = _accept(client, token, name="Eve", password=PASSWORD)
    assert late.status_code == 410 and late.json()["error"]["code"] == "invite_expired"
    # A lapsed invitation no longer blocks a fresh one for the same address.
    _invite(client, admin, ws, "eve@example.com")
    assert client.post("/api/v1/invites/inspect", json={"token": "x" * 43}).json() == {"ok": False, "status": "invalid"}


def test_existing_account_must_sign_in_as_the_invitee(env):
    client, admin, ws, _ = env
    other_ws = client.post("/api/v1/team/workspaces", json={"name": "Other"}, headers=admin).json()["workspace"]["id"]
    frank = _make_login(client, admin, other_ws, "frank@example.com", "viewer")
    grace = _make_login(client, admin, other_ws, "grace@example.com", "viewer")
    _invite(client, admin, ws, "frank@example.com", role="viewer")
    token = _token_for("frank@example.com")
    probe = client.post("/api/v1/invites/inspect", json={"token": token}, headers=frank).json()
    assert probe["account"] == "existing" and probe["signed_in_as_invitee"] is True

    anon = _accept(client, token)
    assert anon.status_code == 401 and anon.json()["error"]["code"] == "sign_in_required"
    wrong = _accept(client, token, headers=grace)
    assert wrong.status_code == 403 and wrong.json()["error"]["code"] == "invite_email_mismatch"
    ok = _accept(client, token, headers=frank)
    assert ok.status_code == 200, ok.text
    assert ok.json()["account_created"] is False and "token" not in ok.json()
    assert team_store.get_workspace_role(workspace_id=ws, email="frank@example.com") == "viewer"


def test_rbac_editor_cannot_invite_and_admin_role_needs_workspace_admin(env):
    client, admin, ws, tmp_path = env
    editor = _make_login(client, admin, ws, "ed@example.com", "editor")
    for role in ("viewer", "admin"):
        res = client.post(f"/api/v1/team/workspaces/{ws}/invites", headers={**editor, "X-Workspace-Id": ws},
                          json={"email": f"new-{role}@example.com", "role": role})
        assert res.status_code == 403, res.text
    assert "team.invite.denied" in _actions(tmp_path)
    ws_admin = _make_login(client, admin, ws, "wa@example.com", "admin")
    res = client.post(f"/api/v1/team/workspaces/{ws}/invites", headers={**ws_admin, "X-Workspace-Id": ws},
                      json={"email": "lead@example.com", "role": "admin"})
    assert res.status_code == 200, res.text
    listing = client.get(f"/api/v1/team/workspaces/{ws}/invites", headers={**editor, "X-Workspace-Id": ws})
    assert listing.status_code == 200
    invite_id = listing.json()["invites"][0]["id"]
    revoke = client.delete(f"/api/v1/team/workspaces/{ws}/invites/{invite_id}", headers={**editor, "X-Workspace-Id": ws})
    assert revoke.status_code == 403


def test_inviter_who_lost_authority_cannot_still_add_someone(env):
    client, admin, ws, tmp_path = env
    ws_admin = _make_login(client, admin, ws, "wa@example.com", "admin")
    _invite(client, {**ws_admin, "X-Workspace-Id": ws}, ws, "hal@example.com", role="viewer")
    token = _token_for("hal@example.com")
    assert client.delete(f"/api/v1/team/workspaces/{ws}/members/wa@example.com", headers=admin).status_code == 200
    res = _accept(client, token, name="Hal", password=PASSWORD)
    assert res.status_code == 410 and res.json()["error"]["code"] == "invite_invalid"
    assert user_store.get_user("hal@example.com") is None
    assert team_store.get_workspace_role(workspace_id=ws, email="hal@example.com") == ""
    assert any(e.get("action") == "team.invite.accept_failed" and e.get("reason_code") == "inviter_lost_authority"
               for e in _events(tmp_path))


def test_invites_are_rate_limited_per_actor(env, monkeypatch):
    client, admin, ws, _ = env
    monkeypatch.setenv("DATAWRAP_INVITE_ACTOR_HOURLY", "2")
    _invite(client, admin, ws, "r1@example.com")
    _invite(client, admin, ws, "r2@example.com")
    limited = _invite(client, admin, ws, "r3@example.com", expect=429)
    assert limited["error"]["code"] == "rate_limited"
    assert invite_store.pending_count(ws) == 2


def test_cross_workspace_invite_ids_are_not_reachable(env):
    client, admin, ws, _ = env
    invite = _invite(client, admin, ws, "ivy@example.com")["invite"]
    other = client.post("/api/v1/team/workspaces", json={"name": "Other"}, headers=admin).json()["workspace"]["id"]
    res = client.delete(f"/api/v1/team/workspaces/{other}/invites/{invite['id']}", headers=admin)
    assert res.status_code == 404


def _race_one_token(world, email: str, racers: int = 2) -> None:
    """``racers`` clients accept one token at the same instant: exactly one
    wins, the rest see invite_accepted, and one membership exists."""
    from src.main import app

    client, admin, ws, tmp_path = world
    _invite(client, admin, ws, email, role="editor")
    token = _token_for(email)
    accepted_before = _actions(tmp_path).count("team.invite.accepted")
    barrier = threading.Barrier(racers)
    results: list[int] = []
    errors: list[str] = []

    def run() -> None:
        own = TestClient(app)
        barrier.wait()
        res = _accept(own, token, name="Race", password=PASSWORD)
        results.append(res.status_code)
        if res.status_code != 200:
            errors.append(res.json()["error"]["code"])

    threads = [threading.Thread(target=run) for _ in range(racers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert sorted(results) == [200] + [410] * (racers - 1), (results, errors)
    assert errors == ["invite_accepted"] * (racers - 1)
    assert team_store.get_workspace_role(workspace_id=ws, email=email) == "editor"
    members = client.get(f"/api/v1/team/workspaces/{ws}/members", headers=admin).json()["members"]
    assert [m["email"] for m in members].count(email) == 1
    record, _ = invite_store.find_by_token(token)
    assert record is not None and record["status"] == "accepted"
    assert all(i["email"] != email for i in invite_store.list_for_workspace(ws))
    assert _actions(tmp_path).count("team.invite.accepted") == accepted_before + 1


def test_concurrent_accepts_of_one_token_yield_exactly_one_membership(env):
    _race_one_token(env, "race@example.com")


def test_concurrent_accepts_are_atomic_on_live_mongodb(mongo_env):
    assert invite_store._database() is not None, "invite store must be on the Mongo tier"
    for round_no in range(3):
        _race_one_token(mongo_env, f"race{round_no}@example.com", racers=2 + round_no)


@pytest.mark.parametrize("world", ["env", "mongo_env"])
def test_failed_resend_keeps_the_emailed_link_working(world, request, monkeypatch):
    client, admin, ws, _ = request.getfixturevalue(world)
    invite = _invite(client, admin, ws, "fay@example.com")["invite"]
    first = _token_for("fay@example.com")
    monkeypatch.setattr("services.workspace_invites.RESEND_COOLDOWN_SEC", 0)

    def _down(*_a, **_k):
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr("services.workspace_invites._queue_invite_mail", _down)
    res = client.post(f"/api/v1/team/workspaces/{ws}/invites/{invite['id']}/resend", headers=admin)
    assert res.status_code == 503 and res.json()["error"]["code"] == "email_send_failed"
    assert client.post("/api/v1/invites/inspect", json={"token": first}).json()["ok"] is True
    assert _accept(client, first, name="Fay", password=PASSWORD).status_code == 200


@pytest.mark.parametrize("world", ["env", "mongo_env"])
def test_failed_overlapping_resend_never_revives_an_older_link(world, request):
    from datetime import timedelta

    from services import workspace_invite_store as store

    client, admin, ws, _ = request.getfixturevalue(world)
    invite = _invite(client, admin, ws, "gus@example.com")["invite"]
    stale_read = store.get(invite["id"])
    mailed, _ = store.rotate_token(invite["id"], ttl=timedelta(days=7))
    _, failed = store.rotate_token(invite["id"], ttl=timedelta(days=7))
    assert store.restore_token(invite["id"], failed=failed, previous=stale_read)
    assert store.get(invite["id"])["token_hash"] == store.hash_token(mailed)
    assert client.post("/api/v1/invites/inspect", json={"token": mailed}).json()["ok"] is True
    assert client.post("/api/v1/invites/inspect", json={"token": _token_for("gus@example.com")}).json()["ok"] is False


@pytest.mark.parametrize("world", ["env", "mongo_env"])
def test_restored_link_keeps_its_own_expiry_not_the_failed_resends(world, request):
    from datetime import timedelta

    from services import workspace_invite_store as store

    client, admin, ws, _ = request.getfixturevalue(world)
    invite = _invite(client, admin, ws, "hal@example.com")["invite"]
    stale_read = store.get(invite["id"])
    mailed, mailed_doc = store.rotate_token(invite["id"], ttl=timedelta(days=1))
    _, failed = store.rotate_token(invite["id"], ttl=timedelta(days=30))
    assert store.restore_token(invite["id"], failed=failed, previous=stale_read)
    row = store.get(invite["id"])
    assert row["token_hash"] == store.hash_token(mailed)
    assert row["expires_at"] == mailed_doc["expires_at"]
