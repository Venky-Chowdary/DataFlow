"""Two-factor authentication end to end (QA S7, round 11).

QA: a workspace can turn on "require MFA", it is saved, and nothing enforces
it. Every test here drives the real REST routes of the real app with
authentication on, against a throwaway database on the live local MongoDB
(``r8_ent_harness``: accounts, memberships, API keys, audit chain and the
second-factor store all in Mongo). Codes come from an independent RFC 6238
computation over the secret the enrolment route hands out.

All of these fail on the base: there is no ``/auth/mfa`` route, ``/auth/login``
always returns a session, and ``mfa_required`` holds nobody.
"""

from __future__ import annotations

import time
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from r8_ent_harness import ADMIN_EMAIL, ADMIN_PASSWORD, ent  # noqa: F401
from services import totp

MEMBER = "ana.member@acme.test"


# ── a clock we can move (codes are per 30-second step) ──────────────────────


@pytest.fixture
def clock(monkeypatch):
    real = time.time
    offset = [0.0]
    monkeypatch.setattr(time, "time", lambda: real() + offset[0])

    def advance(seconds: float = 30.0) -> None:
        offset[0] += seconds

    return advance


def code_for(secret: str, *, step_delta: int = 0) -> str:
    key = totp.secret_bytes(secret)
    return totp.hotp(key, totp.time_step(time.time()) + step_delta)


def wrong_code(secret: str) -> str:
    valid = {code_for(secret, step_delta=d) for d in (-1, 0, 1)}
    return next(c for c in ("000000", "111111", "222222", "333333") if c not in valid)


# ── route helpers ───────────────────────────────────────────────────────────


def password_login(ent, email: str, password: str):
    from services.auth_rate_limit import reset_auth_rate_limits

    reset_auth_rate_limits()
    return ent.anonymous().post("/api/v1/auth/login", json={"email": email, "password": password})


def enroll(client, *, clock, body: dict | None = None) -> tuple[str, list[str]]:
    """Enrol through the routes; returns (secret, recovery codes)."""
    started = client.post("/api/v1/auth/mfa/totp/enroll", json=body or {})
    assert started.status_code == 200, started.text
    data = started.json()
    secret = data["secret"]
    assert data["otpauth_uri"].startswith("otpauth://totp/Datawrap:")
    assert f"secret={secret}" in data["otpauth_uri"]
    confirmed = client.post("/api/v1/auth/mfa/totp/confirm", json={"code": code_for(secret)})
    assert confirmed.status_code == 200, confirmed.text
    codes = confirmed.json()["recovery_codes"]
    clock()  # the confirming code is spent; the next one belongs to the next step
    return secret, codes


def mfa_login(ent, email: str, password: str, *, code: str = "", recovery_code: str = ""):
    first = password_login(ent, email, password)
    assert first.status_code == 202, first.text
    body = first.json()
    assert body["mfa_required"] is True and "token" not in body
    return body["challenge"], ent.anonymous().post(
        "/api/v1/auth/login/mfa",
        json={"challenge": body["challenge"], "code": code, "recovery_code": recovery_code},
    )


def audit_rows(ent, action: str) -> list[dict]:
    """The audit chain as stored (Mongo ``audit_events``), oldest first."""
    rows = ent.db["audit_events"].find({"action": action}, {"_id": False})
    return sorted((dict(r) for r in rows), key=lambda r: str(r.get("timestamp") or r.get("ts") or ""))


def audit_text(ent) -> str:
    return str(list(ent.db["audit_events"].find({}, {"_id": False})))


def require_mfa(ent, client, workspace_id: str):
    return client.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": workspace_id, "name": "Acme", "mfa_required": True},
        headers={"X-Workspace-Id": workspace_id},
    )


# ── 1. enrol, then sign in with a code ──────────────────────────────────────


def test_enroll_then_login_requires_and_accepts_a_code(ent, clock):
    ws = ent.workspace("Acme")
    ent.member(email=MEMBER, workspace_id=ws, role="editor")
    member = ent.login(MEMBER, _member_password(ent))

    secret, codes = enroll(member, clock=clock)
    assert len(codes) == 10 and len(set(codes)) == 10

    # At rest: the secret is vault-encrypted, recovery codes only as hashes.
    doc = ent.db["auth_mfa_factors"].find_one({"_id": MEMBER})
    assert doc["active"]["secret_enc"].startswith("enc:v1:")
    assert secret not in str(doc)
    assert not any(code in str(doc) or code.replace("-", "") in str(doc) for code in codes)

    status = member.get("/api/v1/auth/mfa").json()
    assert status["enrolled"] is True and status["recovery_codes_remaining"] == 10

    # Password alone no longer yields a session.
    first = password_login(ent, MEMBER, _member_password(ent))
    assert first.status_code == 202
    assert "token" not in first.json()
    held = audit_rows(ent, "auth.login")
    assert held and held[-1]["outcome"] == "held"

    challenge, done = mfa_login(ent, MEMBER, _member_password(ent), code=code_for(secret))
    assert done.status_code == 200, done.text
    session = ent.bearer(done.json()["token"], ws)
    assert session.get("/api/v1/auth/me").status_code == 200
    assert done.json()["mfa_method"] == "totp"

    # The challenge bought one session: reusing it (even with a fresh code) fails.
    clock()
    again = ent.anonymous().post(
        "/api/v1/auth/login/mfa", json={"challenge": challenge, "code": code_for(secret)}
    )
    assert again.status_code == 401
    # A forged / tampered challenge is refused outright.
    forged = ent.anonymous().post(
        "/api/v1/auth/login/mfa", json={"challenge": challenge[:-2] + "xx", "code": code_for(secret)}
    )
    assert forged.status_code == 401
    assert audit_rows(ent, "auth.mfa.enrolled") and audit_rows(ent, "auth.mfa.challenge.passed")


def test_env_bootstrap_admin_can_enroll_and_is_then_challenged(ent, clock):
    secret, _codes = enroll(ent.admin, clock=clock)
    _challenge, done = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret))
    assert done.status_code == 200, done.text
    assert done.json()["user"]["email"] == ADMIN_EMAIL


# ── 2. wrong codes, then lockout ────────────────────────────────────────────


def test_wrong_codes_lock_out_and_are_audited(ent, clock, monkeypatch):
    monkeypatch.setenv("DATAFLOW_MFA_MAX_FAILURES", "3")
    secret, _ = enroll(ent.admin, clock=clock)
    bad = wrong_code(secret)

    _c, first = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=bad)
    assert first.status_code == 401
    assert "not correct" in first.json()["detail"]
    _c, second = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=bad)
    assert second.status_code == 401
    _c, third = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=bad)
    assert third.status_code == 429 and third.headers.get("Retry-After")

    # Locked: even the right code is refused until the lock expires.
    _c, right = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret))
    assert right.status_code == 429

    failed = audit_rows(ent, "auth.mfa.challenge.failed")
    assert len(failed) == 3 and {r["reason_code"] for r in failed} == {"bad_code"}
    locked = audit_rows(ent, "auth.mfa.locked")
    assert len(locked) == 1 and locked[0]["actor"] == ADMIN_EMAIL
    # Codes never reach the audit trail.
    assert all(bad not in str(r.get("details")) for r in failed)

    clock(901)  # past DATAFLOW_MFA_LOCKOUT_SEC (900)
    _c, after = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret))
    assert after.status_code == 200, after.text


# ── 3. replay ───────────────────────────────────────────────────────────────


def test_a_code_is_accepted_once(ent, clock):
    secret, _ = enroll(ent.admin, clock=clock)
    code = code_for(secret)
    _c, first = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code)
    assert first.status_code == 200, first.text
    _c, replay = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code)
    assert replay.status_code == 401
    assert "already used" in replay.json()["detail"]
    # An older code inside the drift window is a replay too once a newer step was used.
    _c, older = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret, step_delta=-1))
    assert older.status_code == 401
    assert audit_rows(ent, "auth.mfa.challenge.failed")[0]["reason_code"] == "replayed"
    # The confirming code from enrolment cannot be replayed either (checked by
    # enroll(): it advanced one step before this sign-in used the next code).
    clock()
    _c, fresh = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret))
    assert fresh.status_code == 200


def test_drift_of_one_step_is_accepted(ent, clock):
    secret, _ = enroll(ent.admin, clock=clock)
    _c, late = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret, step_delta=1))
    assert late.status_code == 200, late.text
    clock(90)
    _c, too_far = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret, step_delta=2))
    assert too_far.status_code == 401


# ── 4. recovery codes ───────────────────────────────────────────────────────


def test_recovery_code_works_exactly_once(ent, clock):
    _secret, codes = enroll(ent.admin, clock=clock)
    spelled = codes[0].upper().replace("-", " ")  # forgiving about case and separators
    _c, used = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, recovery_code=spelled)
    assert used.status_code == 200, used.text
    assert used.json()["mfa_method"] == "recovery"
    assert used.json()["recovery_codes_remaining"] == 9
    _c, again = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, recovery_code=codes[0])
    assert again.status_code == 401
    rows = audit_rows(ent, "auth.mfa.recovery_code.used")
    assert len(rows) == 1 and rows[0]["details"]["recovery_codes_remaining"] == 9
    assert codes[0] not in audit_text(ent)


# ── 5. enforcement ──────────────────────────────────────────────────────────

_RPC = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "list_connectors", "arguments": {}}}
_RPC_HEADERS = {"Accept": "application/json, text/event-stream"}


def test_require_mfa_holds_unenrolled_member_until_they_enroll(ent, clock):
    ws = ent.workspace("Acme")
    admin = ent.admin
    admin.headers["X-Workspace-Id"] = ws
    member = ent.member(email=MEMBER, workspace_id=ws, role="editor")
    assert member.get("/api/v1/connectors").status_code == 200  # before the policy
    assert member.post("/api/v1/mcp", json=_RPC, headers=_RPC_HEADERS).status_code == 200

    # The admin cannot switch it on from a session the policy would hold.
    refused = require_mfa(ent, admin, ws)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["reason"] == "mfa_enroll_self_first"

    enroll(admin, clock=clock)  # confirm upgrades the admin's own session
    turned_on = require_mfa(ent, admin, ws)
    assert turned_on.status_code == 200, turned_on.text
    assert admin.get("/api/v1/connectors").status_code == 200  # admin keeps working

    posture = admin.get("/api/v1/workspace/security/posture").json()
    assert posture["mfa_required"] is True and posture["mfa_enforced"] is True
    assert posture["enforcement"]["mfa"]["sso_exempt"] is True
    assert posture["enforcement"]["mfa"]["members_enrolled"] == 1
    report = admin.get("/api/v1/workspace/security/report").text
    assert "Two-factor authentication (TOTP): required and enforced" in report
    assert "login MFA is not wired" not in report

    # The member's existing session (and a new password sign-in) is held.
    for session in (member, ent.login(MEMBER, _member_password(ent))):
        session.headers["X-Workspace-Id"] = ws
        blocked = session.get("/api/v1/connectors")
        assert blocked.status_code == 403
        assert blocked.json()["reason"] == "mfa_enrollment_required"
        for path in ("/api/v1/jobs", "/api/v1/workspace/settings", "/api/v1/team/workspaces"):
            assert session.get(path).status_code == 403, path
        assert session.post("/api/v1/workspace/api-keys", json={"name": "x", "role": "viewer"}).status_code == 403
        # Public JSON-RPC MCP endpoint treats a held session as anonymous.
        rpc = session.post("/api/v1/mcp", json=_RPC, headers=_RPC_HEADERS)
        assert rpc.status_code == 401 and "Authentication required" in rpc.text
        me = session.get("/api/v1/auth/me")
        assert me.status_code == 200
        assert me.json()["mfa"]["hold"] == "mfa_enrollment_required"
    assert audit_rows(ent, "auth.mfa.enforcement.held")

    # Enrol from the held session -> the same session gets full access.
    secret, _ = enroll(member, clock=clock)
    assert member.get("/api/v1/connectors").status_code == 200
    assert member.get("/api/v1/auth/me").json()["mfa"]["hold"] is None
    # From now on the member signs in with a code.
    _c, done = mfa_login(ent, MEMBER, _member_password(ent), code=code_for(secret))
    assert done.status_code == 200
    assert ent.bearer(done.json()["token"], ws).get("/api/v1/connectors").status_code == 200
    # Turning it off while the workspace requires it is refused.
    off = member.post("/api/v1/auth/mfa/totp/disable", json={"password": _member_password(ent)})
    assert off.status_code == 409


def test_enrolled_member_with_old_password_session_steps_up(ent, clock):
    ws = ent.workspace("Acme")
    admin = ent.admin
    admin.headers["X-Workspace-Id"] = ws
    member = ent.member(email=MEMBER, workspace_id=ws, role="editor")
    old_session = ent.login(MEMBER, _member_password(ent))  # password-only, before enrolling
    old_session.headers["X-Workspace-Id"] = ws
    secret, _ = enroll(member, clock=clock)
    enroll(admin, clock=clock)
    assert require_mfa(ent, admin, ws).status_code == 200

    blocked = old_session.get("/api/v1/connectors")
    assert blocked.status_code == 403 and blocked.json()["reason"] == "mfa_verification_required"
    step = old_session.post("/api/v1/auth/mfa/verify", json={"code": code_for(secret)})
    assert step.status_code == 200, step.text
    assert old_session.get("/api/v1/connectors").status_code == 200


def test_platform_admin_acting_in_an_mfa_workspace_is_held(ent, clock):
    """Not a member, but naming the workspace: its policy applies."""
    ws = ent.workspace("Acme")
    enroll(ent.admin, clock=clock)
    ent.admin.headers["X-Workspace-Id"] = ws
    assert require_mfa(ent, ent.admin, ws).status_code == 200
    from services import team_store, user_store

    user_store.create_user(email="ops-admin@example.com", password="Ops-Admin-Password-26",
                           role="admin", created_by=ADMIN_EMAIL)
    other = ent.login("ops-admin@example.com", "Ops-Admin-Password-26")
    assert not team_store.list_memberships_for_user("ops-admin@example.com")
    assert other.get("/api/v1/connectors").status_code == 200  # no workspace named: not held
    other.headers["X-Workspace-Id"] = ws
    assert other.get("/api/v1/connectors").status_code == 403


# ── 6. SSO is exempt ────────────────────────────────────────────────────────


def _sso_login(ent, monkeypatch, email: str) -> str:
    from services import integrations_store, oidc_client, sso_state

    monkeypatch.setattr(sso_state, "STATE_PATH", ent.tmp / "sso_state.json")
    monkeypatch.setattr(sso_state, "data_dir", lambda: ent.tmp)
    config = {
        "issuer": "https://issuer.example", "client_id": "client-123",
        "client_secret": "secret-never-in-state",
        "redirect_uri": "https://testserver/api/v1/auth/sso/oidc/callback",
        "scopes": "openid email profile",
    }
    monkeypatch.setattr(integrations_store, "get_sso_config_raw", lambda _t: dict(config))
    monkeypatch.setattr(integrations_store, "validate_sso_config", lambda _t: {"ready": True, "message": "ok"})
    metadata = oidc_client.OidcProviderMetadata(
        issuer="https://issuer.example", authorization_endpoint="https://issuer.example/authorize",
        token_endpoint="https://issuer.example/token", jwks_uri="https://issuer.example/jwks",
        id_token_signing_alg_values_supported=("RS256",),
    )
    monkeypatch.setattr(oidc_client, "discover", lambda _issuer: metadata)
    monkeypatch.setattr(oidc_client, "validate_id_token", lambda *_a, **_k: {"sub": "idp-user", "amr": ["mfa"]})
    monkeypatch.setattr(oidc_client, "email_from_claims", lambda _c, **_k: email)
    monkeypatch.setattr(
        oidc_client, "_http_client",
        lambda: httpx.Client(transport=httpx.MockTransport(
            lambda _r: httpx.Response(200, json={"id_token": "id-token"})), timeout=10.0),
    )
    sso_state.set_state("cb-state", "oidc", {
        "code_verifier": "verifier", "nonce": "nonce", "issuer": "https://issuer.example",
        "redirect_uri": config["redirect_uri"], "client_id": "client-123",
    })
    res = ent.anonymous().get(
        "/api/v1/auth/sso/oidc/callback", params={"code": "auth-code", "state": "cb-state"},
        follow_redirects=False,
    )
    assert res.status_code == 302, res.text
    return parse_qs(urlsplit(res.headers["location"]).fragment)["sso_token"][0]


def test_sso_session_is_exempt_and_password_session_is_not(ent, clock, monkeypatch):
    ws = ent.workspace("Acme")
    ent.admin.headers["X-Workspace-Id"] = ws
    ent.member(email=MEMBER, workspace_id=ws, role="editor")
    enroll(ent.admin, clock=clock)
    assert require_mfa(ent, ent.admin, ws).status_code == 200

    sso = ent.bearer(_sso_login(ent, monkeypatch, MEMBER), ws)
    assert sso.get("/api/v1/connectors").status_code == 200
    assert sso.get("/api/v1/auth/me").json()["mfa"]["via_sso"] is True

    pwd = ent.login(MEMBER, _member_password(ent))
    pwd.headers["X-Workspace-Id"] = ws
    assert pwd.get("/api/v1/connectors").status_code == 403

    # Operators who want SSO held too can say so; the posture reports it.
    monkeypatch.setenv("DATAFLOW_MFA_SSO_EXEMPT", "0")
    assert sso.get("/api/v1/connectors").status_code == 403
    assert ent.admin.get("/api/v1/workspace/security/posture").json()["enforcement"]["mfa"]["sso_exempt"] is False


# ── 7. API keys are not affected ────────────────────────────────────────────


def test_api_keys_and_service_accounts_are_not_held(ent, clock):
    ws = ent.workspace("Acme")
    key = ent.api_key(role="editor", workspace_id=ws)
    enroll(ent.admin, clock=clock)
    ent.admin.headers["X-Workspace-Id"] = ws
    assert require_mfa(ent, ent.admin, ws).status_code == 200
    as_key = ent.bearer(key, ws)
    assert as_key.get("/api/v1/connectors").status_code == 200
    # ...and a key cannot enrol a second factor (it is not a person).
    assert as_key.post("/api/v1/auth/mfa/totp/enroll", json={}).status_code in (403, 401)


# ── 8. platform-admin reset (break-glass) ───────────────────────────────────


def test_admin_reset_requires_reason_is_audited_and_ends_sessions(ent, clock):
    ws = ent.workspace("Acme")
    member = ent.member(email=MEMBER, workspace_id=ws, role="admin")
    enroll(member, clock=clock)
    enroll(ent.admin, clock=clock)

    # A workspace admin (not platform admin) cannot reset anyone.
    denied = member.post("/api/v1/iam/mfa/reset", json={"email": ADMIN_EMAIL, "reason": "lost my phone yesterday"})
    assert denied.status_code == 403
    # Platform admin: a reason is required, and not for themselves.
    short = ent.admin.post("/api/v1/iam/mfa/reset", json={"email": MEMBER, "reason": "lost"})
    assert short.status_code == 422
    self_reset = ent.admin.post("/api/v1/iam/mfa/reset", json={"email": ADMIN_EMAIL, "reason": "testing self reset"})
    assert self_reset.status_code == 400

    reason = "Ticket SR-1042: phone lost, recovery codes not stored; identity verified by video call"
    done = ent.admin.post("/api/v1/iam/mfa/reset", json={"email": MEMBER, "reason": reason})
    assert done.status_code == 200, done.text
    assert done.json()["had_factor"] is True and done.json()["sessions_revoked"] >= 1

    rows = [r for r in audit_rows(ent, "auth.mfa.reset") if r["outcome"] == "succeeded"]
    assert len(rows) == 1
    assert rows[0]["actor"] == ADMIN_EMAIL
    assert rows[0]["details"]["target"] == MEMBER
    assert rows[0]["details"]["reason"] == reason
    refused = [r for r in audit_rows(ent, "auth.mfa.reset") if r["outcome"] == "failed"]
    assert len(refused) >= 2

    # The member's old session is dead; a password sign-in works again (no factor).
    assert member.get("/api/v1/auth/me").status_code == 401
    assert password_login(ent, MEMBER, _member_password(ent)).status_code == 200


def test_cli_break_glass_resets_the_sole_admin_and_is_audited(ent, clock):
    """The env bootstrap admin lost phone and codes, and no other admin exists."""
    import importlib.util
    from pathlib import Path

    enroll(ent.admin, clock=clock)
    spec = importlib.util.spec_from_file_location(
        "mfa_reset_cli", Path(__file__).resolve().parents[1] / "scripts" / "mfa_reset.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    with pytest.raises(SystemExit):
        cli.main(["--email", ADMIN_EMAIL, "--reason", "short", "--operator", "jane.ops"])
    assert cli.main(["--email", ADMIN_EMAIL, "--reason", "SR-77 sole admin lost phone and codes",
                     "--operator", "jane.ops"]) == 0
    row = audit_rows(ent, "auth.mfa.reset")[-1]
    assert row["actor"] == "cli:jane.ops"
    assert row["details"]["via"] == "cli" and row["details"]["target"] == ADMIN_EMAIL
    assert ent.admin.get("/api/v1/auth/me").status_code == 401  # sessions revoked
    assert password_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD).status_code == 200


def test_disable_and_reenroll_need_a_fresh_code_or_password(ent, clock):
    secret, _ = enroll(ent.admin, clock=clock)
    # Re-enrol without proof is refused; with a code it issues a new secret, and
    # the old factor keeps working until the new one is confirmed.
    assert ent.admin.post("/api/v1/auth/mfa/totp/enroll", json={}).status_code == 400
    renewed = ent.admin.post("/api/v1/auth/mfa/totp/enroll", json={"code": code_for(secret)})
    assert renewed.status_code == 200
    new_secret = renewed.json()["secret"]
    assert new_secret != secret
    clock()
    assert ent.admin.post("/api/v1/auth/mfa/totp/confirm", json={"code": code_for(new_secret)}).status_code == 200
    clock()
    _c, old_code = mfa_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD, code=code_for(secret))
    assert old_code.status_code == 401
    # Disable: wrong password refused; right password removes it.
    assert ent.admin.post("/api/v1/auth/mfa/totp/disable", json={"password": "not-the-password"}).status_code == 403
    assert ent.admin.post("/api/v1/auth/mfa/totp/disable", json={"password": ADMIN_PASSWORD}).status_code == 200
    assert audit_rows(ent, "auth.mfa.removed")
    assert password_login(ent, ADMIN_EMAIL, ADMIN_PASSWORD).status_code == 200


def test_base_posture_reports_mfa_not_enforced_without_the_policy(ent):
    ws = ent.workspace("Acme")
    ent.admin.headers["X-Workspace-Id"] = ws
    created = ent.admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Acme"})
    assert created.status_code == 200
    posture = ent.admin.get("/api/v1/workspace/security/posture").json()
    assert posture["mfa_enforced"] is False
    assert posture["enforcement"]["mfa"]["missing"] == ["tenant.mfa_required"]


# ── helpers that read fixture state ─────────────────────────────────────────


def _member_password(ent) -> str:
    """The member's password: the harness issued a temporary one; reset it to a known value once."""
    from services import user_store

    if not getattr(ent, "_member_pw", None):
        user_store.set_password(email=MEMBER, password="Ana-Password-2026!")
        ent._member_pw = "Ana-Password-2026!"
    return ent._member_pw
