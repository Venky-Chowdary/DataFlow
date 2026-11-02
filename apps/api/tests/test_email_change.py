"""P4-2 — email change: OTP to the new address, account/membership/org
rekey, sessions revoked, old address notified."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import auth_state, email_quota, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "email-change-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setattr(signup_store, "_pending_path",
                        lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path",
                        lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    monkeypatch.setattr(team_store, "_store_path",
                        lambda: tmp_path / "team.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    # Auth mail rides the outbox lane since item 7 — run it inline so each
    # test's sends land before the test ends (no cross-test stragglers).
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    import src.services.auth_service as auth_mod
    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    from services.auth_rate_limit import reset_auth_rate_limits
    reset_auth_rate_limits()
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app, base_url="https://testserver")


def _verified_user(c: TestClient, email: str = "ada@example.com") -> dict:
    r = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def _request_change(c: TestClient, new_email: str, email: str = "ada@example.com"):
    csrf = c.cookies["dw_csrf"]
    return c.post("/api/v1/auth/email/change",
                  json={"new_email": new_email},
                  headers={"x-csrf-token": csrf})


def test_request_sends_code_to_new_address_and_notice_to_old():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    assert resp.status_code == 202
    assert resp.json()["challenge_id"]
    # signup code + change code (the old-address notice goes out at confirm)
    assert len(ConsoleEmailSender.OUTBOX) == 2
    assert ConsoleEmailSender.OUTBOX[-1].to == "ada2@example.com"
    assert "new Datawrap email" in ConsoleEmailSender.OUTBOX[-1].subject \
        or "Confirm" in ConsoleEmailSender.OUTBOX[-1].subject


def test_request_requires_auth():
    c = _client()
    resp = c.post("/api/v1/auth/email/change",
                  json={"new_email": "x@example.com"})
    assert resp.status_code == 401


def test_request_refuses_taken_address():
    c = _client()
    _verified_user(c)
    _verified_user(c, email="bob@example.com")
    resp = _request_change(c, "ada@example.com")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "email_taken"


def test_request_refuses_disposable_domain():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada@mailinator.com")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "blocked_address"


def test_confirm_rekeys_account_memberships_orgs_and_revokes_sessions():
    c = _client()
    _verified_user(c)
    state = auth_state.get_auth_state()
    assert state.list_sessions("ada@example.com")
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    ok = c.post("/api/v1/auth/email/change/confirm",
                json={"challenge_id": cid, "code": code},
                headers={"x-csrf-token": c.cookies["dw_csrf"]})
    assert ok.status_code == 200
    assert ok.json()["email"] == "ada2@example.com"
    assert user_store.get_user("ada@example.com") is None
    new_doc = user_store.get_user("ada2@example.com")
    assert new_doc is not None
    assert user_store.find_by_canonical("ada2@example.com") is not None
    assert user_store.find_by_canonical("ada@example.com") is None
    # memberships + orgs followed the account
    assert team_store._read_memberships(email="ada@example.com") == []
    assert team_store._read_memberships(email="ada2@example.com")
    # every old session is dead; cookie no longer authenticates
    assert state.list_sessions("ada@example.com") == []
    # old address was notified
    subjects = [m.subject for m in ConsoleEmailSender.OUTBOX]
    assert any("changed" in s for s in subjects)


def test_confirm_wrong_code_400_and_single_use():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    bad = c.post("/api/v1/auth/email/change/confirm",
                 json={"challenge_id": cid, "code": "000000"},
                 headers={"x-csrf-token": c.cookies["dw_csrf"]})
    assert bad.status_code == 400
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    assert c.post("/api/v1/auth/email/change/confirm",
                  json={"challenge_id": cid, "code": code},
                  headers={"x-csrf-token": c.cookies["dw_csrf"]}).status_code == 200
    again = c.post("/api/v1/auth/email/change/confirm",
                   json={"challenge_id": cid, "code": code},
                   headers={"x-csrf-token": c.cookies["dw_csrf"]})
    # the successful rekey revoked every session, so the replay is refused
    # by the middleware before the single-use check can answer 410
    assert again.status_code in (401, 410)


def test_confirm_requires_auth():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    anon = _client()
    r = anon.post("/api/v1/auth/email/change/confirm",
                  json={"challenge_id": cid, "code": code})
    assert r.status_code == 401
    assert user_store.get_user("ada@example.com") is not None


def test_confirm_other_account_forbidden():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    other = _client()
    _verified_user(other, email="bob@example.com")
    r = other.post("/api/v1/auth/email/change/confirm",
                   json={"challenge_id": cid, "code": code},
                   headers={"x-csrf-token": other.cookies["dw_csrf"]})
    assert r.status_code == 403
    assert user_store.get_user("ada@example.com") is not None


def test_concurrent_same_target_only_one_wins():
    """Two accounts racing to claim the same canonical address: exactly one
    rekey lands; the loser gets email_taken instead of a clobbered account."""
    import threading

    c1, c2 = _client(), _client()
    _verified_user(c1)
    _verified_user(c2, email="bob@example.com")
    target = "shared@example.com"
    results: list[int] = []

    def claim(client: TestClient) -> None:
        resp = client.post("/api/v1/auth/email/change",
                           json={"new_email": target},
                           headers={"x-csrf-token": client.cookies["dw_csrf"]})
        if resp.status_code != 202:
            results.append(resp.status_code)
            return
        cid = resp.json()["challenge_id"]
        # OUTBOX[-1] races under concurrency — match this challenge's mail
        # by its idempotency key (otp:<cid>:1) instead.
        code = re.search(
            r"\b(\d{6})\b",
            [m for m in ConsoleEmailSender.OUTBOX
             if m.idempotency_key == f"otp:{cid}:1"][-1].text).group(1)
        r = client.post("/api/v1/auth/email/change/confirm",
                        json={"challenge_id": cid, "code": code},
                        headers={"x-csrf-token": client.cookies["dw_csrf"]})
        results.append(r.status_code)

    t1 = threading.Thread(target=claim, args=(c1,))
    t2 = threading.Thread(target=claim, args=(c2,))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(results) == [200, 400]
    owners = [e for e in ("ada@example.com", "bob@example.com")
              if user_store.get_user(e) is None]
    assert len(owners) == 1  # exactly one account rekeyed to shared@


# ---------------------------------------------------------------------------
# Review-4 regression tests: data-loss ordering, fail-closed index, typed
# errors, legacy-canonical compare, retryable follow-on rekeys.
# ---------------------------------------------------------------------------


class _FakeColl:
    """Dict-backed Mongo stand-in; failure flags simulate server errors."""

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.fail_insert = False
        self.fail_delete_keys: set[str] = set()
        self.fail_index = False

    def find_one(self, query, *_a, **_k):
        return dict(self.docs.get(query.get("_id")) or {}) or None

    def find(self, _query, *_a, **_k):
        return [dict(d) for d in self.docs.values()]

    def insert_one(self, doc, *_a, **_k):
        if self.fail_insert or doc["_id"] in self.docs:
            from pymongo.errors import DuplicateKeyError
            raise DuplicateKeyError("dup")
        self.docs[doc["_id"]] = dict(doc)
        return None

    def replace_one(self, query, doc, upsert=False, **_k):
        self.docs[query["_id"]] = dict(doc)
        return None

    def delete_one(self, query, *_a, **_k):
        if query["_id"] in self.fail_delete_keys:
            raise RuntimeError("delete failed")
        self.docs.pop(query["_id"], None)
        return None

    def create_index(self, *_a, **_k):
        if self.fail_index:
            raise RuntimeError("duplicate keys block index")
        return "canonical_email_unique"

    def count_documents(self, *_a, **_k):
        return len(self.docs)


class _FakeDb:
    def __init__(self, coll):
        self._coll = coll
        self.name = "fake-mongo"

    def __getitem__(self, _name):
        return self._coll


def _mongo_mode(monkeypatch, coll: _FakeColl) -> None:
    monkeypatch.setattr(user_store, "mongo_database",
                        lambda: _FakeDb(coll))
    monkeypatch.setattr(user_store, "_CANONICAL_INDEXED", set())
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: "/unused-in-mongo-mode")


def _seed_user(coll: _FakeColl, email: str) -> dict:
    doc = {
        "email": email,
        "name": "Ada",
        "role": "member",
        "status": "active",
        "password_hash": "hash-value",
        "password_algo": "bcrypt",
        "canonical_email": email,
        "email_verified_at": "2026-01-01T00:00:00+00:00",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    coll.docs[email] = doc
    return doc


def test_mongo_duplicate_insert_leaves_old_account_intact(monkeypatch):
    """A lost canonical race must not delete the original account."""
    coll = _FakeColl()
    _mongo_mode(monkeypatch, coll)
    _seed_user(coll, "ada@example.com")
    coll.fail_insert = True
    with pytest.raises(user_store.EmailTakenError):
        user_store.change_email(email="ada@example.com",
                                new_email="ada2@example.com")
    intact = coll.docs["ada@example.com"]
    assert intact["password_hash"] == "hash-value"
    assert intact["status"] == "active"
    assert "ada2@example.com" not in coll.docs


def test_mongo_delete_failure_compensates_insert(monkeypatch):
    """If the old-key delete fails after a committed insert, the new row is
    removed again so the account does not exist under two keys."""
    coll = _FakeColl()
    _mongo_mode(monkeypatch, coll)
    _seed_user(coll, "ada@example.com")
    coll.fail_delete_keys.add("ada@example.com")
    with pytest.raises(RuntimeError):
        user_store.change_email(email="ada@example.com",
                                new_email="ada2@example.com")
    assert "ada@example.com" in coll.docs
    assert "ada2@example.com" not in coll.docs


def test_index_ensure_failure_fails_closed(monkeypatch):
    """When the unique index cannot be ensured, rekey refuses (503 path)
    instead of silently running without the atomic guarantee."""
    coll = _FakeColl()
    _mongo_mode(monkeypatch, coll)
    _seed_user(coll, "ada@example.com")
    coll.fail_index = True
    with pytest.raises(user_store.IndexUnavailableError):
        user_store.change_email(email="ada@example.com",
                                new_email="ada2@example.com")


def test_file_store_legacy_row_blocks_canonical(tmp_path, monkeypatch):
    """Legacy rows without canonical_email still count as the canonical
    owner — deriving it from their stored email."""
    import json
    from services.email_canonical import canonical_email

    legacy = "a.da@gmail.com"
    target = "ada@gmail.com"
    assert canonical_email(legacy) == canonical_email(target)
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [
        {"email": legacy, "password_hash": "h1"},
        {"email": "bob@example.com", "password_hash": "h2",
         "canonical_email": "bob@example.com"},
    ]}))
    monkeypatch.setattr(user_store, "_store_path", lambda: users)
    with pytest.raises(user_store.EmailTakenError):
        user_store.change_email(email="bob@example.com",
                                new_email=target)


def test_partial_rekey_flags_and_finish_retries():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)

    def _boom(*_a, **_k):
        raise RuntimeError("membership store unavailable")

    real = team_store.rekey_memberships
    signup_service.team_store.rekey_memberships = _boom
    try:
        ok = c.post("/api/v1/auth/email/change/confirm",
                    json={"challenge_id": cid, "code": code},
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    finally:
        signup_service.team_store.rekey_memberships = real
    assert ok.status_code == 200
    # Consistent + retryable: the account moved, carries the pending flag,
    # and memberships are still on the old address.
    assert user_store.rekey_pending("ada2@example.com") == "ada@example.com"
    assert team_store._read_memberships(email="ada@example.com")
    assert signup_service.finish_pending_rekeys(
        email="ada2@example.com") is True
    assert user_store.rekey_pending("ada2@example.com") is None
    assert team_store._read_memberships(email="ada@example.com") == []
    assert team_store._read_memberships(email="ada2@example.com")


def test_backfill_reports_duplicate_canonicals(tmp_path, monkeypatch):
    import json
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [
        {"email": "ada@gmail.com", "password_hash": "h1"},
        {"email": "a.da@gmail.com", "password_hash": "h2"},
        {"email": "bob@example.com", "password_hash": "h3"},
    ]}))
    monkeypatch.setattr(user_store, "_store_path", lambda: users)
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    monkeypatch.setattr(user_store, "_CANONICAL_DUPLICATES", [])
    user_store.backfill_signup_fields()
    assert user_store.canonical_email_duplicates() == ["ada@gmail.com"]
    rows = json.loads(users.read_text())["users"]
    by_email = {r["email"]: r for r in rows}
    assert by_email["ada@gmail.com"]["canonical_email"] == "ada@gmail.com"
    assert by_email["a.da@gmail.com"]["canonical_email"] == "ada@gmail.com"


# ---------------------------------------------------------------------------
# Review-5 regression tests: rekey self-heal on session resolution, index
# ensure at create, same-address guard, empty-canonical backfill skip.
# ---------------------------------------------------------------------------


def test_pending_rekey_heals_on_next_authenticated_request():
    """Force the follow-on rekey to fail at confirm; the flag persists; the
    next cookie-authenticated request completes the move and clears it."""
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)

    def _boom(*_a, **_k):
        raise RuntimeError("membership store unavailable")

    real = team_store.rekey_memberships
    signup_service.team_store.rekey_memberships = _boom
    try:
        ok = c.post("/api/v1/auth/email/change/confirm",
                    json={"challenge_id": cid, "code": code},
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    finally:
        signup_service.team_store.rekey_memberships = real
    assert ok.status_code == 200
    assert user_store.rekey_pending("ada2@example.com") == "ada@example.com"
    assert team_store._read_memberships(email="ada@example.com")

    # Sessions were revoked at rekey; sign in again with the new address and
    # any authenticated request heals the flag.
    session = auth_state.get_auth_state().create_session(
        user_id="ada2@example.com", device_id="d2", ip="", ua="",
        csrf_token="tok")
    # Fresh client: the old jar still carries the revoked __Host-dw_session.
    c2 = _client()
    c2.cookies.set("__Host-dw_session", session["jti"])
    c2.cookies.set("dw_csrf", "tok")
    c2.post("/api/v1/auth/email/change",
            json={"new_email": "irrelevant@example.com"},
            headers={"x-csrf-token": "tok"})  # any authed request heals it
    assert user_store.rekey_pending("ada2@example.com") is None
    assert team_store._read_memberships(email="ada@example.com") == []
    assert team_store._read_memberships(email="ada2@example.com")


def test_rekey_flag_write_failure_surfaces():
    """If mark_rekey_pending itself fails, the account stays half-moved AND
    the failure is surfaced (logged at ERROR + security event), not dropped."""
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)

    def _boom(*_a, **_k):
        raise RuntimeError("down")

    real_rekey = team_store.rekey_memberships
    real_mark = user_store.mark_rekey_pending
    signup_service.team_store.rekey_memberships = _boom
    signup_service.user_store.mark_rekey_pending = _boom
    try:
        ok = c.post("/api/v1/auth/email/change/confirm",
                    json={"challenge_id": cid, "code": code},
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    finally:
        signup_service.team_store.rekey_memberships = real_rekey
        signup_service.user_store.mark_rekey_pending = real_mark
    assert ok.status_code == 200  # the move itself committed; flag write failed
    moved = user_store.get_user("ada2@example.com")
    assert moved is not None  # account rekeyed, healing is by other means


def test_concurrent_create_canonical_variants_exactly_one_account(tmp_path, monkeypatch):
    """On a fresh store, two racing variant-canonical creates produce one
    account — the file lock (or the Mongo unique index) decides."""
    import threading

    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    from services.email_canonical import canonical_email

    variants = ["ada@gmail.com", "a.da@gmail.com"]
    assert canonical_email(variants[0]) == canonical_email(variants[1])
    errors: list[Exception] = []

    def make(addr: str) -> None:
        try:
            user_store.create_signup_user(
                email=addr, name="Ada", password_hash="argon-hash",
                password_algo="argon2id",
                canonical=canonical_email(addr))
        except Exception as exc:  # noqa: BLE001 - collected for assertion
            errors.append(exc)

    threads = [threading.Thread(target=make, args=(v,)) for v in variants]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(errors) == 1
    assert isinstance(errors[0], user_store.EmailTakenError)
    all_users = user_store._read_all()
    assert len(all_users) == 1


def test_create_fails_closed_when_index_unavailable(monkeypatch):
    coll = _FakeColl()
    _mongo_mode(monkeypatch, coll)
    coll.fail_index = True
    with pytest.raises(user_store.IndexUnavailableError):
        user_store.create_signup_user(
            email="ada@example.com", name="Ada", password_hash="h",
            password_algo="argon2id", canonical="ada@example.com")


def test_same_canonical_change_rejected_as_same_address():
    c = _client()
    _verified_user(c, email="ada@gmail.com")
    # dot-variant folds to the account's own canonical → same_address, not email_taken
    resp = _request_change(c, "a.da@gmail.com")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "same_address"


def test_store_same_address_raises_typed_error(tmp_path, monkeypatch):
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    user_store.create_user(email="ada+old@gmail.com", password="x" * 12)
    doc = user_store._read_one("ada+old@gmail.com")
    doc["canonical_email"] = "ada@gmail.com"  # backfill-style fold
    user_store._write_one(doc)
    with pytest.raises(user_store.SameAddressError):
        user_store.change_email(email="ada+old@gmail.com",
                                new_email="ada+new@gmail.com",
                                canonical="ada@gmail.com")


def test_backfill_skips_empty_canonical_keys(tmp_path, monkeypatch):
    import json
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [
        {"password_hash": "h1"},
        {"password_hash": "h2"},
    ]}))
    monkeypatch.setattr(user_store, "_store_path", lambda: users)
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    monkeypatch.setattr(user_store, "_CANONICAL_DUPLICATES", [])
    user_store.backfill_signup_fields()
    assert user_store.canonical_email_duplicates() == []


def test_rekey_heal_is_single_flight():
    """Two parallel authenticated requests on a flagged account must not
    double-move memberships — the lease hands the heal to exactly one."""
    import threading

    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)

    def _boom(*_a, **_k):
        raise RuntimeError("membership store unavailable")

    real = team_store.rekey_memberships
    signup_service.team_store.rekey_memberships = _boom
    try:
        ok = c.post("/api/v1/auth/email/change/confirm",
                    json={"challenge_id": cid, "code": code},
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    finally:
        signup_service.team_store.rekey_memberships = real
    assert ok.status_code == 200
    assert user_store.rekey_pending("ada2@example.com") == "ada@example.com"

    session = auth_state.get_auth_state().create_session(
        user_id="ada2@example.com", device_id="d2", ip="", ua="",
        csrf_token="tok")

    calls = {"rekey": 0}
    count_lock = threading.Lock()
    real_rekey = team_store.rekey_memberships

    def _counted(*a, **k):
        with count_lock:
            calls["rekey"] += 1
        return real_rekey(*a, **k)

    signup_service.team_store.rekey_memberships = _counted
    try:
        barrier = threading.Barrier(3)

        def hit():
            barrier.wait()
            c2 = _client()
            c2.cookies.set("__Host-dw_session", session["jti"])
            c2.cookies.set("dw_csrf", "tok")
            c2.post("/api/v1/auth/email/change",
                    json={"new_email": "irrelevant@example.com"},
                    headers={"x-csrf-token": "tok"})

        threads = [threading.Thread(target=hit) for _ in range(2)]
        for t in threads:
            t.start()
        barrier.wait()
        for t in threads:
            t.join()
    finally:
        signup_service.team_store.rekey_memberships = real_rekey

    assert calls["rekey"] == 1  # one healer moved memberships, never two
    assert user_store.rekey_pending("ada2@example.com") is None
    assert team_store._read_memberships(email="ada@example.com") == []
    assert team_store._read_memberships(email="ada2@example.com")


def test_rekey_heal_failure_keeps_flag_and_emits_event(monkeypatch):
    """A failed heal frees the lease (the next request retries), keeps the
    flag, and emits the same security event as the flag-write failure."""
    from services import audit_coverage

    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)

    def _boom(*_a, **_k):
        raise RuntimeError("membership store unavailable")

    real = team_store.rekey_memberships
    signup_service.team_store.rekey_memberships = _boom
    try:
        ok = c.post("/api/v1/auth/email/change/confirm",
                    json={"challenge_id": cid, "code": code},
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    finally:
        signup_service.team_store.rekey_memberships = real
    assert ok.status_code == 200
    assert user_store.rekey_pending("ada2@example.com") == "ada@example.com"

    session = auth_state.get_auth_state().create_session(
        user_id="ada2@example.com", device_id="d2", ip="", ua="",
        csrf_token="tok")
    events: list[tuple[tuple, dict]] = []
    monkeypatch.setattr(
        audit_coverage, "record_security_event",
        lambda *a, **k: events.append((a, k)))

    def _still_down(*_a, **_k):
        raise RuntimeError("still down")

    signup_service.team_store.rekey_memberships = _still_down
    try:
        c2 = _client()
        c2.cookies.set("__Host-dw_session", session["jti"])
        c2.cookies.set("dw_csrf", "tok")
        c2.post("/api/v1/auth/email/change",
                json={"new_email": "irrelevant@example.com"},
                headers={"x-csrf-token": "tok"})
    finally:
        signup_service.team_store.rekey_memberships = real

    # Flag survived the failed heal; the event was emitted; and because the
    # lease was released, the next authenticated request heals successfully.
    assert user_store.rekey_pending("ada2@example.com") == "ada@example.com"
    assert any(args and args[0] == "email_change.rekey_flag_failed"
               for args, _k in events)

    c3 = _client()
    c3.cookies.set("__Host-dw_session", session["jti"])
    c3.cookies.set("dw_csrf", "tok")
    c3.post("/api/v1/auth/email/change",
            json={"new_email": "irrelevant@example.com"},
            headers={"x-csrf-token": "tok"})
    assert user_store.rekey_pending("ada2@example.com") is None
    assert team_store._read_memberships(email="ada2@example.com")
