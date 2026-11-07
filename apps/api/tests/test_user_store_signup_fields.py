"""P1-7 — platform_users gains verification/canonical/algo fields + backfill.

Regression: an existing (pre-signup) admin still authenticates after the
migration stamps the new fields. Banned accounts are withheld from
credentials like disabled ones.
"""

from __future__ import annotations

import pytest

from services import user_store


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    yield
    monkeypatch.setattr(user_store, "_BACKFILLED", False)


def _legacy_user(email="admin@example.com"):
    return {
        "email": email,
        "name": "Admin",
        "role": "admin",
        "status": "active",
        "password_hash": "$2b$12$existinghashvalue",
        "must_change_password": False,
        "created_at": "2025-01-01T00:00:00+00:00",
        "created_by": "env",
        "updated_at": "2025-01-01T00:00:00+00:00",
        "last_login_at": None,
    }


def test_backfill_stamps_legacy_rows(monkeypatch, tmp_path):
    import json

    path = tmp_path / "users.json"
    path.write_text(json.dumps({"users": [_legacy_user()]}))
    updated = user_store.backfill_signup_fields()
    assert updated == 1
    doc = user_store._read_one("admin@example.com")
    assert doc["canonical_email"] == "admin@example.com"
    assert doc["email_verified_at"] == "2025-01-01T00:00:00+00:00"
    assert doc["password_algo"] == "bcrypt"
    # idempotent
    assert user_store.backfill_signup_fields() == 0


def test_legacy_admin_still_authenticates_after_migration(tmp_path):
    import json

    (tmp_path / "users.json").write_text(json.dumps({"users": [_legacy_user()]}))
    creds = user_store.credentials_for_auth()  # triggers ensure_backfilled
    assert len(creds) == 1
    assert creds[0]["email"] == "admin@example.com"
    assert creds[0]["password_algo"] == "bcrypt"


def test_banned_status_blocks_credentials():
    user, _ = user_store.create_user(email="bad@example.com", password="a-very-long-password")
    user_store.update_user(email="bad@example.com", status="banned")
    assert all(c["email"] != "bad@example.com" for c in user_store.credentials_for_auth())
    assert user_store.get_user("bad@example.com")["status"] == "banned"


def test_create_user_marks_verified_and_stamps_fields():
    user, issued = user_store.create_user(email="new@example.com", name="New")
    doc = user_store._read_one("new@example.com")
    assert doc["email_verified_at"]
    assert doc["canonical_email"] == "new@example.com"
    assert doc["password_algo"] == "bcrypt"
    assert issued is not None  # one-time password still minted for admins


def test_create_signup_user_inserts_verified():
    user_store.create_signup_user(
        email="ada@example.com",
        name="Ada",
        password_hash="$argon2id$v=19$m=19456,t=2,p=1$fake",
        password_algo="argon2id",
        canonical="ada@example.com",
        signup_ip="1.2.3.4",
        signup_risk=0.2,
    )
    doc = user_store._read_one("ada@example.com")
    assert doc["email_verified_at"]
    assert doc["password_algo"] == "argon2id"
    assert doc["created_by"] == "signup"
    assert doc["signup_ip"] == "1.2.3.4"
    assert user_store.find_by_canonical("ada@example.com")["email"] == "ada@example.com"


def test_find_by_canonical_and_mark_verified():
    user_store.create_signup_user(
        email="b@example.com", name="B", password_hash="h",
        password_algo="argon2id", canonical="b@example.com")
    assert user_store.find_by_canonical("b@example.com") is not None
    assert user_store.find_by_canonical("none@example.com") is None
    pub = user_store.mark_email_verified("b@example.com")
    assert pub["email_verified_at"]
    with pytest.raises(user_store.UserStoreError):
        user_store.mark_email_verified("ghost@example.com")
