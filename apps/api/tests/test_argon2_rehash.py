"""P0-4 — Argon2id hasher + transparent bcrypt→argon2id rehash on login.

Old bcrypt user logs in once and is rehashed; flag-off hashing stays
byte-for-byte bcrypt with no rehash.
"""

from __future__ import annotations

import pytest

from services import password_hash, user_store


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")


def test_argon2_hash_params_and_verify():
    h = password_hash.hash_password_argon2("correct horse battery")
    assert h.startswith("$argon2id$")
    assert "$m=19456,t=2,p=1$" in h
    assert password_hash.verify_password("correct horse battery", h)
    assert not password_hash.verify_password("wrong", h)
    assert password_hash.algo_for_hash(h) == "argon2id"


def test_flag_on_hash_is_argon2id(monkeypatch):
    assert password_hash.hash_password("pw").startswith("$argon2id$")


def test_flag_off_hash_stays_bcrypt_and_no_rehash(monkeypatch):
    monkeypatch.delenv("DATAWRAP_SIGNUP_ENABLED", raising=False)
    monkeypatch.delenv("DATAFLOW_SIGNUP_ENABLED", raising=False)
    h = password_hash.hash_password("pw")
    assert h.startswith("$2b$")
    assert password_hash.needs_rehash(h) is False  # flag off: never rehash


def test_bcrypt_still_verifies():
    import bcrypt as _b

    legacy = _b.hashpw(b"secret123456", _b.gensalt(rounds=12)).decode()
    assert password_hash.verify_password("secret123456", legacy)
    assert password_hash.needs_rehash(legacy) is True


def test_bcrypt_user_rehashed_on_login():
    """authenticate() upgrades the stored hash once, flag on."""
    import bcrypt as _b

    h = _b.hashpw(b"a-very-long-password", _b.gensalt(rounds=12)).decode()
    user_store.create_signup_user(
        email="ada@example.com", name="Ada", password_hash=h,
        password_algo="bcrypt", canonical="ada@example.com")

    from src.services.auth_service import authenticate

    user = authenticate("ada@example.com", "a-very-long-password")
    assert user is not None
    doc = user_store._read_one("ada@example.com")
    assert doc["password_algo"] == "argon2id"
    assert doc["password_hash"].startswith("$argon2id$")
    # second login still works on the new hash
    assert authenticate("ada@example.com", "a-very-long-password") is not None


def test_flag_off_no_rehash_on_login(monkeypatch):
    import bcrypt as _b

    h = _b.hashpw(b"a-very-long-password", _b.gensalt(rounds=12)).decode()
    user_store.create_signup_user(
        email="bob@example.com", name="Bob", password_hash=h,
        password_algo="bcrypt", canonical="bob@example.com")
    monkeypatch.delenv("DATAWRAP_SIGNUP_ENABLED", raising=False)
    monkeypatch.delenv("DATAFLOW_SIGNUP_ENABLED", raising=False)

    from src.services.auth_service import authenticate

    assert authenticate("bob@example.com", "a-very-long-password") is not None
    assert user_store._read_one("bob@example.com")["password_algo"] == "bcrypt"


def test_wrong_password_no_rehash():
    import bcrypt as _b

    h = _b.hashpw(b"real-password-here", _b.gensalt(rounds=12)).decode()
    user_store.create_signup_user(
        email="eve@example.com", name="Eve", password_hash=h,
        password_algo="bcrypt", canonical="eve@example.com")

    from src.services.auth_service import authenticate

    assert authenticate("eve@example.com", "wrong-password") is None
    assert user_store._read_one("eve@example.com")["password_algo"] == "bcrypt"
