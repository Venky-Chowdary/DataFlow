from src.services.auth_service import authenticate, create_token, hash_password, verify_password, verify_token


def test_hash_and_login():
    hashed = hash_password("password123")
    assert hashed != "password123"
    assert hashed.startswith("$2b$")
    assert verify_password("password123", hashed)

    # Dev user (bcrypt) still authenticates
    user = authenticate("test@gmail.com", "password123")
    assert user is not None
    assert user["email"] == "test@gmail.com"
    assert authenticate("test@gmail.com", "wrong") is None


def test_legacy_sha256_hash_still_verifies():
    # Legacy unsalted SHA-256 hashes continue to work during migration.
    legacy_hash = "ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f"
    assert verify_password("password123", legacy_hash)
    assert not verify_password("wrong", legacy_hash)


def test_token_roundtrip():
    user = authenticate("test@gmail.com", "password123")
    assert user is not None
    token, _expires = create_token(user["email"])
    assert verify_token(token) == "test@gmail.com"
    assert verify_token("bad-token") is None
