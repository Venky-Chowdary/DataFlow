from src.services.auth_service import authenticate, create_token, hash_password, verify_token


def test_hash_and_login():
    assert hash_password("password123") == "ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f"
    user = authenticate("test@gmail.com", "password123")
    assert user is not None
    assert user["email"] == "test@gmail.com"
    assert authenticate("test@gmail.com", "wrong") is None


def test_token_roundtrip():
    user = authenticate("test@gmail.com", "password123")
    assert user is not None
    token, _expires = create_token(user["email"])
    assert verify_token(token) == "test@gmail.com"
    assert verify_token("bad-token") is None
