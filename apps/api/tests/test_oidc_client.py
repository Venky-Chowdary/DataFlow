"""Security tests for OIDC metadata, keys, ID tokens, and identity claims."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from services import oidc_client

ISSUER = "https://issuer.example"
CLIENT_ID = "client-123"
NONCE = "expected-nonce"
JWKS_URI = f"{ISSUER}/jwks"


@pytest.fixture(autouse=True)
def clear_oidc_caches(monkeypatch):
    oidc_client.reset_caches()
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.delenv("DATAWRAP_OIDC_METADATA_TTL_SEC", raising=False)
    monkeypatch.delenv("DATAFLOW_OIDC_METADATA_TTL_SEC", raising=False)
    monkeypatch.delenv("DATAWRAP_OIDC_JWKS_TTL_SEC", raising=False)
    monkeypatch.delenv("DATAFLOW_OIDC_JWKS_TTL_SEC", raising=False)
    monkeypatch.delenv("DATAWRAP_OIDC_JWKS_REFRESH_COOLDOWN_SEC", raising=False)
    monkeypatch.delenv("DATAFLOW_OIDC_JWKS_REFRESH_COOLDOWN_SEC", raising=False)
    monkeypatch.delenv("DATAWRAP_OIDC_ALLOWED_ALGS", raising=False)
    monkeypatch.delenv("DATAFLOW_OIDC_ALLOWED_ALGS", raising=False)
    monkeypatch.delenv("DATAWRAP_OIDC_REQUIRE_EMAIL_VERIFIED", raising=False)
    monkeypatch.delenv("DATAFLOW_OIDC_REQUIRE_EMAIL_VERIFIED", raising=False)
    yield
    oidc_client.reset_caches()


def _metadata(algorithms: tuple[str, ...] = ("RS256",)):
    return oidc_client.OidcProviderMetadata(
        issuer=ISSUER,
        authorization_endpoint=f"{ISSUER}/authorize",
        token_endpoint=f"{ISSUER}/token",
        jwks_uri=JWKS_URI,
        id_token_signing_alg_values_supported=algorithms,
    )


def _set_transport(monkeypatch, handler):
    monkeypatch.setattr(
        oidc_client,
        "_http_client",
        lambda: httpx.Client(
            transport=httpx.MockTransport(handler),
            timeout=10.0,
        ),
    )


def _rsa_jwk(key, kid: str = "rsa-key", alg: str = "RS256"):
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "use": "sig", "alg": alg})
    return jwk


def _ec_jwk(key, kid: str = "ec-key", alg: str = "ES256"):
    jwk = json.loads(jwt.algorithms.ECAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "use": "sig", "alg": alg})
    return jwk


def _claims(**overrides):
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {
        "iss": ISSUER,
        "aud": CLIENT_ID,
        "sub": "subject-1",
        "iat": now,
        "exp": now + 300,
        "nonce": NONCE,
        "email": "Alice@example.com",
        "email_verified": True,
    }
    claims.update(overrides)
    return claims


def _token(key, *, algorithm="RS256", kid="rsa-key", claims=None):
    return jwt.encode(
        claims or _claims(),
        key,
        algorithm=algorithm,
        headers={"kid": kid},
    )


def _validate(monkeypatch, key, jwk, *, token=None, algorithms=("RS256",), leeway=60):
    _set_transport(
        monkeypatch,
        lambda request: httpx.Response(200, json={"keys": [jwk]}),
    )
    id_token = token or _token(key)
    return oidc_client.validate_id_token(
        id_token,
        metadata=_metadata(algorithms),
        client_id=CLIENT_ID,
        nonce=NONCE,
        algorithms=algorithms,
        leeway=leeway,
    )


def test_discovery_endpoint_and_cache(monkeypatch):
    requests = []
    document = {
        "issuer": f"{ISSUER}/",
        "authorization_endpoint": f"{ISSUER}/authorize",
        "token_endpoint": f"{ISSUER}/token",
        "jwks_uri": JWKS_URI,
        "id_token_signing_alg_values_supported": ["RS256", "ES256"],
    }

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, json=document)

    _set_transport(monkeypatch, handler)

    first = oidc_client.discover(ISSUER)
    second = oidc_client.discover(ISSUER)

    assert first == second
    assert requests == [f"{ISSUER}/.well-known/openid-configuration"]
    assert first.id_token_signing_alg_values_supported == ("RS256", "ES256")


def test_discovery_rejects_issuer_mismatch(monkeypatch):
    _set_transport(
        monkeypatch,
        lambda _request: httpx.Response(
            200,
            json={
                "issuer": "https://attacker.example",
                "authorization_endpoint": "https://attacker.example/auth",
                "token_endpoint": "https://attacker.example/token",
                "jwks_uri": "https://attacker.example/keys",
            },
        ),
    )
    with pytest.raises(oidc_client.OidcConfigError) as exc:
        oidc_client.discover(ISSUER)
    assert exc.value.reason == "issuer_mismatch"


def test_discovery_requires_secure_endpoints(monkeypatch):
    _set_transport(
        monkeypatch,
        lambda _request: httpx.Response(
            200,
            json={
                "issuer": ISSUER,
                "authorization_endpoint": "http://idp.example/authorize",
                "token_endpoint": f"{ISSUER}/token",
                "jwks_uri": JWKS_URI,
            },
        ),
    )
    with pytest.raises(oidc_client.OidcConfigError) as exc:
        oidc_client.discover(ISSUER)
    assert exc.value.reason == "insecure_endpoint"


def test_unknown_kid_refetches_once_then_succeeds(monkeypatch):
    old_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    new_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    requests = 0

    def handler(_request):
        nonlocal requests
        requests += 1
        keys = [_rsa_jwk(old_key)]
        if requests == 2:
            keys.append(_rsa_jwk(new_key, "rotated-key"))
        return httpx.Response(200, json={"keys": keys})

    _set_transport(monkeypatch, handler)
    clock = [100.0]
    monkeypatch.setattr(oidc_client.time, "monotonic", lambda: clock[0])
    oidc_client._find_jwk(
        JWKS_URI,
        "rsa-key",
        issuer=ISSUER,
        sso_type="oidc",
    )
    clock[0] += 61
    token = _token(new_key, kid="rotated-key")
    claims = oidc_client.validate_id_token(
        token,
        metadata=_metadata(),
        client_id=CLIENT_ID,
        nonce=NONCE,
        algorithms=("RS256",),
        leeway=60,
    )

    assert claims["sub"] == "subject-1"
    assert requests == 2


def test_second_unknown_kid_within_cooldown_does_not_refetch(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    requests = []
    _set_transport(
        monkeypatch,
        lambda request: (
            requests.append(request.url.path)
            or httpx.Response(200, json={"keys": [_rsa_jwk(key)]})
        ),
    )
    metadata = _metadata()

    for missing_kid in ("missing-one", "missing-two"):
        with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
            oidc_client.validate_id_token(
                _token(key, kid=missing_kid),
                metadata=metadata,
                client_id=CLIENT_ID,
                nonce=NONCE,
                algorithms=("RS256",),
                leeway=60,
            )
        assert exc.value.reason == "unknown_kid"

    assert len(requests) == 1


def test_cold_jwks_fetch_does_not_refetch_immediately_for_unknown_kid(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    requests = []
    _set_transport(
        monkeypatch,
        lambda request: (
            requests.append(request.url.path)
            or httpx.Response(200, json={"keys": [_rsa_jwk(key)]})
        ),
    )

    assert oidc_client._find_jwk(
        JWKS_URI,
        "rsa-key",
        issuer=ISSUER,
        sso_type="oidc",
    )["kid"] == "rsa-key"
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        oidc_client._find_jwk(
            JWKS_URI,
            "missing-key",
            issuer=ISSUER,
            sso_type="oidc",
        )

    assert exc.value.reason == "unknown_kid"
    assert requests == ["/jwks"]


def test_cold_jwks_fetch_is_single_flight_for_same_uri(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    request_count = 0
    request_lock = threading.Lock()
    barrier = threading.Barrier(5)

    def handler(_request):
        nonlocal request_count
        with request_lock:
            request_count += 1
        return httpx.Response(200, json={"keys": [_rsa_jwk(key)]})

    _set_transport(monkeypatch, handler)
    results = []
    errors = []

    def lookup():
        try:
            barrier.wait(timeout=2)
            results.append(
                oidc_client._find_jwk(
                    JWKS_URI,
                    "missing-key",
                    issuer=ISSUER,
                    sso_type="oidc",
                )
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=lookup) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert len(errors) == 5
    assert all(
        isinstance(error, oidc_client.OidcTokenInvalid)
        and error.reason == "unknown_kid"
        for error in errors
    )
    assert not results
    assert request_count == 1


def test_cached_jwks_lookup_for_other_uri_is_not_blocked_by_fetch(monkeypatch):
    key_a = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_b = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    uri_a = "https://issuer-a.example/jwks"
    uri_b = "https://issuer-b.example/jwks"
    fetch_started = threading.Event()
    release_fetch = threading.Event()
    lookup_finished = threading.Event()
    errors = []
    b_results = []

    def handler(request):
        if str(request.url) == uri_a:
            fetch_started.set()
            if not release_fetch.wait(timeout=3):
                raise AssertionError("test did not release blocked JWKS fetch")
            return httpx.Response(200, json={"keys": [_rsa_jwk(key_a, "key-a")]})
        return httpx.Response(200, json={"keys": [_rsa_jwk(key_b, "key-b")]})

    _set_transport(monkeypatch, handler)
    assert oidc_client._find_jwk(
        uri_b,
        "key-b",
        issuer="https://issuer-b.example",
        sso_type="oidc",
    )["kid"] == "key-b"

    def fetch_a():
        try:
            oidc_client._find_jwk(
                uri_a,
                "key-a",
                issuer="https://issuer-a.example",
                sso_type="oidc",
            )
        except Exception as exc:
            errors.append(exc)

    def lookup_b():
        try:
            b_results.append(
                oidc_client._find_jwk(
                    uri_b,
                    "key-b",
                    issuer="https://issuer-b.example",
                    sso_type="oidc",
                )
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            lookup_finished.set()

    fetch_thread = threading.Thread(target=fetch_a)
    fetch_thread.start()
    assert fetch_started.wait(timeout=2)
    lookup_thread = threading.Thread(target=lookup_b)
    lookup_thread.start()
    completed_without_waiting = lookup_finished.wait(timeout=0.5)
    release_fetch.set()
    fetch_thread.join(timeout=3)
    lookup_thread.join(timeout=3)

    assert completed_without_waiting
    assert not fetch_thread.is_alive()
    assert not lookup_thread.is_alive()
    assert not errors
    assert b_results == [_rsa_jwk(key_b, "key-b")]


def test_jwks_unavailable_fails_closed(monkeypatch):
    _set_transport(
        monkeypatch,
        lambda _request: httpx.Response(503, json={"error": "unavailable"}),
    )
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(oidc_client.OidcIdpUnavailable) as exc:
        oidc_client.validate_id_token(
            _token(key),
            metadata=_metadata(),
            client_id=CLIENT_ID,
            nonce=NONCE,
            algorithms=("RS256",),
            leeway=60,
        )
    assert exc.value.reason == "jwks_unavailable"


def test_oct_jwk_is_ignored(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    oct_key = {
        "kid": "rsa-key",
        "kty": "oct",
        "use": "sig",
        "k": "secret",
    }
    _set_transport(
        monkeypatch,
        lambda _request: httpx.Response(
            200,
            json={"keys": [oct_key, _rsa_jwk(key)]},
        ),
    )
    assert oidc_client.validate_id_token(
        _token(key),
        metadata=_metadata(),
        client_id=CLIENT_ID,
        nonce=NONCE,
        algorithms=("RS256",),
        leeway=60,
    )["sub"] == "subject-1"


def test_ec_signing_key_is_supported(monkeypatch):
    key = ec.generate_private_key(ec.SECP256R1())
    assert _validate(
        monkeypatch,
        key,
        _ec_jwk(key),
        token=_token(key, algorithm="ES256", kid="ec-key"),
        algorithms=("ES256",),
    )["sub"] == "subject-1"


def test_rs512_token_is_rejected_when_allow_list_is_rs256(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate(
            monkeypatch,
            key,
            _rsa_jwk(key, alg="RS512"),
            token=_token(key, algorithm="RS512"),
            algorithms=("RS256",),
        )
    assert exc.value.reason == "alg_not_allowed"


@pytest.mark.parametrize("algorithm", ["none", "HS256"])
def test_unsafe_token_algorithms_are_rejected(monkeypatch, algorithm):
    claims = _claims()
    if algorithm == "none":
        token = jwt.encode(claims, "", algorithm="none", headers={"kid": "rsa-key"})
    else:
        token = jwt.encode(claims, "not-a-provider-key", algorithm="HS256", headers={"kid": "rsa-key"})
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate(
            monkeypatch,
            key,
            _rsa_jwk(key),
            token=token,
            algorithms=("RS256",),
        )
    assert exc.value.reason == "alg_not_allowed"


def test_expired_token_within_leeway_is_accepted(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = int(datetime.now(timezone.utc).timestamp())
    claims = _claims(iat=now - 30, exp=now - 30)
    assert _validate(
        monkeypatch,
        key,
        _rsa_jwk(key),
        token=_token(key, claims=claims),
        leeway=60,
    )["sub"] == "subject-1"


def test_token_past_leeway_is_expired(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = int(datetime.now(timezone.utc).timestamp())
    claims = _claims(iat=now - 120, exp=now - 120)
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate(
            monkeypatch,
            key,
            _rsa_jwk(key),
            token=_token(key, claims=claims),
            leeway=60,
        )
    assert exc.value.reason == "expired"


def test_clock_skew_rejects_values_over_five_minutes(monkeypatch):
    monkeypatch.setenv("DATAFLOW_OIDC_CLOCK_SKEW_SEC", "301")
    with pytest.raises(oidc_client.OidcConfigError) as exc:
        oidc_client.clock_skew_leeway()
    assert exc.value.reason == "invalid_clock_skew"


@pytest.mark.parametrize("token_nonce", [None, "different-nonce"])
def test_nonce_is_mandatory_and_must_match(monkeypatch, token_nonce):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    claims = _claims()
    if token_nonce is None:
        claims.pop("nonce")
    else:
        claims["nonce"] = token_nonce
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate(
            monkeypatch,
            key,
            _rsa_jwk(key),
            token=_token(key, claims=claims),
        )
    assert exc.value.reason == "nonce_mismatch"


def test_multivalued_audience_requires_matching_azp(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    claims = _claims(aud=[CLIENT_ID, "other-client"], azp="other-client")
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate(
            monkeypatch,
            key,
            _rsa_jwk(key),
            token=_token(key, claims=claims),
        )
    assert exc.value.reason == "bad_audience"


def test_email_claim_requires_verified_true_by_default(monkeypatch):
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        oidc_client.email_from_claims(
            {"email": "alice@example.com", "email_verified": False},
            sso_type="oidc",
        )
    assert exc.value.reason == "no_verified_email"
    with pytest.raises(oidc_client.OidcTokenInvalid):
        oidc_client.email_from_claims({"email": "alice@example.com"}, sso_type="oidc")


def test_email_verification_can_be_disabled_but_false_stays_rejected(monkeypatch):
    monkeypatch.setenv("DATAFLOW_OIDC_REQUIRE_EMAIL_VERIFIED", "0")
    assert oidc_client.email_from_claims(
        {"email": "Alice@Example.com"},
        sso_type="oidc",
    ) == "alice@example.com"
    with pytest.raises(oidc_client.OidcTokenInvalid):
        oidc_client.email_from_claims(
            {"email": "alice@example.com", "email_verified": False},
            sso_type="oidc",
        )


def test_sub_only_claim_is_not_an_email():
    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        oidc_client.email_from_claims({"sub": "alice@example.com"}, sso_type="oidc")
    assert exc.value.reason == "no_verified_email"


def test_azure_uses_preferred_username_when_email_is_missing():
    assert oidc_client.email_from_claims(
        {"preferred_username": "Alice@Example.com", "sub": "not-an-email"},
        sso_type="azure_ad",
    ) == "alice@example.com"


def test_azure_email_claim_logs_name_without_email_value(caplog):
    email = "Alice@Example.com"
    caplog.set_level(logging.INFO, logger=oidc_client.__name__)

    assert oidc_client.email_from_claims(
        {"preferred_username": email},
        sso_type="azure_ad",
    ) == "alice@example.com"

    assert "preferred_username" in caplog.text
    assert email not in caplog.text


def test_oidc_algorithms_are_constrained_by_provider_metadata(monkeypatch):
    monkeypatch.setenv("DATAFLOW_OIDC_ALLOWED_ALGS", "RS256")
    assert oidc_client.allowed_algorithms(
        "oidc",
        _metadata(("RS256",)),
    ) == ("RS256",)
    with pytest.raises(oidc_client.OidcConfigError) as exc:
        oidc_client.allowed_algorithms(
            "oidc",
            _metadata(("ES256",)),
        )
    assert exc.value.reason == "no_common_alg"


def test_oidc_algorithms_reject_hmac(monkeypatch):
    monkeypatch.setenv("DATAFLOW_OIDC_ALLOWED_ALGS", "HS256")
    with pytest.raises(oidc_client.OidcConfigError) as exc:
        oidc_client.allowed_algorithms("oidc", _metadata())
    assert exc.value.reason == "alg_not_permitted"
