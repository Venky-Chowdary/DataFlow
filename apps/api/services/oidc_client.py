"""Strict OIDC discovery, signing-key retrieval, and ID-token validation."""

from __future__ import annotations

import hmac
import logging
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlsplit
from typing import Any

import httpx
import jwt
from jwt.exceptions import (
    ExpiredSignatureError,
    InvalidAudienceError,
    InvalidIssuerError,
    InvalidSignatureError,
    PyJWTError,
)

from services.brand_env import getenv_brand
from services.platform_config import is_production

logger = logging.getLogger(__name__)

_ALLOWED_ALGORITHMS = frozenset(
    {
        "RS256",
        "RS384",
        "RS512",
        "PS256",
        "PS384",
        "PS512",
        "ES256",
        "ES384",
        "ES512",
    }
)
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_CACHE_LOCK = threading.Lock()
_METADATA_CACHE: dict[str, tuple[float, "OidcProviderMetadata"]] = {}
_JWKS_LOCKS_REGISTRY_LOCK = threading.Lock()
_JWKS_URI_LOCKS: dict[str, threading.Lock] = {}
_JWKS_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_JWKS_REFRESHED: dict[str, float] = {}


class OidcError(Exception):
    """Base error with a short client-safe reason code."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class OidcConfigError(OidcError):
    """The configured OIDC provider metadata or policy is invalid."""


class OidcIdpUnavailable(OidcError):
    """The identity provider could not be reached or returned a server error."""


class OidcTokenInvalid(OidcError):
    """The ID token or its claims failed validation."""


@dataclass(frozen=True)
class OidcProviderMetadata:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    id_token_signing_alg_values_supported: tuple[str, ...]


def _http_client() -> httpx.Client:
    """Create the shared HTTP client shape; tests may inject a MockTransport."""
    return httpx.Client(timeout=10.0)


def _cache_ttl(name: str, default: int) -> int:
    raw = getenv_brand(name, str(default))
    try:
        ttl = int(raw or default)
    except (TypeError, ValueError) as exc:
        raise OidcConfigError("invalid_cache_ttl") from exc
    if ttl < 0:
        raise OidcConfigError("invalid_cache_ttl")
    return ttl


def _normalize_issuer(value: str) -> str:
    return value.rstrip("/")


def _validate_url(value: Any, issuer: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OidcConfigError("missing_endpoint")
    parsed = urlsplit(value)
    host = (parsed.hostname or "").lower()
    allow_local_http = host in _LOCAL_HOSTS and not is_production()
    if not parsed.netloc or not host:
        raise OidcConfigError("invalid_endpoint")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and allow_local_http
    ):
        logger.warning(
            "OIDC endpoint rejected (issuer=%s reason=insecure_endpoint)",
            issuer,
        )
        raise OidcConfigError("insecure_endpoint")
    return value


def discover(issuer: str) -> OidcProviderMetadata:
    """Discover and validate the provider metadata for a configured issuer."""
    issuer = (issuer or "").strip()
    if not issuer:
        raise OidcConfigError("missing_issuer")
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _METADATA_CACHE.get(issuer)
        if cached and cached[0] > now:
            return cached[1]

    discovery_url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
    try:
        with _http_client() as client:
            response = client.get(discovery_url)
    except httpx.HTTPError as exc:
        logger.warning(
            "OIDC discovery failed (issuer=%s reason=discovery_failed)",
            issuer,
        )
        raise OidcIdpUnavailable("discovery_failed") from exc
    if response.status_code >= 500:
        logger.warning(
            "OIDC discovery failed (issuer=%s reason=discovery_failed)",
            issuer,
        )
        raise OidcIdpUnavailable("discovery_failed")
    if response.status_code >= 400:
        raise OidcConfigError("discovery_rejected")
    try:
        document = response.json()
    except (ValueError, TypeError) as exc:
        raise OidcConfigError("bad_discovery_document") from exc
    if not isinstance(document, dict):
        raise OidcConfigError("bad_discovery_document")

    discovered_issuer = document.get("issuer")
    if not isinstance(discovered_issuer, str):
        raise OidcConfigError("bad_discovery_document")
    if _normalize_issuer(discovered_issuer) != _normalize_issuer(issuer):
        logger.warning(
            "OIDC issuer rejected (issuer=%s reason=issuer_mismatch)",
            issuer,
        )
        raise OidcConfigError("issuer_mismatch")

    _validate_url(issuer, issuer)
    authorization_endpoint = _validate_url(
        document.get("authorization_endpoint"),
        issuer,
    )
    token_endpoint = _validate_url(document.get("token_endpoint"), issuer)
    jwks_uri = _validate_url(document.get("jwks_uri"), issuer)
    raw_algorithms = document.get("id_token_signing_alg_values_supported", [])
    if not isinstance(raw_algorithms, list) or any(
        not isinstance(algorithm, str) for algorithm in raw_algorithms
    ):
        raise OidcConfigError("bad_discovery_document")
    metadata = OidcProviderMetadata(
        issuer=discovered_issuer,
        authorization_endpoint=authorization_endpoint,
        token_endpoint=token_endpoint,
        jwks_uri=jwks_uri,
        id_token_signing_alg_values_supported=tuple(raw_algorithms),
    )
    expires_at = time.monotonic() + _cache_ttl("OIDC_METADATA_TTL_SEC", 3600)
    with _CACHE_LOCK:
        _METADATA_CACHE[issuer] = (expires_at, metadata)
    return metadata


def _jwks_ttl() -> int:
    return _cache_ttl("OIDC_JWKS_TTL_SEC", 3600)


def _jwks_refresh_cooldown() -> int:
    return _cache_ttl("OIDC_JWKS_REFRESH_COOLDOWN_SEC", 60)


def _jwks_lock(jwks_uri: str) -> threading.Lock:
    with _JWKS_LOCKS_REGISTRY_LOCK:
        lock = _JWKS_URI_LOCKS.get(jwks_uri)
        if lock is None:
            lock = threading.Lock()
            _JWKS_URI_LOCKS[jwks_uri] = lock
        return lock


def _fetch_jwks(jwks_uri: str, issuer: str, sso_type: str) -> list[dict[str, Any]]:
    try:
        with _http_client() as client:
            response = client.get(jwks_uri)
    except httpx.HTTPError as exc:
        logger.warning(
            "OIDC signing keys unavailable (sso_type=%s issuer=%s reason=jwks_unavailable)",
            sso_type,
            issuer,
        )
        raise OidcIdpUnavailable("jwks_unavailable") from exc
    if response.status_code >= 400:
        logger.warning(
            "OIDC signing keys unavailable (sso_type=%s issuer=%s reason=jwks_unavailable)",
            sso_type,
            issuer,
        )
        raise OidcIdpUnavailable("jwks_unavailable")
    try:
        document = response.json()
    except (ValueError, TypeError) as exc:
        raise OidcIdpUnavailable("jwks_unavailable") from exc
    if not isinstance(document, dict) or not isinstance(document.get("keys"), list):
        raise OidcIdpUnavailable("jwks_unavailable")
    keys = [
        key
        for key in document["keys"]
        if isinstance(key, dict)
        and key.get("kty") in ("RSA", "EC")
        and key.get("use") in (None, "sig")
    ]
    if not keys:
        raise OidcIdpUnavailable("jwks_unavailable")
    return keys


def _find_jwk(
    jwks_uri: str,
    kid: str,
    *,
    issuer: str,
    sso_type: str,
) -> dict[str, Any]:
    with _jwks_lock(jwks_uri):
        now = time.monotonic()
        cached = _JWKS_CACHE.get(jwks_uri)
        if cached and cached[0] > now:
            keys = cached[1]
        else:
            keys = _fetch_jwks(jwks_uri, issuer, sso_type)
            refreshed_at = time.monotonic()
            _JWKS_CACHE[jwks_uri] = (
                refreshed_at + _jwks_ttl(),
                keys,
            )
            _JWKS_REFRESHED[jwks_uri] = refreshed_at
        match = next((key for key in keys if key.get("kid") == kid), None)
        if match:
            return match

        now = time.monotonic()
        cooldown = _jwks_refresh_cooldown()
        last_refreshed = _JWKS_REFRESHED.get(jwks_uri)
        if last_refreshed is not None and now - last_refreshed < cooldown:
            logger.warning(
                "OIDC signing key id not found (sso_type=%s issuer=%s kid=%s reason=unknown_kid)",
                sso_type,
                issuer,
                kid,
            )
            raise OidcTokenInvalid("unknown_kid")
        _JWKS_REFRESHED[jwks_uri] = now
        keys = _fetch_jwks(jwks_uri, issuer, sso_type)
        _JWKS_CACHE[jwks_uri] = (
            time.monotonic() + _jwks_ttl(),
            keys,
        )
        match = next((key for key in keys if key.get("kid") == kid), None)
        if match:
            return match
    logger.warning(
        "OIDC signing key id not found (sso_type=%s issuer=%s kid=%s reason=unknown_kid)",
        sso_type,
        issuer,
        kid,
    )
    raise OidcTokenInvalid("unknown_kid")


def reset_caches() -> None:
    """Clear discovery, JWKS, and rotation-refresh caches."""
    with _CACHE_LOCK:
        _METADATA_CACHE.clear()
    with _JWKS_LOCKS_REGISTRY_LOCK:
        locks = tuple(_JWKS_URI_LOCKS.values())
    for lock in locks:
        lock.acquire()
    try:
        _JWKS_CACHE.clear()
        _JWKS_REFRESHED.clear()
    finally:
        for lock in reversed(locks):
            lock.release()


def allowed_algorithms(
    sso_type: str,
    metadata: OidcProviderMetadata,
) -> tuple[str, ...]:
    """Return the configured signing algorithms constrained by the provider."""
    if sso_type == "azure_ad":
        configured = ("RS256",)
    elif sso_type == "oidc":
        raw = getenv_brand("OIDC_ALLOWED_ALGS", "RS256") or ""
        configured = tuple(algorithm.strip() for algorithm in raw.split(","))
        if not configured or any(
            algorithm not in _ALLOWED_ALGORITHMS for algorithm in configured
        ):
            raise OidcConfigError("alg_not_permitted")
    else:
        raise OidcConfigError("unsupported_sso_type")
    if metadata.id_token_signing_alg_values_supported:
        supported = set(metadata.id_token_signing_alg_values_supported)
        configured = tuple(
            algorithm for algorithm in configured if algorithm in supported
        )
    if not configured:
        raise OidcConfigError("no_common_alg")
    return configured


def _token_error_reason(exc: PyJWTError) -> str:
    if isinstance(exc, ExpiredSignatureError):
        return "expired"
    if isinstance(exc, InvalidSignatureError):
        return "bad_signature"
    if isinstance(exc, InvalidAudienceError):
        return "bad_audience"
    if isinstance(exc, InvalidIssuerError):
        return "bad_issuer"
    return "malformed"


def validate_id_token(
    id_token: str,
    *,
    metadata: OidcProviderMetadata,
    client_id: str,
    nonce: str,
    algorithms: tuple[str, ...],
    leeway: int,
    sso_type: str = "oidc",
) -> dict[str, Any]:
    """Validate an ID token against its selected provider key and OIDC claims."""
    try:
        header = jwt.get_unverified_header(id_token)
    except (PyJWTError, TypeError, ValueError) as exc:
        raise OidcTokenInvalid("malformed") from exc
    algorithm = header.get("alg")
    if not isinstance(algorithm, str) or algorithm not in algorithms:
        logger.warning(
            "OIDC token algorithm rejected (sso_type=%s issuer=%s kid=%s reason=alg_not_allowed)",
            sso_type,
            metadata.issuer,
            str(header.get("kid") or "missing"),
        )
        raise OidcTokenInvalid("alg_not_allowed")
    kid = header.get("kid")
    if not isinstance(kid, str) or not kid:
        raise OidcTokenInvalid("unknown_kid")
    jwk = _find_jwk(
        metadata.jwks_uri,
        kid,
        issuer=metadata.issuer,
        sso_type=sso_type,
    )
    if jwk.get("alg") is not None and jwk["alg"] != algorithm:
        raise OidcTokenInvalid("alg_not_allowed")
    try:
        key = jwt.PyJWK(jwk, algorithm=algorithm)
        claims = jwt.decode(
            id_token,
            key,
            algorithms=list(algorithms),
            audience=client_id,
            issuer=metadata.issuer,
            leeway=leeway,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except PyJWTError as exc:
        raise OidcTokenInvalid(_token_error_reason(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise OidcTokenInvalid("malformed") from exc

    audience = claims.get("aud")
    if isinstance(audience, list) and len(audience) > 1:
        if claims.get("azp") != client_id:
            raise OidcTokenInvalid("bad_audience")
    claim_nonce = claims.get("nonce")
    if (
        not isinstance(nonce, str)
        or not nonce
        or not isinstance(claim_nonce, str)
        or not hmac.compare_digest(claim_nonce, nonce)
    ):
        raise OidcTokenInvalid("nonce_mismatch")
    return claims


def clock_skew_leeway() -> int:
    """Return a validated OIDC clock-skew allowance in seconds."""
    raw = getenv_brand("OIDC_CLOCK_SKEW_SEC", "60")
    try:
        leeway = int(raw or "60")
    except (TypeError, ValueError) as exc:
        raise OidcConfigError("invalid_clock_skew") from exc
    if not 0 <= leeway <= 300:
        raise OidcConfigError("invalid_clock_skew")
    return leeway


def email_from_claims(claims: dict[str, Any], *, sso_type: str) -> str:
    """Extract a verified provider email, normalized for account matching."""
    candidates: tuple[tuple[str, Any], ...]
    if sso_type == "oidc":
        email = claims.get("email")
        if not isinstance(email, str) or "@" not in email:
            raise OidcTokenInvalid("no_verified_email")
        verified = claims.get("email_verified")
        require_verified = (getenv_brand("OIDC_REQUIRE_EMAIL_VERIFIED", "1") or "1") != "0"
        if verified is False or (require_verified and verified is not True):
            raise OidcTokenInvalid("no_verified_email")
        candidates = (("email", email),)
    elif sso_type == "azure_ad":
        candidates = (
            ("email", claims.get("email")),
            ("preferred_username", claims.get("preferred_username")),
            ("upn", claims.get("upn")),
        )
    else:
        raise OidcConfigError("unsupported_sso_type")
    for claim_name, candidate in candidates:
        if isinstance(candidate, str) and "@" in candidate:
            if sso_type == "azure_ad":
                logger.info(
                    "Entra email claim selected (sso_type=azure_ad claim=%s)",
                    claim_name,
                )
            return candidate.strip().lower()
    raise OidcTokenInvalid("no_verified_email")
