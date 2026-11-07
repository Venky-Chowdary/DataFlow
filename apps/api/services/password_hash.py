"""One password hasher for every account store.

Login (``auth_service``) and account creation (``user_store``) must agree on the
hash format, and ``auth_service`` cannot import the store it authenticates
against without a cycle, so the primitive lives on its own.

Algorithms (P0-4): Argon2id (19 MiB, t=2, p=1) is the hasher for new
passwords once ``DATAWRAP_SIGNUP_ENABLED`` is on; bcrypt remains readable
forever and rows are transparently rehashed on the next successful login
(``needs_rehash`` + ``user_store.update_password_hash`` in authenticate).
Flag off: identical bcrypt behaviour to before — nothing is rehashed.
"""

from __future__ import annotations

import hashlib
import hmac
import re

import bcrypt

from services.brand_env import getenv_brand
from services.platform_config import is_production
from services.signup_config import signup_enabled

# Legacy unsalted SHA-256 hashes are exactly 64 hex characters.
LEGACY_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_BCRYPT_ROUNDS = 12

# Argon2id parameters from the handover: 19 MiB memory, t=2, p=1.
_ARGON2_MEMORY_KIB = 19 * 1024
_ARGON2_TIME_COST = 2
_ARGON2_PARALLELISM = 1


def _argon2_hasher():
    from argon2 import PasswordHasher

    return PasswordHasher(
        time_cost=_ARGON2_TIME_COST,
        memory_cost=_ARGON2_MEMORY_KIB,
        parallelism=_ARGON2_PARALLELISM,
    )


def is_argon2_hash(password_hash: str) -> bool:
    return (password_hash or "").startswith("$argon2")


def is_bcrypt_hash(password_hash: str) -> bool:
    return (password_hash or "").startswith(("$2a$", "$2b$", "$2y$"))


def algo_for_hash(password_hash: str) -> str:
    if is_argon2_hash(password_hash):
        return "argon2id"
    if is_bcrypt_hash(password_hash):
        return "bcrypt"
    if is_legacy_hash(password_hash):
        return "sha256-legacy"
    return "unknown"


def hash_password(password: str) -> str:
    """Hash a password. Argon2id once signup is enabled; bcrypt when the flag
    is off so flag-off production stays byte-for-byte."""
    if signup_enabled():
        return _argon2_hasher().hash(password)
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)).decode("utf-8")


def hash_password_argon2(password: str) -> str:
    """Always argon2id — the signup flow's explicit choice (flag-independent:
    a pending signup must not change algorithm if the flag flips mid-flow)."""
    return _argon2_hasher().hash(password)


def needs_rehash(password_hash: str) -> bool:
    """True when the stored hash should be upgraded to argon2id on next login."""
    if not signup_enabled():
        return False
    h = password_hash or ""
    if is_argon2_hash(h):
        return False
    return is_bcrypt_hash(h) or is_legacy_hash(h)


def is_legacy_hash(password_hash: str) -> bool:
    return bool(LEGACY_SHA256_RE.match(password_hash or ""))


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a stored hash.

    Accepts argon2id and bcrypt always (existing rows must keep working in
    every flag state). Legacy unsalted SHA-256 is still accepted in
    development for backwards compatibility, but it is rejected in production
    because it is not suitable for regulated deployments.
    """
    if not password_hash:
        return False
    if is_argon2_hash(password_hash):
        try:
            return bool(_argon2_hasher().verify(password_hash, password))
        except Exception:  # argon2 raises on malformed hash / mismatch
            return False
    if is_legacy_hash(password_hash):
        if is_production():
            return False
        expected = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(password_hash, expected)
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False
