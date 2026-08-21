"""One password hasher for every account store.

Login (``auth_service``) and account creation (``user_store``) must agree on the
hash format, and ``auth_service`` cannot import the store it authenticates
against without a cycle, so the primitive lives on its own.
"""

from __future__ import annotations

import hashlib
import hmac
import re

import bcrypt

from services.platform_config import is_production

# Legacy unsalted SHA-256 hashes are exactly 64 hex characters.
LEGACY_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_BCRYPT_ROUNDS = 12


def hash_password(password: str) -> str:
    """Hash a password with bcrypt (adaptive, salted, slow)."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)).decode("utf-8")


def is_legacy_hash(password_hash: str) -> bool:
    return bool(LEGACY_SHA256_RE.match(password_hash or ""))


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt hash.

    Legacy unsalted SHA-256 is still accepted in development for backwards
    compatibility, but it is rejected in production because it is not suitable
    for regulated deployments.
    """
    if not password_hash:
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
