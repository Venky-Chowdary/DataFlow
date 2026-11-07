"""Time-based one-time passwords: RFC 4226 (HOTP) and RFC 6238 (TOTP).

No crypto beyond what the RFCs specify: HMAC-SHA1 over the 8-byte big-endian
counter, dynamic truncation, modulo 10^digits. ``pyotp`` is not a dependency
of this API, so the ~20 lines the RFCs define live here, checked against the
RFC 4226 Appendix D and RFC 6238 Appendix B test vectors
(tests/test_totp_rfc6238.py).

Verification compares every candidate in the drift window with
``hmac.compare_digest`` and never returns early, so response time does not
say which step (if any) matched.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
from urllib.parse import quote, urlencode

#: RFC 6238 default time step (seconds) and the code length authenticator apps show.
STEP_SECONDS = 30
DIGITS = 6
#: ±1 step accepted for clock drift between the server and the phone.
DRIFT_STEPS = 1
#: 160-bit shared secret — the HMAC-SHA1 block-size recommendation of RFC 4226 §4.
SECRET_BYTES = 20

_DIGESTS = {"SHA1": hashlib.sha1, "SHA256": hashlib.sha256, "SHA512": hashlib.sha512}


def generate_secret() -> str:
    """A new random shared secret, base32 without padding (what apps accept)."""
    return base64.b32encode(secrets.token_bytes(SECRET_BYTES)).decode("ascii").rstrip("=")


def secret_bytes(secret_b32: str) -> bytes:
    """Decode a base32 secret, tolerating lower case, spaces and missing padding."""
    text = "".join((secret_b32 or "").split()).upper()
    text += "=" * (-len(text) % 8)
    return base64.b32decode(text, casefold=True)


def hotp(key: bytes, counter: int, *, digits: int = DIGITS, algorithm: str = "SHA1") -> str:
    """RFC 4226 §5.3: HOTP(K, C) = Truncate(HMAC(K, C)) mod 10^digits."""
    digest = hmac.new(key, struct.pack(">Q", int(counter)), _DIGESTS[algorithm]).digest()
    offset = digest[-1] & 0x0F
    binary = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(binary % (10**digits)).zfill(digits)


def time_step(at: float | None = None, *, step: int = STEP_SECONDS) -> int:
    """RFC 6238 §4.2: T = floor((now - T0) / X) with T0 = 0."""
    return int((time.time() if at is None else at) // step)


def totp(
    key: bytes,
    at: float | None = None,
    *,
    step: int = STEP_SECONDS,
    digits: int = DIGITS,
    algorithm: str = "SHA1",
) -> str:
    return hotp(key, time_step(at, step=step), digits=digits, algorithm=algorithm)


def normalize_code(code: str) -> str:
    """Digits only: authenticator apps display ``123 456``; users paste it so."""
    return "".join(ch for ch in str(code or "") if not ch.isspace() and ch != "-")


def matching_step(
    key: bytes,
    code: str,
    *,
    at: float | None = None,
    drift: int = DRIFT_STEPS,
    step: int = STEP_SECONDS,
    digits: int = DIGITS,
) -> int | None:
    """The time step whose code equals ``code`` within ±``drift``, else ``None``.

    Every candidate is computed and compared (constant time); the caller
    decides about replay with the returned step.
    """
    supplied = normalize_code(code)
    well_formed = len(supplied) == digits and supplied.isdigit()
    # Compare against a same-length placeholder for malformed input so the
    # work done does not depend on what was typed.
    probe = supplied if well_formed else "0" * digits
    current = time_step(at, step=step)
    found: int | None = None
    for candidate in range(current - drift, current + drift + 1):
        expected = hotp(key, candidate, digits=digits)
        if hmac.compare_digest(expected.encode("ascii"), probe.encode("ascii")) and found is None:
            found = candidate
    return found if well_formed else None


def provisioning_uri(secret_b32: str, *, account: str, issuer: str) -> str:
    """``otpauth://totp/Issuer:account?secret=…&issuer=…`` (Key Uri Format)."""
    label = quote(f"{issuer}:{account}", safe="@:")
    query = urlencode(
        {
            "secret": secret_b32,
            "issuer": issuer,
            "algorithm": "SHA1",
            "digits": DIGITS,
            "period": STEP_SECONDS,
        }
    )
    return f"otpauth://totp/{label}?{query}"


__all__ = [
    "DIGITS",
    "DRIFT_STEPS",
    "STEP_SECONDS",
    "generate_secret",
    "hotp",
    "matching_step",
    "normalize_code",
    "provisioning_uri",
    "secret_bytes",
    "time_step",
    "totp",
]
