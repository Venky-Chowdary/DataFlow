"""TOTP/HOTP against the published test vectors (QA S7, round 11).

RFC 4226 Appendix D (HOTP, SHA-1, 6 digits) and RFC 6238 Appendix B (TOTP,
8 digits, SHA-1 / SHA-256 / SHA-512 seeds), plus the verifier's drift window
and its refusal of malformed input. Fails on the base: services.totp does not
exist there.
"""

from __future__ import annotations

import pytest

from services import totp

RFC4226_SECRET = b"12345678901234567890"
RFC4226_HOTP = ["755224", "287082", "359152", "969429", "338314",
                "254676", "287922", "162583", "399871", "520489"]

SEEDS = {
    "SHA1": b"12345678901234567890",
    "SHA256": b"12345678901234567890123456789012",
    "SHA512": b"1234567890" * 6 + b"1234",
}
RFC6238 = [
    (59, "SHA1", "94287082"), (59, "SHA256", "46119246"), (59, "SHA512", "90693936"),
    (1111111109, "SHA1", "07081804"), (1111111109, "SHA256", "68084774"), (1111111109, "SHA512", "25091201"),
    (1111111111, "SHA1", "14050471"), (1111111111, "SHA256", "67062674"), (1111111111, "SHA512", "99943326"),
    (1234567890, "SHA1", "89005924"), (1234567890, "SHA256", "91819424"), (1234567890, "SHA512", "93441116"),
    (2000000000, "SHA1", "69279037"), (2000000000, "SHA256", "90698825"), (2000000000, "SHA512", "38618901"),
    (20000000000, "SHA1", "65353130"), (20000000000, "SHA256", "77737706"), (20000000000, "SHA512", "47863826"),
]


@pytest.mark.parametrize("counter,expected", list(enumerate(RFC4226_HOTP)))
def test_hotp_rfc4226_appendix_d(counter, expected):
    assert totp.hotp(RFC4226_SECRET, counter) == expected


@pytest.mark.parametrize("at,algorithm,expected", RFC6238)
def test_totp_rfc6238_appendix_b(at, algorithm, expected):
    assert totp.totp(SEEDS[algorithm], at, digits=8, algorithm=algorithm) == expected


def test_time_step_is_floor_of_thirty_seconds():
    assert totp.time_step(59) == 1
    assert totp.time_step(1111111109) == 0x23523EC
    assert totp.time_step(20000000000) == 0x27BC86AA


def test_verifier_accepts_exactly_one_step_of_drift():
    key = totp.secret_bytes(totp.generate_secret())
    now = 1_700_000_015.0
    step = totp.time_step(now)
    for delta in (-1, 0, 1):
        code = totp.hotp(key, step + delta)
        assert totp.matching_step(key, code, at=now) == step + delta
    for delta in (-2, 2):
        code = totp.hotp(key, step + delta)
        if code not in {totp.hotp(key, step + d) for d in (-1, 0, 1)}:
            assert totp.matching_step(key, code, at=now) is None


@pytest.mark.parametrize("bad", ["", "12345", "1234567", "abcdef", "12 34 5", None])
def test_verifier_refuses_malformed_codes(bad):
    key = totp.secret_bytes(totp.generate_secret())
    assert totp.matching_step(key, bad, at=1_700_000_000.0) is None


def test_code_with_display_spacing_is_accepted():
    key = totp.secret_bytes(totp.generate_secret())
    now = 1_700_000_000.0
    code = totp.totp(key, now)
    assert totp.matching_step(key, f"{code[:3]} {code[3:]}", at=now) == totp.time_step(now)


def test_secret_is_160_bit_base32_and_uri_is_key_uri_format():
    secret = totp.generate_secret()
    assert len(totp.secret_bytes(secret)) == 20
    assert totp.secret_bytes(secret.lower()) == totp.secret_bytes(secret)
    uri = totp.provisioning_uri(secret, account="ana@acme.test", issuer="Datawrap")
    assert uri.startswith("otpauth://totp/Datawrap:ana@acme.test?")
    assert f"secret={secret}" in uri and "issuer=Datawrap" in uri
    assert "digits=6" in uri and "period=30" in uri and "algorithm=SHA1" in uri


def test_secrets_are_not_reused():
    assert len({totp.generate_secret() for _ in range(50)}) == 50
