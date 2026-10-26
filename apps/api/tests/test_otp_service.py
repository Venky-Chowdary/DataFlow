"""P1-2 — OTP service: generation, HMAC storage, constant-time verify, burn.

6th attempt fails even with the right code; codes are never stored raw.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from services import otp_service, signup_store


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")


def test_code_format_and_uniqueness():
    codes = {otp_service.generate_code() for _ in range(200)}
    assert all(len(c) == 6 and c.isdigit() for c in codes)
    assert len(codes) > 150  # uniform draw


def test_issue_stores_only_hmac():
    challenge, code = otp_service.issue("psu_1", "signup")
    row = signup_store.get_challenge(challenge["id"])
    assert row["code_hmac"] != code
    assert row["code_hmac"] == otp_service.code_hmac(challenge["id"], "signup", code)
    assert len(row["code_hmac"]) == 64  # sha256 hex
    # raw code appears nowhere in the stored document
    assert code not in str(row)


def test_verify_ok_consumes():
    challenge, code = otp_service.issue("psu_1", "signup")
    assert otp_service.verify(challenge["id"], code) == "ok"
    # second use rejected — single use
    assert otp_service.verify(challenge["id"], code) == "consumed"


def test_wrong_code_counts_attempts():
    challenge, code = otp_service.issue("psu_1", "signup")
    assert otp_service.verify(challenge["id"], "000000" if code != "000000" else "111111") == "invalid"
    assert signup_store.get_challenge(challenge["id"])["attempts"] == 1


def test_sixth_attempt_fails_even_with_right_code():
    challenge, code = otp_service.issue("psu_1", "signup")
    wrong = "999999" if code != "999999" else "888888"
    for i in range(5):
        assert otp_service.verify(challenge["id"], wrong) == "invalid", i
    # burned after 5 — right code now loses
    assert otp_service.verify(challenge["id"], code) == "consumed"
    row = signup_store.get_challenge(challenge["id"])
    assert row["consumed_at"]
    assert row["attempts"] == 5


def test_new_issue_voids_old_challenge():
    first, code1 = otp_service.issue("psu_1", "signup")
    second, code2 = otp_service.issue("psu_1", "signup")
    assert first["id"] != second["id"]
    assert otp_service.verify(first["id"], code1) == "consumed"
    assert otp_service.verify(second["id"], code2) == "ok"


def test_expired_challenge_not_found():
    ch = signup_store.create_challenge(
        purpose="signup", subject_id="x", code_hmac="h", ttl=timedelta(seconds=-1))
    assert otp_service.verify(ch["id"], "123456") == "not_found"


def test_reissue_refreshes():
    challenge, _ = otp_service.issue("psu_1", "signup")
    updated, code2 = otp_service.reissue(challenge["id"])
    assert updated["sent_count"] == 2
    assert otp_service.verify(challenge["id"], code2) == "ok"


def test_pepper_changes_hmac(monkeypatch):
    h1 = otp_service.code_hmac("c1", "signup", "123456")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "other-pepper")
    h2 = otp_service.code_hmac("c1", "signup", "123456")
    assert h1 != h2
