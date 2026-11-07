"""P1-1 — pending_signups + otp_challenges stores, and canonical folding.

Expired rows disappear; a second pending row for the same canonical email
replaces the first; a new challenge voids the previous one.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from services import signup_store
from services.email_canonical import canonical_email


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")


def test_canonical_email_foldings():
    assert canonical_email("Ada@Example.COM") == "ada@example.com"
    assert canonical_email("J.Doe+promo@gmail.com") == "jdoe@gmail.com"
    assert canonical_email("j.d.o.e@googlemail.com") == "jdoe@gmail.com"
    assert canonical_email("a+b@outlook.com") == "a@outlook.com"
    # domains without plus-addressing keep the tag
    assert canonical_email("a+b@corp.example") == "a+b@corp.example"
    # non-plus providers keep dots
    assert canonical_email("j.doe@corp.example") == "j.doe@corp.example"


def test_pending_row_replaces_same_canonical():
    signup_store.upsert_pending_signup(
        email="john@gmail.com", name="John", password_hash="h1")
    second = signup_store.upsert_pending_signup(
        email="J.O.H.N+promo@gmail.com", name="Johnny", password_hash="h2")
    row = signup_store.get_pending_signup(email="john@gmail.com")
    assert row["id"] == second["id"]
    assert row["password_hash"] == "h2"
    assert row["name"] == "Johnny"
    # exactly one row exists for the canonical key
    rows = signup_store.load_json_doc(
        signup_store._pending_path(), {"pending_signups": []})["pending_signups"]
    assert len(rows) == 1


def test_expired_pending_row_gone():
    signup_store.upsert_pending_signup(
        email="a@b.com", name="A", password_hash="h",
        ttl=timedelta(seconds=-1))
    assert signup_store.get_pending_signup(email="a@b.com") is None
    assert signup_store.purge_expired_pending() == 1
    assert signup_store.purge_expired_pending() == 0


def test_delete_pending():
    row = signup_store.upsert_pending_signup(
        email="a@b.com", name="A", password_hash="h")
    signup_store.delete_pending_signup(pending_id=row["id"])
    assert signup_store.get_pending_signup(email="a@b.com") is None


def test_new_challenge_voids_previous():
    first = signup_store.create_challenge(
        purpose="signup", subject_id="psu_1", code_hmac="h1",
        ttl=timedelta(minutes=10))
    second = signup_store.create_challenge(
        purpose="signup", subject_id="psu_1", code_hmac="h2",
        ttl=timedelta(minutes=10))
    # first is consumed/void, second is the active one
    assert signup_store.get_challenge(first["id"])["consumed_at"]
    active = signup_store.active_challenge("psu_1", "signup")
    assert active["id"] == second["id"]
    assert active["code_hmac"] == "h2"


def test_challenge_defaults_and_purpose_validation():
    ch = signup_store.create_challenge(
        purpose="reset", subject_id="u@x.com", code_hmac="h",
        ttl=timedelta(minutes=10))
    assert ch["attempts"] == 0 and ch["max_attempts"] == 5
    assert ch["sent_count"] == 1 and ch["consumed_at"] is None
    with pytest.raises(ValueError):
        signup_store.create_challenge(
            purpose="bogus", subject_id="x", code_hmac="h",
            ttl=timedelta(minutes=1))


def test_expired_challenge_not_returned():
    ch = signup_store.create_challenge(
        purpose="signup", subject_id="x", code_hmac="h",
        ttl=timedelta(seconds=-1))
    assert signup_store.get_challenge(ch["id"]) is None
    assert signup_store.active_challenge("x", "signup") is None


def test_attempt_burn_and_resend_refresh():
    ch = signup_store.create_challenge(
        purpose="signup", subject_id="x", code_hmac="h",
        ttl=timedelta(minutes=10))
    row = signup_store.record_attempt(ch["id"])
    assert row["attempts"] == 1 and row["consumed_at"] is None
    resent = signup_store.mark_sent(ch["id"], code_hmac="h2", ttl=timedelta(minutes=10))
    assert resent["sent_count"] == 2
    assert resent["attempts"] == 0
    assert resent["code_hmac"] == "h2"
