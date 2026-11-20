"""D323: signup with an existing account's address — honest copy, no enumeration oracle.

QA (Nov 10): signup with an existing address answered 202 with a decoy
challenge, the inbox held only "You already have a Datawrap account", and
Resend answered 410 code_expired ("Couldn't resend the code").

That 410 was more than bad copy. The decoy id (``otc_<uuid>``) was never
stored, so every follow-up call told an attacker the address has an account
(OWASP Authentication Cheat Sheet, "Authentication and Error Messages" /
account enumeration: every response must be the same whether or not the
account exists):

- resend straight away: a real challenge 429 resend_too_soon, the decoy 410;
- a wrong code: a real challenge 400 invalid_code with attempts left, the
  decoy 410.

Now the existing-account path stores a real challenge whose code is never
mailed. Resend and verify answer exactly as for a new address; a resend
mails the owner the sign-in notice again (never a code), and the code
screen's copy covers both cases (web, ``SignupPage``).
"""

from __future__ import annotations

import pytest

from services import signup_store, user_store
from tests.helpers.mail_wait import await_outbox
from tests.test_resend import _age_last_sent, _client, _isolate, _signup  # noqa: F401 - autouse fixture


@pytest.fixture
def pair():
    user_store.create_user(email="owner@example.com", password="violet-harbor-quartz-2026")
    client = _client()
    return client, _signup(client, "owner@example.com"), _signup(client, "newcomer@example.com")


def _shape(resp):
    body = resp.json()
    err = body.get("error") or {}
    return resp.status_code, err.get("code"), sorted(k for k in (err or body) if k not in {"message"})


def test_resend_straight_away_answers_the_same_for_both(pair):
    client, existing, new = pair
    a = client.post("/api/v1/auth/resend", json={"challenge_id": existing})
    b = client.post("/api/v1/auth/resend", json={"challenge_id": new})
    assert _shape(a) == _shape(b) == (429, "resend_too_soon", ["code", "retry_after_sec"])


def test_a_wrong_code_answers_the_same_for_both(pair):
    client, existing, new = pair
    a = client.post("/api/v1/auth/verify-email", json={"challenge_id": existing, "code": "000000"})
    b = client.post("/api/v1/auth/verify-email", json={"challenge_id": new, "code": "000000"})
    assert a.status_code == b.status_code == 400, (a.text, b.text)
    assert a.json()["error"]["code"] == b.json()["error"]["code"] == "invalid_code"
    assert a.json()["error"].get("attempts_remaining") == b.json()["error"].get("attempts_remaining")


def test_a_resend_after_the_cooldown_mails_the_owner_the_notice_never_a_code(pair):
    client, existing, new = pair
    notices_before = len(await_outbox("owner@example.com"))
    for cid in (existing, new):
        _age_last_sent(cid, 61)
    a = client.post("/api/v1/auth/resend", json={"challenge_id": existing})
    b = client.post("/api/v1/auth/resend", json={"challenge_id": new})
    assert a.status_code == b.status_code == 202, (a.text, b.text)
    assert set(a.json()) == set(b.json())
    mails = await_outbox("owner@example.com", min_count=notices_before + 1)
    assert len(mails) == notices_before + 1
    assert all("already have a datawrap account" in m.subject.lower() for m in mails)
    # The owner's inbox never receives a code it could not use.
    assert not any("verification" in m.subject.lower() for m in mails)
    # The stored challenge belongs to no pending signup: it can never create
    # a second account for the address.
    row = signup_store.get_challenge(existing)
    assert row is not None and signup_store.get_pending_signup(pending_id=row["subject_id"]) is None
