"""D340: the signup door refuses a breached password through the real route.

``test_d340_breached_password_policy`` proves the shared policy function; this
proves ``POST /api/v1/auth/signup`` calls it before anything is stored or
mailed (``password1234`` is the brief's public test vector, not a credential).
"""

from __future__ import annotations

from tests.test_signup_request import _body, _client, _isolate  # noqa: F401 - autouse fixture

VECTOR = "password1234"


def test_signup_route_refuses_the_breached_vector():
    from services.email_sender import ConsoleEmailSender

    res = _client().post("/api/v1/auth/signup", json=_body(email="d340-signup@example.com", password=VECTOR))
    assert res.status_code == 400, res.text
    assert res.json()["error"]["code"] == "pwned_password"
    assert not [m for m in ConsoleEmailSender.OUTBOX if m.to == "d340-signup@example.com"]
