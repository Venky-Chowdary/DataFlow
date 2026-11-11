"""D340: one password policy at every path that sets a password.

Before the fix invite-accept and ``/auth/change-password`` accepted the
well-known breached password ``password1234``: the only breach check was the
HIBP range lookup, flag-gated and off by default (``pwned_count`` → -1 →
fail-open), and change-password did not call the policy at all.

Now ``signup_service.check_new_password`` (length, an always-on bundled list of
the most common breached passwords, then HIBP) runs on signup, invite-accept,
reset, change-password and an admin-set password. HIBP is on by default in
production and fail-open; the network layer is stubbed here (no internet).
"""

from __future__ import annotations

import hashlib
import uuid

import pytest

from services.api_errors import ApiError
from tests.test_workspace_invites import _accept, _invite, _login, _token_for, env  # noqa: F401

VECTOR = "password1234"  # the brief's well-known breached test vector, never a real credential
STRONG = "violet-harbor-quartz-2026"


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    import services.hibp as hibp

    def refuse(*_a, **_kw):
        raise AssertionError("HIBP must not be called from tests without a stub")

    monkeypatch.setattr("httpx.get", refuse)
    hibp._reset_for_tests()
    yield
    hibp._reset_for_tests()


@pytest.mark.parametrize("pw", [
    VECTOR, "Password1234", "PASSWORD1234!", "qwertyuiop123", "123456789012",
    "iloveyou1234", "passwordpassword", "aaaaaaaaaaaa", "Football2026!",
])
def test_offline_list_refuses_common_breached_passwords(pw):
    from services.signup_service import check_new_password

    with pytest.raises(ApiError) as exc:
        check_new_password(pw)
    assert exc.value.status_code == 400 and exc.value.code == "pwned_password"


@pytest.mark.parametrize("pw", [STRONG, "a-very-long-password", "Tr0ub4dour&3-horse-staple"])
def test_offline_list_keeps_ordinary_long_passwords(pw):
    from services.signup_service import check_new_password

    check_new_password(pw)


def test_invite_accept_refuses_the_breached_vector(env):
    client, admin, ws, _tmp = env
    _invite(client, admin, ws, "bree@example.com")
    token = _token_for("bree@example.com")
    res = _accept(client, token, name="Bree", password=VECTOR)
    assert res.status_code == 400, res.text
    assert res.json()["error"]["code"] == "pwned_password"
    ok = _accept(client, token, name="Bree", password=STRONG)
    assert ok.status_code == 200, ok.text


def test_change_password_refuses_the_breached_vector(env):
    client, _admin, _ws, _tmp = env
    from services import user_store

    email = f"d340-{uuid.uuid4().hex[:8]}@example.com"
    user_store.create_user(email=email, password=STRONG, role="member")
    me = _login(client, email, STRONG)
    res = client.post("/api/v1/auth/change-password", headers=me,
                      json={"current_password": STRONG, "new_password": VECTOR})
    assert res.status_code == 400, res.text
    assert res.json()["error"]["code"] == "pwned_password"
    assert client.post("/api/v1/auth/change-password", headers=me, json={
        "current_password": STRONG, "new_password": "cobalt-meadow-lantern-77"}).status_code == 200


def test_admin_set_password_uses_the_same_policy(env):
    client, admin, _ws, _tmp = env
    from services import user_store

    email = f"d340a-{uuid.uuid4().hex[:8]}@example.com"
    user_store.create_user(email=email, password=STRONG, role="member")
    res = client.post(f"/api/v1/team/users/{email}/password", headers=admin,
                      json={"password": VECTOR})
    assert res.status_code == 400, res.text
    assert res.json()["error"]["code"] == "pwned_password"


def test_signup_and_reset_refuse_the_breached_vector(monkeypatch):
    from services import signup_service

    calls: list[str] = []
    monkeypatch.setattr(signup_service, "_refuse_pwned_password", lambda pw: calls.append(pw))
    # Both public paths call the shared policy before anything else.
    with pytest.raises(ApiError) as exc:
        signup_service.confirm_password_reset(token="unused", new_password=VECTOR)
    assert exc.value.code == "pwned_password"
    with pytest.raises(ApiError) as exc:
        signup_service.check_new_password(VECTOR)
    assert exc.value.code == "pwned_password"
    assert calls == []  # the offline list answered first, no network needed


# ── HIBP (k-anonymity range API), stubbed HTTP ─────────────────────────────


class _Resp:
    def __init__(self, text: str, status: int = 200):
        self.text = text
        self.status_code = status


def _range_for(password: str, count: int) -> str:
    digest = hashlib.sha1(password.encode()).hexdigest().upper()
    return f"0000000000000000000000000000000000A:1\n{digest[5:]}:{count}"


def test_hibp_is_on_by_default_in_production_and_off_elsewhere(monkeypatch):
    from services import hibp

    monkeypatch.delenv("DATAWRAP_HIBP_ENABLED", raising=False)
    monkeypatch.delenv("DATAFLOW_HIBP_ENABLED", raising=False)
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    assert hibp.enabled()
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "0")
    assert not hibp.enabled()  # an operator can still turn it off (air-gapped)
    monkeypatch.delenv("DATAWRAP_HIBP_ENABLED")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    assert not hibp.enabled()
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    assert hibp.enabled()


def test_production_policy_refuses_a_password_hibp_reports_breached(monkeypatch):
    from services.signup_service import check_new_password

    monkeypatch.delenv("DATAWRAP_HIBP_ENABLED", raising=False)
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    pw = "not-on-the-offline-list-7731"
    seen: list[tuple[str, dict]] = []

    def stub(url, **kw):
        seen.append((url, kw))
        return _Resp(_range_for(pw, 4242))

    monkeypatch.setattr("httpx.get", stub)
    with pytest.raises(ApiError) as exc:
        check_new_password(pw)
    assert exc.value.code == "pwned_password"
    full = hashlib.sha1(pw.encode()).hexdigest().upper()
    (url, kw), = seen
    assert url.endswith("/range/" + full[:5])  # only the 5-char prefix leaves the process
    assert full[5:] not in url and pw not in url
    assert kw.get("timeout") == 3.0  # bounded outbound call


@pytest.mark.parametrize("failure", ["timeout", "http500"])
def test_hibp_outage_fails_open_but_the_offline_list_still_refuses(monkeypatch, failure):
    from services.signup_service import check_new_password

    monkeypatch.delenv("DATAWRAP_HIBP_ENABLED", raising=False)
    monkeypatch.setenv("DATAWRAP_ENV", "production")

    def down(url, **kw):
        if failure == "timeout":
            import httpx

            raise httpx.ConnectTimeout("range API unreachable")
        return _Resp("", status=500)

    monkeypatch.setattr("httpx.get", down)
    check_new_password(STRONG)  # fail-open: a HIBP outage never blocks a real user
    with pytest.raises(ApiError) as exc:
        check_new_password(VECTOR)
    assert exc.value.code == "pwned_password"


def test_hibp_cache_holds_the_range_not_one_passwords_count(monkeypatch):
    """Two passwords sharing a 5-char SHA-1 prefix: the second is judged by its own suffix."""
    from services import hibp

    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    seen: dict[str, str] = {}
    i = 0
    while True:
        cand = f"prefix-share-probe-{i}"
        prefix = hashlib.sha1(cand.encode()).hexdigest().upper()[:5]
        if prefix in seen:
            clean, breached = seen[prefix], cand
            break
        seen[prefix] = cand
        i += 1
    calls: list[str] = []

    def stub(url, **kw):
        calls.append(url)
        return _Resp(_range_for(breached, 77))

    assert hibp.pwned_count(clean, http_get=stub) == 0
    assert hibp.pwned_count(breached, http_get=stub) == 77
    assert len(calls) == 1  # the second answer came from the cached range
