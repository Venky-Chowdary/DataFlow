"""CLM-13: a scheduled Salesforce run gets a token that has not expired.

The only auth was a pasted session token with no refresh, so scheduled runs
failed once it expired (two hours by default) while the tile said Certified.
Now OAuth client credentials mint a fresh token per run, and a schedule
saved on a pasted session token says it will stop working.
"""

from __future__ import annotations

import pytest

from connectors import salesforce_auth


class _Resp:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> dict:
        return self._body


@pytest.fixture(autouse=True)
def _fresh_cache():
    salesforce_auth.clear_cache()
    yield
    salesforce_auth.clear_cache()


def _cc_cfg() -> dict:
    return {
        "host": "https://w5clm.my.salesforce.com",
        "username": "3MVG9-consumer-key",
        "password": "consumer-secret",
        "auth_mode": "user_pass",
    }


def test_client_credentials_mint_and_refresh(monkeypatch) -> None:
    calls: list[dict] = []
    tokens = iter(["tok-1", "tok-2"])

    def fake_post(url, data=None, **_kw):
        calls.append({"url": url, **(data or {})})
        return _Resp(200, {"access_token": next(tokens), "instance_url": "https://w5clm.my.salesforce.com"})

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("services.egress_guard.assert_connector_dial_allowed", lambda *a, **k: None)
    assert salesforce_auth.auth_mode(_cc_cfg()) == salesforce_auth.CLIENT_CREDENTIALS
    assert salesforce_auth.access_token(_cc_cfg()) == "tok-1"
    assert calls[0]["url"] == "https://w5clm.my.salesforce.com/services/oauth2/token"
    assert calls[0]["grant_type"] == "client_credentials"
    assert calls[0]["client_id"] == "3MVG9-consumer-key"
    # Reused inside its window, re-minted when it ages out (or on demand).
    assert salesforce_auth.access_token(_cc_cfg()) == "tok-1"
    key = next(iter(salesforce_auth._cache))
    salesforce_auth._cache[key] = ("tok-1", 0.0)
    assert salesforce_auth.access_token(_cc_cfg()) == "tok-2"
    assert len(calls) == 2


def test_a_refused_mint_names_the_connected_app(monkeypatch) -> None:
    monkeypatch.setattr("requests.post", lambda *a, **k: _Resp(400, {"error": "invalid_client"}))
    monkeypatch.setattr("services.egress_guard.assert_connector_dial_allowed", lambda *a, **k: None)
    with pytest.raises(ValueError, match="client-credentials"):
        salesforce_auth.access_token(_cc_cfg())


def test_reader_and_writer_use_the_minted_token(monkeypatch) -> None:
    from connectors import salesforce

    monkeypatch.setattr(salesforce_auth, "_mint", lambda *a: "minted-token")
    tok, base = salesforce._access(_cc_cfg())
    assert tok == "minted-token"
    assert base == "https://w5clm.my.salesforce.com"


def test_a_pasted_session_token_is_labelled_and_warned() -> None:
    cfg = {"host": "https://w5clm.my.salesforce.com", "api_key": "00D!session"}
    assert salesforce_auth.auth_mode(cfg) == salesforce_auth.SESSION_TOKEN
    assert salesforce_auth.access_token(cfg) == "00D!session"
    warning = salesforce_auth.session_token_schedule_warning(cfg, name="Source CRM")
    assert "cannot refresh" in warning and "client credentials" in warning
    assert salesforce_auth.session_token_schedule_warning(_cc_cfg()) == ""


def test_the_form_offers_client_credentials_and_labels_the_session_token() -> None:
    from services.connector_form_schema import _load

    form = _load()["salesforce"]
    modes = {m.value: m for m in form.auth_modes}
    assert form.default_auth_mode == "user_pass"
    assert modes["user_pass"].label == "OAuth client credentials"
    assert {f.key for f in modes["user_pass"].fields} >= {"host", "username", "password"}
    token_field = next(f for f in modes["api_key"].fields if f.key == "apiKey")
    assert "not refreshed" in token_field.label


def test_schedule_save_warns_on_a_session_token(monkeypatch) -> None:
    from types import SimpleNamespace

    from services import schedule_claim_warnings as scw

    conns = {
        "sf": SimpleNamespace(
            name="CRM", type="salesforce", auth_mode="api_key", api_key="00D!session",
            connection_string="", username="", password="",
        ),
    }
    monkeypatch.setattr("services.connector_store.get_connector", lambda cid, *a, **k: conns.get(cid))
    out = scw.schedule_claim_warnings("", "sf")
    assert any("cannot refresh" in w for w in out), out
