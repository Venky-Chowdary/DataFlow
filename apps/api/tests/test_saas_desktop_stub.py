"""Local SaaS stub + bearer token honesty (not a customer org)."""

from __future__ import annotations

from connectors.saas_common import token
from tests.saas_desktop_stub import STORE, start_saas_stub


def test_token_accepts_password_only_private_app() -> None:
    assert token(password="pat-xxx") == "pat-xxx"
    assert token(api_key="ak", password="pat-xxx") == "ak"
    assert token(username="user", password="pat-xxx") == "user:pat-xxx"


def test_saas_stub_describe_and_upsert() -> None:
    server, url = start_saas_stub()
    try:
        import requests

        desc = requests.get(f"{url}/services/data/v58.0/sobjects/Account/describe", timeout=5)
        assert desc.status_code == 200
        names = {f["name"] for f in desc.json()["fields"]}
        assert {"id", "email", "Name", "description"} <= names
        posted = requests.post(
            f"{url}/services/data/v58.0/composite/sobjects",
            json={"records": [{"id": "1", "Name": "Acme"}]},
            timeout=5,
        )
        assert posted.status_code == 200
        assert len(STORE.rows["Account"]) == 1
    finally:
        server.shutdown()
        server.server_close()
