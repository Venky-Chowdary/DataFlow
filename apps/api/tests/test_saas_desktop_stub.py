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
        posted2 = requests.post(
            f"{url}/services/data/v58.0/composite/sobjects",
            json={"records": [{"id": "1", "Name": "Acme Updated"}]},
            timeout=5,
        )
        assert posted2.status_code == 200
        assert len(STORE.rows["Account"]) == 1
        assert STORE.rows["Account"][0]["Name"] == "Acme Updated"
    finally:
        server.shutdown()
        server.server_close()


def test_rest_api_uniqueness_scan_uses_batch_reader() -> None:
    from tests.saas_desktop_stub import seed_tabular_fixture
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    server, url = start_saas_stub()
    try:
        seed_tabular_fixture()
        result = probe_source_duplicate_keys_result(
            source_config={
                "type": "rest_api",
                "host": url,
                "password": "stub-token",
                "api_key": "stub-token",
                "extra": {"pagination_type": "none"},
            },
            source_table="records",
            primary_key="id",
        )
        assert result.status == "ran", result.message
        assert result.findings == []
    finally:
        server.shutdown()
        server.server_close()
