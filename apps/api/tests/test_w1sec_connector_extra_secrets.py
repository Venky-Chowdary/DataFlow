"""SEC-3: a saved connector's secret ``extra`` keys are masked in every response
and encrypted at rest, with the same vault as the top-level password.

Only 8 key names were masked and none encrypted: ``client_secret``,
``refresh_token``, ``aws_secret_access_key``, ``headers.Authorization`` and
about 20 more came back in clear text to every member (viewers included) and
sat in plain text in ``connectors.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.tenancy_world import tenancy_world  # noqa: F401

TOK = "w1secTOK_s3ntinel_7719"
KEYS = [
    "token", "client_secret", "aws_secret_access_key", "secret_access_key", "refresh_token", "access_token",
    "sasl_password", "ssh_password", "ssh_private_key", "private_key_passphrase", "password", "api_secret",
    "bearer_token", "account_key", "sas_token", "credentials_json", "service_account_json",
    "oauth_client_secret", "consumer_secret", "webhook_secret", "basic_auth_password", "proxy_password",
    "keystore_password", "truststore_password", "auth_token",
]
#: Configuration that happens to contain a secret word stays readable.
PLAIN = {"auth_type": "oauth", "primary_key": "id", "aws_access_key_id": "AKIAW1SECEXAMPLE",
         "token_url": "https://auth.example.com/token", "keyspace": "ks"}


def _extra() -> dict:
    extra = {k: f"{TOK}-{k}" for k in KEYS}
    extra["headers"] = {"Authorization": f"Bearer {TOK}-hdr", "Accept": "application/json"}
    extra["service_account_info"] = {"private_key": f"{TOK}-nested"}
    extra.update(PLAIN)
    return extra


@pytest.fixture
def saved(tenancy_world):  # noqa: F811
    r = tenancy_world.a.post("/api/v1/connectors/saved", json={
        "name": "w1sec-extra", "type": "snowflake", "host": "w1sec.snowflakecomputing.com",
        "username": "u", "extra": _extra()})
    assert r.status_code in (200, 201), r.text
    assert TOK not in r.text, "the create response echoed a secret extra"
    return tenancy_world, r.json()["id"]


def test_every_response_masks_secret_extras_viewers_included(saved):
    world, cid = saved
    viewer = world.member("viewer-a@tenancy.example.com", world.workspace_a, "viewer")
    for client in (world.a, viewer):
        for path in ("/api/v1/connectors/saved", f"/api/v1/connectors/saved/{cid}"):
            r = client.get(path)
            assert r.status_code == 200, r.text
            assert TOK not in r.text, (path, [k for k in KEYS if f"{TOK}-{k}" in r.text])
    shown = world.a.get(f"/api/v1/connectors/saved/{cid}").json()["extra"]
    assert shown["client_secret"] == "****" and shown["headers"]["Authorization"] == "****"
    assert shown["headers"]["Accept"] == "application/json"
    for key, value in PLAIN.items():
        assert shown[key] == value, key


def test_secret_extras_are_encrypted_at_rest_and_still_usable(saved):
    from services import connector_store

    world, cid = saved
    raw = Path(connector_store.STORE_PATH).read_text()
    assert TOK not in raw, [k for k in KEYS if f"{TOK}-{k}" in raw]
    assert "AKIAW1SECEXAMPLE" in raw  # configuration stays plain
    with world.as_workspace(world.workspace_a):
        conn = connector_store.get_connector(cid, world.workspace_a)
    assert conn.extra["client_secret"] == f"{TOK}-client_secret"
    assert conn.extra["headers"]["Authorization"] == f"Bearer {TOK}-hdr"
    assert conn.extra["service_account_info"]["private_key"] == f"{TOK}-nested"


def test_an_edit_that_sends_masks_back_keeps_the_stored_secrets(saved):
    from services import connector_store

    world, cid = saved
    shown = world.a.get(f"/api/v1/connectors/saved/{cid}").json()
    body = {"name": "w1sec-extra-renamed", "type": "snowflake", "host": "w1sec.snowflakecomputing.com",
            "username": "u", "extra": {**shown["extra"], "refresh_token": f"{TOK}-rotated"}}
    r = world.a.put(f"/api/v1/connectors/saved/{cid}", json=body)
    assert r.status_code == 200, r.text
    assert TOK not in r.text
    with world.as_workspace(world.workspace_a):
        conn = connector_store.get_connector(cid, world.workspace_a)
    assert conn.extra["client_secret"] == f"{TOK}-client_secret"
    assert conn.extra["headers"]["Authorization"] == f"Bearer {TOK}-hdr"
    assert conn.extra["refresh_token"] == f"{TOK}-rotated"
    assert TOK not in Path(connector_store.STORE_PATH).read_text()


def test_a_legacy_plaintext_record_is_sealed_on_read(tenancy_world):  # noqa: F811
    from services import connector_store

    with tenancy_world.as_workspace(tenancy_world.workspace_a):
        conn = connector_store.create_connector({
            "name": "w1sec-legacy", "type": "snowflake", "host": "w1sec.snowflakecomputing.com",
            "workspace_id": tenancy_world.workspace_a, "extra": {"note": "x"}})
    path = Path(connector_store.STORE_PATH)
    data = json.loads(path.read_text())
    for doc in data["connectors"]:
        if doc["id"] == conn.id:
            doc["extra"] = {"client_secret": f"{TOK}-legacy", "note": "x"}
    path.write_text(json.dumps(data))
    r = tenancy_world.a.get(f"/api/v1/connectors/saved/{conn.id}")
    assert r.status_code == 200 and TOK not in r.text, r.text
    assert TOK not in path.read_text(), "a read left the legacy secret in plain text"
    with tenancy_world.as_workspace(tenancy_world.workspace_a):
        assert connector_store.get_connector(conn.id).extra["client_secret"] == f"{TOK}-legacy"


def test_secret_field_rule_unit():
    from services.secret_fields import is_secret_field_name

    assert all(is_secret_field_name(k) for k in KEYS + ["Authorization", "X-Api-Key", "authToken"])
    assert not any(is_secret_field_name(k) for k in list(PLAIN) + ["authSource", "sort_key", "known_hosts"])
