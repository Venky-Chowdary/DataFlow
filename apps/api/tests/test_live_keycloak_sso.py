"""Live OIDC/SAML flows against the opt-in local Keycloak instance."""

from __future__ import annotations

import base64
import html
import os
import secrets
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

import httpx
import pytest
from fastapi.testclient import TestClient

KEYCLOAK_URL = os.getenv("DATAFLOW_LIVE_KEYCLOAK_URL", "").rstrip("/")
KEYCLOAK_PASSWORD = os.getenv("DATAFLOW_LIVE_KEYCLOAK_ADMIN_PASSWORD", "")
REALM = "datawrap-iam-test"
CLIENT_ID = "dataflow-iam-oidc"
CLIENT_SECRET = "live-test-client-secret"
USERNAME = "sso-test@example.test"
USER_PASSWORD = "QaOnly!234567"
ACS_URL = "https://testserver/api/v1/auth/sso/saml/callback"
SP_ENTITY_ID = "https://testserver/api/v1/auth/sso/saml/metadata"

pytestmark = pytest.mark.skipif(
    not (KEYCLOAK_URL and KEYCLOAK_PASSWORD),
    reason=(
        "Set DATAFLOW_LIVE_KEYCLOAK_URL and "
        "DATAFLOW_LIVE_KEYCLOAK_ADMIN_PASSWORD to run live Keycloak tests"
    ),
)


class _Forms(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms: list[dict] = []
        self._form: dict | None = None

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "form":
            self._form = {"attrs": attributes, "inputs": {}}
            self.forms.append(self._form)
        elif tag == "input" and self._form is not None:
            name = attributes.get("name")
            if name:
                self._form["inputs"][name] = attributes.get("value", "")

    def handle_endtag(self, tag):
        if tag == "form":
            self._form = None


def _forms(document: str) -> list[dict]:
    parser = _Forms()
    parser.feed(document)
    return parser.forms


def _cookie_header(client: httpx.Client) -> str:
    return "; ".join(
        f"{cookie.name}={cookie.value}"
        for cookie in client.cookies.jar
    )


def _keycloak_admin():
    client = httpx.Client(base_url=KEYCLOAK_URL, timeout=15.0)
    response = client.post(
        "/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "admin",
            "password": KEYCLOAK_PASSWORD,
        },
    )
    response.raise_for_status()
    return client, {"Authorization": f"Bearer {response.json()['access_token']}"}


def _saml_signing_certificate(client: httpx.Client) -> str:
    response = client.get(f"/realms/{REALM}/protocol/saml/descriptor")
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    signing_key = root.find(
        ".//{urn:oasis:names:tc:SAML:2.0:metadata}KeyDescriptor[@use='signing']"
    )
    assert signing_key is not None
    cert_node = signing_key.find(
        ".//{http://www.w3.org/2000/09/xmldsig#}X509Certificate"
    )
    assert cert_node is not None and cert_node.text
    return cert_node.text.strip()


@pytest.fixture(scope="session")
def live_keycloak_setup(tmp_path_factory):
    from services import integrations_store
    from src.main import app

    patch = pytest.MonkeyPatch()
    admin, headers = _keycloak_admin()
    realms = admin.get("/admin/realms", headers=headers)
    realms.raise_for_status()
    if any(realm.get("realm") == REALM for realm in realms.json()):
        deleted = admin.delete(f"/admin/realms/{REALM}", headers=headers)
        deleted.raise_for_status()
    created = admin.post(
        "/admin/realms",
        headers=headers,
        json={"realm": REALM, "enabled": True, "displayName": REALM},
    )
    created.raise_for_status()
    realm_response = admin.get(f"/admin/realms/{REALM}", headers=headers)
    realm_response.raise_for_status()
    realm_id = realm_response.json()["id"]

    oidc_client = {
        "clientId": CLIENT_ID,
        "name": "DataFlow live OIDC test",
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "secret": CLIENT_SECRET,
        "standardFlowEnabled": True,
        "directAccessGrantsEnabled": True,
        "redirectUris": [
            "https://testserver/api/v1/auth/sso/oidc/callback",
        ],
        "webOrigins": ["+"],
        "attributes": {
            "pkce.code.challenge.method": "S256",
            "post.logout.redirect.uris": "https://testserver/*",
        },
    }
    response = admin.post(
        f"/admin/realms/{REALM}/clients",
        headers=headers,
        json=oidc_client,
    )
    response.raise_for_status()

    saml_client = {
        "clientId": SP_ENTITY_ID,
        "name": "DataFlow live SAML test",
        "enabled": True,
        "protocol": "saml",
        "publicClient": False,
        "redirectUris": [ACS_URL],
        "attributes": {
            "saml.assertion.signature": "true",
            "saml.server.signature": "false",
            "saml.client.signature": "false",
            "saml.force.post.binding": "true",
            "saml_name_id_format": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
            "saml.force.name.id.format": "true",
            "saml_assertion_consumer_url_post": ACS_URL,
            "saml_signature_algorithm": "RSA_SHA256",
        },
    }
    response = admin.post(
        f"/admin/realms/{REALM}/clients",
        headers=headers,
        json=saml_client,
    )
    response.raise_for_status()
    clients = admin.get(f"/admin/realms/{REALM}/clients", headers=headers)
    clients.raise_for_status()
    saml_client_id = next(
        item["id"] for item in clients.json()
        if item.get("clientId") == SP_ENTITY_ID
    )
    scopes = admin.get(
        f"/admin/realms/{REALM}/clients/{saml_client_id}/default-client-scopes",
        headers=headers,
    )
    scopes.raise_for_status()
    role_list = next(
        (scope for scope in scopes.json() if scope.get("name") == "role_list"),
        None,
    )
    assert role_list is not None
    role_mappers = admin.get(
        f"/admin/realms/{REALM}/client-scopes/{role_list['id']}/protocol-mappers/models",
        headers=headers,
    )
    role_mappers.raise_for_status()
    role_mapper = next(
        mapper for mapper in role_mappers.json()
        if mapper.get("protocolMapper") == "saml-role-list-mapper"
    )
    role_mapper["config"]["single"] = "true"
    updated_role_mapper = admin.put(
        f"/admin/realms/{REALM}/client-scopes/{role_list['id']}/protocol-mappers/models/{role_mapper['id']}",
        headers=headers,
        json=role_mapper,
    )
    updated_role_mapper.raise_for_status()

    user = {
        "username": USERNAME,
        "email": USERNAME,
        "firstName": "SSO",
        "lastName": "Integration",
        "enabled": True,
        "emailVerified": True,
        "requiredActions": [],
        "credentials": [
            {"type": "password", "value": USER_PASSWORD, "temporary": False},
        ],
    }
    response = admin.post(
        f"/admin/realms/{REALM}/users",
        headers=headers,
        json=user,
    )
    response.raise_for_status()

    idp_certificate = _saml_signing_certificate(admin)
    admin.close()

    data_dir = tmp_path_factory.mktemp("keycloak-sso")
    patch.setattr(integrations_store, "STORE_PATH", data_dir / "integrations.json")
    patch.setenv("DATAFLOW_SECRETS_KEY", "k" * 32)
    patch.setenv("DATAFLOW_AUTH_SECRET", "a" * 64)
    patch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    patch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    patch.setenv("DATAFLOW_SSO_ALLOWED_DOMAINS", "example.test")
    patch.setenv("DATAFLOW_ENV", "test")
    integrations_store.update_sso_config(
        "oidc",
        {
            "enabled": True,
            "issuer": f"{KEYCLOAK_URL}/realms/{REALM}",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": "https://testserver/api/v1/auth/sso/oidc/callback",
            "scopes": "openid email profile",
        },
    )
    integrations_store.update_sso_config(
        "saml",
        {
            "enabled": True,
            "entity_id": f"{KEYCLOAK_URL}/realms/{REALM}",
            "sso_url": f"{KEYCLOAK_URL}/realms/{REALM}/protocol/saml",
            "x509_cert": idp_certificate,
            "email_attribute": "email",
        },
    )
    yield {
        "admin_url": KEYCLOAK_URL,
        "app": app,
        "client_secret": CLIENT_SECRET,
        "data_dir": data_dir,
        "headers": headers,
        "realm_id": realm_id,
        "realm": REALM,
    }
    patch.undo()


@pytest.fixture
def live_client(live_keycloak_setup, monkeypatch):
    from services import integrations_store, sso_state

    monkeypatch.setattr(
        integrations_store,
        "STORE_PATH",
        live_keycloak_setup["data_dir"] / "integrations.json",
    )
    monkeypatch.setattr(sso_state, "STATE_PATH", live_keycloak_setup["data_dir"] / "state.json")
    monkeypatch.setattr(sso_state, "data_dir", lambda: live_keycloak_setup["data_dir"])
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "k" * 32)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "a" * 64)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    monkeypatch.setenv("DATAFLOW_SSO_ALLOWED_DOMAINS", "example.test")
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    return TestClient(live_keycloak_setup["app"], base_url="https://testserver")


def _login_at_keycloak(location: str) -> httpx.Response:
    with httpx.Client(timeout=20.0, follow_redirects=False) as client:
        page = client.get(location)
        page.raise_for_status()
        forms = _forms(page.text)
        login_form = next(
            form for form in forms
            if form["attrs"].get("id") == "kc-form-login"
        )
        action = html.unescape(login_form["attrs"]["action"])
        credentials = dict(login_form["inputs"])
        credentials.update({"username": USERNAME, "password": USER_PASSWORD})
        response = client.post(
            action,
            data=credentials,
            headers={"Cookie": _cookie_header(client)},
        )
        return response


def _oidc_code_state(location: str) -> tuple[str, str]:
    response = _login_at_keycloak(location)
    assert response.status_code in (302, 303), response.status_code
    query = parse_qs(urlsplit(response.headers["location"]).query)
    assert query.get("code") and query.get("state")
    return query["code"][0], query["state"][0]


def _assert_sso_fragment(response):
    assert response.status_code in (302, 303), response.status_code
    fragment = parse_qs(urlsplit(response.headers["location"]).fragment)
    token = fragment["sso_token"][0]
    from src.services.auth_service import verify_token

    assert verify_token(token) == USERNAME


def test_live_keycloak_oidc_happy_path_and_callback_replay(live_client):
    from services import oidc_client

    oidc_client.reset_caches()
    start = live_client.get("/api/v1/auth/sso/oidc/start", follow_redirects=False)
    assert start.status_code == 302
    code, state = _oidc_code_state(start.headers["location"])
    callback = live_client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": code, "state": state},
        follow_redirects=False,
    )
    _assert_sso_fragment(callback)
    replay = live_client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": code, "state": state},
        follow_redirects=False,
    )
    assert replay.status_code == 400


def test_live_keycloak_oidc_rotation_refetches_jwks_once(
    live_client,
    live_keycloak_setup,
    monkeypatch,
):
    from services import oidc_client

    oidc_client.reset_caches()
    start = live_client.get("/api/v1/auth/sso/oidc/start", follow_redirects=False)
    code, state = _oidc_code_state(start.headers["location"])
    callback = live_client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": code, "state": state},
        follow_redirects=False,
    )
    _assert_sso_fragment(callback)

    admin, headers = _keycloak_admin()
    components_response = admin.get(
        f"/admin/realms/{REALM}/components",
        headers=headers,
        params={"type": "org.keycloak.keys.KeyProvider"},
    )
    components_response.raise_for_status()
    old_keys = [
        component
        for component in components_response.json()
        if component.get("providerId") == "rsa-generated"
    ]
    assert old_keys, "Keycloak realm has no rsa-generated signing key"
    new_component = {
        "name": "dataflow-live-rotated-rsa",
        "providerId": "rsa-generated",
        "providerType": "org.keycloak.keys.KeyProvider",
        "parentId": live_keycloak_setup["realm_id"],
        "config": {
            "priority": ["200"],
            "enabled": ["true"],
            "active": ["true"],
            "keySize": ["2048"],
            "algorithm": ["RS256"],
        },
    }
    created = admin.post(
        f"/admin/realms/{REALM}/components",
        headers=headers,
        json=new_component,
    )
    created.raise_for_status()
    for component in old_keys:
        component["config"]["enabled"] = ["false"]
        disabled = admin.put(
            f"/admin/realms/{REALM}/components/{component['id']}",
            headers=headers,
            json=component,
        )
        disabled.raise_for_status()
    from services import integrations_store

    integrations_store.update_sso_config(
        "saml",
        {"x509_cert": _saml_signing_certificate(admin)},
    )
    admin.close()

    calls = []
    fetch_jwks = oidc_client._fetch_jwks

    def counted_fetch(*args, **kwargs):
        calls.append(args[0])
        return fetch_jwks(*args, **kwargs)

    monkeypatch.setattr(oidc_client, "_fetch_jwks", counted_fetch)
    metadata = oidc_client.discover(f"{KEYCLOAK_URL}/realms/{REALM}")
    oidc_client._JWKS_REFRESHED.pop(metadata.jwks_uri, None)
    start = live_client.get("/api/v1/auth/sso/oidc/start", follow_redirects=False)
    code, state = _oidc_code_state(start.headers["location"])
    callback = live_client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": code, "state": state},
        follow_redirects=False,
    )
    _assert_sso_fragment(callback)
    assert len(calls) == 1


def _saml_response_from_login(start_location: str) -> tuple[str, str]:
    start = urlsplit(start_location)
    relay_state = parse_qs(start.query).get("RelayState", [""])[0]
    with httpx.Client(timeout=20.0, follow_redirects=False) as client:
        page = client.get(start_location)
        page.raise_for_status()
        forms = _forms(page.text)
        login_form = next(
            form for form in forms
            if form["attrs"].get("id") == "kc-form-login"
        )
        login_response = client.post(
            html.unescape(login_form["attrs"]["action"]),
            data={
                **login_form["inputs"],
                "username": USERNAME,
                "password": USER_PASSWORD,
            },
            headers={"Cookie": _cookie_header(client)},
        )
        if login_response.status_code in (302, 303):
            page = client.get(
                login_response.headers["location"],
                headers={"Cookie": _cookie_header(client)},
            )
        else:
            page = login_response
        forms = _forms(page.text)
        post_form = next(
            (
                form for form in forms
                if "SAMLResponse" in form["inputs"]
            ),
            None,
        )
        assert post_form is not None, "Keycloak did not return a SAML POST form"
        relay_state = post_form["inputs"].get("RelayState", relay_state)
        return post_form["inputs"]["SAMLResponse"], relay_state


def _start_and_get_saml_response(live_client):
    start = live_client.get("/api/v1/auth/sso/saml/start", follow_redirects=False)
    assert start.status_code == 302
    response, relay = _saml_response_from_login(start.headers["location"])
    return response, relay


def test_live_keycloak_saml_happy_path(live_client):
    saml_response, relay = _start_and_get_saml_response(live_client)
    result = live_client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": saml_response, "RelayState": relay},
        follow_redirects=False,
    )
    _assert_sso_fragment(result)


def test_live_keycloak_saml_tamper_and_assertion_replay(live_client):
    from services import sso_state

    saml_response, relay = _start_and_get_saml_response(live_client)
    state_info = sso_state.get_state(relay, "saml")
    assert state_info and state_info["extra"]["request_id"]
    sso_state.set_state(relay, "saml", state_info["extra"])

    decoded = base64.b64decode(saml_response).decode("utf-8")
    assert USERNAME in decoded
    tampered = base64.b64encode(
        decoded.replace(USERNAME, "attacker@example.test").encode("utf-8")
    ).decode("ascii")
    rejected = live_client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": tampered, "RelayState": relay},
        follow_redirects=False,
    )
    assert rejected.status_code == 401

    sso_state.set_state(relay, "saml", state_info["extra"])
    success = live_client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": saml_response, "RelayState": relay},
        follow_redirects=False,
    )
    _assert_sso_fragment(success)

    replay_relay = secrets.token_urlsafe(16)
    sso_state.set_state(replay_relay, "saml", state_info["extra"])
    replay = live_client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": saml_response, "RelayState": replay_relay},
        follow_redirects=False,
    )
    assert replay.status_code == 401
    assert "assertion_replay" in replay.json()["detail"]


def test_live_keycloak_saml_rejects_fresh_request_id_for_old_response(live_client):
    from services import sso_state

    saml_response, _old_relay = _start_and_get_saml_response(live_client)
    new_start = live_client.get("/api/v1/auth/sso/saml/start", follow_redirects=False)
    new_relay = parse_qs(urlsplit(new_start.headers["location"]).query)["RelayState"][0]
    result = live_client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": saml_response, "RelayState": new_relay},
        follow_redirects=False,
    )
    assert result.status_code == 401
    assert sso_state.get_state(new_relay, "saml") is None
