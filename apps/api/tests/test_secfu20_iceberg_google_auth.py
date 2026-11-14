"""Follow-up review 1: an Iceberg REST catalog never authenticates with the API host's own cloud credentials.

pyiceberg 0.12 registers six REST auth managers. ``noop``, ``basic``,
``oauth2`` and ``legacyoauth2`` carry only what their configuration supplies
(the tenant's own secret). Two take credentials from the host the API runs on:
``google`` (``google.auth.default()``, the API's Application Default
Credentials, or ``google.auth.load_credentials_from_file(credentials_path)``,
a file on the API host) and ``entra`` (``azure.identity.DefaultAzureCredential``:
environment, managed identity, Azure CLI). Either signs every catalog request
with the platform's bearer token and sends it to the tenant's catalog server.

``auth`` with either type is refused from every source: the connector's
settings (at save and in ``load_catalog``), the REST server's ``/v1/config``
defaults and overrides (before the session's auth manager is built) and a
table's configuration. No credential lookup happens. The message names the
property and the source and repeats no value. The operator's
platform-AWS-credentials opt-in does not change this.
"""

from __future__ import annotations

import json
import sys
import types
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_REST = "https://catalog.example.com/v1"
_SECRET = "tenant-secret-value"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": _SECRET}
_LOCATION = "s3://bucket/wh/ns/t"
_PLATFORM_TOKEN = "platform-adc-token"
_HOST_FILE = "/var/run/secrets/platform-sa.json"

_AMBIENT = [
    pytest.param({"type": "google"}, id="google-adc"),
    pytest.param({"type": "google", "google": {"credentials_path": _HOST_FILE}}, id="google-file"),
    pytest.param(
        {
            "type": "google",
            "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]},
        },
        id="google-scopes",
    ),
    pytest.param({"type": "entra"}, id="entra"),
]


@pytest.fixture(params=["off", "on"])
def mode(request, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_ALLOW_PLATFORM_AWS_CREDENTIALS", raising=False)
    if request.param == "on":
        monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS", "1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    return request.param


@pytest.fixture
def lookups(monkeypatch):
    """Every host-credential lookup an auth manager could make, recorded (and answered with a fake token)."""
    import google.auth

    seen: list[str] = []

    class Creds:
        token = _PLATFORM_TOKEN

        def refresh(self, _request):
            seen.append("refresh")

    def default(*_a, **_k):
        seen.append("google.auth.default")
        return Creds(), "platform-project"

    def from_file(*_a, **_k):
        seen.append("google.auth.load_credentials_from_file")
        return Creds(), "platform-project"

    monkeypatch.setattr(google.auth, "default", default)
    monkeypatch.setattr(google.auth, "load_credentials_from_file", from_file)

    class DefaultAzureCredential:
        def __init__(self, *_a, **_k):
            seen.append("DefaultAzureCredential")

        def get_token(self, *_a, **_k):
            seen.append("azure.get_token")
            return SimpleNamespace(token=_PLATFORM_TOKEN, expires_on=2**31)

    azure = types.ModuleType("azure")
    identity = types.ModuleType("azure.identity")
    identity.DefaultAzureCredential = DefaultAzureCredential
    azure.identity = identity
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    return seen


def _table_response(config: dict) -> dict:
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.types import LongType, NestedField

    metadata = new_table_metadata(
        Schema(NestedField(1, "id", LongType(), required=False)),
        UNPARTITIONED_PARTITION_SPEC,
        UNSORTED_SORT_ORDER,
        _LOCATION,
        {},
    )
    return {
        "metadata-location": f"{_LOCATION}/metadata/v1.metadata.json",
        "metadata": json.loads(metadata.model_dump_json()),
        "config": config,
    }


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog server, answered in-process (no socket); records each request's Authorization."""
    import requests

    state = SimpleNamespace(defaults={}, overrides={}, table_config={}, calls=[], authorization=[])

    def request(self, method, url, *args, **kwargs):
        # Apply the session's auth as a real send would, so the header the server would see is recorded.
        prepared = self.prepare_request(
            requests.Request(
                method=method, url=url, headers=kwargs.get("headers"), params=kwargs.get("params")
            )
        )
        state.authorization.append(prepared.headers.get("Authorization"))
        path = urlsplit(url).path
        state.calls.append((method, path))
        if path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif path.endswith("/tables/t") and method == "GET":
            body = _table_response(state.table_config)
        else:
            raise AssertionError(f"unexpected catalog call {method} {path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return state


def _rest_endpoint(**extra) -> dict:
    return {
        "type": "iceberg",
        "name": "ice",
        "schema": "ns",
        "table": "t",
        "connection_string": _REST,
        "extra": {"catalog_type": "rest", **_KEYS, **extra},
    }


def _refused(message: str, where: str, auth: dict) -> None:
    assert "auth.type" in message, message
    assert where in message, message
    for value in (auth["type"], _HOST_FILE, _SECRET, "googleapis"):
        assert value not in message.lower(), message


def _no_platform_credential(lookups, server) -> None:
    assert lookups == []
    assert _PLATFORM_TOKEN not in str(server.authorization)


# --- the connector's own settings ---------------------------------------------


@pytest.mark.parametrize("auth", _AMBIENT)
def test_a_tenants_settings_cannot_pick_a_host_credential_auth_manager(mode, lookups, server, auth):
    from connectors.iceberg_catalog import load_catalog
    from services.egress_guard import connector_save_refusal
    from services.iceberg_class_properties import SETTINGS

    endpoint = _rest_endpoint(auth=auth)
    _refused(connector_save_refusal(dict(endpoint)), SETTINGS, auth)
    with pytest.raises(ValueError) as caught:
        load_catalog(dict(endpoint))
    _refused(str(caught.value), SETTINGS, auth)
    assert server.calls == []  # refused before the catalog was asked
    _no_platform_credential(lookups, server)


# --- the REST server's /v1/config ---------------------------------------------


@pytest.mark.parametrize("where", ["defaults", "overrides"])
@pytest.mark.parametrize("auth", _AMBIENT)
def test_a_rest_servers_config_cannot_pick_a_host_credential_auth_manager(
    mode, lookups, server, where, auth
):
    from connectors.iceberg_catalog import load_catalog
    from services.iceberg_class_properties import SERVER_CONFIG

    getattr(server, where)["auth"] = auth
    with pytest.raises(ValueError) as caught:
        load_catalog(_rest_endpoint())
    _refused(str(caught.value), SERVER_CONFIG, auth)
    assert [path for _method, path in server.calls] == [
        "/v1/v1/config"
    ]  # nothing after the config call
    _no_platform_credential(lookups, server)


# --- a table's configuration from its catalog --------------------------------


@pytest.mark.parametrize("auth", _AMBIENT)
def test_a_tables_config_cannot_carry_a_host_credential_auth_manager(mode, lookups, server, auth):
    from connectors.iceberg_catalog import load_table
    from services.iceberg_class_properties import TABLE_CONFIG

    server.table_config.update({**_KEYS, "auth": auth})
    with pytest.raises(ValueError) as caught:
        load_table(_rest_endpoint())
    # Refused on the table response itself, before the FileIO merge (follow-up review 2, H2).
    _refused(str(caught.value), TABLE_CONFIG, auth)
    _no_platform_credential(lookups, server)


# --- auth managers that carry the tenant's own secret still work -------------


@pytest.mark.parametrize(
    ("auth", "header"),
    [
        ({"type": "noop"}, None),
        ({"type": "basic", "basic": {"username": "tenant", "password": _SECRET}}, "Basic "),
    ],
)
def test_auth_managers_with_the_tenants_own_secret_are_accepted(
    mode, lookups, server, auth, header
):
    from connectors.iceberg_catalog import load_table
    from services.iceberg_class_properties import SERVER_CONFIG, SETTINGS, class_property_refusal

    assert class_property_refusal({"auth": auth}, SETTINGS) == ""
    server.overrides["auth"] = auth
    server.table_config.update(_KEYS)
    load_table(_rest_endpoint())
    assert class_property_refusal({"auth": auth}, SERVER_CONFIG) == ""
    last = server.authorization[-1]
    assert (last is None) if header is None else last.startswith(header)
    _no_platform_credential(lookups, server)


def test_oauth2_with_the_tenants_client_credentials_is_accepted(mode, lookups):
    from services.iceberg_class_properties import SERVER_CONFIG, SETTINGS, class_property_refusal

    auth = {
        "type": "oauth2",
        "oauth2": {"client_id": "c", "client_secret": _SECRET, "token_url": f"{_REST}/t"},
    }
    assert class_property_refusal({"auth": auth}, SETTINGS) == ""
    assert class_property_refusal({"auth": auth}, SERVER_CONFIG) == ""
    assert lookups == []
