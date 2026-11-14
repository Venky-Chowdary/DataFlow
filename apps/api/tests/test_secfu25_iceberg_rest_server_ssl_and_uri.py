"""Follow-up review 2, M1: a REST catalog server's configuration cannot set the session's TLS or URI.

pyiceberg's ``RestCatalog.__init__`` runs ``_fetch_config`` (the server's
``/v1/config``: ``defaults`` under the connector's properties, ``overrides``
over them) and only then ``_create_session``, which reads ``ssl``: ``ssl.client.cert``
/ ``ssl.client.key`` become ``requests``' client certificate, read from a
path on the API host, and ``ssl.cabundle`` becomes ``verify`` (``false``
turns verification off). A tenant runs its own catalog server, so it could make
every later catalog request present the platform's client certificate (a host
path it names) and accept any server certificate. ``overrides.uri`` likewise
moves every later request to a host the connector never named (and the egress
check never judged).

Now ``ssl`` from the server (defaults or overrides) and a ``uri`` override that
differs from the connector's are refused before the session is built, naming
the property and never its value. The connector's own settings do not reach
``ssl`` at all (only ``rest.*``, token / credential and storage properties are
forwarded), so a host path cannot be named there either; a test pins that.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_REST = "https://catalog.example.com/v1"
_CERT = "/etc/platform/client.pem"
_KEYFILE = "/etc/platform/client.key"
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog server; records each request's host, client cert and verify."""
    import requests

    state = SimpleNamespace(defaults={}, overrides={}, requests=[])

    def request(self, method, url, *args, **kwargs):
        parts = urlsplit(url)
        state.requests.append((parts.hostname, parts.path, self.cert, self.verify))
        if parts.path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif parts.path.endswith("/namespaces"):
            body = {"namespaces": []}
        else:
            raise AssertionError(f"unexpected catalog call {method} {parts.path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return state


def _endpoint(**extra) -> dict:
    return {
        "type": "iceberg", "name": "ice", "schema": "ns", "table": "t",
        "connection_string": _REST, "extra": {"catalog_type": "rest", **_KEYS, **extra},
    }


_SSL = [
    pytest.param({"client": {"cert": _CERT, "key": _KEYFILE}}, id="client-cert-and-key"),
    pytest.param({"client": {"cert": _CERT}}, id="client-cert"),
    pytest.param({"cabundle": False}, id="no-verify"),
    pytest.param({"cabundle": "/etc/ssl/tenant-ca.pem"}, id="cabundle-path"),
    pytest.param({"client": {"cert": _CERT, "key": _KEYFILE}, "cabundle": False}, id="both"),
    pytest.param({}, id="empty"),
    pytest.param(0, id="zero"),
    pytest.param(False, id="false"),
    pytest.param([], id="empty-list"),
]


def _refused(message: str, prop: str) -> None:
    assert prop in message, message
    for value in (_CERT, _KEYFILE, "tenant-ca", "169.254", "evil.example"):
        assert value not in message, message


@pytest.mark.parametrize("ssl", _SSL)
@pytest.mark.parametrize("where", ["overrides", "defaults"])
def test_the_servers_ssl_is_refused_before_the_session_is_built(server, where, ssl):
    from connectors.iceberg_catalog import load_catalog

    getattr(server, where)["ssl"] = ssl
    with pytest.raises(ValueError) as caught:
        load_catalog(_endpoint()).list_namespaces()
    _refused(str(caught.value), "ssl")
    # Only the config request went out, with no client certificate and verification on.
    assert [(path, cert, verify) for _h, path, cert, verify in server.requests] == [("/v1/v1/config", None, True)]


@pytest.mark.parametrize(
    "uri",
    [
        "http://169.254.169.254/latest",
        "https://evil.example/v1",
        "http://catalog.example.com/v1",  # a downgrade to plain HTTP
        "https://catalog.example.com:8443/v1",
        "",
    ],
)
def test_a_uri_override_that_moves_the_catalog_is_refused(server, uri):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["uri"] = uri
    with pytest.raises(ValueError) as caught:
        load_catalog(_endpoint()).list_namespaces()
    _refused(str(caught.value), "uri")
    assert {host for host, *_ in server.requests} == {"catalog.example.com"}


@pytest.mark.parametrize("uri", [_REST, _REST + "/"])
def test_a_uri_override_naming_the_same_catalog_is_allowed(server, uri):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["uri"] = uri
    load_catalog(_endpoint()).list_namespaces()
    assert {host for host, *_ in server.requests} == {"catalog.example.com"}


def test_a_uri_default_cannot_move_the_catalog(server):
    """Defaults sit under the connector's own ``uri``, so they never change it."""
    from connectors.iceberg_catalog import load_catalog

    server.defaults["uri"] = "https://evil.example/v1"
    load_catalog(_endpoint()).list_namespaces()
    assert {host for host, *_ in server.requests} == {"catalog.example.com"}


def test_ordinary_server_configuration_still_applies(server):
    from connectors.iceberg_catalog import load_catalog

    server.defaults["prefix"] = "wh"
    server.overrides["s3.region"] = "eu-west-1"
    catalog = load_catalog(_endpoint())
    catalog.list_namespaces()
    assert catalog.properties["s3.region"] == "eu-west-1"
    assert all(cert is None and verify is True for *_x, cert, verify in server.requests)


@pytest.mark.parametrize("key", ["ssl", "rest.ssl", "ssl.client.cert"])
def test_the_connectors_own_settings_never_reach_the_sessions_tls(server, key):
    """Decision: no connector setting names a client certificate or CA path on the API host."""
    from connectors.iceberg_catalog import load_catalog

    value = {"client": {"cert": _CERT, "key": _KEYFILE}, "cabundle": False} if key == "ssl" else _CERT
    catalog = load_catalog(_endpoint(**{key: value}))
    catalog.list_namespaces()
    assert "ssl" not in catalog.properties
    assert all(cert is None and verify is True for *_x, cert, verify in server.requests)
