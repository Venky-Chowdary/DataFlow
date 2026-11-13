"""Follow-up review 2 (Iceberg REST, server-supplied hosts): a catalog server cannot pick where the API dials.

A tenant runs its own REST catalog, so it controls ``/v1/config`` (``defaults``
under the connector's properties, ``overrides`` over them) and every table
response's ``config``. pyiceberg reads hosts from those merged properties:

* ``oauth2-server-uri``: ``LegacyOAuth2AuthManager`` POSTs the connector's
  ``credential`` (client id and secret) there while the catalog is built, so a
  server could collect the secret on its own host, or make the API POST to
  169.254.169.254 or a private address;
* the FileIO's ``s3.endpoint`` / ``s3.proxy-uri`` / ``gcs.service.host`` /
  ``adls.blob-storage-authority`` / ``adls.dfs-storage-authority`` /
  ``hdfs.host``: pyarrow's filesystem dials them.

Now a server-supplied ``oauth2-server-uri`` must be on the catalog's own origin
(the connector cannot name another one), and every storage host the server
supplies (any value the connector did not set itself) passes the egress check
the connector's own address passes: cloud metadata and link-local always
refused, private and loopback refused under ``DATAFLOW_EGRESS_ENFORCE_NEW``.
Refusals name the property and its source, never the value. ``header.*``,
``prefix``, ``token``, ``credential``, ``warehouse`` and the SigV4 signing name
/ region choose no host and move no connector secret; tests pin that.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

_REST = "https://catalog.example.com/v1"
_SECRET = "tenant-client-secret"
_CRED = f"tenant-client:{_SECRET}"
_OWN_TOKEN = ("https", "catalog.example.com", None, "/v1/v1/oauth/tokens")
_KEYS = {"s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "tenant-secret", "s3.region": "us-east-1"}


class Dialled(Exception):
    """A filesystem was about to dial (recorded; never a ValueError)."""


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "ALLOW_PLATFORM_AWS_CREDENTIALS",
                     "EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )


@pytest.fixture
def server(monkeypatch):
    """The tenant's REST catalog (and whatever host it names); records every request."""
    import requests

    state = SimpleNamespace(defaults={}, overrides={}, requests=[])

    def request(self, method, url, *args, **kwargs):
        parts = urlsplit(url)
        data = kwargs.get("data") or {}
        state.requests.append((method.upper(), parts.scheme, parts.hostname, parts.port, parts.path,
                               json.dumps(data) if isinstance(data, dict) else str(data)))
        if parts.path.endswith("/config"):
            body = {"defaults": state.defaults, "overrides": state.overrides}
        elif method.upper() == "POST" and "token" in parts.path:
            body = {"access_token": "issued", "token_type": "bearer", "expires_in": 3600}
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


@pytest.fixture
def filesystems(monkeypatch):
    """pyarrow's remote filesystems record their arguments instead of dialling."""
    import pyarrow.fs

    made: list[tuple[str, dict]] = []

    def recorder(name):
        def build(*args, **kwargs):
            made.append((name, kwargs))
            raise Dialled(name)

        return build

    for name in ("S3FileSystem", "GcsFileSystem", "AzureFileSystem", "HadoopFileSystem"):
        monkeypatch.setattr(pyarrow.fs, name, recorder(name))
    return made


def _endpoint(**extra) -> dict:
    return {
        "type": "iceberg", "name": "ice", "schema": "ns", "table": "t",
        "connection_string": _REST, "extra": {"catalog_type": "rest", **_KEYS, **extra},
    }


def _refused(message: str, prop: str, source: str) -> None:
    assert prop in message, message
    assert source in message, message
    for value in ("evil", "169.254", "127.0.0.1", "10.0.0", "192.168", _SECRET, ":8443", "9000"):
        assert value not in message, message


def _secret_sent_to(server) -> list[tuple]:
    return [(scheme, host, port, path) for _m, scheme, host, port, path, body in server.requests if _SECRET in body]


# ------------------------------------------------------------------ oauth2-server-uri

_FOREIGN_OAUTH = [
    pytest.param("https://evil.example/oauth/tokens", id="another-host"),
    pytest.param("https://catalog.example.com.evil.example/oauth/tokens", id="suffix-host"),
    pytest.param("http://169.254.169.254/latest/api/token", id="metadata"),
    pytest.param("http://127.0.0.1:8181/v1/oauth/tokens", id="loopback"),
    pytest.param("http://10.0.0.7/oauth/tokens", id="private"),
    pytest.param("http://catalog.example.com/v1/oauth/tokens", id="http-downgrade"),
    pytest.param("https://catalog.example.com:8443/v1/oauth/tokens", id="another-port"),
]


@pytest.mark.parametrize("uri", _FOREIGN_OAUTH)
@pytest.mark.parametrize("where", ["overrides", "defaults"])
def test_the_credential_is_never_posted_to_a_server_chosen_host(server, where, uri):
    from connectors.iceberg_catalog import load_catalog

    getattr(server, where)["oauth2-server-uri"] = uri
    with pytest.raises(ValueError) as caught:
        load_catalog(_endpoint(credential=_CRED))
    _refused(str(caught.value), "oauth2-server-uri", "REST catalog server's configuration")
    # Only the config request's own token fetch, on the catalog's origin, carried the secret.
    assert set(_secret_sent_to(server)) <= {_OWN_TOKEN}
    assert {(s, h, p) for _m, s, h, p, _path, _b in server.requests} == {("https", "catalog.example.com", None)}


def test_a_server_chosen_oauth_host_is_refused_without_a_credential_too(server):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["oauth2-server-uri"] = "http://169.254.169.254/latest/api/token"
    with pytest.raises(ValueError) as caught:
        load_catalog(_endpoint(token="tenant-token"))
    _refused(str(caught.value), "oauth2-server-uri", "REST catalog server's configuration")


def test_the_catalogs_own_origin_may_serve_tokens(server):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["oauth2-server-uri"] = "https://catalog.example.com/auth/realms/x/token"
    load_catalog(_endpoint(credential=_CRED)).list_namespaces()
    sent = _secret_sent_to(server)  # the config request's session fetches a token first
    assert {(scheme, host, port) for scheme, host, port, _path in sent} == {("https", "catalog.example.com", None)}
    assert sent[-1][3] == "/auth/realms/x/token"


def test_without_a_server_oauth_uri_the_credential_goes_to_the_catalog(server):
    from connectors.iceberg_catalog import load_catalog

    load_catalog(_endpoint(credential=_CRED)).list_namespaces()
    sent = _secret_sent_to(server)
    assert sent and set(sent) == {_OWN_TOKEN}


# ------------------------------------------------------------------ properties that choose no host

@pytest.mark.parametrize("config", [
    pytest.param({"header.Host": "evil.example", "header.X-Forward-To": "http://169.254.169.254"}, id="headers"),
    pytest.param({"prefix": "@evil.example/x"}, id="prefix"),
    pytest.param({"token": "server-token", "credential": "server:cred"}, id="token-credential"),
    pytest.param({"warehouse": "s3://evil-bucket/wh"}, id="warehouse"),
    pytest.param({"rest.signing-name": "s3", "rest.signing-region": "eu-west-1"}, id="signing"),
])
def test_other_server_properties_move_no_request_and_no_secret(server, config):
    from connectors.iceberg_catalog import load_catalog

    server.overrides.update(config)
    load_catalog(_endpoint(credential=_CRED)).list_namespaces()
    assert {host for _m, _s, host, _p, _path, _b in server.requests} == {"catalog.example.com"}
    assert all(host == "catalog.example.com" for _s, host, _p, _path in _secret_sent_to(server))


# ------------------------------------------------------------------ FileIO hosts

_VENDED = {"gcs.oauth2.token": "server-vended", "adls.sas-token": "sv=server-vended"}
_HOST_PROPS = [
    pytest.param("s3.endpoint", "s3://bucket/t", id="s3-endpoint"),
    pytest.param("s3.proxy-uri", "s3://bucket/t", id="s3-proxy"),
    pytest.param("gcs.service.host", "gs://bucket/t", id="gcs-service-host"),
    pytest.param("adls.blob-storage-authority", "abfss://c@account.dfs.core.windows.net/t", id="adls-blob"),
    pytest.param("adls.dfs-storage-authority", "abfss://c@account.dfs.core.windows.net/t", id="adls-dfs"),
    pytest.param("hdfs.host", "hdfs:///t", id="hdfs-host"),
]
_FORBIDDEN = [
    pytest.param("http://169.254.169.254", "", id="metadata"),
    pytest.param("http://127.0.0.1:9000", "1", id="loopback-enforced"),
    pytest.param("http://10.0.0.5:9000", "1", id="private-enforced"),
    pytest.param("http://192.168.1.10", "1", id="private-192-enforced"),
]


def _bare(prop: str, value: str) -> str:
    return value.split("://", 1)[1] if prop in {"hdfs.host", "adls.blob-storage-authority",
                                                 "adls.dfs-storage-authority"} else value


def _open(catalog, location: str, properties=None) -> None:
    io = catalog._load_file_io(properties or {}, location=location)
    io.new_input(f"{location}/data/f.parquet").open()


@pytest.mark.parametrize("value, enforce", _FORBIDDEN)
@pytest.mark.parametrize("prop, location", _HOST_PROPS)
def test_a_server_config_storage_host_must_pass_the_egress_check(server, filesystems, monkeypatch,
                                                                 prop, location, value, enforce):
    from connectors.iceberg_catalog import load_catalog

    if enforce:
        monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", enforce)
    server.overrides.update({**_VENDED, prop: _bare(prop, value)})
    with pytest.raises(ValueError) as caught:
        _open(load_catalog(_endpoint()), location)
    _refused(str(caught.value), prop, "REST catalog server's configuration")
    assert filesystems == []


@pytest.mark.parametrize("value, enforce", _FORBIDDEN)
@pytest.mark.parametrize("prop, location", _HOST_PROPS)
def test_a_table_config_storage_host_must_pass_the_egress_check(server, filesystems, monkeypatch,
                                                                prop, location, value, enforce):
    from connectors.iceberg_catalog import load_catalog

    if enforce:
        monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", enforce)
    catalog = load_catalog(_endpoint())
    with pytest.raises(ValueError) as caught:
        _open(catalog, location, {**_VENDED, prop: _bare(prop, value)})
    _refused(str(caught.value), prop, "table configuration the catalog returned")
    assert filesystems == []


def test_a_public_server_endpoint_is_dialled(server, filesystems):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["s3.endpoint"] = "https://52.216.8.10"
    with pytest.raises(Dialled):
        _open(load_catalog(_endpoint()), "s3://bucket/t")
    assert filesystems[0][1]["endpoint_override"] == "https://52.216.8.10"


def test_the_connectors_own_endpoint_is_its_own_choice(server, filesystems, monkeypatch):
    """A value the connector set itself was judged as the connector's; the server echoing it is no new host."""
    from connectors.iceberg_catalog import load_catalog

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    server.overrides["s3.endpoint"] = "http://10.0.0.5:9000"
    with pytest.raises(Dialled):
        _open(load_catalog(_endpoint(**{"s3.endpoint": "http://10.0.0.5:9000"})), "s3://bucket/t")


def test_without_the_enforce_flag_a_private_server_endpoint_is_audited_like_the_connectors_own(
    server, filesystems
):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["s3.endpoint"] = "http://10.0.0.5:9000"
    with pytest.raises(Dialled):
        _open(load_catalog(_endpoint()), "s3://bucket/t")


def test_a_foreign_oauth_host_is_refused_before_any_token_request(server):
    from connectors.iceberg_catalog import load_catalog

    server.overrides["oauth2-server-uri"] = "https://evil.example/t"
    with pytest.raises(ValueError):
        load_catalog(_endpoint(credential=_CRED))
    assert [path for _m, _s, _h, _p, path, _b in server.requests] == ["/v1/v1/oauth/tokens", "/v1/v1/config"]
