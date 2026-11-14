"""Follow-up review 3 (M2): a REST catalog server cannot redirect the connector's requests to another host.

``iceberg_server_hosts.check_server_oauth_uri`` keeps the OAuth2 token request
on the catalog's own origin, but pyiceberg's ``requests.Session`` followed
redirects: a 307 / 308 answer to the token POST re-sent the body, client secret
included, to any host, and every catalog GET followed a redirect off the
catalog's origin as well. The guarded REST catalog's session now refuses a
redirect whose target is not the catalog's own origin (scheme, host, port)
before it is followed.
"""

from __future__ import annotations

import json
import socket
from urllib.parse import urlsplit

import pytest

pytest.importorskip("pyiceberg.catalog.rest")

SECRET = "tenant-client-secret"
_CATALOG = "https://catalog.example.com/v1"
_ORIGIN = "https://catalog.example.com:443"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    monkeypatch.setattr(
        socket, "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port or 0))],
    )


def _server(monkeypatch, *, token_to: str | None = None, config_to: str | None = None):
    """A fake catalog (requests' adapter): optional redirects on the token POST / the config GET."""
    import requests
    import requests.adapters

    log: list[tuple[str, str, str, bool]] = []

    def send(self, req, **_kw):
        parts = urlsplit(req.url)
        body = req.body if isinstance(req.body, str) else (req.body or b"").decode()
        origin = f"{parts.scheme}://{parts.hostname}:{parts.port or 443}"
        log.append((req.method, origin, parts.path, SECRET in (body or "")))
        response = requests.Response()
        response.url, response.request, response.connection = req.url, req, self
        response.headers["Content-Type"] = "application/json"
        response.status_code, response._content = 200, b"{}"
        on_catalog = origin == _ORIGIN
        if on_catalog and "token" in parts.path and token_to and not parts.path.endswith("/again"):
            response.status_code, response._content = 307, b""
            response.headers["Location"] = token_to
        elif on_catalog and parts.path.endswith("/config") and config_to:
            response.status_code, response._content = 302, b""
            response.headers["Location"] = config_to
        elif "token" in parts.path:
            response._content = json.dumps({"access_token": "t", "token_type": "bearer", "expires_in": 3600}).encode()
        else:
            response._content = json.dumps({"defaults": {}, "overrides": {}}).encode()
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return log


def _load():
    from connectors.iceberg_catalog import load_catalog

    return load_catalog({
        "type": "iceberg", "name": "ice", "schema": "ns", "table": "t", "connection_string": _CATALOG,
        "extra": {"catalog_type": "rest", "credential": f"tenant-client:{SECRET}",
                  "s3.access-key-id": "AKIATENANT", "s3.secret-access-key": "x", "s3.region": "us-east-1"},
    })


@pytest.mark.parametrize("target", ["https://evil.example/collect", "http://catalog.example.com/v1/v1/oauth/tokens",
                                     "https://catalog.example.com:8443/v1/v1/oauth/tokens", "//evil.example/x"])
def test_the_token_request_is_not_redirected_off_the_catalog(monkeypatch, target):
    log = _server(monkeypatch, token_to=target)
    with pytest.raises(ValueError) as caught:
        _load()
    assert "evil" not in str(caught.value) and SECRET not in str(caught.value)
    assert [entry for entry in log if entry[1] != _ORIGIN] == []
    assert len([entry for entry in log if entry[3]]) == 1, log  # the secret went out once, to the catalog


def test_a_catalog_get_is_not_redirected_off_the_catalog(monkeypatch):
    log = _server(monkeypatch, config_to="https://evil.example/config")
    with pytest.raises(ValueError):
        _load()
    assert [entry for entry in log if entry[1] != _ORIGIN] == []


def test_a_redirect_on_the_catalogs_own_origin_is_followed(monkeypatch):
    log = _server(monkeypatch, token_to="https://catalog.example.com/v1/oauth/tokens/again")
    _load()
    assert ("POST", _ORIGIN, "/v1/oauth/tokens/again", True) in log
    assert all(entry[1] == _ORIGIN for entry in log)
