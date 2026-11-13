"""Follow-up review 3 (MongoDB OIDC, high): a tenant's MongoDB connector authenticates only with its own secrets.

``mongodb://host/d?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s``
makes PyMongo read the API pod's Kubernetes service-account token
(``/var/run/secrets/kubernetes.io/serviceaccount/token``; ``ENVIRONMENT:azure``
/ ``gcp`` mint a token from the host's instance metadata service,
``ENVIRONMENT:test`` reads ``OIDC_TOKEN_FILE``) and send it to the tenant's
server. ``MONGODB-AWS`` signs with the host's ambient AWS credentials and
``GSSAPI`` uses the host's Kerberos ticket cache / keytab.

``authMechanism`` is now an allowlist for connector URIs, client options and
settings: DEFAULT, SCRAM-SHA-1, SCRAM-SHA-256, PLAIN, MONGODB-X509 (its
certificate is confined by the TLS-file rule), refused at save and in
``guarded_mongo_client``.
"""

from __future__ import annotations

import socket

import pytest


class Dialled(Exception):
    """MongoClient was constructed (recorded, never a socket)."""


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("SOURCE_PATH_ROOTS", "EGRESS_ENFORCE_NEW", "EGRESS_ALLOWLIST"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setattr(
        "socket.socket.connect",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled")),
    )
    monkeypatch.setattr(
        socket, "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port or 0))],
    )


@pytest.fixture
def client(monkeypatch):
    import pymongo

    calls: list[tuple[tuple, dict]] = []

    class Recorder:
        def __init__(self, *args, **kwargs):
            calls.append((args, kwargs))
            raise Dialled("MongoClient")

    monkeypatch.setattr(pymongo, "MongoClient", Recorder)
    return calls


_REFUSED_URIS = [
    pytest.param("mongodb://db1.example.com:27017/d?authMechanism=MONGODB-OIDC"
                 "&authMechanismProperties=ENVIRONMENT:k8s", id="oidc-k8s"),
    pytest.param("mongodb://db1.example.com:27017/d?authMechanism=MONGODB-OIDC"
                 "&authMechanismProperties=ENVIRONMENT:azure,TOKEN_RESOURCE:api%3A%2F%2Fx", id="oidc-azure"),
    pytest.param("mongodb://db1.example.com/d?authmechanism=mongodb-oidc"
                 "&authmechanismproperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:x", id="oidc-gcp-lower-case"),
    pytest.param("mongodb://u:p@db1.example.com/d?authMechanism=MONGODB-AWS", id="aws-ambient"),
    pytest.param("mongodb://u%40EXAMPLE.COM@db1.example.com/d?authMechanism=GSSAPI", id="gssapi"),
    pytest.param("mongodb://db1.example.com/d?tls=true;authMechanism=MONGODB-OIDC;"
                 "authMechanismProperties=ENVIRONMENT:k8s", id="semicolon-delimited"),
    pytest.param("mongodb://db1.example.com/d?authMechanism=MONGODB%2DOIDC"
                 "&authMechanismProperties=ENVIRONMENT%3Ak8s", id="percent-encoded"),
    pytest.param("mongodb+srv://cluster.example.com/d?authMechanism=MONGODB-OIDC"
                 "&authMechanismProperties=ENVIRONMENT:k8s", id="srv"),
]


@pytest.mark.parametrize("uri", _REFUSED_URIS)
def test_save_refuses_an_ambient_auth_mechanism(uri):
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal({"type": "mongodb", "name": "x", "connection_string": uri})
    assert "authMechanism" in refusal, refusal


@pytest.mark.parametrize("uri", _REFUSED_URIS)
def test_the_client_refuses_an_ambient_auth_mechanism(client, uri):
    from connectors.mongodb_common import guarded_mongo_client

    with pytest.raises(ValueError):
        guarded_mongo_client(uri, serverSelectionTimeoutMS=1000)
    assert client == []


def test_the_client_refuses_the_mechanism_in_its_options(client):
    from connectors.mongodb_common import guarded_mongo_client

    with pytest.raises(ValueError):
        guarded_mongo_client("mongodb://db1.example.com/d", authMechanism="MONGODB-OIDC",
                             authMechanismProperties={"ENVIRONMENT": "k8s"})
    assert client == []


@pytest.mark.parametrize("data", [
    pytest.param({"type": "mongodb", "name": "x", "host": "db1.example.com", "port": 27017,
                  "extra": {"authMechanism": "MONGODB-OIDC", "authMechanismProperties": "ENVIRONMENT:k8s"}},
                 id="extra"),
    pytest.param({"type": "mongodb", "name": "x", "host": "db1.example.com", "port": 27017,
                  "extra": {"connect_args": {"auth_mechanism": "MONGODB-AWS"}}}, id="connect-args"),
    pytest.param({"type": "mongodb", "name": "x",
                  "host": "mongodb://db1.example.com/d?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
                  }, id="uri-in-host"),
])
def test_save_refuses_the_mechanism_in_settings(data):
    from services.egress_guard import connector_save_refusal

    assert "authMechanism" in connector_save_refusal(data)


@pytest.mark.parametrize("uri", [
    "mongodb://u:p@db1.example.com/d?authSource=admin",
    "mongodb://u:p@db1.example.com/d?authMechanism=SCRAM-SHA-256",
    "mongodb://u:p@db1.example.com/d?authMechanism=SCRAM-SHA-1",
    "mongodb://u:p@db1.example.com/d?authMechanism=PLAIN&authSource=%24external",
    "mongodb://db1.example.com/d?authMechanism=MONGODB-X509&tls=true",
])
def test_the_connectors_own_secrets_still_work(client, uri):
    from connectors.mongodb_common import guarded_mongo_client
    from services.egress_guard import connector_save_refusal

    assert connector_save_refusal({"type": "mongodb", "name": "x", "connection_string": uri}) == ""
    with pytest.raises(Dialled):
        guarded_mongo_client(uri)
    assert len(client) == 1
