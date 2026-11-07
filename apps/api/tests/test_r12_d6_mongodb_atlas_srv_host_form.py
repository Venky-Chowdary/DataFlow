"""QA r12 MongoDB D6: the host/port form must reach an Atlas (SRV) cluster.

QA (production 8f7d246d): host ``datawrap-qa.nlrrxmc.mongodb.net`` plus
user/password in the host/port form gave "Connection timed out". An Atlas
cluster name is an SRV record (``_mongodb._tcp.<name>``), not a host listening
on :27017; the form built ``mongodb://<name>:27017`` and dialled nothing.

Now the form builds ``mongodb+srv://<name>/?tls=true`` for ``*.mongodb.net``
cluster names (with no port or the form's default 27017) and for any DNS host
given with no port whose SRV record resolves, and a failed probe names the
``mongodb+srv://`` fix. Real Atlas cannot be dialled from this sandbox, so the
URI building and detection are unit-tested here with a resolver stub.
"""

from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlsplit

import pytest

import connectors.mongodb_common as common

ATLAS = "datawrap-qa.nlrrxmc.mongodb.net"


class _Resolver:
    def __init__(self, *srv_names: str) -> None:
        self.srv = set(srv_names)
        self.asked: list[str] = []

    def __call__(self, name: str) -> bool:
        self.asked.append(name)
        return name in self.srv


def _install(monkeypatch, *srv_names: str) -> _Resolver:
    """Stub the SRV lookup (never real DNS) and clear the per-process cache."""
    resolver = _Resolver(*srv_names)
    monkeypatch.setattr(common, "_dns_srv_record_exists", resolver, raising=False)
    monkeypatch.setattr(common, "_srv_cache", {}, raising=False)
    return resolver


def normalize_mongodb_connection_string(*args, **kwargs):
    return common.normalize_mongodb_connection_string(*args, **kwargs)


def _parts(uri: str):
    split = urlsplit(uri)
    return split, {k: v[0] for k, v in parse_qs(split.query).items()}


@pytest.mark.parametrize("port", [0, 27017])
def test_atlas_cluster_name_builds_srv_uri_with_tls(monkeypatch, port):
    resolver = _install(monkeypatch)
    uri = normalize_mongodb_connection_string(
        "", database="shop", host=ATLAS, port=port, username="qa user",
        password="p@ss:w/rd",
    )
    split, qs = _parts(uri)
    assert split.scheme == "mongodb+srv", uri
    assert split.hostname == ATLAS and split.port is None, uri
    assert unquote(split.username) == "qa user", uri
    assert unquote(split.password) == "p@ss:w/rd", uri
    assert split.path == "/shop"
    assert qs["tls"] == "true" and qs["authSource"] == "admin", qs
    assert ":27017" not in uri
    # An Atlas name needs no DNS round trip to decide.
    assert resolver.asked == []


def test_atlas_per_node_host_stays_direct_with_tls(monkeypatch):
    _install(monkeypatch)
    host = "datawrap-qa-shard-00-00.nlrrxmc.mongodb.net"
    uri = normalize_mongodb_connection_string(
        "", host=host, port=27017, username="u", password="p",
    )
    split, qs = _parts(uri)
    assert split.scheme == "mongodb" and split.port == 27017, uri
    assert qs.get("ssl") == "true" or qs.get("tls") == "true", qs


def test_atlas_name_on_a_non_default_port_is_not_srv(monkeypatch):
    _install(monkeypatch)
    uri = normalize_mongodb_connection_string("", host=ATLAS, port=27018)
    assert uri.startswith(f"mongodb://{ATLAS}:27018/"), uri


def test_portless_host_with_srv_record_builds_srv_uri(monkeypatch):
    resolver = _install(monkeypatch, "_mongodb._tcp.mongo.example.com")
    uri = normalize_mongodb_connection_string(
        "", host="mongo.example.com", port=0, username="u", password="p",
        
    )
    split, qs = _parts(uri)
    assert split.scheme == "mongodb+srv" and split.hostname == "mongo.example.com", uri
    assert qs["tls"] == "true"
    assert resolver.asked == ["_mongodb._tcp.mongo.example.com"]


def test_portless_host_without_srv_record_stays_direct(monkeypatch):
    resolver = _install(monkeypatch)
    uri = normalize_mongodb_connection_string(
        "", host="mongo.example.com", port=0,
    )
    split, _ = _parts(uri)
    assert split.scheme == "mongodb" and split.port == 27017, uri
    assert resolver.asked == ["_mongodb._tcp.mongo.example.com"]


@pytest.mark.parametrize(
    "host,port",
    [("mongo.example.com", 27017), ("localhost", 0), ("127.0.0.1", 0), ("mongo", 0), ("10.0.0.5", 0)],
)
def test_explicit_port_ip_and_local_names_never_look_up_srv(monkeypatch, host, port):
    resolver = _install(monkeypatch, f"_mongodb._tcp.{host}")
    uri = normalize_mongodb_connection_string("", host=host, port=port)
    assert uri.startswith("mongodb://"), uri
    assert resolver.asked == []


def test_pasted_connection_string_is_untouched(monkeypatch):
    _install(monkeypatch)
    pasted = f"mongodb+srv://u:p@{ATLAS}/?retryWrites=true&w=majority"
    uri = normalize_mongodb_connection_string(pasted)
    split, qs = _parts(uri)
    assert split.scheme == "mongodb+srv" and qs["retryWrites"] == "true"
    direct = "mongodb://u:p@db.example.com:27017/"
    assert normalize_mongodb_connection_string(direct).startswith("mongodb://u:p@db.example.com:27017/")


def test_hint_names_the_srv_connection_string():
    mongodb_srv_hint = getattr(common, "mongodb_srv_hint", lambda *a: "")
    assert "mongodb+srv://" in mongodb_srv_hint(ATLAS, 27017)
    assert "mongodb+srv://" in mongodb_srv_hint("mongo.example.com", 0)
    assert mongodb_srv_hint("mongo.example.com", 27017) == ""
    assert mongodb_srv_hint("127.0.0.1", 0) == ""


def test_probe_dials_srv_and_a_timeout_names_the_fix(monkeypatch):
    """The Test-connection probe dials the SRV URI; a failure says what to paste."""
    from src.transfer.connector_registry import _run_probe_impl

    _install(monkeypatch)

    dialled: list[str] = []

    def fake_client(conn_str: str):
        dialled.append(conn_str)
        raise TimeoutError("No servers found yet, Timeout: 10.0s (ServerSelectionTimeoutError)")

    monkeypatch.setattr(common, "_mongo_client", fake_client)
    monkeypatch.setattr(
        "services.egress_guard.metadata_endpoint_refusal", lambda *a, **k: "", raising=False
    )
    ok, message = _run_probe_impl(
        "mongodb",
        {"type": "mongodb", "host": ATLAS, "port": 27017, "username": "u",
         "password": "p", "database": "shop"},
    )
    assert not ok
    assert dialled and all(u.startswith(f"mongodb+srv://u:p@{ATLAS}/") for u in dialled), dialled
    assert "mongodb+srv://" in message, message
