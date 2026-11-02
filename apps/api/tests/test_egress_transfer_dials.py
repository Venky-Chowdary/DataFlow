"""Egress policy on transfer, introspect, precount, shared clients and save.

The probe path was the only place the policy ran. These tests pin the same
refusal on every other dial a user-controlled connector config can steer.

These run with ``DATAFLOW_EGRESS_ENFORCE_NEW=1``: the newer sites are
audit-only by default (``test_egress_audit_only_new_points.py``).
"""

from __future__ import annotations

import socket
import threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from services.egress_guard import (
    EgressRefused,
    follow_url_allowed,
    pinned_egress,
    redirect_guard_hook,
)

METADATA = "169.254.169.254"
PRIVATE = "10.0.0.5"
PUBLIC = "93.184.216.34"


@pytest.fixture(autouse=True)
def _enforce_new_points(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")


def _public_dns(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PUBLIC, port or 0))]


def test_read_via_registry_refuses_metadata_before_reader(monkeypatch):
    from src.transfer import connector_dispatch

    called: list[dict] = []
    monkeypatch.setattr(connector_dispatch, "load_reader", lambda d: lambda **kw: called.append(kw))
    with pytest.raises(EgressRefused):
        connector_dispatch.read_via_registry(
            "rest_api", cfg={"type": "rest_api", "host": f"http://{METADATA}/latest"}, table="x"
        )
    assert called == []


def test_write_via_registry_refuses_private_before_writer(monkeypatch):
    from src.transfer import connector_dispatch

    called: list[dict] = []
    monkeypatch.setattr(connector_dispatch, "load_writer", lambda d: lambda **kw: called.append(kw))
    with pytest.raises(EgressRefused):
        connector_dispatch.write_via_registry("mongodb", common={"connection_string": f"mongodb://{PRIVATE}:27017"})
    assert called == []


def test_read_batch_impl_refuses_metadata_before_driver():
    from src.transfer.batch_readers import _read_batch_impl

    with pytest.raises(EgressRefused):
        _read_batch_impl("couchbase", {"type": "couchbase", "host": METADATA}, "t", None, 0, 10)


def test_transfer_dial_refuses_dns_rebind_at_connect_time(monkeypatch):
    """Public at the pre-dial check, metadata on the lookup the driver uses."""
    from src.transfer import connector_dispatch

    answers = iter([PUBLIC, METADATA])

    def rebinding(host, port, *args, **kwargs):
        if str(host) == "rebind.example":
            ip = next(answers, METADATA)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port or 0))]
        return _public_dns(host, port)

    monkeypatch.setattr(socket, "getaddrinfo", rebinding)

    def reader(**_kw):
        return socket.getaddrinfo("rebind.example", 443)

    monkeypatch.setattr(connector_dispatch, "load_reader", lambda d: reader)
    with pytest.raises(EgressRefused):
        connector_dispatch.read_via_registry(
            "rest_api", cfg={"type": "rest_api", "host": "https://rebind.example"}, table="x"
        )


def test_pinned_egress_is_scoped_to_its_own_context(monkeypatch):
    """A transfer scope must not refuse control-plane dials on other threads."""
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PRIVATE, port or 0))],
    )
    inside = threading.Event()
    release = threading.Event()
    other: list[object] = []

    def scoped():
        with pinned_egress(allowlist=()):
            inside.set()
            release.wait(5)

    t = threading.Thread(target=scoped)
    t.start()
    assert inside.wait(5)
    try:
        other.append(socket.getaddrinfo("mongo.private", 27017))
        with pytest.raises(EgressRefused):
            with pinned_egress(allowlist=()):
                socket.getaddrinfo("mongo.private", 27017)
    finally:
        release.set()
        t.join(5)
    assert other and other[0][0][4][0] == PRIVATE


def test_guarded_mongo_client_refuses_private_and_metadata():
    from connectors.mongodb_common import _new_mongo_client, guarded_mongo_client

    for uri in (f"mongodb://{METADATA}:27017", f"mongodb://u:p@{PRIVATE}:27017/db"):
        with pytest.raises(EgressRefused):
            guarded_mongo_client(uri, serverSelectionTimeoutMS=10)
        with pytest.raises(EgressRefused):
            _new_mongo_client(uri)


def test_precount_mongo_refuses_metadata():
    from services import dest_precount

    with pytest.raises(EgressRefused):
        dest_precount._mongodb_key_list(
            {"type": "mongodb", "host": METADATA, "port": 27017, "database": "d"},
            schema="",
            table_name="t",
            cols=["_id"],
        )


def test_introspect_refuses_metadata_before_dial():
    from services.schema_introspect import _introspect_schema

    with pytest.raises(EgressRefused):
        _introspect_schema("mongodb", host=METADATA, port=27017, database="d")
    with pytest.raises(EgressRefused):
        _introspect_schema("weaviate", host=PRIVATE, port=8080)


def test_introspect_endpoint_reports_refusal():
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    out = introspect_endpoint(EndpointConfig.from_dict("database", {"format": "mongodb", "host": METADATA}))
    assert out["connected"] is False
    assert "169.254.169.254" in out["error"]


@pytest.mark.parametrize(
    "build",
    [
        lambda: __import__("connectors.redis_reader", fromlist=["x"])._redis_client({"host": METADATA}),
        lambda: __import__("connectors.redis_reader", fromlist=["x"])._redis_client(
            {"connection_string": f"redis://{PRIVATE}:6379/0"}
        ),
        lambda: __import__("connectors.qdrant_writer", fromlist=["x"])._base_url(PRIVATE, 6333, False),
        lambda: __import__("connectors.pinecone_writer", fromlist=["x"])._index_url(f"http://{METADATA}"),
        lambda: __import__("connectors.weaviate_writer", fromlist=["x"])._base_url("", 0, False, f"http://{PRIVATE}:8080"),
        lambda: __import__("connectors.couchbase", fromlist=["x"])._url(METADATA, 8093, False),
        lambda: __import__("connectors.influxdb", fromlist=["x"])._url(PRIVATE, 8086, False),
        lambda: __import__("connectors.elasticsearch_reader", fromlist=["x"])._client({"host": METADATA, "port": 9200}),
        lambda: __import__("connectors.gcs_common", fromlist=["x"]).gcs_client(
            {"connection_string": f"http://{METADATA}/storage"}
        ),
    ],
)
def test_shared_client_constructors_refuse_blocked_hosts(build):
    with pytest.raises(EgressRefused):
        build()


def test_schema_registry_refuses_metadata():
    from connectors.confluent_schema_registry import SchemaRegistryError, fetch_schema, register_json_schema

    with pytest.raises(SchemaRegistryError, match="169.254.169.254"):
        fetch_schema(f"http://{METADATA}", 1)
    with pytest.raises(SchemaRegistryError):
        register_json_schema(f"http://{PRIVATE}:8081", "s-value", "{}")


def test_email_write_refuses_private_smtp(monkeypatch):
    import smtplib

    from connectors import email as email_mod

    def boom(*_a, **_k):  # pragma: no cover - must never be reached
        raise AssertionError("SMTP dialed a refused host")

    monkeypatch.setattr(smtplib, "SMTP", boom)
    monkeypatch.setattr(smtplib, "SMTP_SSL", boom)
    res = email_mod.write_mapped_rows(
        connection_string=f"smtp://{PRIVATE}:25?to=a@b.co&from=a@b.co",
        headers=["a"],
        data_rows=[["1"]],
        mappings=[{"source": "a", "target": "a"}],
        column_types={"a": "VARCHAR"},
    )
    assert res.ok is False


def test_rest_api_next_url_cannot_leave_for_blocked_host():
    from connectors.rest_api import _get_url

    cfg = {"host": f"http://{PUBLIC}/api", "object_path": "items"}
    assert _get_url(cfg, {}, "/api/items?page=2") == f"http://{PUBLIC}/api/items?page=2"
    for bad in (f"http://{METADATA}/latest/meta-data", f"http://{PRIVATE}/x", "file:///etc/passwd"):
        with pytest.raises(EgressRefused):
            _get_url(cfg, {}, bad)


def test_rest_api_request_refuses_metadata_without_socket(monkeypatch):
    import requests

    from connectors.rest_api import _request_with_retry

    monkeypatch.setattr(requests, "request", lambda *a, **k: pytest.fail("dialed"))
    with pytest.raises(EgressRefused):
        _request_with_retry("GET", f"http://{METADATA}/latest", max_retries=1)


def test_redirect_hook_refuses_private_targets():
    hook = redirect_guard_hook(f"https://{PUBLIC}/v1")
    for location in (f"http://{METADATA}/", f"http://{PRIVATE}:8080/admin", "http://localhost/"):
        resp = SimpleNamespace(is_redirect=True, headers={"Location": location}, url=f"https://{PUBLIC}/v1")
        with pytest.raises(EgressRefused):
            hook(resp)
    same = SimpleNamespace(is_redirect=True, headers={"Location": "/v2"}, url=f"https://{PUBLIC}/v1")
    assert hook(same) is same
    assert follow_url_allowed(f"https://{PUBLIC}/next", f"https://{PUBLIC}/v1")


@pytest.mark.parametrize(
    "host",
    [
        "https://10.0.0.5/x.zendesk.com",
        "https://acme.zendesk.com.evil.io",
        "http://acme.zendesk.com",
        "https://user@acme.zendesk.com",
        "https://acme.zendesk.com:8443",
        "https://acme.zendesk.com/api",
        "https://evilzendesk.com",
        "",
    ],
)
def test_zendesk_origin_requires_exact_https_host(host):
    from connectors.zendesk_writer import zendesk_origin

    assert zendesk_origin(host) == ""


def test_zendesk_url_is_exact_origin_and_safe_resource():
    from connectors.zendesk_writer import _make_url, zendesk_origin

    assert zendesk_origin("acme.zendesk.com") == "https://acme.zendesk.com"
    assert _make_url("https://Acme.zendesk.com/", "tickets", "7") == "https://acme.zendesk.com/api/v2/tickets/7.json"
    for table in ("../../admin", "tickets?x=1", "tickets/../users"):
        with pytest.raises(ValueError):
            _make_url("acme.zendesk.com", table)
    assert _make_url("acme.zendesk.com", "users", "../1").endswith("/users/..%2F1.json")


def test_zendesk_writer_refuses_substring_host():
    from connectors.zendesk_writer import write_mapped_rows

    res = write_mapped_rows(
        host="https://10.0.0.5/x.zendesk.com",
        table_name="tickets",
        headers=["subject"],
        data_rows=[["a"]],
        mappings=[{"source": "subject", "target": "subject"}],
        api_key="k",
    )
    assert res.ok is False


def test_zendesk_next_page_must_stay_on_origin():
    from connectors.zendesk import _same_origin_next

    first = "https://acme.zendesk.com/api/v2/ticket_fields.json"
    assert _same_origin_next("https://acme.zendesk.com/api/v2/ticket_fields.json?page=2", first)
    with pytest.raises(RuntimeError):
        _same_origin_next(f"http://{METADATA}/latest", first)


@pytest.mark.parametrize(
    "module", ["src.routers.connectors_router", "src.routers.saved_connectors_router"]
)
def test_connector_save_refuses_blocked_hosts(module):
    mod = __import__(module, fromlist=["_refuse_unsafe_egress"])
    for data in (
        {"type": "postgresql", "host": METADATA},
        {"type": "mongodb", "connection_string": f"mongodb://{PRIVATE}:27017"},
        {"type": "s3", "endpoint_url": "http://metadata.google.internal"},
    ):
        with pytest.raises(HTTPException) as exc:
            mod._refuse_unsafe_egress(data)
        assert exc.value.status_code == 400
    mod._refuse_unsafe_egress({"type": "sqlite", "host": "localhost"})
    mod._refuse_unsafe_egress({"type": "postgresql", "host": "127.0.0.1"})
