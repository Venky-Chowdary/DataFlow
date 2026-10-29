"""The newer egress sites audit private hosts by default and refuse metadata always.

Production (37c0e268) enforced the egress policy at connector Test, the SQL
drivers, CDC and webhooks. Connector save/update, transfer reads and writes,
introspection, precount, shared HTTP/SMTP/Mongo clients and redirects are newer:
a private or loopback target there logs ``egress.would_block`` and is allowed
unless ``DATAFLOW_EGRESS_ENFORCE_NEW=1``. Metadata and link-local are refused
everywhere. Saving an existing connector with an unchanged private host is never
refused for being private.
"""

from __future__ import annotations

import socket
import threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from services import would_apply_audit
from services.egress_guard import (
    EgressRefused,
    assert_connector_dial_allowed,
    connector_dial_scope,
    connector_save_refusal,
    follow_url_allowed,
    host_class,
    pinned_egress,
)

METADATA = "169.254.169.254"
PRIVATE = "10.0.0.5"
LOOPBACK = "127.0.0.1"


@pytest.fixture(autouse=True)
def audit_events(monkeypatch):
    monkeypatch.delenv("DATAFLOW_EGRESS_ENFORCE_NEW", raising=False)
    monkeypatch.delenv("DATAWRAP_EGRESS_ENFORCE_NEW", raising=False)
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    would_apply_audit.reset_dedupe()
    events: list[dict] = []
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw))
    return events


def test_new_point_audits_private_host_and_allows(audit_events):
    assert_connector_dial_allowed(
        f"http://{PRIVATE}:9200", point="http_client", workspace_id="ws-1", connector="es-prod"
    )
    assert len(audit_events) == 1
    event = audit_events[0]
    assert event["action"] == "egress.would_block"
    assert event["workspace_id"] == "ws-1"
    assert event["resource"] == "connector:es-prod"
    assert event["details"]["host"] == PRIVATE
    assert event["details"]["point"] == "http_client"
    assert event["details"]["host_class"] == "private"
    assert "10.x" in event["details"]["reason"]


def test_production_site_without_point_still_refuses_private(audit_events):
    with pytest.raises(EgressRefused):
        assert_connector_dial_allowed(f"http://{PRIVATE}:9200")
    assert audit_events == []


@pytest.mark.parametrize("host", [METADATA, "metadata.google.internal", "fe80::1"])
def test_new_point_always_refuses_metadata_and_link_local(host, audit_events):
    with pytest.raises(EgressRefused):
        assert_connector_dial_allowed(f"http://[{host}]/" if ":" in host else f"http://{host}/", point="smtp_client")
    assert audit_events == []


def test_enforce_flag_makes_new_points_refuse_private(monkeypatch, audit_events):
    monkeypatch.setenv("DATAWRAP_EGRESS_ENFORCE_NEW", "1")
    with pytest.raises(EgressRefused):
        assert_connector_dial_allowed(f"http://{LOOPBACK}:6333", point="http_client")
    assert audit_events == []


def test_audit_is_deduplicated_per_connector_and_host(audit_events):
    for _ in range(5):
        assert_connector_dial_allowed(f"http://{PRIVATE}", point="http_client", connector="a")
    assert_connector_dial_allowed(f"http://{PRIVATE}", point="http_client", connector="b")
    assert [e["resource"] for e in audit_events] == ["connector:a", "connector:b"]


def test_audit_write_failure_never_blocks_the_dial(monkeypatch):
    import services.audit_log as audit_log

    def boom(**_kw):
        raise OSError("disk full")

    monkeypatch.setattr(audit_log, "append_audit_event", boom)
    assert_connector_dial_allowed(f"http://{PRIVATE}", point="http_client")


def test_dial_scope_allows_loopback_connect_and_audits(audit_events):
    server = socket.socket()
    server.bind((LOOPBACK, 0))
    server.listen(1)
    port = server.getsockname()[1]
    accepted = threading.Thread(target=server.accept, daemon=True)
    accepted.start()
    try:
        cfg = {"type": "rest_api", "host": f"http://{LOOPBACK}:{port}", "name": "lab-api"}
        with connector_dial_scope(cfg, point="transfer"):
            client = socket.create_connection((LOOPBACK, port), timeout=5)
            client.close()
    finally:
        server.close()
    assert {e["details"]["point"] for e in audit_events} == {"transfer"}
    assert {e["resource"] for e in audit_events} == {"connector:lab-api"}


def test_dial_scope_refuses_metadata_at_connect_even_when_audit_only(monkeypatch):
    real = socket.getaddrinfo

    def rebind(host, port, *args, **kwargs):
        if host == "rebind.example":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (METADATA, port or 0))]
        return real(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", rebind)
    with connector_dial_scope({"type": "rest_api", "host": "https://example.com"}, point="transfer"):
        with pytest.raises(EgressRefused):
            socket.getaddrinfo("rebind.example", 443)


def test_dial_scope_audit_only_for_private_dns_answer(monkeypatch, audit_events):
    real = socket.getaddrinfo

    def private(host, port, *args, **kwargs):
        if host == "db.corp.example":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PRIVATE, port or 0))]
        return real(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", private)
    with pinned_egress(point="transfer"):
        assert socket.getaddrinfo("db.corp.example", 5432)[0][4][0] == PRIVATE
    assert [e["details"]["host"] for e in audit_events] == ["db.corp.example"]
    with pytest.raises(EgressRefused):
        with pinned_egress():
            socket.getaddrinfo("db.corp.example", 5432)


def test_internal_name_is_audited_not_blocked_at_new_points(audit_events):
    assert_connector_dial_allowed("http://svc.corp.internal:8080", point="http_client")
    assert audit_events[0]["details"]["reason"] == "private network name"
    with pytest.raises(EgressRefused):
        assert_connector_dial_allowed("http://svc.corp.internal:8080")


def test_redirect_follow_private_is_audited_metadata_refused(audit_events):
    origin = "https://api.example.com/v1"
    assert follow_url_allowed(f"http://{PRIVATE}/next", origin) is True
    assert [e["details"]["point"] for e in audit_events] == ["redirect"]
    assert follow_url_allowed(f"http://{METADATA}/latest", origin) is False
    assert follow_url_allowed("file:///etc/passwd", origin) is False


def test_redirect_follow_private_refused_under_flag(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "true")
    assert follow_url_allowed(f"http://{PRIVATE}/next", "https://api.example.com/v1") is False


def test_save_new_private_connector_is_allowed_and_audited(audit_events):
    data = {"name": "warehouse", "type": "postgresql", "host": PRIVATE, "workspace_id": "ws-1"}
    assert connector_save_refusal(data) == ""
    assert audit_events[0]["details"]["point"] == "connector_save"
    assert audit_events[0]["resource"] == "connector:warehouse"


def test_save_metadata_host_is_always_refused(monkeypatch):
    data = {"name": "x", "type": "postgresql", "host": METADATA}
    assert "cloud metadata" in connector_save_refusal(data)
    existing = SimpleNamespace(host=METADATA, endpoint_url="", connection_string="")
    assert "cloud metadata" in connector_save_refusal(data, existing)


def test_edit_or_rename_with_unchanged_private_host_is_never_refused(monkeypatch, audit_events):
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    existing = SimpleNamespace(host=PRIVATE, endpoint_url="", connection_string="")
    renamed = {"name": "renamed", "type": "postgresql", "host": PRIVATE}
    assert connector_save_refusal(renamed, existing) == ""
    assert audit_events == []
    moved = {"name": "renamed", "type": "postgresql", "host": "10.0.0.6"}
    assert "10.x" in connector_save_refusal(moved, existing)


def test_saved_connector_rename_route_keeps_private_connector(monkeypatch):
    from src.routers import saved_connectors_router as router

    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    existing = SimpleNamespace(host=PRIVATE, endpoint_url="", connection_string="")
    router._refuse_unsafe_egress({"name": "renamed", "type": "postgresql", "host": PRIVATE}, existing)
    with pytest.raises(HTTPException) as exc:
        router._refuse_unsafe_egress({"name": "n", "type": "postgresql", "host": "10.9.9.9"}, existing)
    assert exc.value.status_code == 400


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (LOOPBACK, "loopback"),
        (PRIVATE, "private"),
        ("100.64.1.1", "cgnat"),
        ("169.254.10.10", "link-local"),
        (METADATA, "metadata"),
        ("metadata.google.internal", "metadata"),
        ("8.8.8.8", "public"),
    ],
)
def test_host_class(host, expected):
    assert host_class(host) == expected
