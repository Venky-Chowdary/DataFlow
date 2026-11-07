"""The API's own store stays dialable inside a connector egress scope, even when
the platform names it ``*.internal`` (Railway private networking)."""

from __future__ import annotations

import socket

import pytest

from services.egress_guard import EgressRefused, pinned_egress

PRIVATE = "10.1.2.3"


@pytest.fixture(autouse=True)
def _private_dns(monkeypatch):
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (PRIVATE, port or 0))],
    )


def test_configured_internal_control_plane_name_is_not_refused(monkeypatch):
    monkeypatch.setenv("MONGODB_URL", "mongodb://mongo.railway.internal:27017/x")
    with pinned_egress(allowlist=()):
        assert socket.getaddrinfo("mongo.railway.internal", 27017)[0][4][0] == PRIVATE


def test_unconfigured_internal_name_is_still_refused(monkeypatch):
    monkeypatch.setenv("MONGODB_URL", "mongodb://mongo.railway.internal:27017/x")
    with pinned_egress(allowlist=()):
        with pytest.raises(EgressRefused):
            socket.getaddrinfo("other.railway.internal", 27017)


def test_metadata_name_is_never_a_control_plane_exemption(monkeypatch):
    monkeypatch.setenv("MONGODB_URL", "mongodb://metadata.google.internal:27017/x")
    with pinned_egress(allowlist=()):
        with pytest.raises(EgressRefused):
            socket.getaddrinfo("metadata.google.internal", 80)
