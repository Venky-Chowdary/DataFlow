"""127.0.0.0/8 covers the name localhost, not every other hostname."""

from __future__ import annotations

import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.egress_guard import classify_host


def test_loopback_cidr_covers_localhost_only(monkeypatch):
    real = socket.getaddrinfo

    def fake(host, port, *args, **kwargs):
        shown = str(host)
        if shown in {"localhost", "other.example"}:
            return [
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 0, 0, 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0)),
            ]
        return real(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", fake)
    assert classify_host("localhost", allowlist=["127.0.0.0/8"]) is None
    assert classify_host("localhost", allowlist=()) == "loopback"
    assert classify_host("other.example", allowlist=["127.0.0.0/8"]) == "loopback"
    assert classify_host("10.1.2.3", allowlist=["127.0.0.0/8"]) is not None
