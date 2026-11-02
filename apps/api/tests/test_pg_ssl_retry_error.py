"""CI-1 regression, kept under strict TLS - a required-TLS connection has no retry.

History: ``ssl=True`` -> ``sslmode=require`` failed with "server does not
support SSL", a ``prefer`` retry then failed for the *real* reason, and the
stale SSL message masked it. A required-TLS connection is now never retried in
another mode, so the one error raised is the real one.
"""

from __future__ import annotations

import psycopg2
import pytest

import connectors.postgresql_conn as pgc


def test_required_tls_failure_is_reported_once_and_not_retried(monkeypatch):
    calls: list[str] = []

    def fake_connect(**kwargs):
        calls.append(kwargs["sslmode"])
        raise psycopg2.Error("password authentication failed for user \"df\"")

    monkeypatch.setattr(psycopg2, "connect", fake_connect)
    with pytest.raises(psycopg2.Error, match="password authentication failed"):
        pgc.get_connection(
            host="db.example.com",
            port=5432,
            database="d",
            username="df",
            password="pw",
            connection_string="",
            ssl=True,
        )
    assert calls == ["require"]
