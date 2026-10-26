"""CI-1 regression — a failed sslmode retry must surface the retry's error.

Pre-fix: ``ssl=True`` -> ``sslmode=require`` failed with "server does not
support SSL", the ``prefer`` retry failed for the *real* reason (auth,
refused, timeout), and ``get_connection`` raised the first error — the stale
SSL message masked the true failure in CI's PG-SSL legs.
"""

from __future__ import annotations

import psycopg2
import pytest

import connectors.postgresql_conn as pgc


def test_failed_ssl_retry_raises_the_retry_error_not_the_stale_first(monkeypatch):
    calls: list[str] = []

    def fake_connect(**kwargs):
        calls.append(kwargs["sslmode"])
        if kwargs["sslmode"] == "require":
            raise psycopg2.Error("server does not support SSL, but SSL was required")
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
    assert calls == ["require", "prefer"]
