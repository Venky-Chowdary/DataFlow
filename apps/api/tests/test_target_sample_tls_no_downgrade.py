"""Destination sample reads never downgrade a TLS-required PostgreSQL connector to plaintext."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors import postgresql_conn
from services.reconciliation import TargetSampleUnavailable, read_target_sample

_DEST = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "db",
    "username": "u",
    "password": "p",
}


def _read(db_type: str, dest: dict) -> list:
    return read_target_sample(
        db_type,
        dest,
        schema="public",
        table_name="t",
        columns=["id"],
        limit=10,
    )


@pytest.mark.parametrize("db_type", ["postgresql", "redshift"])
def test_ssl_required_sample_fails_closed_without_plaintext_retry(db_type):
    fake = MagicMock(side_effect=RuntimeError("SSL SYSCALL error: EOF detected"))
    with patch("connectors.postgresql_conn.get_connection", fake):
        with pytest.raises(TargetSampleUnavailable, match="plaintext fallback is not attempted"):
            _read(db_type, {**_DEST, "ssl": True})
    assert fake.call_count == 1
    assert fake.call_args.kwargs["ssl"] is True
    assert fake.call_args.kwargs["allow_ssl_downgrade"] is False


def test_ssl_required_sample_query_failure_does_not_retry_plaintext():
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError(
        "connection reset by peer"
    )
    fake = MagicMock(return_value=conn)
    with patch("connectors.postgresql_conn.get_connection", fake):
        with pytest.raises(TargetSampleUnavailable, match="connection reset by peer"):
            _read("postgresql", {**_DEST, "ssl": True})
    assert [c.kwargs["ssl"] for c in fake.call_args_list] == [True]


def test_non_ssl_sample_may_upgrade_to_tls():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.description = [("id",)]
    cur.fetchall.return_value = [(1,)]
    fake = MagicMock(side_effect=[RuntimeError("no pg_hba.conf entry ... no encryption"), conn])
    with patch("connectors.postgresql_conn.get_connection", fake):
        assert _read("postgresql", {**_DEST, "ssl": False}) == [{"id": 1}]
    assert [c.kwargs["ssl"] for c in fake.call_args_list] == [False, True]


def _no_ssl_error():
    import psycopg2

    return psycopg2.OperationalError("server does not support SSL, but SSL was required")


def _pg_kwargs(**overrides):
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "db",
        "username": "u",
        "password": "p",
        "connection_string": "",
        "ssl": True,
        **overrides,
    }


def test_get_connection_refuses_downgrade_when_disallowed():
    pytest.importorskip("psycopg2")
    connect = MagicMock(side_effect=_no_ssl_error())
    with patch("psycopg2.connect", connect):
        with pytest.raises(Exception, match="does not support SSL"):
            postgresql_conn.get_connection(**_pg_kwargs(allow_ssl_downgrade=False))
    assert [c.kwargs["sslmode"] for c in connect.call_args_list] == ["require"]


def test_get_connection_default_keeps_legacy_prefer_fallback():
    pytest.importorskip("psycopg2")
    conn = MagicMock()
    connect = MagicMock(side_effect=[_no_ssl_error(), conn])
    with (
        patch("psycopg2.connect", connect),
        patch("connectors.write_resilience.apply_postgres_session_guards"),
    ):
        assert postgresql_conn.get_connection(**_pg_kwargs()) is conn
    assert [c.kwargs["sslmode"] for c in connect.call_args_list] == ["require", "prefer"]
