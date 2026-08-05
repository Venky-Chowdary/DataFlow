"""MySQL DDL / small-write paths must fail fast on locks — not hang demos."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_lock_wait_timeout_is_not_retriable():
    from services.error_handling import classify_error, humanize_transfer_failure

    exc = RuntimeError("Lock wait timeout exceeded; try restarting transaction (1205)")
    classified = classify_error(exc)
    assert classified["retriable"] is False
    assert any("lock wait" in e or "1205" in e for e in classified["evidence"])

    op = humanize_transfer_failure(exc)
    assert op["code"] == "destination_lock_timeout"
    assert "locked" in op["title"].lower()


def test_full_refresh_drop_lock_gets_operator_fix():
    from services.error_handling import FullRefreshDropFailed, humanize_transfer_failure

    err = FullRefreshDropFailed(
        "sample_orders",
        "Lock wait timeout exceeded; try restarting transaction",
    )
    op = humanize_transfer_failure(err)
    assert op["code"] == "full_refresh_drop_failed"
    assert "locked" in op["title"].lower()
    assert "Workbench" in op["fix"] or "session" in op["fix"].lower()


def test_mysql_get_connection_ddl_uses_short_lock_wait():
    from connectors import mysql_conn

    fake_conn = MagicMock()
    with (
        patch.object(mysql_conn, "is_public_proxy_host", return_value=True),
        patch("pymysql.connect", return_value=fake_conn) as connect,
        patch("connectors.write_resilience.apply_mysql_session_guards") as guards,
    ):
        mysql_conn.get_connection(
            host="proxy.railway.app",
            port=3306,
            database="railway",
            username="u",
            password="p",
            connection_string="",
            ssl=True,
            purpose="ddl",
        )

    kwargs = connect.call_args.kwargs
    assert kwargs["connect_timeout"] == 10
    assert kwargs["read_timeout"] == 45
    guards.assert_called_once()
    assert guards.call_args.kwargs["lock_wait_seconds"] == 30


def test_mysql_setup_on_proxy_keeps_short_lock_longer_io():
    from connectors import mysql_conn

    fake_conn = MagicMock()
    with (
        patch.object(mysql_conn, "is_public_proxy_host", return_value=True),
        patch("pymysql.connect", return_value=fake_conn) as connect,
        patch("connectors.write_resilience.apply_mysql_session_guards") as guards,
    ):
        mysql_conn.get_connection(
            host="proxy.railway.app",
            port=3306,
            database="railway",
            username="u",
            password="p",
            connection_string="",
            ssl=True,
            purpose="setup",
        )

    kwargs = connect.call_args.kwargs
    assert kwargs["read_timeout"] == 180
    assert guards.call_args.kwargs["lock_wait_seconds"] == 30


def test_mysql_get_connection_write_keeps_longer_proxy_io():
    from connectors import mysql_conn

    fake_conn = MagicMock()
    with (
        patch.object(mysql_conn, "is_public_proxy_host", return_value=True),
        patch("pymysql.connect", return_value=fake_conn) as connect,
        patch("connectors.write_resilience.apply_mysql_session_guards") as guards,
    ):
        mysql_conn.get_connection(
            host="proxy.railway.app",
            port=3306,
            database="railway",
            username="u",
            password="p",
            connection_string="",
            ssl=True,
            purpose="write",
        )

    kwargs = connect.call_args.kwargs
    assert kwargs["read_timeout"] == 300
    assert guards.call_args.kwargs["lock_wait_seconds"] == 120
