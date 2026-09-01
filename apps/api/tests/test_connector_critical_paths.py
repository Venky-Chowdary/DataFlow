"""Critical Snowflake / Excel / SFTP shared-path honesty.

These are the routes that used to pass Test and then lose data or auth on
Map/transfer. Live warehouse login is not claimed here.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from connectors.snowflake_reader import snapshot_order_sql
from connectors.snowflake_writer import copy_into_written_or_raise
from connectors.sftp_common import parse_sftp_config
from connectors.sftp_common import test_sftp as probe_sftp
from services.excel_parser import XLS_UNSUPPORTED_MSG, require_xlsx
from src.transfer.connector_dispatch import writer_extra_kwargs
from transfer.adapters import resolve_connector_config
from transfer.models import EndpointConfig


def test_copy_into_zero_rows_is_not_success():
    with pytest.raises(RuntimeError, match="loaded 0 of 200"):
        copy_into_written_or_raise(0, 200, "ORDERS")


def test_copy_into_partial_batch_is_not_success():
    with pytest.raises(RuntimeError, match="loaded 3 of 200"):
        copy_into_written_or_raise(3, 200, "ORDERS")


def test_copy_into_full_batch_returns_written():
    assert copy_into_written_or_raise(200, 200, "ORDERS") == 200


def test_snapshot_order_includes_pk_tiebreak():
    sql = snapshot_order_sql(["updated_at", "payload"], primary_key="id")
    assert "updated_at" in sql
    assert "id" in sql


def test_snapshot_order_without_columns_fails_closed():
    with pytest.raises(RuntimeError, match="no columns"):
        snapshot_order_sql([], primary_key="")


def test_writer_extra_kwargs_threads_snowflake_key_pair():
    extra = writer_extra_kwargs(
        "snowflake",
        cfg={
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----",
            "role": "both",
            "auth_role": "SYSADMIN",
        },
    )
    assert extra["private_key"].startswith("-----BEGIN PRIVATE KEY-----")
    assert extra["role"] == "SYSADMIN"


def test_writer_extra_kwargs_threads_sqlserver_tls():
    extra = writer_extra_kwargs(
        "sqlserver",
        cfg={"trust_server_certificate": True, "encrypt": "yes"},
    )
    assert extra["trust_server_certificate"] is True
    assert extra["encrypt"] == "yes"


def test_writer_extra_kwargs_threads_sqlserver_tls_from_endpoint_extra():
    dest = type("_Dest", (), {"extra": {"trust_server_certificate": True, "encrypt": "yes"}})()
    extra = writer_extra_kwargs("sqlserver", cfg={}, dest=dest)
    assert extra["trust_server_certificate"] is True
    assert extra["encrypt"] == "yes"


def test_writer_extra_kwargs_threads_sftp_key():
    extra = writer_extra_kwargs(
        "sftp",
        cfg={"private_key": "-----BEGIN OPENSSH PRIVATE KEY-----", "host_key": "SHA256:abc"},
    )
    assert extra["private_key"].startswith("-----BEGIN OPENSSH")
    assert extra["host_key"] == "SHA256:abc"


def test_sftp_writer_forwards_private_key():
    from connectors.sftp_writer import write_mapped_rows

    source = inspect.getsource(write_mapped_rows)
    assert 'private_key=str(_kwargs.get("private_key")' in source


def test_sftp_test_proves_configured_file():
    cfg = parse_sftp_config(
        host="ftp.example.com",
        username="alice",
        password="x",
        connection_string="sftp://alice:x@ftp.example.com/incoming/data.csv",
    )
    transport = MagicMock()
    sftp = MagicMock()
    st = MagicMock()
    st.st_mode = 0o100644
    sftp.stat.return_value = st
    handle = MagicMock()
    handle.read.return_value = b","
    sftp.open.return_value.__enter__.return_value = handle

    with (
        patch("connectors.sftp_common.parse_sftp_config", return_value=cfg),
        patch("connectors.sftp_common.connect_sftp", return_value=(transport, sftp)),
    ):
        ok, msg = probe_sftp(host="ftp.example.com", username="alice", password="x")

    assert ok is True
    assert "file" in msg.lower()
    sftp.stat.assert_called_with("/incoming/data.csv")
    sftp.open.assert_called()


def test_sftp_test_fails_closed_on_missing_file():
    cfg = parse_sftp_config(
        host="ftp.example.com",
        username="alice",
        password="x",
        connection_string="sftp://alice:x@ftp.example.com/incoming/data.csv",
    )
    transport = MagicMock()
    sftp = MagicMock()

    def _stat(path):
        if path == "/incoming/data.csv":
            raise OSError("No such file")
        return MagicMock(st_mode=0o040755)

    sftp.stat.side_effect = _stat

    with (
        patch("connectors.sftp_common.parse_sftp_config", return_value=cfg),
        patch("connectors.sftp_common.connect_sftp", return_value=(transport, sftp)),
    ):
        ok, msg = probe_sftp(host="ftp.example.com", username="alice", password="x")

    assert ok is False
    assert "file not found" in msg.lower()


def test_require_xlsx_rejects_legacy_xls():
    with pytest.raises(ValueError, match="xlsx"):
        require_xlsx("/data/legacy.xls")
    require_xlsx("/data/modern.xlsx")
    require_xlsx(b"bytes-have-no-name")
    assert "not supported" in XLS_UNSUPPORTED_MSG.lower()


def test_resolve_connector_config_strips_topology_role_after_merge(monkeypatch):
    from transfer import adapters

    saved = {
        "host": "acct.snowflakecomputing.com",
        "port": 443,
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "username": "svc",
        "password": "secret",
        "warehouse": "COMPUTE_WH",
        "type": "snowflake",
        "role": "both",
        "auth_role": "",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----",
    }

    monkeypatch.setattr(
        adapters, "_lookup_saved_connector", lambda *_a, **_k: saved
    )
    ep = EndpointConfig(
        format="snowflake",
        connector_id="sf1",
        host="",
        auth_role="",
    )
    cfg = resolve_connector_config(ep)
    assert cfg["role"] == ""
    assert cfg["private_key"].startswith("-----BEGIN PRIVATE KEY-----")


def test_snowflake_reader_count_passes_private_key():
    from connectors.snowflake_reader import read_table_batch

    source = inspect.getsource(read_table_batch)
    assert "private_key=private_key" in source
