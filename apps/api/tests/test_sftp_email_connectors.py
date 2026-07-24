"""Targeted unit tests for SFTP and Email connectors."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors import email as email_connector
from connectors.sftp_common import parse_sftp_config, split_remote_path
from connectors.sftp_writer import write_mapped_rows as write_sftp_rows


class TestSFTPConfig:
    def test_parse_sftp_url(self):
        cfg = parse_sftp_config(connection_string="sftp://alice:secret@ftp.example.com:2222/data/file.csv")
        assert cfg.host == "ftp.example.com"
        assert cfg.port == 2222
        assert cfg.username == "alice"
        assert cfg.password == "secret"
        assert cfg.path == "/data/file.csv"

    def test_explicit_fields_override_url(self):
        cfg = parse_sftp_config(
            connection_string="sftp://ftp.example.com/path.csv",
            host="override.example.com",
            port=22,
            username="bob",
            password="pwd",
            database="/uploads",
            table="out.csv",
        )
        assert cfg.host == "override.example.com"
        assert cfg.port == 22
        assert cfg.username == "bob"
        assert cfg.password == "pwd"
        assert cfg.path == "/uploads/out.csv"

    def test_bare_connection_string_becomes_path(self):
        cfg = parse_sftp_config(connection_string="/data/raw/file.jsonl")
        assert cfg.path == "/data/raw/file.jsonl"
        assert cfg.host == ""

    def test_split_remote_path(self):
        assert split_remote_path("/data/file.csv") == ("/data", "file.csv")
        assert split_remote_path("/data/") == ("/data", "")
        assert split_remote_path("file.csv") == ("/", "file.csv")
        assert split_remote_path("") == ("", "")


class TestSFTPWriter:
    def _mock_sftp_client(self, written: dict):
        sftp = MagicMock()
        file_handle = MagicMock()
        written["bytes"] = b""

        def write_all(data: bytes):
            written["bytes"] += data

        file_handle.__enter__ = MagicMock(return_value=file_handle)
        file_handle.__exit__ = MagicMock(return_value=False)
        file_handle.write = write_all
        sftp.file.return_value = file_handle

        transport = MagicMock()
        return transport, sftp

    def test_write_csv_to_sftp(self):
        written: dict = {}
        transport, sftp = self._mock_sftp_client(written)

        with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
            result = write_sftp_rows(
                connection_string="sftp://u:p@host/data/out.csv",
                headers=["id", "amount"],
                data_rows=[["1", "1000.00"], ["2", "2000.50"]],
                mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
                column_types={"id": "INTEGER", "amount": "DECIMAL"},
            )

        assert result.ok is True
        assert result.rows_written == 2
        assert b"id,amount" in written["bytes"]
        assert b"1000.0" in written["bytes"]
        sftp.file.assert_called_once()
        written_path = sftp.file.call_args[0][0]
        assert written_path.endswith(".tmp")
        assert written_path.startswith("/data/out.csv.dataflow-")
        sftp.posix_rename.assert_called_once()
        assert sftp.posix_rename.call_args[0][1] == "/data/out.csv"

    def test_write_jsonl_to_sftp(self):
        written: dict = {}
        transport, sftp = self._mock_sftp_client(written)

        with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
            result = write_sftp_rows(
                connection_string="sftp://u:p@host/data/out.jsonl",
                headers=["id", "amount"],
                data_rows=[["1", "1000.00"]],
                mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
                column_types={"id": "INTEGER", "amount": "DECIMAL"},
            )

        assert result.ok is True
        assert result.rows_written == 1
        assert b'"id"' in written["bytes"]


class TestEmailConnector:
    def test_parse_email_url(self):
        cfg = email_connector._parse_email_config(
            connection_string="smtp://alice:secret@mail.example.com:587?to=team@example.com&subject=Report&format=json"
        )
        assert cfg is not None
        assert cfg.host == "mail.example.com"
        assert cfg.port == 587
        assert cfg.username == "alice"
        assert cfg.password == "secret"
        assert cfg.to_addrs == ["team@example.com"]
        assert cfg.subject == "Report"
        assert cfg.format == "json"

    def test_parse_email_with_database_recipients(self):
        cfg = email_connector._parse_email_config(
            host="mail.example.com",
            username="sender",
            password="secret",
            database="a@example.com, b@example.com",
        )
        assert cfg is not None
        assert cfg.to_addrs == ["a@example.com", "b@example.com"]
        assert cfg.from_addr == "sender"

    def test_email_probe_requires_host(self):
        ok, msg = email_connector.test_email()
        assert ok is False
        assert "host" in msg.lower()

    @patch("connectors.email.smtplib")
    def test_email_probe_success(self, mock_smtplib):
        server = MagicMock()
        mock_smtplib.SMTP.return_value.__enter__ = MagicMock(return_value=server)
        mock_smtplib.SMTP.return_value.__exit__ = MagicMock(return_value=False)

        ok, msg = email_connector.test_email(host="localhost", port=1025, username="u", password="p", database="to@example.com")
        assert ok is True
        assert "reachable" in msg.lower()

    @patch("connectors.email.smtplib")
    def test_write_email_csv(self, mock_smtplib):
        server = MagicMock()
        mock_smtplib.SMTP.return_value.__enter__ = MagicMock(return_value=server)
        mock_smtplib.SMTP.return_value.__exit__ = MagicMock(return_value=False)

        result = email_connector.write_mapped_rows(
            host="localhost",
            port=1025,
            username="u",
            password="p",
            database="to@example.com",
            table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "1000.00"]],
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            column_types={"id": "INTEGER", "amount": "DECIMAL"},
        )

        assert result.ok is True
        assert result.rows_written == 1
        server.sendmail.assert_called_once()
        _from, to, message = server.sendmail.call_args[0]
        assert "to@example.com" in to
        assert "export" in message.lower()
