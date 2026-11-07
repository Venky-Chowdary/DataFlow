"""Wave 39: SFTP Gate-8 verify + quarantine matrix parity with S3/GCS/ADLS."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_sftp():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_sftp_object",
        return_value=(3, "sf"),
    ) as mocked:
        assert verify_target(
            "sftp",
            {"host": "ftp.example", "database": "/exports", "password": "x"},
            schema="",
            table_name="orders.csv",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (3, "sf")
        assert mocked.called


def test_verify_sftp_object_parses_jsonl():
    import io

    from services.reconciliation import verify_sftp_object

    body = b'{"id":"1","n":1}\n{"id":"2","n":2}\n'
    sftp = MagicMock()
    sftp.file.return_value = io.BytesIO(body)
    transport = MagicMock()

    cfg = MagicMock()
    cfg.host = "ftp.example"
    cfg.path = "/exports/orders.jsonl"

    with (
        patch("connectors.sftp_common.parse_sftp_config", return_value=cfg),
        patch("connectors.sftp_common.connect_sftp", return_value=(transport, sftp)),
    ):
        count, chk = verify_sftp_object(
            host="ftp.example",
            table_name="orders.jsonl",
            database="/exports",
            password="x",
            limit=10,
        )
    assert count == 2
    assert isinstance(chk, str) and len(chk) > 0
    sftp.close.assert_called()
    transport.close.assert_called()


def test_read_target_sample_routes_sftp():
    import io

    from services.reconciliation import read_target_sample

    body = b'{"id":"a","email":"a@x.com"}\n'
    sftp = MagicMock()
    sftp.file.return_value = io.BytesIO(body)
    cfg = MagicMock()
    cfg.host = "ftp.example"
    cfg.path = "/exports/contacts.jsonl"

    with (
        patch("connectors.sftp_common.parse_sftp_config", return_value=cfg),
        patch(
            "connectors.sftp_common.connect_sftp",
            return_value=(MagicMock(), sftp),
        ),
    ):
        rows = read_target_sample(
            "sftp",
            {"host": "ftp.example", "password": "x", "database": "/exports"},
            schema="",
            table_name="contacts.jsonl",
            columns=["id", "email"],
            limit=10,
            sort_key="id",
            key_values=["a"],
        )
    assert rows == [{"id": "a", "email": "a@x.com"}]


def test_sftp_writer_quarantines_invalid_binary_before_upload():
    from connectors.sftp_writer import write_mapped_rows

    file_obj = MagicMock()
    file_obj.__enter__ = lambda s: s
    file_obj.__exit__ = MagicMock(return_value=False)
    sftp = MagicMock()
    sftp.file.return_value = file_obj
    sftp.stat.side_effect = Exception("missing")

    with patch(
        "connectors.sftp_writer.connect_sftp",
        return_value=(MagicMock(), sftp),
    ):
        result = write_mapped_rows(
            host="ftp.example",
            username="u",
            password="p",
            database="/exports",
            table_name="blob.jsonl",
            headers=["id", "blob"],
            data_rows=[["1", "!!!not-base64!!!"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "TEXT"},
                {"source": "blob", "target": "blob", "target_type": "BYTEA"},
            ],
            column_types={"id": "TEXT", "blob": "BYTEA"},
            error_policy="quarantine",
        )
    assert result.ok is True
    assert result.rows_written == 0
    assert result.rejected_rows >= 1
    assert any("base64" in (d.get("reason") or "").lower() for d in result.rejected_details)

def test_sftp_writer_stamps_gate8_meta_on_success():
    from connectors.sftp_writer import write_mapped_rows

    file_obj = MagicMock()
    file_obj.__enter__ = lambda s: s
    file_obj.__exit__ = MagicMock(return_value=False)
    sftp = MagicMock()
    sftp.file.return_value = file_obj
    sftp.stat.side_effect = Exception("missing")
    transport = MagicMock()

    with patch(
        "connectors.sftp_writer.connect_sftp",
        return_value=(transport, sftp),
    ):
        result = write_mapped_rows(
            host="ftp.example",
            username="u",
            password="p",
            database="/exports",
            table_name="ok.jsonl",
            headers=["id", "email"],
            data_rows=[["1", "a@x.com"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "email", "target": "email"},
            ],
            column_types={"id": "TEXT", "email": "TEXT"},
        )
    assert result.ok is True
    assert result.rows_written == 1
    assert result.meta.get("source_row_count") == 1
    assert result.meta.get("reconcile_sample")
    assert result.meta["reconcile_sample"][0]["email"] == "a@x.com"


def test_sftp_quarantine_matrix_blocks_invalid_binary_like_s3():
    """Shared matrix path — invalid base64 must not invent UTF-8 into the export."""
    from connectors.writer_common import apply_write_quarantine_matrix

    details: list = []
    out = apply_write_quarantine_matrix(
        [("1", "!!!bad!!!")],
        ["id", "blob"],
        ["TEXT", "BYTEA"],
        details,
        "quarantine",
        dialect_label="SFTP",
    )
    assert out == []
    assert details
    assert "base64" in (details[0].get("reason") or "").lower()
