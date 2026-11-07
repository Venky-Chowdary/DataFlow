"""SFTP uses the shared object-store spool and chunked PUT (no full-body write)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sftp_writer import write_mapped_rows  # noqa: E402


def _mock_sftp(written: dict):
    sftp = MagicMock()
    handle = MagicMock()
    written["bytes"] = b""
    written["writes"] = 0

    def write_all(data: bytes):
        written["bytes"] += data
        written["writes"] += 1

    handle.__enter__ = MagicMock(return_value=handle)
    handle.__exit__ = MagicMock(return_value=False)
    handle.write = write_all
    sftp.file.return_value = handle
    sftp.stat.side_effect = Exception("missing")
    return MagicMock(), sftp


def test_sftp_tsv_uses_shared_spool():
    written: dict = {}
    transport, sftp = _mock_sftp(written)
    with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
        result = write_mapped_rows(
            connection_string="sftp://u:p@host/data/out.tsv",
            headers=["id", "note"],
            data_rows=[["1", "ok"], ["2", "next"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "note", "target": "note"},
            ],
            column_types={"id": "TEXT", "note": "TEXT"},
        )
    assert result.ok, result.error
    assert result.rows_written == 2
    assert written["bytes"].splitlines()[0] == b"id\tnote"
    assert b"ok" in written["bytes"]
    assert result.meta.get("reconcile_sample")


def test_sftp_large_jsonl_streams_in_chunks():
    written: dict = {}
    transport, sftp = _mock_sftp(written)
    rows = [[str(i), "n" * 12] for i in range(20)]
    with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
        result = write_mapped_rows(
            connection_string="sftp://u:p@host/data/out.jsonl",
            headers=["id", "note"],
            data_rows=rows,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "note", "target": "note"},
            ],
            column_types={"id": "TEXT", "note": "TEXT"},
            dest_extra={"spill_max": 40, "sftp_stream_chunk": 25},
        )
    assert result.ok, result.error
    assert result.rows_written == 20
    assert written["writes"] >= 2
    assert written["bytes"].count(b"\n") == 19
    assert result.meta.get("source_row_count") == 20


def test_sftp_capability_mentions_spool():
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    issues = " ".join(CAPABILITY_REGISTRY["sftp"]["common_issues"])
    assert "spool" in issues.lower()
    assert "at-least-once" in issues.lower()
