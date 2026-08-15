"""Email attachments serialize through the shared object-store spill spool.

Honesty: SMTP still materializes the MIME payload (base64 + as_string).
Spill avoids a second records-list + dumps copy. Not exactly-once. No live
SMTP matrix on this host.
"""

from __future__ import annotations

import sys
from email import message_from_string
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors import email as email_connector  # noqa: E402
from connectors.object_store_common import (  # noqa: E402
    serialize_object_store_body,
    serialize_object_store_export,
)


def _send_and_attachment(
    *,
    fmt: str,
    data_rows: list[list[str]],
    dest_extra: dict | None = None,
    column_types: dict[str, str] | None = None,
):
    server = MagicMock()
    with patch("connectors.email.smtplib") as mock_smtplib:
        mock_smtplib.SMTP.return_value.__enter__ = MagicMock(return_value=server)
        mock_smtplib.SMTP.return_value.__exit__ = MagicMock(return_value=False)
        result = email_connector.write_mapped_rows(
            host="localhost",
            port=1025,
            username="u",
            password="p",
            database="to@example.com",
            table_name="payments",
            headers=["id", "note"],
            data_rows=data_rows,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "note", "target": "note"},
            ],
            column_types=column_types or {"id": "TEXT", "note": "TEXT"},
            dest_extra=dest_extra or {},
            connection_string=f"smtp://localhost:1025?to=to@example.com&format={fmt}",
        )
    assert result.ok, result.error
    raw = server.sendmail.call_args[0][2]
    msg = message_from_string(raw)
    payload = None
    filename = None
    for part in msg.walk():
        name = part.get_filename()
        if name:
            filename = name
            payload = part.get_payload(decode=True)
            break
    assert payload is not None
    return result, payload, filename


def test_email_json_attachment_matches_shared_serialize():
    rows = [["1", "ok"], ["2", "next"]]
    result, payload, filename = _send_and_attachment(fmt="json", data_rows=rows)
    assert filename == "export.json"
    assert result.rows_written == 2
    expected, mime = serialize_object_store_body(
        key="export.json",
        mapped_rows=[("1", "ok"), ("2", "next")],
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    assert mime == "application/json"
    assert payload == expected
    assert result.meta.get("reconcile_sample")
    assert result.meta.get("source_row_count") == 2


def test_email_jsonl_attachment_matches_shared_serialize():
    rows = [["1", "ok"], ["2", "next"]]
    result, payload, filename = _send_and_attachment(fmt="jsonl", data_rows=rows)
    assert filename == "export.jsonl"
    expected, _ = serialize_object_store_body(
        key="export.jsonl",
        mapped_rows=[("1", "ok"), ("2", "next")],
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    assert payload == expected
    assert payload.count(b"\n") == 1


def test_email_tsv_attachment_matches_shared_serialize():
    rows = [["1", "ok"], ["2", "next"]]
    result, payload, filename = _send_and_attachment(fmt="tsv", data_rows=rows)
    assert filename == "export.tsv"
    expected, mime = serialize_object_store_body(
        key="export.tsv",
        mapped_rows=[("1", "ok"), ("2", "next")],
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    assert mime == "text/tab-separated-values"
    assert payload == expected
    assert payload.splitlines()[0] == b"id\tnote"


def test_email_passes_spill_max_into_shared_export():
    rows = [[str(i), "n" * 12] for i in range(20)]
    with patch(
        "connectors.email.serialize_object_store_export",
        wraps=serialize_object_store_export,
    ) as spy:
        result, payload, filename = _send_and_attachment(
            fmt="jsonl",
            data_rows=rows,
            dest_extra={"spill_max": 40},
        )
    assert result.ok
    assert filename == "export.jsonl"
    spy.assert_called_once()
    assert spy.call_args.kwargs["spill_max_size"] == 40
    assert payload.count(b"\n") == 19
    assert result.meta.get("source_row_count") == 20


def test_email_parquet_attachment_round_trip():
    pytest.importorskip("pyarrow.parquet")
    import io

    import pyarrow.parquet as pq

    result, payload, filename = _send_and_attachment(
        fmt="parquet",
        data_rows=[["1", "ok"], ["2", "next"]],
        column_types={"id": "INTEGER", "note": "TEXT"},
    )
    assert filename == "export.parquet"
    table = pq.read_table(io.BytesIO(payload))
    assert table.num_rows == 2
    assert result.rows_written == 2
