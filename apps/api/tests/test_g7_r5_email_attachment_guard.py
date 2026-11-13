"""G7 review M3: an email CSV/TSV attachment is a human download — guarded by default.

Before: the email destination serialized ``export.csv``/``export.tsv`` with the
object-store encoder and no formula guard, so a cell or a column name such as
``=HYPERLINK("http://evil…")`` arrived in a person's inbox as a live formula.

Decision: email attachments are human downloads. ``formula_protection``
defaults to ``"prefix"`` for the email destination (headers included);
``"off"`` opts out. Object-store data destinations (S3/GCS/ADLS/SFTP) keep
exact bytes.
"""

from __future__ import annotations

import csv
import io
import sys
from email import message_from_string
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'
HDR = "=cmd|'/c calc'!A1"


def _attachment(fmt: str, dest_extra: dict | None = None) -> list[list[str]]:
    from connectors import email as email_connector

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
            table_name="report",
            headers=["id", HDR],
            data_rows=[["1", HYPERLINK], ["2", "-5"], ["3", "@SUM(A1)"], ["4", "plain"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": HDR, "target": HDR},
            ],
            column_types={"id": "TEXT", HDR: "TEXT"},
            dest_extra=dest_extra or {},
            connection_string=f"smtp://localhost:1025?to=to@example.com&format={fmt}",
        )
    assert result.ok, result.error
    msg = message_from_string(server.sendmail.call_args[0][2])
    payload = next(p.get_payload(decode=True) for p in msg.walk() if p.get_filename())
    delim = "\t" if fmt == "tsv" else ","
    return list(csv.reader(io.StringIO(payload.decode("utf-8"), newline=""), delimiter=delim))


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_email_attachment_is_guarded_by_default(fmt):
    rows = _attachment(fmt)
    # Email target names are sanitized to identifiers; whatever lands is never a formula.
    assert rows[0][0] == "id" and rows[0][1][:1] not in {"=", "+", "-", "@"}
    assert [r[1] for r in rows[1:]] == ["'" + HYPERLINK, "-5", "'@SUM(A1)", "plain"]


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_email_attachment_opt_out_keeps_exact_values(fmt):
    rows = _attachment(fmt, {"formula_protection": "off"})
    assert [r[1] for r in rows[1:]] == [HYPERLINK, "-5", "@SUM(A1)", "plain"]


def test_email_attachment_refuses_unknown_protection():
    from connectors import email as email_connector

    with patch("connectors.email.smtplib"):
        result = email_connector.write_mapped_rows(
            host="localhost", port=1025, username="u", password="p",
            database="to@example.com", table_name="report",
            headers=["id"], data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "TEXT"},
            dest_extra={"formula_protection": "strip"},
            connection_string="smtp://localhost:1025?to=to@example.com&format=csv",
        )
    assert result.ok is False
    assert "formula_protection" in (result.error or "")


def test_object_store_csv_stays_exact():
    """S3/GCS/ADLS/SFTP files are data destinations: no guard on the shared encoder."""
    from connectors.object_store_common import serialize_object_store_export

    export = serialize_object_store_export(
        key="out/data.csv",
        mapped_rows=[("1", HYPERLINK)],
        target_cols=["id", HDR],
        dest_types={"id": "TEXT", HDR: "TEXT"},
    )
    rows = list(csv.reader(io.StringIO(export.read_all().decode("utf-8"), newline="")))
    assert rows == [["id", HDR], ["1", HYPERLINK]]


@pytest.mark.parametrize("key", ["export.csv", "export.tsv"])
def test_guarded_encoder_neutralises_header_and_cells(key):
    """The guard the email destination turns on covers header names too."""
    from connectors.object_store_materialize import ObjectStoreEncoder

    enc = ObjectStoreEncoder(
        key=key, target_cols=["id", HDR], dest_types={"id": "TEXT", HDR: "TEXT"},
        formula_protection="prefix",
    )
    enc.append_rows([("1", HYPERLINK), ("2", "-5"), ("3", None)])
    delim = "\t" if key.endswith(".tsv") else ","
    text = enc.finish().read_all().decode("utf-8")
    rows = list(csv.reader(io.StringIO(text, newline=""), delimiter=delim))
    assert rows[0] == ["id", "'" + HDR]
    assert [r[1] for r in rows[1:]] == ["'" + HYPERLINK, "-5", ""]
