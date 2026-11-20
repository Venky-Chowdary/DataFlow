"""D76 — PostgreSQL bytea could not load into Snowflake on the COPY path.

The staged CSV carries binary cells in DataFlow's canonical wire (base64),
but the COPY file format never said so, and Snowflake decodes a CSV BINARY
field with its default ``BINARY_FORMAT = HEX`` — so every production-volume
load (COPY is used from ``COPY_THRESHOLD`` rows) of a bytea column failed.
fakesnow has no PUT/COPY, so the contract is pinned here: the bytes the
staged file encodes, decoded the way the COPY statement tells Snowflake to
decode them, are the source bytes.
"""

from __future__ import annotations

import base64
import binascii
import csv
import re
from pathlib import Path

from connectors import snowflake_writer as sf

_PAYLOADS = [b"\x00\xff\x10", b"", b"DataFlow\x00\x01"]


class _RecordingCursor:
    def __init__(self) -> None:
        self.staged: list[list[str]] = []
        self.copy_sql = ""
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None):  # noqa: ANN001
        text = " ".join(sql.split())
        if text.startswith("PUT file://"):
            path = text[len("PUT file://"):].split(" ")[0]
            with Path(path).open(encoding="utf-8", newline="") as fh:
                self.staged = list(csv.reader(fh))
        elif text.startswith("COPY INTO"):
            self.copy_sql = text
            loaded = len(self.staged) - 1
            self._rows = [("f.csv.gz", "LOADED", loaded, loaded, 1, 0, None)]
        return self

    def fetchall(self):
        return self._rows


def _decode(cell: str, binary_format: str) -> bytes:
    if binary_format == "BASE64":
        return base64.b64decode(cell, validate=True)
    if binary_format == "UTF8":
        return cell.encode("utf-8")
    return binascii.unhexlify(cell)


def test_d76_copy_stage_binary_decodes_to_source_bytes():
    rows = [(str(i), _PAYLOADS[i % len(_PAYLOADS)]) for i in range(sf.COPY_THRESHOLD)]
    cur = _RecordingCursor()
    method = sf._load_rows_into_table(
        cur, "T", ["ID", "PAYLOAD"], ["NUMBER(38,0)", "BINARY"], rows,
        prefer_copy=True, conn=object(), policy="fail",
    )
    assert method == "copy_into"
    match = re.search(r"BINARY_FORMAT\s*=\s*'?(\w+)'?", cur.copy_sql, re.I)
    # Snowflake's CSV default when the file format names none.
    binary_format = match.group(1).upper() if match else "HEX"
    decoded = [_decode(rec[1], binary_format) for rec in cur.staged[1:]]
    assert decoded == [r[1] for r in rows]


def test_d76_empty_bytes_and_null_stay_apart_on_the_copy_wire():
    """R2 for BINARY: ``b''`` is a value, NULL is not. The staged CSV encloses
    every field, so an empty payload is ``""`` (BASE64 of zero bytes) and a
    NULL is the file format's ``NULL_IF`` token, never the same cell. How
    Snowflake reads the two (``""`` enclosed -> empty, NULL_IF -> NULL) is its
    documented CSV contract; a live account is needed to observe it."""
    rows = [(str(i), [b"\x00\xff", b"", None][i % 3]) for i in range(sf.COPY_THRESHOLD)]
    cur = _RecordingCursor()
    sf._load_rows_into_table(
        cur, "T", ["ID", "PAYLOAD"], ["NUMBER(38,0)", "BINARY"], rows,
        prefer_copy=True, conn=object(), policy="fail",
    )
    null_if = re.search(r"NULL_IF\s*=\s*\(\s*'([^']*)'", cur.copy_sql, re.I)
    assert null_if, cur.copy_sql
    token = null_if.group(1).replace("\\\\", "\\")
    cells = {int(rec[0]): rec[1] for rec in cur.staged[1:]}
    assert cells[1] == "" and base64.b64decode(cells[1]) == b""
    assert cells[2] == token and cells[2] != cells[1]
    assert base64.b64decode(cells[0]) == b"\x00\xff"
