"""D371 / D376(b): TSV uses the CSV quoting dialect, and rows end CRLF.

NULL is an empty unquoted field. ``''`` is ``""``. A quote, tab, CR or LF
is quoted with doubled quotes. RFC 4180 records end with CRLF. Migration
rule 9.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.csv_codec import (  # noqa: E402
    RECORD_TERMINATOR,
    csv_cell_encode,
    format_csv_row,
    iter_csv_records,
)
from services.format_converter import convert_rows  # noqa: E402


def test_tsv_distinguishes_empty_string_from_null_and_quotes() -> None:
    cells = ["", None, 'q"uote', "a\tb", "line\r\nfeed", "  spaced  ", "NULL", "\\N"]
    line = format_csv_row([csv_cell_encode(c) for c in cells], "\t")
    assert line.startswith('""\t\t')
    parsed = next(csv.reader(io.StringIO(line + RECORD_TERMINATOR, newline=""), delimiter="\t"))
    assert parsed[0] == ""
    assert parsed[1] == ""
    assert parsed[2] == 'q"uote'
    assert parsed[3] == "a\tb"
    assert parsed[4] == "line\r\nfeed"
    assert parsed[5] == "  spaced  "
    assert parsed[6] == "NULL"
    assert parsed[7] == "\\N"


def test_csv_and_tsv_exports_end_with_crlf() -> None:
    headers = ["id", "s"]
    rows = [["1", ""], ["2", None], ["3", 'q"uote']]
    csv_bytes, _ = convert_rows(headers, rows, source_format="csv", target_format="csv")
    tsv_bytes, _ = convert_rows(headers, rows, source_format="csv", target_format="tsv")
    assert csv_bytes.endswith(b"\r\n")
    assert csv_bytes.count(b"\n") == csv_bytes.count(b"\r\n")
    assert tsv_bytes.count(b"\n") == tsv_bytes.count(b"\r\n")
    assert b'""' in tsv_bytes
    text = tsv_bytes.decode("utf-8")
    rows_back = list(iter_csv_records(io.StringIO(text, newline=""), "\t"))
    # header + 3 data rows; unquoted empty is NULL, quoted "" is empty text
    assert rows_back[1] == ["1", ""]
    assert rows_back[2] == ["2", None]
    assert rows_back[3] == ["3", 'q"uote']


def test_quoted_crlf_inside_a_field_is_preserved() -> None:
    raw = 'id,note\r\n1,"multi\r\nline"\r\n'
    rows = list(iter_csv_records(io.StringIO(raw, newline=""), ","))
    assert rows[1][1] == "multi\r\nline"
